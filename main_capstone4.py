# %%
# !pip install python-dotenv boto3 pandas sqlalchemy pymysql

# %%
# Import necessary libraries
from dotenv import load_dotenv
import os
import boto3
import pandas as pd
import pymysql
from sqlalchemy import create_engine, text
import logging

# %%
# Load environment variables from the .env file
load_dotenv()

# %%
# Fetch credentials and other configuration data from .env
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
s3_bucket_name = os.getenv('S3_BUCKET_NAME')
aws_region = os.getenv('region')
raw_file = os.getenv('raw_file')

host = os.getenv('RDS_HOST')
db_name = os.getenv('RDS_DB_NAME')
user = os.getenv('RDS_USER')
password = os.getenv('RDS_PASSWORD')
table_name = os.getenv('table_name')

# %%
log_file = os.getenv('LOG_FILE')  # Use the path provided in the .env
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# %%
import os
import pandas as pd
import json
import xml.etree.ElementTree as ET

def load_files_to_dataframe(raw_file):
    """Loads all CSV, JSON, and XML files from a directory into a single DataFrame."""
    all_dataframes = []
    
    # Iterate through all files in the directory
    for file_name in os.listdir(raw_file):
        file_path = os.path.join(raw_file, file_name)
        
        # Handling CSV files
        if file_name.endswith('.csv'):
            try:
                df = pd.read_csv(file_path)  # Read the CSV file into a DataFrame
                all_dataframes.append(df)
                logging.info(f"Successfully loaded CSV file: {file_name}")
            except Exception as e:
                logging.error(f"Error loading CSV {file_name}: {e}")
        
        # Handling JSON files (newline-delimited)
        elif file_name.endswith('.json'):
            try:
                json_data = []
                with open(file_path, 'r') as json_file:
                    for line in json_file:
                        try:
                            json_obj = json.loads(line)  # Parse each line as a JSON object
                            json_data.append(json_obj)
                        except json.JSONDecodeError as e:
                            logging.error(f"Error decoding JSON in {file_name}: {e}")
                
                df = pd.json_normalize(json_data)  # Convert JSON objects to a DataFrame
                all_dataframes.append(df)
                logging.info(f"Successfully loaded JSON file: {file_name}")
            except Exception as e:
                logging.error(f"Error loading JSON {file_name}: {e}")
        
        # Handling XML files
        elif file_name.endswith('.xml'):
            try:
                tree = ET.parse(file_path)
                root = tree.getroot()
                # Extract data from XML file (assuming a simple structure for demo purposes)
                xml_data = []
                for child in root:
                    row_data = {}
                    for elem in child:
                        row_data[elem.tag] = elem.text  # Map each tag to its text content
                    xml_data.append(row_data)
                
                df = pd.DataFrame(xml_data)  # Convert list of dicts to a DataFrame
                all_dataframes.append(df)
                logging.info(f"Successfully loaded XML file: {file_name}")
            except Exception as e:
                logging.error(f"Error loading XML {file_name}: {e}")

    # Concatenate all DataFrames into one
    if all_dataframes:
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        return combined_df
    else:
        logging.warning("No files were found or loaded.")
        return pd.DataFrame()  # Return an empty DataFrame if no files were found

# %%
def transform_data(raw_file):
    """Loads data from files and performs transformations on the DataFrame."""
    combined_df = load_files_to_dataframe(raw_file)  # Load the data

    if combined_df.empty:
        logging.warning("No valid data found for transformation.")
        return pd.DataFrame()  # Return an empty DataFrame if no data is loaded

    df = combined_df.copy()  # Make a copy to avoid modifying the original DataFrame

    # Check and convert height to numeric, coerce errors to NaN
    if 'height' in df.columns:
        df['height'] = pd.to_numeric(df['height'], errors='coerce')  # Convert to numeric
        logging.info("Height column converted to numeric.")
    
    # Check and convert weight to numeric, coerce errors to NaN
    if 'weight' in df.columns:
        df['weight'] = pd.to_numeric(df['weight'], errors='coerce')  # Convert to numeric
        logging.info("Weight column converted to numeric.")

    # Convert height from inches to meters (1 inch = 0.0254 meters)
    if 'height' in df.columns:
        df['height'] = df['height'] * 0.0254
        logging.info("Height converted from inches to meters.")

    # Convert weight from pounds to kilograms (1 pound = 0.453592 kilograms)
    if 'weight' in df.columns:
        df['weight'] = df['weight'] * 0.453592
        logging.info("Weight converted from pounds to kilograms.")

    return df

# %%
def upload_to_s3(s3, file_name, bucket_name, s3_file_name):
    """Uploads a file to an S3 bucket."""
    try:
        s3.upload_file(file_name, bucket_name, s3_file_name)
        logging.info(f"Uploaded {file_name} to {bucket_name}/{s3_file_name}")
    except Exception as e:
        logging.error(f"Error uploading to S3: {e}")

# %%
def create_database(engine, db_name):
    """Create a new database if it doesn't exist."""
    try:
        with engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name}"))
            logging.info(f"Database '{db_name}' created successfully.")
    except Exception as e:
        logging.error(f"Error creating database: {e}")

# %%
def create_table(engine, table_name):
    """Create a new table in the database."""
    try:
        with engine.connect() as conn:
            # Execute the table creation SQL
            conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                name VARCHAR(255),
                height FLOAT,
                weight FLOAT
            );
            """))
            conn.commit()  # Explicitly commit the table creation
            logging.info(f"Table '{table_name}' created successfully.")
    except Exception as e:
        logging.error(f"Error creating table: {e}")

# %%
def load_to_rds(dataframe, db_name, table_name):
    """Loads the DataFrame into an RDS table."""
    try:
        # Create a connection string (connect to the default database initially)
        connection_string = f"mysql+pymysql://{user}:{password}@{host}/mysql"  # Use the default 'mysql' database

        # Create an SQLAlchemy engine
        engine = create_engine(connection_string)

        # Create the database
        create_database(engine, db_name)

        # Change connection string to the new database
        connection_string = f"mysql+pymysql://{user}:{password}@{host}/{db_name}"
        engine = create_engine(connection_string)

        # Create the table in the correct database
        create_table(engine, table_name)

        # Check if the table exists before loading data
        with engine.connect() as conn:
            result = conn.execute(text(f"SHOW TABLES LIKE '{table_name}';"))
            if result.fetchone() is None:
                logging.warning(f"Table '{table_name}' was not created in database '{db_name}'.")
                return  # Exit if the table wasn't created

        # Load the DataFrame into the specified table in RDS
        dataframe.to_sql(name=table_name, con=engine, if_exists='replace', index=False)  # Use 'append' to add data
        logging.info(f"Data loaded into {table_name} table successfully.")
    except Exception as e:
        logging.error(f"Error loading data to RDS: {e}")

# Load and transform the data
transformed_df = transform_data(raw_file)

# %%
# Load and transform the data
transformed_df = transform_data(raw_file)

# Initialize the S3 client
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name=aws_region)

# Save the transformed DataFrame to CSV
if not transformed_df.empty:
    csv_file_name = "transformed_data.csv"
    transformed_df.to_csv(csv_file_name, index=False)  # Save to CSV
    logging.info(f"Transformed DataFrame saved to {csv_file_name}.")

    upload_to_s3(s3, csv_file_name, s3_bucket_name, csv_file_name)

    load_to_rds(transformed_df, db_name, table_name)
else:
    logging.warning("No transformed data to save or load.")

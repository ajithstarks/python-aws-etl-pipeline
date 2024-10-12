\#!/bin/bash

# Log file path
log_file="/mnt/e/capstone4/log/server.log"

# Function to log messages with log levels and timestamps
log() {
    local level=$1
    shift
    echo "$(date '+%Y-%m-%d %H:%M:%S') [$level] - $*" | tee -a "$log_file"
}

# Function to handle errors (logs as ERROR and exits)
handle_error() {
    log "ERROR" "$1"
    exit 1
}

# Start the script and log as INFO
log "INFO" "File Download Initiated"

# Define the URL and download/extract paths
url="https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip"
download_dir="/mnt/e/capstone4/dataset/download/"
extract_directory="/mnt/e/capstone4/dataset/data/"

# Create the download and extraction directories if they don't exist
if ! mkdir -p "$download_dir" 2>>"$log_file"; then
    handle_error "Failed to create the download directory: $download_dir"
fi

log "INFO" "Download directory created: $download_dir"

if ! mkdir -p "$extract_directory" 2>>"$log_file"; then
    handle_error "Failed to create the extraction directory: $extract_directory"
fi

log "INFO" "Extraction directory created: $extract_directory"

# Check if the URL is reachable (log as WARN if unreachable)
if ! wget --spider "$url" 2>>"$log_file"; then
    log "WARN" "The URL is not reachable: $url"
    handle_error "Failed due to unreachable URL."
fi

log "INFO" "URL is reachable: $url"

# Download the file to the specified directory
if ! wget -P "$download_dir" "$url" >>"$log_file" 2>&1; then
    handle_error "Failed to download the file from: $url"
fi

log "INFO" "File downloaded to $download_dir"

# Extract the file name from the URL (source.zip in this case)
zip_file="$download_dir$(basename "$url")"

# Unzip the file to the specified directory (log progress)
if ! unzip "$zip_file" -d "$extract_directory" >>"$log_file" 2>&1; then
    handle_error "Failed to unzip the file to: $extract_directory"
fi

log "INFO" "File unzipped successfully to $extract_directory"

# Script completion log
log "INFO" "File download and extraction completed successfully"

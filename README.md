# Spotify Data Pipeline to Snowflake

This project demonstrates an automated data pipeline for Spotify data, including extraction from the Spotify API, data cleaning, storage in AWS S3, and ingestion into Snowflake using Snowpipe.

## Overview

The project consists of two main components:

1. **Spotify Data Extraction:**
   - A Python script (`spotify_data_extraction.py`) that connects to the Spotify API, retrieves user listening data, and stores it in a CSV file.

2. **Snowflake Data Ingestion:**
   - A Python script (`snowflake_data_ingestion.py`) that sets up the Snowflake environment, creates a Snowflake stage, table, and a Snowpipe for automatic data ingestion from S3.

## Prerequisites

Before running the scripts, make sure you have the following:

- Spotify API credentials (client_id and client_secret).
- Snowflake credentials (user, password, account, database, schema, warehouse, and role).
- AWS credentials (access_key_id, secret_access_key).
- An existing S3 bucket for storing data.

## Usage

1. Run the Spotify Data Extraction script to get listening data from the Spotify API and save it to a CSV file.

   ```bash
   python spotify_data_extraction.py

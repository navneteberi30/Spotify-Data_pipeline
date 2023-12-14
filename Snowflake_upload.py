import snowflake.connector

# Snowflake account and connection details (replace with your own)
SNOWFLAKE_ACCOUNT = "your_account_name"
SNOWFLAKE_USER = "your_username"
SNOWFLAKE_PASSWORD = "your_password"
SNOWFLAKE_WAREHOUSE = "your_warehouse_name"

# Database, schema, and table names (adjust to your needs)
SNOWFLAKE_DATABASE = "your_database_name"
SNOWFLAKE_SCHEMA = "your_schema_name"
SNOWFLAKE_TABLE = "spotify_top_tracks"

# S3 stage and Snowpipe details (replace with your own)
S3_STAGE_NAME = "spotify_top_tracks_stage"
S3_STAGE_URL = "s3://your-s3-bucket-name/data/"
SNOWPIPE_NAME = "spotify_top_tracks_pipe"

def create_resources():
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
    )
    cursor = conn.cursor()

    # Create database and schema if they don't exist
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE}")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_SCHEMA}")

    # Create table for storing Spotify data
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
            track_name VARCHAR(255),
            artist_name VARCHAR(255),
            album_name VARCHAR(255)
        );
        """
    )

    # Create stage for uploading data from S3
    cursor.execute(
        f"""
        CREATE OR REPLACE STAGE {S3_STAGE_NAME}
        URL = '{S3_STAGE_URL}'
        FILE_FORMAT = (type=csv, field_delimiter=',', header_row=TRUE);
        """
    )

    # Create Snowpipe for auto-ingestion
    cursor.execute(
        f"""
        CREATE OR REPLACE PIPE {SNOWPIPE_NAME} AUTO_INGEST = TRUE
        AS COPY INTO {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}
        FROM @{{S3_STAGE_NAME}}
        FILE_FORMAT = (type=csv, field_delimiter=',', header_row=TRUE)
        ON_ERROR = CONTINUE;
        """
    )

    conn.commit()
    cursor.close()
    conn.close()

print(f"Snowflake resources created and Snowpipe '{SNOWPIPE_NAME}' ready for auto-ingestion!")

# Example usage
#create_resources()

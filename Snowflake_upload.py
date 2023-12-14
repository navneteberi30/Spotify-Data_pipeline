import snowflake.connector
import boto3

# Snowflake credentials
snowflake_user = 'YOUR_SNOWFLAKE_USER'
snowflake_password = 'YOUR_SNOWFLAKE_PASSWORD'
snowflake_account = 'YOUR_SNOWFLAKE_ACCOUNT'
snowflake_database = 'YOUR_SNOWFLAKE_DATABASE'
snowflake_schema = 'YOUR_SNOWFLAKE_SCHEMA'
snowflake_warehouse = 'YOUR_SNOWFLAKE_WAREHOUSE'
snowflake_role = 'YOUR_SNOWFLAKE_ROLE'

# AWS credentials
aws_access_key_id = 'YOUR_AWS_ACCESS_KEY_ID'
aws_secret_access_key = 'YOUR_AWS_SECRET_ACCESS_KEY'
s3_bucket_name = 'YOUR_S3_BUCKET_NAME'
s3_stage_name = 'spotify_stage'

def create_snowflake_connection():
    con = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=snowflake_schema,
        role=snowflake_role
    )
    return con

def create_snowflake_database_and_schema(con):
    cursor = con.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {snowflake_database}")
    cursor.execute(f"USE DATABASE {snowflake_database}")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {snowflake_schema}")
    cursor.close()

def create_snowflake_stage(con):
    cursor = con.cursor()
    cursor.execute(f"CREATE STAGE IF NOT EXISTS {s3_stage_name}")
    cursor.close()

def create_snowflake_table(con):
    cursor = con.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {snowflake_schema}.spotify_data (
            "Track Name" STRING,
            "Artist" STRING,
            "Album" STRING,
            "Release Date" STRING,
            "Added At" STRING
        )
    """)
    cursor.close()

def create_snowpipe(con):
    cursor = con.cursor()
    cursor.execute(f"""
        CREATE PIPE IF NOT EXISTS {snowflake_schema}.spotify_pipe
        AUTO_INGEST = TRUE
        AS
        COPY INTO {snowflake_schema}.spotify_data
        FROM @spotify_stage
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"')
    """)
    cursor.close()

if __name__ == "__main__":
    # Extract top 50 tracks from Spotify (optional - requires Spotify API access)
    # Implement your logic to get top tracks and create a Pandas DataFrame

    # For demonstration, let's assume you have a DataFrame named top_tracks
    # top_tracks = get_top_tracks_from_spotify()

    # Create Snowflake connection
    snowflake_connection = create_snowflake_connection()

    # Create Snowflake database and schema
    create_snowflake_database_and_schema(snowflake_connection)

    # Create Snowflake stage
    create_snowflake_stage(snowflake_connection)

    # Create Snowflake table
    create_snowflake_table(snowflake_connection)

    # Create Snowpipe for automatic data ingestion
    create_snowpipe(snowflake_connection)

    # Close Snowflake connection
    snowflake_connection.close()

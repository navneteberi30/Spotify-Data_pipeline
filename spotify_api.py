import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
import boto3

def get_top_tracks_from_spotify():
    # Spotify API credentials
    spotify_client_id = 'YOUR_SPOTIFY_CLIENT_ID'
    spotify_client_secret = 'YOUR_SPOTIFY_CLIENT_SECRET'

    # Spotify API authentication
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=spotify_client_id, client_secret=spotify_client_secret, redirect_uri='http://localhost:8888/callback', scope='user-library-read'))

    # Spotify API request to get top tracks
    top_tracks = sp.current_user_top_tracks(limit=50)
    return top_tracks

def upload_to_s3(data_frame):
    # AWS credentials
    aws_access_key_id = 'YOUR_AWS_ACCESS_KEY_ID'
    aws_secret_access_key = 'YOUR_AWS_SECRET_ACCESS_KEY'
    s3_bucket_name = 'YOUR_S3_BUCKET_NAME'

    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    csv_buffer = data_frame.to_csv(index=False).encode()
    s3_key = f'spotify_data_{pd.to_datetime("today").strftime("%Y%m%d")}.csv'
    s3.put_object(Bucket=s3_bucket_name, Key=s3_key, Body=csv_buffer)

if __name__ == "__main__":
    # Get top tracks from Spotify
    top_tracks = get_top_tracks_from_spotify()

    # Clean and prepare data using Pandas
    track_data = []
    for track in top_tracks['items']:
        track_info = {
            'Track Name': track['name'],
            'Artist': track['artists'][0]['name'],
            'Album': track['album']['name'],
            'Release Date': track['album']['release_date'],
            'Added At': pd.to_datetime('today').strftime('%Y-%m-%d')
        }
        track_data.append(track_info)

    # Create a DataFrame
    df = pd.DataFrame(track_data)

    # Upload data to S3
    upload_to_s3(df)

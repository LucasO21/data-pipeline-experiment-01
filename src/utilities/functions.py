import requests
import json
import polars as pl
from youtube_transcript_api import YouTubeTranscriptApi
from googleapiclient.discovery import build
from sentence_transformers import SentenceTransformer
from datetime import datetime
import os
from dotenv import load_dotenv
# import yaml
# from pprint import pprint

# import pandas as pd
import numpy as np


# Youtube API Key
# load_dotenv(dotenv_path = ".env")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")



def get_video_records(response: requests.models.Response, lookback_days = 30) -> list:
    """
    Function to extract YouTube video metadata from GET request response.

    Dependers:
        - getVideoIDs()
    """
    video_record_list = []

    try:
        # Parse JSON response
        response_data = json.loads(response.text)
    except json.JSONDecodeError:
        print("Invalid JSON response.")
        return []

    # Iterate over items
    for raw_item in response_data.get('items', []):
        # Safely get the video publish date
        published_at = raw_item.get('snippet', {}).get('publishedAt')
        if not published_at:
            continue  # Skip if publishedAt is missing

        try:
            video_date = datetime.datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue  # Skip if date parsing fails

        # Check if video is within the last 30 days
        if video_date >= datetime.datetime.now() - datetime.timedelta(days = lookback_days):
            # Only proceed for YouTube videos
            if raw_item.get('id', {}).get('kind') == "youtube#video":
                video_record = {
                    'video_id': raw_item.get('id', {}).get('videoId'),
                    'datetime': published_at,
                    'title': raw_item.get('snippet', {}).get('title'),
                }
                video_record_list.append(video_record)

    return video_record_list


# raw_item = json.loads(response.text)["items"]
# raw_item = raw_item[0]
# raw_item[0]["id"]
# raw_item["snippet"]["publishedAt"]


# Get video IDs ----
def get_video_ids(channel_id: str = "UCBTy8j2cPy6zw68godcE7MQ", lookback_days = 15) -> list:
    """
    Function to extract video IDs from a YouTube channel.

    Args:
        channel_id (str): YouTube channel ID.

    Returns:
        list: List of video IDs.
    """

    url = "https://www.googleapis.com/youtube/v3/search"
    page_token = None
    channel_id = channel_id
    YOUR_API_KEY = YOUTUBE_API_KEY


    video_record_list = []

    while page_token != 0:
        params = {
            "key": YOUTUBE_API_KEY,
            "channelId": channel_id,
            "part": ["snippet", "id"],
            "order": "date",
            "maxResults": 50,
            "pageToken": page_token,
        }

        response = requests.get(url, params = params)

        # Append video data to video_data list
        video_record_list += get_video_records(response, lookback_days = lookback_days)

        try:
            # Grab next page token
            page_token = json.loads(response.text)["nextPageToken"]
        except KeyError:
            # if no next page token, set page_token to 0
            page_token = 0

    # return video_record_list
    # return pl.DataFrame(video_record_list)
    current_timestamp = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    file_name = f"data/video_ids_{current_timestamp}.parquet"
    pl.DataFrame(video_record_list).write_parquet(file_name)




# video_id_df = get_video_ids(channel_id = "UCBTy8j2cPy6zw68godcE7MQ")

def read_most_recent_file(data_file = "video_ids"):

    # Specify the folder containing the files
    folder_path = "data/"

    # List all files in the folder
    if data_file == "video_ids":
        files = [f for f in os.listdir(folder_path) if f.startswith("video_ids") and f.endswith(".parquet")]
    elif data_file == "video_transcripts":
        files = [f for f in os.listdir(folder_path) if f.startswith("video_transcripts") and f.endswith(".parquet")]
    elif data_file == "video_transcript_special_strings":
        files = [f for f in os.listdir(folder_path) if f.startswith("video_transcript_special_strings") and f.endswith(".parquet")]
    elif data_file == "video_transcript_special_strings_datatypes":
        files = [f for f in os.listdir(folder_path) if f.startswith("video_transcript_special_strings_datatypes") and f.endswith(".parquet")]
    else:
        files = []
    # files = [f for f in os.listdir(folder_path) if f.startswith("video_ids") and f.endswith(".parquet")]

    # Ensure there are matching files
    if len(files) == 0:
        raise FileNotFoundError("No video_ids parquet files found in the data folder.")

    # Sort files by timestamp in descending order to get the most recent file
    files.sort(key=lambda f: os.path.getmtime(os.path.join(folder_path, f)), reverse=True)

    # Get the most recent file
    latest_file = os.path.join(folder_path, files[0])

    # Read the most recent file using Polars
    data = pl.read_parquet(latest_file)



# Get Video Transcripts ----
def get_video_transcripts() -> dict:

    # Load Data
    # data = pl.read_parquet("data/video_ids.parquet")
    data = read_most_recent_file(data_file = "video_ids")
    data = pl.read_parquet('data/video_ids_2024-12-26_11.33.59.parquet')

    # Initialize list to store transcripts
    transcript_text_list = []

    # Iterate over video IDs
    for video_id in data["video_id"]:
        try:
            # Fetch transcript for the video
            transcript = YouTubeTranscriptApi.get_transcript(video_id)
            # Combine all transcript text
            transcript_text = " ".join([entry['text'] for entry in transcript])
        except Exception as e:
            # Handle errors (e.g., no transcript available)
            transcript_text = "No transcript available"

        # Append the transcript text
        transcript_text_list.append(transcript_text)

    # Add transcript column to the DataFrame
    data = data.with_columns(pl.Series(name = "transcript", values = transcript_text_list))

    # Return
    # return data
    current_timestamp = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    file_name = f"data/video_transcripts{current_timestamp}.parquet"
    data.write_parquet(file_name)

# video_transcript_df = get_video_transcripts(video_id_df)


# Handle Special Strings ----
def handle_special_strings(data: pl.dataframe.frame.DataFrame) -> pl.dataframe.frame.DataFrame:
    """
    Function to handle special strings in the transcript text.

    Args:
        data (pl.dataframe.frame.DataFrame): Input DataFrame.

    Returns:
        pl.dataframe.frame.DataFrame: DataFrame with special strings handled.
    """

    # Load Data
    data = pl.read_parquet("data/video_transcripts.parquet")

    # Replace special strings
    special_strings = ["[Music]", "[Applause]", "[Laughter]", "[Music Ends]"]
    special_string_replacements = ["", "", "", ""]

    for i in range(len(special_strings)):
        df = data.with_columns(data['transcript'].str.replace(special_strings[i], special_string_replacements[i]).alias('transcript'))

    # return df
    current_timestamp = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    file_name = f"data/video_transcript_special_strings{current_timestamp}.parquet"
    df.write_parquet(file_name)

# video_transcript_special_strings_df = handle_special_strings(video_transcript_df)


# Set Data Types ----
def setDatatypes(data: pl.dataframe.frame.DataFrame) -> pl.dataframe.frame.DataFrame:
    """
        Function to change data types of columns in polars data frame containing video IDs, dates, titles, and transcripts

        Dependers:
            - transformData()
    """

    # Load Data
    data = pl.read_parquet("data/video_transcript_special_strings.parquet")

    # change datetime to Datetime dtype
    df = data.with_columns(pl.col('datetime').cast(pl.Datetime))

    # return df
    current_timestamp = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    file_name = f"data/video_transcript_special_strings_datatypes{current_timestamp}.parquet"
    df.write_parquet(file_name)

# data_types_df = setDatatypes(video_transcript_special_strings_df)



# # Text Embeddings ----
# def createTextEmbeddings(data: pl.dataframe.frame.DataFrame) -> pl.dataframe.frame.DataFrame:
#     """
#         Function to generate text embeddings of video titles and transcripts
#     """

#     # read data from file
#     # df = pl.read_parquet('data/video-transcripts.parquet')

#     # define embedding model and columns to embed
#     # model_path = 'data/all-MiniLM-L6-v2'
#     # model = SentenceTransformer(model_path)
#     model = SentenceTransformer('all-MiniLM-L6-v2')

#     column_name_list = ['title', 'transcript']

#     for column_name in column_name_list:
#         # generate embeddings
#         embedding_arr = model.encode(data[column_name].to_list())

#         # store embeddings in a dataframe
#         schema_dict = {column_name+'_embedding-'+str(i): float for i in range(embedding_arr.shape[1])}
#         df_embedding = pl.DataFrame(embedding_arr, schema=schema_dict)

#         # append embeddings to video index
#         df = pl.concat([data, df_embedding], how='horizontal')

#     # write data to file
#     df.write_parquet('data/video-index.parquet')



import pandas as pd

from etls.reddit_etl import connect_reddit
from utils.constants import CLIENT_ID, SECRET


def reddit_pipeline(file_name: str, subreddit: str, time_filter='day', limit=None):
    
    #creating a Reddit API client using credentials
    instance = connect_reddit(CLIENT_ID, SECRET, 'Airscholar Agent')

    #extraction (E): Pulling data from Reddit API using PRAW
    posts = extract_posts(instance, subreddit, time_filter, limit)
    post_df = pd.DataFrame(posts)

    #transformation (T): Cleaning and formatting the data

    #loading to csv (L): Writing the data to a CSV, JSON, or S3
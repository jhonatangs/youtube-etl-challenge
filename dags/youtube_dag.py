from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task
from airflow.utils import dates
from airflow.models import Variable
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from utils.utils import get_youtube_video


YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
YOUTUBE_DEVELOPER_API_KEY = Variable.get("YOUTUBE_DEVELOPER_API_KEY")


@dag(
    dag_id="youtube_extractor",
    schedule_interval="30 1 * * *",
    start_date=dates.days_ago(1),
    catchup=False,
    tags=["youtube"],
)
def youtube_extractor_dag():
    @task
    def get_youtube_channel(channel_id):
        youtube = build(
            YOUTUBE_API_SERVICE_NAME,
            YOUTUBE_API_VERSION,
            developerKey=YOUTUBE_DEVELOPER_API_KEY,
        )

        try:
            request = youtube.channels().list(part="snippet, statistics", id=channel_id)
            response = request.execute()

            if "items" in response:
                return response["items"][0]
            else:
                return None
        except HttpError as e:
            print(f"Erro ao acessar a API do YouTube: {e}")

    @task
    def get_top_ten_youtube_channel_videos(channel_id):
        youtube = build(
            YOUTUBE_API_SERVICE_NAME,
            YOUTUBE_API_VERSION,
            developerKey=YOUTUBE_DEVELOPER_API_KEY,
        )

        try:
            request = youtube.search().list(
                part="id, snippet",
                channelId=channel_id,
                order="viewCount",
                maxResults=10,
            )
            response = request.execute()
            return response["items"]
        except HttpError as e:
            print(f"Erro ao acessar a API do YouTube: {e}")

    @task
    def parse_youtube_channel_data(channel_data):
        return {
            "channel_id": channel_data["id"],
            "channel_title": channel_data["snippet"]["title"],
            "channel_description": channel_data["snippet"]["description"],
            "channel_view_count": channel_data["statistics"]["viewCount"],
            "channel_subscriber_count": channel_data["statistics"]["subscriberCount"],
            "channel_video_count": channel_data["statistics"]["videoCount"],
        }

    @task
    def parse_videos(channel_videos):
        videos = []

        for video in channel_videos:
            video_id = video["id"]["videoId"]
            video_statistics_and_details = get_youtube_video(
                video_id,
                YOUTUBE_API_SERVICE_NAME,
                YOUTUBE_API_VERSION,
                YOUTUBE_DEVELOPER_API_KEY,
            )

            videos.append(
                {
                    "video_id": video_id,
                    "video_title": video["snippet"]["title"],
                    "video_duration": video_statistics_and_details["contentDetails"][
                        "duration"
                    ],
                    "video_view_count": video_statistics_and_details["statistics"][
                        "viewCount"
                    ],
                    "video_like_count": video_statistics_and_details["statistics"][
                        "likeCount"
                    ],
                    "video_favorite_count": video_statistics_and_details["statistics"][
                        "favoriteCount"
                    ],
                    "video_comment_count": video_statistics_and_details["statistics"][
                        "commentCount"
                    ],
                }
            )

        return videos

    @task
    def concat_channel_with_videos(channel, videos):
        youtube_data = []

        for video in videos:
            youtube_data.append(channel | video)

        return pd.DataFrame(youtube_data)

    @task
    def load_youtube_data(youtube_df):
        current_datetime = datetime.now().strftime("%Y-%m-%d")
        str_current_datetime = str(current_datetime)
        youtube_df.to_csv(f"/opt/airflow/dags/youtube_data_{str_current_datetime}.csv")

    channel = get_youtube_channel("UCmLGJ3VYBcfRaWbP6JLJcpA")
    videos = get_top_ten_youtube_channel_videos("UCmLGJ3VYBcfRaWbP6JLJcpA")
    channel_parsed = parse_youtube_channel_data(channel)
    videos_parsed = parse_videos(videos)
    youtube_df = concat_channel_with_videos(channel_parsed, videos_parsed)
    load_youtube_data(youtube_df)


youtube_extractor_dag()

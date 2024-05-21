from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


def get_youtube_video(
    video_id, youtube_api_service_name, youtube_api_version, youtube_developer_api_key
):
    youtube = build(
        youtube_api_service_name,
        youtube_api_version,
        developerKey=youtube_developer_api_key,
    )

    try:
        request = youtube.videos().list(
            part="statistics, contentDetails",
            id=video_id,
        )
        response = request.execute()
        return response["items"][0]
    except HttpError as e:
        print(f"Erro ao acessar a API do YouTube: {e}")

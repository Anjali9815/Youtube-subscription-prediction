from googleapiclient.discovery import build
import re

api_key = 'AIzaSyCbvlCGdLve_ykV3PfU0YsegdxCOnDkLFk'
channel_name = 'Corey Schafer'

# Initialize the YouTube API client
youtube = build('youtube', 'v3', developerKey=api_key)

def get_channel_id_by_name(channel_name):
    # Search for the channel by name
    search_response = youtube.search().list(
        part='snippet',
        q=channel_name,
        type='channel',
        maxResults=1
    ).execute()

    # Extract the channel ID
    channel_id = search_response['items'][0]['id']['channelId']
    print("channel_id ", channel_id)
    return channel_id

def get_channel_statistics(channel_id):
    # Fetch channel information
    channel_response = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id
    ).execute()

    channel_info = channel_response['items'][0]
    statistics = channel_info['statistics']
    content_details = channel_info['contentDetails']

    # Total number of views
    total_views = int(statistics['viewCount'])

    # Total number of comments
    total_comments = int(statistics.get('commentCount', 0))

    # Total number of playlists
    total_playlists = len(content_details['relatedPlaylists'])

    # Fetch playlist items (uploads)
    playlist_id = content_details['relatedPlaylists']['uploads']
    playlist_response = youtube.playlistItems().list(
        part='snippet',
        playlistId=playlist_id,
        maxResults=50
    ).execute()

    videos = playlist_response['items']

    total_likes = 0
    total_short_videos = 0
    total_long_videos = 0
    total_live_sessions = 0
    total_videos_count = 0

    for video in videos:
        video_id = video['snippet']['resourceId']['videoId']
        video_response = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=video_id
        ).execute()
        video_info = video_response['items'][0]
        duration = video_info['contentDetails']['duration']
        likes = video_info['statistics'].get('likeCount', 0)

        # Check if the video is a live broadcast
        if video_info['snippet']['liveBroadcastContent'] == 'live':
            total_live_sessions += 1

        # Duration check for short vs long videos
        if 'PT' in duration:
            duration_minutes = re.findall(r'(\d+)M', duration)
            if duration_minutes:
                minutes = int(duration_minutes[0])
                if minutes < 5:
                    total_short_videos += 1
                else:
                    total_long_videos += 1
            else:
                total_short_videos += 1  # Assuming video is less than a minute long

        total_likes += int(likes)
        total_videos_count += 1

    return {
        'total_views': total_views,
        'total_comments': total_comments,
        'total_short_videos': total_short_videos,
        'total_long_videos': total_long_videos,
        'total_likes': total_likes,
        'total_playlists': total_playlists,
        'total_live_sessions': total_live_sessions,
        "total_videos_count" : total_videos_count
    }

# Fetch the channel ID based on the channel name
channel_id = get_channel_id_by_name(channel_name)

# Fetch and print channel statistics
channel_statistics = get_channel_statistics(channel_id)
print(channel_statistics)

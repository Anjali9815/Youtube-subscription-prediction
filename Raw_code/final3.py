from googleapiclient.discovery import build
import isodate
from pymongo import MongoClient
import certifi
import warnings
from datetime import datetime, timezone  # Ensure timezone is imported

warnings.filterwarnings("ignore")

def connect_to_mongodb(MONGO_CONNECTION_STRING):
    try:
        # Create a MongoClient instance with CA bundle specified
        client = MongoClient(MONGO_CONNECTION_STRING, tls=True, tlsCAFile=certifi.where())

        # Attempt to get server information to confirm connection
        client.server_info()  # Forces a call to the server
        print("Successfully connected to MongoDB.")

        # # Access a specific database (replace 'test' with your database name)
        db = client['Project1']
        
        return client, db

    except Exception as e:
        print("Error connecting to MongoDB:", e)
        return None, None

def get_video_statistics(video_id):
    try:
        video_response = youtube.videos().list(part='statistics,contentDetails', id=video_id).execute()
        video_info = video_response.get('items', [])[0] if 'items' in video_response else {}
        stats = video_info.get('statistics', {})
        details = video_info.get('contentDetails', {})
        
        likes = int(stats.get('likeCount', 0))
        comments = int(stats.get('commentCount', 0))
        duration = details.get('duration', 'PT0S')
        duration_seconds = parse_duration_to_seconds(duration)
        
        return likes, comments, duration_seconds
    except Exception as e:
        print(f'Error fetching video data for ID {video_id}: {e}')
        return 0, 0, 0

def parse_duration_to_seconds(duration):
    try:
        duration = isodate.parse_duration(duration)
        return int(duration.total_seconds())
    except Exception as e:
        print(f'Error parsing duration {duration}: {e}')
        return 0

def get_channel_statistics(channel_id, user_start_date):
    try:

        # Check if the channel_id already exists in MongoDB
        existing_channel = db['youtube_channel_data'].find_one({'channel_id': channel_id})
        if existing_channel:
            print(f"Channel ID {channel_id} already exists. Skipping data fetch.")
            return  # Skip fetching data if it already exists

        channel_response = youtube.channels().list(part='snippet,contentDetails,statistics', id=channel_id).execute()
        channel_info = channel_response.get('items', [])[0] if 'items' in channel_response else {}
        print("uuuuuuuu")
        # print(channel_info)
        
        if channel_info:
            snippet = channel_info.get('snippet', {})
            statistics = channel_info.get('statistics', {})
            content_details = channel_info.get('contentDetails', {})
            
            # Fetch channel start date
            channel_start_date = snippet.get('publishedAt', 'NA')
            channel_start_date = datetime.fromisoformat(channel_start_date.replace('Z', '+00:00'))
            
            # Use the user start date for filtering
            # start_date = user_start_date if user_start_date else channel_start_date
            # start_date = channel_start_date
            # Get upload playlist ID
            uploads_playlist_id = content_details.get('relatedPlaylists', {}).get('uploads', 'NA')
            
            if uploads_playlist_id == 'NA':
                print(f'No uploads playlist found for channel ID {channel_id}.')
                return
            
            total_likes = 0
            total_comments = 0
            short_videos_count = 0
            long_videos_count = 0
            
            next_page_token = None
            while True:
                playlist_items_response = youtube.playlistItems().list(
                    part='contentDetails,snippet',
                    playlistId=uploads_playlist_id,
                    maxResults=50,
                    pageToken=next_page_token
                ).execute()
                
                items = playlist_items_response.get('items', [])
                for item in items:
                    video_id = item['contentDetails']['videoId']
                    video_upload_date = item['snippet'].get('publishedAt', 'NA')
                    video_upload_date = datetime.fromisoformat(video_upload_date.replace('Z', '+00:00'))
                    
                    if video_upload_date >= start_date:
                        likes, comments, duration_seconds = get_video_statistics(video_id)
                        total_likes += likes
                        total_comments += comments
                        if duration_seconds < 60:
                            short_videos_count += 1
                        else:
                            long_videos_count += 1
                
                next_page_token = playlist_items_response.get('nextPageToken')
                if not next_page_token:
                    break
            
            # Define the nested structure
            channel_data = {
                'channel_id': channel_id,
                'channel_details': {
                    'channel_name': snippet.get('title', 'NA'),
                    'channel_start_date': channel_start_date.isoformat(),
                    'inception_date' : start_date.isoformat(),
                    'total_no_of_videos': statistics.get('videoCount', 'NA'),
                    'total_no_short_videos': short_videos_count,
                    'total_no_long_videos': long_videos_count,
                    'total_views': statistics.get('viewCount', 'NA'),
                    'total_likes': total_likes,
                    'total_comments': total_comments,
                    'total_subscribers': statistics.get('subscriberCount', 'NA'),

                }
            }

            # Insert or update the data in MongoDB
            collection = db['youtube_channel_data']  # Replace with your collection name
            collection.update_one(
                {'channel_id': channel_id},  # Use channel_id as unique identifier
                {'$set': channel_data},
                upsert=True
            )
            print(channel_data)
            print(f'Data for channel ID {channel_id} inserted/updated in MongoDB.')
        else:
            print(f'Channel with ID {channel_id} not found or no data available.')

    except Exception as e:
        print(f'Error fetching channel data for ID {channel_id}: {e}')


# MongoDB setup
con = "mongodb+srv://anjalijha1507:U54OU4PFxPYlVc4S@youtubedata.shzzp.mongodb.net/?retryWrites=true&w=majority&appName=YoutubeData"
from pymongo import MongoClient

if __name__ == "__main__":
    client, db = connect_to_mongodb(con)

    youtube = build('youtube', 'v3', developerKey='AIzaSyBPJ64uexibg77DCSd6rSGU8loyOTvndjI')
    CHANNEL_IDS = [
        # 'UChYs-_zjKRYhdMddjx-NPLw', #singh in us
        # 'UC0rE2qq81of4fojo-KhO5rg', #Tanmya bhatt 
        # 'UCijVIIfFzspulKc7yWA2Qhg', # Friends
        # 'UCnSFZ-olBoLGLRUS_3RI2Aw', # tmkoc,
        # 'UC4zWG9LccdWGUlF77LZ8toA', 
        # 'UCx1VY57UmjU76Tgq8YwkklA', # unacademy
        # 'UCkDw-LPU1Nnd2WRsfnDbUcA', #byjus
        # 'UC4a-Gbdw7vOaccHmFo40b9g', #khan academy
        # 'UCYQHAFRijUCayPOdzH6ipGw', #DAN ILIES
        # 'UCeYt6blRBKuNrEg_-282fSA',#code bucks
        # 'UCBa5G_ESCn8Yd4vw5U-gIcg', #standford online
        # 'UCD16eo98AXl-9T61Xd711kQ', #neet wallah
        # 'UCUpquzY878NEaZm5bc7m2sQ' #salesforce
        # 'UCWPJwoVXJhv0-ucr3pUs1dA',
        # 'UCD7kbZQyYIR6RgJQYW9w0Tg', # zomato
        # 'UCo8bcnLyZH8tBIH9V1mLgqQ', #the odd
        # 'UCIYI7DDUMDgJLODyyjM2SWQ', #lockdownlife
        # 'UCDiugHjbfNxlWdTZ9g5B9vA', #ramlal
        # 'UCGwu0nbY2wSkW8N-cghnLpA', #Anime
        # 'UCpFFItkfZz1qz5PpHpqzYBw', # Nexpo
        # 'UCmKaoNn0OvxVAe7f_8sXYNQ', #jovian
        # 'UCAuUUnT6oDeKwE6v1NGQxug', # TED
        # 'UCAQg09FkoobmLquNNoO4ulg', 
        # 'UCzBYOHyEEzlkRdDOSobbpvw',
        # 'UCp28L5GnKYqJVqKFvZZX_Bg',
        # 'UCRXiA3h1no_PFkb1JCP0yMA',
        # 'UCAMDWbJh_rMzvH7g25JlaSw',
        # 'UCXTAdFsBmxNK3_c8MUvSviQ',
        # 'UCay_OLhWtf9iklq8zg_or0g',
        # 'UCMu5gPmKp5av0QCAajKTMhw',
        # 'UCBnxEdpoZwstJqC1yZpOjRA', NA
        # 'UCJg9wBPyKMNA5sRDnvzmkdg',
        # 'UCEKAoysiLuf5VixeO_TwSOw',
        # 'UCvC4D8onUfXzvjTOM-dBfEA',
        # 'UC0QHWhjbe5fGJEPz3sVb6nw',
        # 'UCF9imwPMSGz4Vq1NiTWCC7g',
        # 'UCTIvWbKDaa3cv-gYjKlJbHQ',
        # 'UCDVYQ4Zhbm3S2dlz7P1GBDg', NA
        # 'UCt_DaLB_NDqPVxezyvcfRtg',
        # 'UCE_--R1P5-kfBzHTca0dsnw',
        # 'UCL6JmiMXKoXS6bpP1D3bk8g',
        # 'UCIdFcLCIJQ_YMrormG_nU8w',
        # 'UCAov2BBv1ZJav0c_yHEciAw'
        # 'UCuiJB3jUf30fbB2ZrbpNi_g',
        # 'UCjbHBKKBMj8Cki4SFHA_XsQ',
        # 'UCLieTWx74Jty5p3SJAJCxzQ',
        # 'UC8WHXV-B2DmrVPGprIpvLSQ',
        # 'UCAcHeVvVgwRx7wSWw69RgYg',
        # 'UC9xGsFc1c5DMSWCBvdiGQ4g'


        # 'UCPgMAS8woHJ_o_OZdTR7kcQ',
        # 'UCBFWWvVodNSz1LKwfTtgRdw',
        # 'UCTlo9YEeqD-4jDR7v1w3otw',
        # 'UCr12iN_7mUIcMOEtbrkC7bw',
        # 'UCjnUi-TfTqjInwNrSXSVBhg',
        # 'UCKuSDqgt_rrBwvtc7CxSTeQ',
        # 'UChjG0BCNhX55MrbdZQ6rHsw',
        # 'UCnZfB0c8PaM-ZKbsK4bZDvg'


        # 'UCNDfcymFai2vqzX5vFTcA8w',
        # 'UCU3LQslKqMG9meaitNnXWjA',
        # 'UCBFWWvVodNSz1LKwfTtgRdw',
        # 'UCOuE0Pe6cinpj8QoGdlg2dw',
        # 'UCPzbHv9CroLVn_92VZutfUg',
        # 'UCvezh8LDlmkZJeo-5ZtTHQg',
        # 'UCtiddfzHXNevG-yYe5vc_4Q',
        # 'UC6uE84tWIM99CNXW-18F9KA',
        # 'UCpSHdxdRCrxHozjp3wHMMBw',
        # 'UC5x2Ae5t957q50hnj3SFjBw',
        # 'UCELVrunQqzBdxWbgJ5C1iLw',
        # 'UCbx8KUWGjjDoJ9_FUYzR4wA',
        # 'UCa8opd43s6JyO4vlgtmEapA',
        # 'UCj9g38QGYkKJ1nOBExXnHOA',
        # 'UCqOwcxgDCjnHNgMt9A3Ua8w',
        # 'UC5f5IV0Bf79YLp_p9nfInRA',
        # 'UCavvo_FXKmQ324RS7WZ7BXg',
        # 'UCsxLlLqJG8gqbK4i0kB-08Q',
        # 'UCh3UXFPTciJP1luW0r6DBjg'
        # 'UC08LgV5b9HpGIACpw_MLH4g',
        # 'UCiVs2pnGW5mLIc1jS2nxhjg',
        # 'UC4gVyi0hEAdqzW7cp2DAQOg',
        # 'UC3iwxKMRg5l2aDG_8XK2nSQ',
        # 'UCmr_2dlYM0pYHdI9TeF5SIQ',
        # 'UCEb5JC7ZKwb5ROsNKA-VagA',
        # 'UCpshxsDVirv_tuyQmiKfDjA'

        

        ]
    # Get start date from user
    # user_input = input('Enter the start date in YYYY-MM-DD format (data will be fetched from this date onwards): ')

    user_input = '2022-12-01'
    try:
        start_date = datetime.strptime(user_input, '%Y-%m-%d').replace(tzinfo=timezone.utc) if user_input else None
    except ValueError:
        print('Invalid date format. Please use YYYY-MM-DD format.')
        exit()

    # Fetch statistics for each channel and store in MongoDB
    for channel_id in CHANNEL_IDS:
        get_channel_statistics(channel_id, start_date)

    # Close the MongoDB connection
    client.close()


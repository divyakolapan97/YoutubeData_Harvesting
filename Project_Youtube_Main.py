import googleapiclient.discovery
import pandas as pd
import mysql.connector
import streamlit as st
from mysql.connector import (connection)
from sqlalchemy import create_engine

## CONNECTING THROUGH API

api_service_name = "youtube"
api_version = "v3"
api_key = "AIzaSyCVxuwdHuUU8VmEsuyW3E9enTd9tlb0rh0"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

## FETCHING CHANNEL DATA

def channel_data(channel_id):
    request = youtube.channels().list(part="snippet,contentDetails,statistics",
        id=channel_id)
    response = request.execute()
    data = {
        'channel_ids':[response['items'][0]['id']],
        'channel_name':[response['items'][0]['snippet']['title']],
        'channel_des':[response['items'][0]['snippet']['description']],
        'channel_pubat':[response['items'][0]['snippet']['publishedAt']],
        'channel_pid':[response['items'][0]['contentDetails']['relatedPlaylists']['uploads']],
        'channel_subs':[response['items'][0]['statistics']['subscriberCount']],
        'channel_vc':[response['items'][0]['statistics']['videoCount']], 
        'channel_vic':[response['items'][0]['statistics']['viewCount']]
    }

    return pd.DataFrame(data)


## FETCHING VIDEO IDS

def need_video_ids(channel_id):
    
    videoids = []


    request = youtube.channels().list(part="snippet,contentDetails,statistics",
            id=channel_id)
    response = request.execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']


    nextpage_token = None
    while True:

        request1 = youtube.playlistItems().list(part = 'snippet',playlistId = playlist_id,maxResults = 50,pageToken=nextpage_token)
        response1 = request1.execute()

        for i in range(len(response1['items'])):
            videoids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        nextpage_token = response1.get('nextPageToken')
        #response1['items'][0]['snippet']['videoId']

        if nextpage_token is None:
            break

    return videoids

## FECTHING VIDEO DETAILS

def video_details_list(videoids):

    video_details = []
    for ids in videoids:
        request2 = youtube.videos().list(part= "snippet,contentDetails,statistics",id = ids)
        response2 = request2.execute()

        for i in response2['items']:
            output = {'channel_name' : i['snippet']['channelTitle'],
                    'channel_ids':i['snippet']['channelId'],
                    'VideoId':i['id'],
                    'Video_name': i['snippet']['title'],
                    'Video_Description':i['snippet']['description'],
                    'Video_Tags':i['snippet'].get('tags'),
                    'PublishedAt':i['snippet']['publishedAt'],
                    'Views':i['statistics'].get('viewCount'),
                    'Likes':i['statistics'].get('likeCount'),
                    'Dislike':i['statistics'].get('dislikeCount'),
                    'Favorite':i['statistics']['favoriteCount'],
                    'commentcount':i['statistics'].get('commentCount'),
                    'Duration':i['contentDetails']['duration'],
                    'Thumbnail': i['snippet']['thumbnails'],
                    'captionstatus':i['contentDetails'].get('caption')
                    }
            video_details.append(output)
    return pd.DataFrame(video_details)

## FETCHING COMMENT DETAILS

def command_details(VideoIdss):
    command_data = []
    try:
        for videoidd in VideoIdss:
            request3 = youtube.commentThreads().list(part = 'snippet',videoId = videoidd,maxResults = 10)
            response3 = request3.execute()

            for i in response3['items']:
                output = {'commentid':i['snippet']['topLevelComment']['id'],
                        'VideoId':i['snippet']['topLevelComment']['snippet']['videoId'],
                        'commenttext':i['snippet']['topLevelComment']['snippet']['textDisplay'],
                        'commentauthor':i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        'commentpublished':i['snippet']['topLevelComment']['snippet']['publishedAt'],

                }

                command_data.append(output)
            
    except:
        pass

    return pd.DataFrame(command_data)

## INSERTING CHANNEL DATA TO MYSQL

def sql_channel_table():
    conn = connection.MySQLConnection(host ="127.0.0.1",
    user ="root",
    password="1997",
    database = "youtube_harvesting",auth_plugin = 'mysql_native_password')

    cursor = conn.cursor()  

    # drop_query = """drop table if exists channel_details"""
    # cursor.execute(drop_query)
    # conn.commit()   

    create_query = '''create table if not exists channel_details(channel_ids varchar(100) PRIMARY KEY ,channel_name varchar(100) ,channel_des varchar(1000) ,channel_pubat varchar(100) ,channel_pid varchar(100) ,channel_subs bigint, channel_vc bigint,channel_vic bigint)'''
    cursor.execute(create_query)
    conn.commit()  

    
    for index,row in channel_details.iterrows():
        insert_query = ''' INSERT INTO channel_details(
    channel_ids, channel_name, channel_des, channel_pubat, 
    channel_pid, channel_subs, channel_vc, channel_vic
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''

        values = (
        row['channel_ids'],
        row['channel_name'],
        row['channel_des'],
        row['channel_pubat'],
        row['channel_pid'],
        row['channel_subs'],
        row['channel_vc'],
        row['channel_vic']
        )
        try:
            cursor.execute(insert_query,values)
            conn.commit()

        except:
            print("Channel values are already inserted")

        return "Channel data stored successfully"
    
## INSERTING VIDEO DATA TO MYSQL    

def sql_video_table():
    conn = connection.MySQLConnection(host ="127.0.0.1",
    user ="root",
    password="1997",
    database = "youtube_harvesting",auth_plugin = 'mysql_native_password')

    cursor = conn.cursor() 

    # drop_query = """drop table if exists videos_details"""
    # cursor.execute(drop_query)
    # conn.commit()

    create_video_query = '''CREATE TABLE if not exists videos_details (
    channel_name VARCHAR(255),
    channel_ids VARCHAR(255),
    VideoId VARCHAR(255),
    Video_name VARCHAR(255),
    Video_Description TEXT,
    Video_Tags TEXT,
    PublishedAt VARCHAR(255),
    Views INT,
    Likes INT,
    Dislike INT,
    Favorite INT,
    commentcount INT,
    Duration VARCHAR(50),
   
    captionstatus VARCHAR(50)
    )'''

    cursor.execute(create_video_query)
    conn.commit()
    for col in video_data.columns:
        if video_data[col].apply(lambda x: isinstance(x, list)).any():
            video_data[col] = video_data[col].apply(lambda x: ','.join(x) if isinstance(x, list) else x)

    for index, row in video_data.iterrows():  # Assuming video_data is your DataFrame containing video details
        insert_query = '''INSERT INTO videos_details(
    channel_name, channel_ids, VideoId, Video_name, 
    Video_Description, Video_Tags, PublishedAt, 
    Views, Likes, Dislike, Favorite, 
    commentcount, Duration, captionstatus
    ) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
'''

        values = (
        row['channel_name'],
        row['channel_ids'],
        row['VideoId'],
        row['Video_name'],
        row['Video_Description'],
        row['Video_Tags'],
        row['PublishedAt'],
        row['Views'],
        row['Likes'],
        row['Dislike'],
        row['Favorite'],
        row['commentcount'],
        row['Duration'],
        
        row['captionstatus']
    )
        cursor.execute(insert_query,values)
        conn.commit()

## INSERTING COMMENT DATA TO MYSQL
            
def sql_comment_table():
    conn = connection.MySQLConnection(host ="127.0.0.1",
    user ="root",
    password="1997",
    database = "youtube_harvesting",auth_plugin = 'mysql_native_password')

    cursor = conn.cursor()

    # drop_query = """drop table if exists comment_details"""
    # cursor.execute(drop_query)
    # conn.commit()

    create_comment_query = '''CREATE TABLE if not exists comment_details (
    commentid VARCHAR(255),
    VideoId VARCHAR(255),
    commenttext TEXT,
    commentauthor VARCHAR(255),
    commentpublished VARCHAR(255)
        );'''
    cursor.execute(create_comment_query)
    conn.commit()

    for index, row in command_data.iterrows():
        insert_query = '''
        INSERT INTO comment_details (
        commentid, VideoId, commenttext, 
        commentauthor, commentpublished
        ) VALUES (%s, %s, %s, %s, %s)'''

        values = (
        row['commentid'],
        row['VideoId'],
        row['commenttext'],
        row['commentauthor'],
        row['commentpublished']
    )
        cursor.execute(insert_query,values)
        conn.commit()


## STREAMLIT EXECUTION

st.title('YouTube Data Harvesting')

channel_id = st.text_input("Enter Channel ID")

if st.button('Store'):
    conn = connection.MySQLConnection(host ="127.0.0.1",
    user ="root",
     password="1997",
     database = "youtube_harvesting",auth_plugin = 'mysql_native_password')

    cursor = conn.cursor()
    
    select_query = """select channel_ids from channel_details"""
    cursor.execute(select_query)
    rows = cursor.fetchall()
    column_entries = [row[0] for row in rows]

    if channel_id in column_entries:
        st.warning("Data already exists")
    else:
        channel_details = channel_data(channel_id)
        VideoIds = need_video_ids(channel_id)
        video_data = video_details_list(VideoIds)
        command_data = command_details(VideoIds)
        sql_channel_table()
        sql_video_table()
        sql_comment_table()
        st.success('Data stored successfully')


conn = connection.MySQLConnection(host ="127.0.0.1",
user ="root",
    password="1997",
    database = "youtube_harvesting",auth_plugin = 'mysql_native_password')

cursor = conn.cursor()
def execute_query(query):
    conn = connection.MySQLConnection(host ="127.0.0.1",
    user ="root",
     password="1997",
     database = "youtube_harvesting",auth_plugin = 'mysql_native_password')

    #conn = sqlite3.connect('youtube_data.db')
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


queries = {
    "Names of all videos and their corresponding channels": "SELECT Video_name, channel_name FROM videos_details",
    "Channels with the most number of videos": "SELECT channel_name, COUNT(*) as video_count FROM videos_details GROUP BY channel_name ORDER BY video_count DESC",
    "Top 10 most viewed videos and their respective channels": "SELECT Video_name, channel_name, Views FROM videos_details ORDER BY Views DESC LIMIT 10",
    "Number of comments on each video and their corresponding video names": "SELECT Video_name, commentcount FROM videos_details ",
    "Videos with the highest number of likes and their corresponding channel names": "SELECT Video_name, channel_name, Likes FROM videos_details ORDER BY Likes DESC",
    "Total number of likes and dislikes for each video and their corresponding video names": "SELECT Video_name, (Likes + Dislike) as total_likes_dislikes FROM videos_details",
    "Total number of views for each channel and their corresponding channel names": "SELECT channel_name, SUM(Views) as total_views FROM videos_details GROUP BY channel_name",
    "Channels that have published videos in the year 2022": "SELECT DISTINCT channel_name FROM videos_details WHERE PublishedAt LIKE '2022%'",
    "Average duration of all videos in each channel and their corresponding channel names": "SELECT  channel_name,AVG(SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'M', 1), 'PT', -1) * 60 + SUBSTRING_INDEX(SUBSTRING_INDEX(Duration, 'M', -1), 'S', 1)) AS average_seconds FROM videos_details WHERE Duration REGEXP '^PT[0-9]+M[0-9]+S$' group by channel_name",
    "Videos with the highest number of comments": "SELECT Video_name, channel_name, commentcount FROM videos_details ORDER BY commentcount DESC"
}
  
st.title("YouTube Data Queries")

query_option = st.selectbox('Select Query', list(queries.keys()))

if st.button('Run Query'):
    with st.spinner('Executing query...'):
        df_result = execute_query(queries[query_option])
        st.dataframe(df_result)
        st.success('Query executed successfully!')



    

















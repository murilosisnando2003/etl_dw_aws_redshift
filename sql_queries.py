import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = " DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = " DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = " DROP TABLE IF EXISTS songplays"
user_table_drop = " DROP TABLE IF EXISTS users"
song_table_drop = " DROP TABLE IF EXISTS songs  "
artist_table_drop = " DROP TABLE IF EXISTS artists "
time_table_drop = " DROP TABLE IF EXISTS time "

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events (
artist VARCHAR,
auth VARCHAR,
firstName VARCHAR,
gender VARCHAR,
itemInSession INTEGER,
lastName VARCHAR,
length NUMERIC,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration VARCHAR,
sessionId VARCHAR,
song VARCHAR,
status VARCHAR,
ts VARCHAR,
userAgent VARCHAR,
start_time TIMESTAMP,
userId VARCHAR
)
DISTSTYLE even
SORTKEY (start_time);
""")



staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs (
num_songs INTEGER,
artist_id VARCHAR ,
artist_latitude NUMERIC,
artist_longitude NUMERIC,
artist_location VARCHAR,
artist_name VARCHAR,
song_id VARCHAR,
title VARCHAR,
duration NUMERIC,
year INTEGER
)
DISTSTYLE even
SORTKEY (song_id);
""")



songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays
(
songplay_id INTEGER IDENTITY(0,1) NOT NULL,
start_time TIMESTAMP NOT NULL,
user_id VARCHAR NOT NULL,
level VARCHAR ,
song_id VARCHAR ,
artist_id VARCHAR NOT NULL,
session_id VARCHAR,
location VARCHAR ,
user_agent VARCHAR,
PRIMARY KEY (songplay_id)
)
DISTSTYLE even
SORTKEY (song_id);
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users 
(
user_id VARCHAR NOT NULL,
first_name VARCHAR ,
last_name VARCHAR ,
gender VARCHAR ,
level VARCHAR,
PRIMARY KEY (user_id)
)
DISTSTYLE all
SORTKEY(user_id);
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs 
(
song_id VARCHAR NOT NULL,
title VARCHAR ,
artist_id VARCHAR NOT NULL,
year INTEGER ,
duration INTEGER ,
PRIMARY KEY (song_id)

)
DISTSTYLE all
SORTKEY (song_id);
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists 
(
artist_id VARCHAR NOT NULL ,
name VARCHAR ,
location VARCHAR ,
lattitude INTEGER ,
longitude INTEGER ,
PRIMARY KEY (artist_id)
)
DISTSTYLE all
SORTKEY(artist_id);
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time (
start_time TIMESTAMP NOT NULL, 
hour INTEGER , 
day INTEGER , 
week INTEGER , 
month INTEGER, 
year INTEGER , 
weekday INTEGER,
PRIMARY KEY (start_time)
)
DISTSTYLE even
SORTKEY(start_time);
""")

# STAGING TABLES

staging_events_copy =("""
    copy staging_events from '{}'
    credentials 'aws_iam_role={}'
    json 'auto'
    region 'us-west-2';
   
""").format(config['S3']['LOG_JSONPATH'], config['IAM_ROLE']['ARN'] )


staging_songs_copy = ("""
    copy staging_songs from '{}'
    credentials 'aws_iam_role={}'
    json 'auto'
    region 'us-west-2';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'])



# FINAL TABLES

user_table_insert = (""" INSERT INTO users (user_id, first_name, last_name, gender, level)
(select  distinct userId,firstName,lastName,gender,level FROM staging_events where userId is not null );""")

song_table_insert = (""" INSERT INTO songs (song_id, title, artist_id, year, duration)
(SELECT  distinct song_id,title,artist_id,year,duration FROM staging_songs where song_id is not null );""")
    
 
artist_table_insert = ("""INSERT INTO  artists(artist_id, name, location, lattitude, longitude)
(SELECT  distinct artist_id,artist_name,artist_location,artist_latitude,artist_longitude FROM staging_songs where artist_id is not null);
""")
    
time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
(SELECT distinct 
        start_time,
        extract(hour FROM start_time)      AS hour,
        extract(day FROM start_time)       AS day,
        extract(week FROM start_time)      AS week,
        extract(month FROM start_time)     AS month,
        extract(year FROM start_time)      AS year,
        extract(dayofweek FROM start_time) AS weekday
    FROM staging_events where start_time is not null
 )
""")    


songplay_table_insert = (""" INSERT INTO songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
(SELECT se.start_time,se.userId,se.level,ss.song_id,ss.artist_id,se.sessionId,se.location,se.userAgent
    FROM staging_events as se JOIN staging_songs ss on ss.song_id = se.song where se.page = 'NextSong' and ss.song_id is not null);
    
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

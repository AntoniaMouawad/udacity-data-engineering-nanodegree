import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABlE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE  IF EXISTS time"

# CREATE TABLES
# create_table = 'CREATE TABLE IF NOT EXISTS'

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events
    (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender CHAR,
        itemInSession INTEGER,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration VARCHAR,
        sessionId INTEGER,
        song VARCHAR,
        status INTEGER,
        ts BIGINT,
        userAgent VARCHAR,
        userId INTEGER
    )
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
    (
        num_songs INTEGER,
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration FLOAT,
        year INTEGER
    )
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id INTEGER NOT NULL IDENTITY(0,1) PRIMARY KEY DISTKEY, 
        start_time TIMESTAMP NOT NULL,
        user_id INTEGER NOT NULL,
        level VARCHAR,
        song_id VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        session_id INTEGER,
        location VARCHAR,
        user_agent VARCHAR
    )
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
        user_id INTEGER PRIMARY KEY, 
        first_name VARCHAR, 
        last_name VARCHAR, 
        gender CHAR, 
        level VARCHAR
)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
    (
        song_id VARCHAR PRIMARY KEY, 
        title VARCHAR, 
        artist_id VARCHAR, 
        year INTEGER, 
        duration FLOAT
    )
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
    (
        artist_id VARCHAR PRIMARY KEY, 
        name VARCHAR, 
        location VARCHAR, 
        lattitude FLOAT, 
        longitude FLOAT
    )
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
    (
        start_time  TIMESTAMP PRIMARY KEY DISTKEY,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        weekday VARCHAR
    );

""")

# STAGING TABLES
staging_events_copy = (""" 
COPY staging_events
FROM {}
iam_role {}
FORMAT AS JSON {}
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])


staging_songs_copy = ("""
COPY staging_songs
FROM {}
iam_role {}
FORMAT AS JSON 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
       TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,
                se.userId,
                se.level,
                ss.song_id,
                ss.artist_id,
                se.sessionId,
                se.location,
                se.userAgent
FROM staging_songs ss
JOIN staging_events se
ON ss.title = se.song AND ss.artist_name = se.artist AND ss.duration = se.length
AND se.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM staging_events
WHERE userId IS NOT NULL AND userId NOT IN (SELECT DISTINCT user_id FROM users)
AND page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs
SELECT
    DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL AND song_id NOT IN (SELECT DISTINCT song_id FROM songs);
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT
    DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL AND artist_id NOT IN (SELECT DISTINCT artist_id FROM artists);
""")

time_table_insert = ("""
insert into time
SELECT DISTINCT
       TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
       EXTRACT(HOUR FROM start_time) AS hour,
       EXTRACT(DAY FROM start_time) AS day,
       EXTRACT(WEEKS FROM start_time) AS week,
       EXTRACT(MONTH FROM start_time) AS month,
       EXTRACT(YEAR FROM start_time) AS year,
       to_char(start_time, 'Day') AS weekday
FROM staging_events
WHERE start_time IS NOT NULL AND start_time NOT IN (SELECT DISTINCT start_time FROM time);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

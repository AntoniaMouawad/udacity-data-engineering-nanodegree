import os
import glob
import psycopg2
import pandas as pd
from datetime import datetime
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Processes a single song file and inserts artist data and song data in their respective tables
    :param cur: Cursor to the database
    :param filepath: filepath to a song file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude',
                      'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Extracts time data and user data from the dataframe and inserts them in their respective
    tables. Extracts artist_id and song_id from artists and songs table, and the rest of the
    information from the log file, and inserts songplay records in its table.
    :param cur: Cursor to the database
    :param filepath: filepath to a log file
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = df['ts'].apply(lambda s: datetime.fromtimestamp(s/1000))
    
    # insert time data records
    time_data = (df['ts'], t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, 
                 t.dt.year,t.dt.weekday)
    column_labels = ('ts', 'hour', 'day', 'weekofyear', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_dict({label: value for label, value in zip(column_labels, 
                                                                           time_data)})

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Processes all the files in filepath based on a function func, and results in inserting data 
    to a database using cur and conn.
    :param cur: Cursor to the database
    :param conn: Connection to the database
    :param filepath: Filepath to the folder containing all the files we want to extract 
    information from
    :param func: Function to apply on filepath (e.g. process_song_file, process_log_file)
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
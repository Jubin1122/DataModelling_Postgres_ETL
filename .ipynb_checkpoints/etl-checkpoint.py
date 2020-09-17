import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import numpy as np

def process_song_file(cur, filepath):
    """
    Read songs log file records one by one, clean the dataframe(removing nan values) and insert the data into artists and song table
    
    cur: cursor to the sparkiyy database 
    filepath: the path of the json file which is required for reading,
    
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].drop_duplicates()
    artist_data = artist_data.replace({np.nan: None})
    artist_data  = artist_data.values[0].tolist()
    
    
    cur.execute(artist_table_insert, artist_data)
    
    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].drop_duplicates()
    song_data = song_data.replace({np.nan: None})
    song_data  = song_data.values[0].tolist()
    
    cur.execute(song_table_insert, song_data)
    


def process_log_file(cur, filepath):
    """
    Read songs log file records one by one, filter data frames, conversion to timestamp, clean the dataframe(removing nan values)
    and insert the data into time, user, and songplay table
    
    cur: cursor to the sparkiyy database 
    filepath: the path of the json file which is required for reading.
    
    """
    
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df.query("page == 'NextSong'")

    # parsed the ts column using to_datetime and convert it into a pandas dataframe
    t_s = pd.DataFrame({
        'StartTime': pd.to_datetime(df['ts'], unit='ms')
    })
    
    # New Columns
    t_s['hour'], t_s['day'], t_s['week'], t_s['month'], t_s['year'], t_s['weekday'] = t_s['StartTime'].dt.hour,\
    t_s['StartTime'].dt.day,t_s['StartTime'].dt.weekofyear, t_s['StartTime'].dt.month, t_s['StartTime'].dt.year, t_s['StartTime'].dt.weekday
    
    t_s.drop_duplicates()
    
    # insert time data records
    for i, row in t_s.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]].drop_duplicates()
    user_df = user_df.replace({np.nan:None})
        
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
        songplay_data = (
            pd.to_datetime(row.ts, unit='ms'), 
            row.userId, 
            row.level, 
            songid, 
            artistid, 
            row.sessionId,
            row.location, 
            row.userAgent
        ) 
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Walks through the entire tree structure of the directory and enlist all the file paths into a list.
    
    Parameters:
    cur: cursor to the sparkify database
    conn: psycopg2.connect(); Establish connection with the Postgres Database Schema
    filepath: Path of source log files.
    func: Function to perform transformation and loading into the database
    
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
    """
    It is first called when the file is executed.
    This function encompasses extract, transform and loading funcionality.
    
    1. Establish connection
    2. Cursor to Sparkify Database is created.
    3. Functions for log files transformation and loading is called.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
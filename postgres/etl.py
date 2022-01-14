import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Processes each song file

    - Extracts data from the song file

    - Writes data in the songs and artists dimensions
    """

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_df = df[["song_id", "title", "artist_id", "year", "duration"]]
    song_data = song_df.values.tolist()
    for row, t in enumerate(song_data):
        cur.execute(song_table_insert, song_data[row])

    # insert artist record
    artist_df = df[["artist_id", "artist_name", "artist_location",
                    "artist_latitude", "artist_longitude"]]
    artist_df = artist_df.fillna(0)
    artist_data = artist_df.values.tolist()
    for row, t in enumerate(artist_data):
        cur.execute(artist_table_insert, artist_data[row])


def process_log_file(cur, filepath):
    """
    - Processes each log file

    - Filters the log file based on NextSong action

    - Converts the timestamp column to datetime

    - Loads data in the time dimension

    - Loads data in the users dimension

    - Loads data in the fact table songplays after
    performing a few transformations
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page.eq("NextSong")]

    # convert timestamp column to datetime
    df['t'] = pd.to_datetime(df['ts'], unit='ms')
    df['timestamp'] = df['ts'].astype('int')

    # insert time data records
    timestamp = df['ts'].astype('int')
    hour = df['t'].dt.hour
    day = df['t'].dt.day
    week_of_year = df['t'].dt.weekofyear
    month = df['t'].dt.month
    year = df['t'].dt.year
    weekday = df['t'].dt.weekday_name
    time_data = [timestamp, hour, day, week_of_year, month, year, weekday]
    column_labels = ['timestamp', 'hour', 'day', 'week_of_year',
                     'month', 'year', 'weekday']
    time_dict = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame.from_dict(time_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

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
        songplay_data = [row.timestamp, row.userId, row.level,
                         songid, artistid, row.sessionId,
                         row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - Processes the JSON files

    - Calculates the total number of files and print it

    - Iterates over each file to extract the data
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    - Establishes connection with the sparkify database and gets
    cursor to it.

    - Processes the song files.

    - Processes the log files.

    - Finally, closes the connection.
    """
    conn_str = "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()

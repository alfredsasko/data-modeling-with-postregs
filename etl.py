from datetime import datetime
import os
import glob
import uuid

import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, orient='index').transpose()

    # insert artist record
    artist_data = df[[
        'artist_id',
        'artist_name',
        'artist_location',
        'artist_latitude',
        'artist_longitude'
    ]].loc[0].to_list()

    cur.execute(artist_table_insert, artist_data)

    # insert song record
    song_data = df[[
        'song_id',
        'title',
        'artist_id',
        'year',
        'duration'
    ]].loc[0].to_list()

    cur.execute(song_table_insert, song_data)

def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True, orient='record')

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = (df.ts / 1000).apply(datetime.fromtimestamp)

    # insert time data records
    column_labels = [
        'start_time',
        'hour',
        'day',
        'week',
        'month',
        'year',
        'weekday'
    ]

    time_data = [
        t,
        t.dt.hour,
        t.dt.day,
        t.dt.week,
        t.dt.month,
        t.dt.year,
        t.dt.weekday
    ]

    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[[
        'userId',
        'firstName',
        'lastName',
        'gender',
        'level'
    ]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        results = cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            song_id, artist_id = results
        else:
            # if artist and song id not in tables ad them
            # song_id = uuid.uuid4().hex[:18].upper()
            # artist_id = uuid.uuid4().hex[:18].upper()
            song_id, artist_id = None, None

            # cur.execute(artist_table_insert, (
            #     artist_id,
            #     row.artist,
            #     None,
            #     None,
            #     None
            # ))
            #
            # cur.execute(song_table_insert, (
            #     song_id,
            #     row.song,
            #     artist_id,
            #     datetime.fromtimestamp(row.ts / 1000).year,
            #     row.length
            # ))

        # insert songplay record
        songplay_data = [
            row.userId,
            datetime.fromtimestamp(row.ts / 1000),
            row.level,
            song_id,
            artist_id,
            row.sessionId,
            row.location,
            row.userAgent
        ]

        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()

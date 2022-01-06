"""Main module to extract, transform and load data from JSON log and song files to Postgres snowflake schema"""

import os
import glob
from io import StringIO
import re
# import uuid

import psycopg2
from sql_queries import *


def process_song_file(cur, conn, filepath):
    """Extracts data from song file and inserts them to artists and songs tables.

    Args:
    -----
    cur : cursor object
        Any cursor object of any python database library

    conn: connection object
        Any connection object of any python database library

    filepath: string
        Absolute path of the JSON song file to extract data from
        """

    # Open song file
    with open(filepath, 'r') as file:

        with conn:

            # Copy song file to temporary table as jsonb type
            cur.execute(tmp_table_create)

            try:
                cur.copy_expert(song_copy_query, file)
            except psycopg2.Error:
                print(filepath)

            # Inserts records to artists table
            cur.execute(artist_table_insert)

            # Inserts records to song table
            cur.execute(song_table_insert)

            # Delete records from temporary table
            cur.execute(tmp_table_delete)


def process_log_file(cur, conn, filepath):
    """Extracts data from log file and inserts them to time and songplays tables.

    Args:
    -----
    cur : cursor object
        Any cursor object of any python database library

    conn: connection object
        Any connection object of any python database library

    filepath: string
        Absolute path of the JSON log file to extract data from
        """

    # Open log file
    with open(filepath, 'r') as file:
        # Read json file as text
        json_txt = file.read()

        # Fix quotes
        json_txt = re.sub(r'\\[\"\']', '', json_txt)

        with conn:

            # Copy log file to temporary table as jsonb type
            cur.execute(tmp_table_create)

            json_io = StringIO(json_txt)
            try:
                cur.copy_expert(log_copy_query, json_io)
            except psycopg2.Error:
                print(filepath)

            # Filter by "NextSong" action
            cur.execute(tmp_table_filter_next_song)

            # Inserts records to time table
            cur.execute(time_table_insert)

            # Inserts records to user table
            cur.execute(user_table_insert)

            # Inserts records to songplays table
            cur.execute(songplay_table_insert)

            # Delete records from temporary table
            cur.execute(tmp_table_delete)


def process_data(cur, conn, filepath, func):
    """Extracts json files, transforms and loads data to Database

    Args:
    -----
    cur : cursor object
        Any cursor object of any python database library

    conn: connection object
        Any connection object of any python database library

    filepath: string
        Absolute or relative path for root directory with json files

    func: python function with signature def func(cur: CursorObject, filepath: Path-Like object)
        Any python function transforming and loading data to database from filepath via cursor object

    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('\n{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, conn, datafile)
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()

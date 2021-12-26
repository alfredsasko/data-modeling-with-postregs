"""Module includes all sql statements for the ELT pipeline"""


# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songsplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id VARCHAR,
        first_name VARCHAR NOT NULL, 
        last_name VARCHAR NOT NULL, 
        gender VARCHAR, 
        level VARCHAR NOT NULL,
        PRIMARY KEY(user_id)
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR, 
        name VARCHAR NOT NULL, 
        location VARCHAR, 
        latitude DECIMAL, 
        longitude DECIMAL,
        PRIMARY KEY(artist_id)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP, 
        hour INT NOT NULL, 
        day INT NOT NULL, 
        week INT NOT NULL, 
        month INT NOT NULL, 
        year INT NOT NULL, 
        weekday INT NOT NULL,
        PRIMARY KEY(start_time)
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR(18),
        title VARCHAR,
        artist_id VARCHAR(18),
        year INTEGER,
        duration DECIMAL,
        PRIMARY KEY(song_id),
        FOREIGN KEY(artist_id) REFERENCES artists(artist_id)
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id SERIAL,
        user_id VARCHAR NOT NULL,
        start_time TIMESTAMP NOT NULL,
        level VARCHAR NOT NULL,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id VARCHAR NOT NULL,
        location VARCHAR,
        user_agent VARCHAR,
        PRIMARY KEY(songplay_id),
        FOREIGN KEY(user_id) REFERENCES users(user_id),
        FOREIGN KEY(start_time) REFERENCES time(start_time),
        FOREIGN KEY(song_id) REFERENCES songs(song_id),
        FOREIGN KEY(artist_id) REFERENCES artists(artist_id)
    );
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays (
        user_id,
        start_time,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level
    ) 
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (user_id) DO
            UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                gender = EXCLUDED.gender,
                level = EXCLUDED.level
""")

song_table_insert = ("""
    INSERT INTO songs(
        song_id,
        title,
        artist_id,
        year,
        duration
    ) 
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (song_id) DO
            UPDATE SET
                title = EXCLUDED.title,
                artist_id = EXCLUDED.artist_id,
                year = EXCLUDED.year,
                duration = EXCLUDED.duration
""")

artist_table_insert = ("""
    INSERT INTO artists(
        artist_id,
        name,
        location,
        latitude,
        longitude
    ) 
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (artist_id) DO 
            UPDATE SET
                name = EXCLUDED.name,
                location = EXCLUDED.location,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude 
""")

time_table_insert = ("""
    INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    ) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING 
""")

# FIND SONGS

song_select = ("""
    SELECT
        s.song_id AS song_id,
        s.artist_id AS artist_id
    FROM songs AS s
    JOIN artists AS a
        ON s.artist_id = a.artist_id
    WHERE s.title = %s AND a.name = %s AND s.duration = %s;
""")

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, time_table_create, song_table_create,
                        songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

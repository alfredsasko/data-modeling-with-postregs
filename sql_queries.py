"""Module includes all sql statements for the ELT pipeline"""


# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songsplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# DELETE TABLES

tmp_table_delete = "DELETE FROM tmp_table;"
tmp_table_filter_next_song = """
    DELETE
    FROM tmp_table
    WHERE json_record ->> 'page' != 'NextSong';
"""

# CREATE TABLES

tmp_table_create = """
    CREATE TEMPORARY TABLE IF NOT EXISTS tmp_table
    (
        json_record jsonb
    );
"""
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        user_id    VARCHAR,
        first_name VARCHAR NOT NULL,
        last_name  VARCHAR NOT NULL,
        gender     VARCHAR(1) CHECK (gender = 'F' OR gender = 'M' OR gender IS NULL),
        level      VARCHAR NOT NULL,
        PRIMARY KEY (user_id)
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
        artist_id VARCHAR,
        name      VARCHAR NOT NULL,
        location  VARCHAR,
        latitude  DECIMAL,
        longitude DECIMAL,
        PRIMARY KEY (artist_id)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        start_time TIMESTAMPTZ,
        hour       INT NOT NULL,
        day        INT NOT NULL,
        week       INT NOT NULL,
        month      INT NOT NULL,
        year       INT NOT NULL,
        weekday    INT NOT NULL,
        PRIMARY KEY (start_time)
    );  
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (
        song_id   VARCHAR(18),
        title     VARCHAR,
        artist_id VARCHAR(18),
        year      INTEGER,
        duration  DECIMAL,
        PRIMARY KEY (song_id),
        FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id SERIAL,
        user_id     VARCHAR   NOT NULL,
        start_time  TIMESTAMP NOT NULL,
        level       VARCHAR   NOT NULL,
        song_id     VARCHAR,
        artist_id   VARCHAR,
        session_id  VARCHAR   NOT NULL,
        location    VARCHAR,
        user_agent  VARCHAR,
        PRIMARY KEY (songplay_id),
        FOREIGN KEY (user_id) REFERENCES users (user_id),
        FOREIGN KEY (start_time) REFERENCES time (start_time),
        FOREIGN KEY (song_id) REFERENCES songs (song_id),
        FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
    );
""")

# INSERT RECORDS

songplay_table_insert = ("""
    WITH expanded_json_tb AS (
        SELECT json_record ->> 'userId'                                    AS user_id,
               TO_TIMESTAMP(LEFT(TRIM(json_record ->> 'ts'), -3)::INTEGER) AS start_time,
               json_record ->> 'level'                                     AS level,
               json_record ->> 'sessionId'                                 AS session_id,
               json_record ->> 'location'                                  AS location,
               json_record ->> 'userAgent'                                 AS user_agent,
               json_record ->> 'song'                                      AS title,
               json_record ->> 'artist'                                    AS artist,
               (json_record ->> 'length')::DECIMAL                         AS duration
        FROM tmp_table
    )
    INSERT
    INTO songplays (user_id,
                    start_time,
                    level,
                    song_id,
                    artist_id,
                    session_id,
                    location,
                    user_agent)
    SELECT js.user_id,
           js.start_time,
           js.level,
           s.song_id,
           a.artist_id,
           js.session_id,
           js.location,
           js.user_agent
    FROM songs AS s
             JOIN artists AS a
                  ON s.artist_id = a.artist_id
             RIGHT JOIN expanded_json_tb AS js
                        ON s.title = js.title AND a.name = js.artist AND s.duration = js.duration;
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT ON (json_record ->> 'userId') (json_record ->> 'userId')    AS user_id,
                                                  (json_record ->> 'firstName') AS first_name,
                                                  (json_record ->> 'lastName')  AS last_name,
                                                  (json_record ->> 'gender')    AS gender,
                                                  (json_record ->> 'level')     AS level
    FROM tmp_table
    WHERE ((json_record ->> 'userId') IS NOT NULL)
      AND ((json_record ->> 'firstName') IS NOT NULL)
      AND ((json_record ->> 'lastName') IS NOT NULL)
      AND ((json_record ->> 'level') IS NOT NULL)
    ORDER BY (json_record ->> 'userId'), (json_record ->> 'ts')::BIGINT DESC
    ON CONFLICT (user_id) DO UPDATE SET first_name = EXCLUDED.first_name,
                                        last_name  = EXCLUDED.last_name,
                                        gender     = EXCLUDED.gender,
                                        level      = EXCLUDED.level  
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT ON (json_record ->> 'song_id') (json_record ->> 'song_id')           AS song_id,
                                               json_record ->> 'title'               AS title,
                                               json_record ->> 'artist_id'           AS artist_id,
                                               (json_record ->> 'year')::INTEGER     AS year,
                                               (json_record ->> 'duration')::DECIMAL AS duration
FROM tmp_table
ORDER BY (json_record ->> 'song_id'), (json_record ->> 'title'), (json_record ->> 'artist_id')
ON CONFLICT (song_id) DO UPDATE SET title     = EXCLUDED.title,
                                    artist_id = EXCLUDED.artist_id,
                                    year      = EXCLUDED.year,
                                    duration  = EXCLUDED.duration;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT ON (json_record ->> 'artist_id') (json_record ->> 'artist_id')                 AS artist_id,
                                                 json_record ->> 'artist_name'                 AS name,
                                                 json_record ->> 'artist_location'             AS location,
                                                 (json_record ->> 'artist_latitude')::DECIMAL  AS latitude,
                                                 (json_record ->> 'artist_longitude')::DECIMAL AS longitude
FROM tmp_table
WHERE ((json_record ->> 'artist_id') IS NOT NULL)
  AND ((json_record ->> 'artist_name') IS NOT NULL)
ORDER BY (json_record ->> 'artist_id'), (json_record ->> 'artist_name'), (json_record ->> 'artist_location') DESC
ON CONFLICT (artist_id) DO UPDATE SET name      = EXCLUDED.name,
                                      location  = EXCLUDED.location,
                                      latitude  = EXCLUDED.latitude,
                                      longitude = EXCLUDED.longitude;
""")

time_table_insert = ("""
    WITH timestamp_tb AS (
        SELECT TO_TIMESTAMP(LEFT(TRIM(json_record ->> 'ts'), -3)::INTEGER) AS start_time
        FROM tmp_table
    )
    
    INSERT
    INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT start_time,
           EXTRACT(HOUR FROM start_time)   AS hour,
           EXTRACT(DAY FROM start_time)    AS day,
           EXTRACT(WEEK FROM start_time)   AS week,
           EXTRACT(MONTH FROM start_time)  AS month,
           EXTRACT(YEAR FROM start_time)   AS year,
           EXTRACT(ISODOW FROM start_time) AS weekday
    FROM timestamp_tb
    ON CONFLICT DO NOTHING;
""")

# COPY QUERIES

log_copy_query = ("""
    COPY tmp_table 
    FROM STDIN;
""")

song_copy_query = ("""
    COPY tmp_table 
    FROM STDIN;
""")

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, time_table_create, song_table_create,
                        songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

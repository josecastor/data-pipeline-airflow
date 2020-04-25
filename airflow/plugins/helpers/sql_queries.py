class SqlQueries:
 
    table_drop = ("""
        SET search_path TO sparkify;
        DROP TABLE IF EXISTS staging_events;
        DROP TABLE IF EXISTS staging_songs;
        DROP TABLE IF EXISTS songplays;
        DROP TABLE IF EXISTS users;
        DROP TABLE IF EXISTS songs;
        DROP TABLE IF EXISTS artists;
        DROP TABLE IF EXISTS time;
    """)
    
    schema_create = ("""
        CREATE SCHEMA IF NOT EXISTS sparkify;
    """)

    staging_events_table_create = ("""
        SET search_path TO sparkify;
        CREATE TABLE IF NOT EXISTS staging_events (
            artist          VARCHAR,
            auth            VARCHAR,
            firstName       VARCHAR,
            gender          VARCHAR,
            itemInSession   INTEGER,        
            lastName        VARCHAR,
            length          FLOAT,
            level           VARCHAR,
            location        VARCHAR,
            method          VARCHAR,
            page            VARCHAR,
            registration    VARCHAR,
            sessionId       INTEGER,
            song            VARCHAR,
            status          INTEGER,
            ts              TIMESTAMP,
            userAgent       VARCHAR,
            userId          FLOAT
        );
    """)

    staging_songs_table_create = ("""
        SET search_path TO sparkify;
        CREATE TABLE staging_songs (
            num_songs int4,
            artist_id varchar(256),
            artist_name varchar(256),
            artist_latitude numeric(18,0),
            artist_longitude numeric(18,0),
            artist_location varchar(256),
            song_id varchar(256),
            title varchar(256),
            duration numeric(18,0),
            "year" int4
        );
    """)

    songplay_table_create = ("""
        SET search_path TO sparkify;
        CREATE TABLE IF NOT EXISTS songplays (
            songplay_id INTEGER IDENTITY(1,1) DISTKEY, 
            start_time  TIMESTAMP NOT NULL, 
            user_id     INTEGER NOT NULL, 
            level       VARCHAR NOT NULL, 
            song_id     VARCHAR NOT NULL SORTKEY, 
            artist_id   VARCHAR NOT NULL, 
            session_id  INTEGER NOT NULL, 
            location    VARCHAR, 
            user_agent  VARCHAR
        );
    """)

    user_table_create = ("""
        SET search_path TO sparkify;
        CREATE TABLE IF NOT EXISTS users (
            user_id     INTEGER NOT NULL DISTKEY, 
            first_name  VARCHAR NOT NULL, 
            last_name   VARCHAR NOT NULL, 
            gender      VARCHAR NOT NULL, 
            level       VARCHAR NOT NULL
        );
    """)

    song_table_create = ("""
        SET search_path TO sparkify;
        CREATE TABLE IF NOT EXISTS songs (
            song_id     VARCHAR DISTKEY, 
            title       VARCHAR NOT NULL, 
            artist_id   VARCHAR NOT NULL SORTKEY, 
            year        INTEGER, 
            duration    FLOAT
        );
    """)

    artist_table_create = ("""
        SET search_path TO sparkify;
        CREATE TABLE IF NOT EXISTS artists (
            artist_id   VARCHAR DISTKEY, 
            name        VARCHAR NOT NULL SORTKEY, 
            location    VARCHAR, 
            latitude    FLOAT, 
            longitude   FLOAT
        );
    """)

    time_table_create = ("""
        SET search_path TO sparkify;
        CREATE TABLE IF NOT EXISTS time (
            start_time  TIMESTAMP DISTKEY, 
            hour        INTEGER, 
            day         INTEGER, 
            week        INTEGER, 
            month       INTEGER, 
            year        INTEGER, 
            weekday     INTEGER
        );
    """)
    
    songplay_table_insert = ("""
        INSERT INTO songplays 
            (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
        SELECT DISTINCT 
            TO_TIMESTAMP(TO_CHAR(s_e.ts, '9999-99-99 99:99:99'), 'YYYY-MM-DD HH24:MI:SS') AS start_time,
            s_e.userId      AS user_id,
            s_e.level       AS level,
            s_s.song_id     AS song_id,
            s_s.artist_id   AS artist_id,
            s_e.sessionId   AS session_id,
            s_e.location    AS location,
            s_e.userAgent   AS user_agent
        FROM staging_events s_e 
        JOIN staging_songs s_s ON s_s.title = s_e.song AND s_s.artist_name = s_e.artist 
        WHERE s_e.page = 'NextSong';
    """)

    user_table_insert = ("""
        INSERT INTO users 
            (user_id, first_name, last_name, gender, level) 
        SELECT DISTINCT 
            userId      AS user_id,
            firstName   AS first_name,
            lastName    AS last_name,
            gender      AS gender,
            level       AS level
        FROM staging_events
        WHERE userId IS NOT NULL;
    """)

    song_table_insert = ("""
        INSERT INTO songs 
            (song_id, title, artist_id, year, duration) 
        SELECT DISTINCT 
            song_id     AS song_id,
            title       AS title,
            artist_id   AS artist_id,
            year        AS year,
            duration    AS duration
        FROM staging_songs
        WHERE song_id IS NOT NULL;
    """)

    artist_table_insert = ("""
        INSERT INTO artists 
            (artist_id, name, location, latitude, longitude) 
        SELECT DISTINCT 
            artist_id           AS artist_id,
            artist_name         AS name,
            artist_location     AS location,
            artist_latitude     AS latitude,
            artist_longitude    AS longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL;
    """)

    time_table_insert = ("""
        INSERT INTO time 
            (start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT ts,
            EXTRACT(hour from ts),
            EXTRACT(day from ts),
            EXTRACT(week from ts),
            EXTRACT(month from ts),
            EXTRACT(year from ts),
            EXTRACT(weekday from ts)
        FROM staging_events
        WHERE ts IS NOT NULL;
    """)
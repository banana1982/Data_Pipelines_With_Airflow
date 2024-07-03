class SqlStatementQueries:
    CREATE_STAGING_EVENTS_TABLE_SQL = ("""
        CREATE TABLE IF NOT EXISTS staging_events (
            artist VARCHAR,
            auth VARCHAR,
            firstName VARCHAR,
            gender VARCHAR(1),
            itemInSession INT,
            lastName VARCHAR,
            length NUMERIC,
            level VARCHAR,
            location TEXT,
            method VARCHAR,
            page VARCHAR,
            registration NUMERIC,
            sessionId INT,
            song VARCHAR,
            status INT,
            ts TIMESTAMP,
            userAgent TEXT,
            userId NUMERIC
        )
    """)

    CREATE_STAGING_SONGS_TABLE_SQL = ("""
        CREATE TABLE IF NOT EXISTS staging_songs (
            num_songs INT,
            artist_id VARCHAR,
            artist_latitude FLOAT,
            artist_location VARCHAR(max),
            artist_longitude FLOAT,
            artist_name VARCHAR(max),
            song_id VARCHAR,
            title VARCHAR(max),
            duration NUMERIC,
            year SMALLINT
        )
    """)

    CREATE_STAGING_SONGPLAYS_TABLE_SQL = """
        CREATE TABLE IF NOT EXISTS song_plays (
            songplay_id INT IDENTITY(0,1) PRIMARY KEY,
            start_time TIMESTAMP NOT NULL,
            user_id INT NOT NULL,
            level VARCHAR NOT NULL,
            song_id VARCHAR NOT NULL,
            artist_id TEXT NOT NULL,
            session_id BIGINT NOT NULL,
            location VARCHAR NOT NULL,
            user_agent VARCHAR NOT NULL
        )
    """

    USER_TABLE_CREATE = ("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INT PRIMARY KEY,
            first_name VARCHAR NOT NULL,
            last_name VARCHAR NOT NULL,
            gender VARCHAR(1) NOT NULL,
            level VARCHAR NOT NULL
        )
    """)

    SONG_TABLE_CREATE = ("""
        CREATE TABLE IF NOT EXISTS songs (
            song_id VARCHAR PRIMARY KEY,
            title VARCHAR(max) NOT NULL,
            artist_id VARCHAR NOT NULL,
            year SMALLINT NOT NULL,
            duration REAL NOT NULL
        )
    """)

    ARTIST_TABLE_CREATE = ("""
        CREATE TABLE IF NOT EXISTS artists (
            artist_id VARCHAR PRIMARY KEY,
            name VARCHAR(max) NOT NULL,
            location VARCHAR(max),
            latitude FLOAT,
            longitude FLOAT
        )
    """)

    TIME_TABLE_CREATE = ("""
        CREATE TABLE IF NOT EXISTS times (
            start_time TIMESTAMP PRIMARY KEY,
            hour SMALLINT NOT NULL,
            day SMALLINT NOT NULL,
            week SMALLINT NOT NULL,
            month SMALLINT NOT NULL,
            year SMALLINT NOT NULL,
            weekday SMALLINT NOT NULL
        )
    """)
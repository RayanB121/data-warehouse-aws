import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_event"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_song"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = """
    CREATE TABLE IF NOT EXISTS staging_events(
        event_id BIGINT IDENTITY(0,1) PRIMARY KEY,
        artist TEXT,
        auth TEXT,
        first_name TEXT,
        gender TEXT,
        item_in_session INTEGER,
        last_name TEXT,
        length DOUBLE PRECISION,
        level TEXT,
        location TEXT,
        method TEXT,
        page TEXT,
        registration TEXT,    
        session_id BIGINT,
        song_title TEXT,
        status INTEGER,
        ts TEXT,
        user_agent TEXT,
        user_id TEXT
    )
"""

staging_songs_table_create = """
    CREATE TABLE IF NOT EXISTS staging_songs(
        song_id TEXT PRIMARY KEY,
        num_songs INTEGER,
        artist_id TEXT,
        artist_latitude DOUBLE PRECISION,
        artist_longitude DOUBLE PRECISION,
        artist_location TEXT,
        artist_name TEXT,
        title TEXT,
        duration DOUBLE PRECISION,
        year INTEGER
    )
"""

songplay_table_create = """
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP,
        user_id TEXT,
        level TEXT,
        song_id TEXT,
        artist_id TEXT,
        session_id TEXT,
        location TEXT,
        user_agent TEXT
    )
"""

user_table_create = """
    CREATE TABLE IF NOT EXISTS users(
        user_id TEXT PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender CHAR(1),
        level TEXT
    )
"""

song_table_create = """
    CREATE TABLE IF NOT EXISTS songs(
        song_id TEXT PRIMARY KEY,
        title TEXT,
        artist_id TEXT,
        year SMALLINT,
        duration FLOAT4
    )
"""

artist_table_create = """
    CREATE TABLE IF NOT EXISTS artists(
        artist_id TEXT PRIMARY KEY,
        name TEXT,
        location TEXT,
        latitude FLOAT4,
        longitude FLOAT4
    )
"""

time_table_create = """
    CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP PRIMARY KEY,
        hour SMALLINT,
        day SMALLINT,
        week SMALLINT,
        month SMALLINT,
        year SMALLINT,
        weekday SMALLINT
    )
"""
# STAGING TABLES
staging_events_copy = ("""
                       copy staging_events from {} 
                       credentials 'aws_iam_role={}'
                       json {} 
                       region 'us-west-2'; 
""").format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE',"ARN"),config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
                       copy staging_songs from {} 
                       credentials 'aws_iam_role={}' 
                       JSON 'auto'
                       region 'us-west-2'; 
""").format(config.get('S3','SONG_DATA'),config.get('IAM_ROLE',"ARN"))

# FINAL TABLES

songplay_table_insert = ("""
                         INSERT INTO songplays(songplay_id, 
                                  start_time  ,
                                  user_id ,
                                  level ,
                                  song_id ,
                                  artist_id ,
                                  session_id ,
                                  location ,
                                  user_agent )
                         SELECT 
                         DISTINCT TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second') as start_time,
                         e.user_id as user_id,
                         e.level as level,
                         s.song_id as song_id,
                         s.artist_id as artist_id,
                         e.session_id as session_id,
                         e.location as location,
                         e.user_agent as user_agent
                         
                         from staging_events e 
                         left join 
                         staging_songs s 
                         on 
                         e.song_title=s.title
                         AND
                         e.artist = s.artist_name
                         WHERE 
                         e.page = 'NextSong'
      
""")

user_table_insert = ("""
                     INSERT INTO users(
                     user_id,
                     last_name,
                     gender,
                     level
                     )
                     SELECT 
                     DISTINCT user_id,
                     last_name,
                     gender,
                     level
                     FROM
                     staging_events
                     WHERE user_id != ''
                     
""")

song_table_insert = ("""
                     INSERT INTO songs(song_id,
                     title,
                     artist_id,
                     year,
                     duration)
                     
                     SELECT 
                     DISTINCT song_id ,
                     title,
                     artist_id,
                     year,
                     duration
                     
                     FROM
                     staging_songs
                     WHERE song_id IS NOT NULL
                     
                     
""")

artist_table_insert = ("""
                       INSERT INTO artists(
                       artist_id,
                       name,
                       location,
                       latitude,
                       longitude 
                       )
                       SELECT 
                       DISTINCT artist_id,
                       artist_name,
                       artist_location,
                       artist_latitude,
                       artist_longitude 
                       FROM staging_songs
""")

time_table_insert = ("""
                     INSERT INTO time(start_time,hour,day,week,month,year,weekday)
                     SELECT
                     DISTINCT start_time,
                     extract(hour from start_time),
                     extract(day from start_time),
                     extract(week from start_time),
                     extract(month from start_time),
                     extract(year from start_time),
                     extract(weekday from start_time)
                     from
                     (SELECT
                     TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time 
                     FROM staging_events )

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

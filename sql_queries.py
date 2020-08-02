import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE STAGING TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS stg_events
(
    artist          VARCHAR (255),
    auth            VARCHAR (255),
    firstName       VARCHAR (255),
    gender          CHAR(1),
    itemInSession   INTEGER,
    lastName        VARCHAR (255),
    length          DECIMAL,
    level           VARCHAR (20),
    location        VARCHAR (512),
    method          VARCHAR (10),
    page            VARCHAR (255),
    registration    DECIMAL,
    sessionId       INTEGER,
    song            VARCHAR (255),
    status          INTEGER,
    ts              DECIMAL,
    userAgent       VARCHAR,
    userId          INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS stg_songs
(
    num_songs       INTEGER,
    artist_id       VARCHAR (255),
    artist_latitude DECIMAL,
    artist_longitude DECIMAL,
    artist_location VARCHAR (512),
    artist_name     VARCHAR (1024),
    song_id         VARCHAR (255),
    title           VARCHAR (1024),
    duration        DECIMAL,
    year            INT
);
""")

# CREATE DWH TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
    id              INTEGER         IDENTITY(0, 1) NOT NULL PRIMARY KEY,
    start_time      TIMESTAMP       NOT NULL SORTKEY,
    user_id         INTEGER         NOT NULL REFERENCES users(id),
    level           VARCHAR (255),
    song_id         VARCHAR (255)   REFERENCES songs(id),
    artist_id       VARCHAR (255)   REFERENCES artists(id) DISTKEY,
    session_id      INTEGER         NOT NULL,
    location        VARCHAR (255),
    user_agent      VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    id              INTEGER         NOT NULL PRIMARY KEY,
    first_name      VARCHAR (255),
    last_name       VARCHAR (255)   SORTKEY,
    gender          CHAR(1),
    level           VARCHAR (20)
)
DISTSTYLE AUTO;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
    id              VARCHAR (255)   NOT NULL PRIMARY KEY SORTKEY,
    title           VARCHAR (1024)   NOT NULL,
    artist_id       VARCHAR         NOT NULL REFERENCES artists(id),
    year            INT             NOT NULL,
    duration        DECIMAL         NOT NULL
)
DISTSTYLE AUTO;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
    id              VARCHAR (255)    NOT NULL PRIMARY KEY SORTKEY,
    name            VARCHAR (1024)   NOT NULL,
    location        VARCHAR (512),
    latitude        DECIMAL,
    longitude       DECIMAL
)
DISTSTYLE AUTO;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    start_time      TIMESTAMP        NOT NULL UNIQUE SORTKEY,
    hour            SMALLINT         NOT NULL,
    day             SMALLINT         NOT NULL,
    week            SMALLINT         NOT NULL,
    month           SMALLINT         NOT NULL,
    year            INT              NOT NULL,
    weekday         SMALLINT         NOT NULL
)
DISTSTYLE AUTO;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY stg_events
FROM {}
CREDENTIALS 'aws_iam_role={}'
FORMAT AS json {}
REGION 'us-west-2';
""").format(config.get("S3", "LOG_DATA"),
            config.get("IAM_ROLE", "ARN"),
            config.get("S3", "LOG_JSONPATH")
            )

staging_songs_copy = ("""
COPY stg_songs
FROM {}
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON 'auto'
REGION 'us-west-2';
""").format(config.get("S3", "SONG_DATA"),
            config.get("IAM_ROLE", "ARN")
            )

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays
    (start_time,
     user_id,
     level,
     song_id,
     artist_id,
     session_id,
     location,
     user_agent)
SELECT
    timestamp 'epoch' + e.ts * interval '1 second' AS start_time,
    e.userId as user_id,
    e.level,
    s.id,
    s.artist_id,
    e.sessionId as session_id,
    e.location,
    e.userAgent as user_agent
FROM stg_events e LEFT JOIN songs s ON (e.song = s.title AND e.length = s.duration)
WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users
SELECT DISTINCT
    userId as id,
    firstName as first_name,
    lastName as last_name,
    gender,
    level
FROM stg_events m
WHERE page = 'NextSong' AND
userId IS NOT NULL AND
ts = (SELECT MAX(ts)
      FROM stg_events s
      WHERE s.userId = m.userId
    )
""")

song_table_insert = ("""
INSERT INTO songs
SELECT DISTINCT
    song_id as id,
    title,
    artist_id,
    year,
    duration
FROM stg_songs
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT DISTINCT
    artist_id as id,
    artist_name as name,
    artist_location as location,
    artist_latitude as latitude,
    artist_longitude as longitude
FROM stg_songs
""")

time_table_insert = ("""
INSERT INTO time
SELECT DISTINCT
    ts.start_time,
    DATE_PART(hour, ts.start_time) as hour,
    DATE_PART(day, ts.start_time) as day,
    DATE_PART(week, ts.start_time) as week,
    DATE_PART(month, ts.start_time) as month,
    DATE_PART(year, ts.start_time) as year,
    date_part(dow, ts.start_time) as weekday
FROM (SELECT DISTINCT
            timestamp 'epoch' + ts * interval '1 second' AS start_time
      FROM stg_events
      WHERE page = 'NextSong') ts
""")

# DATA QUALITY QUERY

data_qual = """
SELECT COUNT(*)
FROM songplays
"""

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, time_table_create, artist_table_create, song_table_create, user_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [time_table_insert, artist_table_insert, song_table_insert, user_table_insert, songplay_table_insert]

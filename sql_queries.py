import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_log_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_song_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE "staging_log_table" (
    "artist" varchar,
    "auth" varchar,
    "firstName" varchar,
    "gender" varchar,
    "iteminSession" varchar,
    "lastName" varchar,
    "length" varchar,
    "level" varchar,
    "location" varchar,
    "method" varchar,
    "page" varchar,
    "registration" bigint,
    "sessionid" integer,
    "song" varchar,
    "status" bigint,
    "ts" timestamp,
    "userAgent" varchar,
    "userId" integer
);
""")

staging_songs_table_create = ("""

    CREATE TABLE IF NOT EXISTS "staging_song_table" (
    "num_songs" integer,
    "artist_id" varchar,
    "artist_latitude" float,
    "artist_longitude" float,
    "artist_location" varchar(max),
    "artist_name" varchar(max),
    "song_id" varchar,
    "title" varchar,
    "duration" float,
    "year" integer
);
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
    songplay_id bigint identity(0,1),
    start_time timestamp not null,
    user_id varchar not null,
    level varchar not null,
    song_id varchar not null,
    artist_id varchar not null,
    session_id varchar not null,
    location varchar not null,
    user_agent varchar(max) not null,
    primary key(songplay_id)
)
    DISTKEY (songplay_id)
    SORTKEY (start_time,session_id); 
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
    user_id varchar,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar,
    primary key(user_id)
)
diststyle all;
ALTER TABLE songplays ADD CONSTRAINT FK_1 FOREIGN KEY (user_id) REFERENCES users(user_id);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
    song_id varchar,
    title varchar(max) ,
    artist_id varchar,
    year int,
    duration float,
    primary key(song_id)
)
diststyle all;
ALTER TABLE songplays ADD CONSTRAINT FK_2 FOREIGN KEY (song_id) REFERENCES songs(song_id)
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar,
    name varchar(max),
    location varchar(max),
    latitude float,
    longitude float,
    primary key(artist_id)
)
diststyle all;
ALTER TABLE songplays ADD CONSTRAINT FK_3 FOREIGN KEY (artist_id) REFERENCES artists(artist_id);
ALTER TABLE songs ADD CONSTRAINT FK_1 FOREIGN KEY (artist_id) REFERENCES artists(artist_id);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
    start_time timestamp,
    hour integer,
    day integer,
    week integer,
    month integer, 
    year integer, 
    weekday char,
    primary key(start_time)
);
ALTER TABLE songplays ADD CONSTRAINT FK_4 FOREIGN KEY (start_time) REFERENCES time(start_time);
""")

# STAGING TABLES

staging_events_copy = (""" 
    copy staging_song_table from 's3://udacity-dend/song-data/A'
    iam_role {}
    json 'auto'
    COMPUPDATE OFF
    region 'us-west-2';
""").format(config['IAM_ROLE']['ARN'])

staging_songs_copy = ("""
    copy staging_log_table from 's3://udacity-dend/log-data'
    iam_role {}
    json 's3://udacity-dend/log_json_path.json'
    region 'us-west-2'
      ;
""").format(config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    insert into songplays(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
    
    (Select TIMESTAMP 'epoch' + cast(ts as bigint)/1000 * interval '1second',
    userid,level,song_id,artists.artist_id,sessionid,staging_log_table.location,useragent
    From staging_log_table inner join songs 
    on staging_log_table.song = songs.title And staging_log_table.length = songs.duration
    inner join artists on staging_log_table.artist = artists.name
    where page = 'NextSong')
""")

user_table_insert = ("""
     insert into users
     (Select distinct userID,firstName,lastName,gender,level 
      from staging_log_table
      where userID is not null)
""")

song_table_insert = ("""
     insert into songs
     (Select distinct song_id,title,artist_id,year,cast(duration as float) from staging_song_table)
""")

artist_table_insert = ("""
     insert into artists
     (Select distinct artist_id,artist_name,artist_location,cast(artist_latitude as float),cast(artist_longitude as float) 
     from staging_song_table)
""")

time_table_insert = ("""
     insert into time
     (select distinct start_time as start_time, 
     extract(h from start_time) as hour,
     extract(d from start_time) as day,
     extract(w from start_time) as week,
     extract(mon from start_time) as month,
     extract(y from start_time) as year,
     to_char (start_time,'D') as weekday
     from songplays)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

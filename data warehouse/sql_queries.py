import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = "CREATE TABLE IF NOT EXISTS staging_events\
                                (artist varchar(200) default 'artist',\
                                auth varchar(20), firstName varchar(100), \
                                gender varchar(1), itemInSession int,\
                                lastName varchar(100), length numeric \
                                default 0, level varchar(10), \
                                location varchar(200), method varchar(10), \
                                page varchar(100), registration numeric, \
                                sessionId int, song varchar(200), \
                                status int, ts bigint, \
                                userAgent varchar(200),userId int)"

staging_songs_table_create = "CREATE TABLE IF NOT EXISTS staging_songs\
                                (artist_id varchar(200) primary key, \
                                artist_latitude numeric, \
                                artist_longitude numeric, \
                                artist_location varchar(200), \
                                artist_name varchar(1000), \
                                song_id varchar(200), title varchar(1000), \
                                duration numeric, year int)"

songplay_table_create = "CREATE TABLE IF NOT EXISTS songplays\
                        (songplay_id int GENERATED ALWAYS AS IDENTITY,\
                        start_time bigint, userId int, level varchar(10), \
                        songid varchar(200), \
                        artistid varchar(200), \
                        sessionId int, location varchar(200), \
                        userAgent varchar(200))"

user_table_create = "CREATE TABLE IF NOT EXISTS users(userId int ,\
                    firstName varchar(100), lastName varchar(100),\
                    gender varchar(1), level varchar(10))"

song_table_create = "CREATE TABLE IF NOT EXISTS songs(song_id varchar(200) PRIMARY KEY,\
                    title varchar(1000), artist_id varchar(200),\
                    year int, duration numeric)"

artist_table_create = "CREATE TABLE IF NOT EXISTS artists(artist_id varchar(200) PRIMARY KEY,\
                        artist_name varchar(1000), \
                        artist_location varchar(200),\
                        artist_latitude numeric, artist_longitude numeric)"

time_table_create = "CREATE TABLE IF NOT EXISTS time(start_time bigint PRIMARY KEY,\
                    hour int, day int, week int, month int,\
                    year int, weekday varchar(10))"

# STAGING TABLES

staging_events_copy = """copy staging_events from 's3://udacity-dend/log_data'
                            iam_role 'arn:aws:iam::082876160679:role/dwhRole'
                            NULL As ''
                            format as json 'auto';
""".format()

staging_songs_copy = """copy staging_songs from 's3://udacity-dend/song_data/'
                            iam_role 'arn:aws:iam::082876160679:role/dwhRole'
                            NULL As ''
                            format as json 'auto';
""".format()

# FINAL TABLES

songplay_table_insert = "INSERT INTO songplays(start_time,userId,\
                         level,songid,artistid,sessionId,location,userAgent) \
                         SELECT e.ts,e.userId,e.level,s.song_id,s.artist_id,\
                         e.sessionId,e.location,e.userAgent from \
                         staging_events e inner join staging_songs s\
                         on e.artist = s.artist_name "

user_table_insert = "INSERT INTO users(userId,firstName,lastName,gender,level) \
                     SELECT userId, firstName, lastName, \
                     gender, level from staging_events"


song_table_insert = "INSERT INTO songs(song_id,title,artist_id,year,duration) \
                    SELECT song_id, title, artist_id, year,\
                    duration from staging_songs"

artist_table_insert = "INSERT INTO artists(artist_id,artist_name,artist_location,\
                        artist_latitude,artist_longitude) \
                        SELECT artist_id, artist_name, artist_location, \
                        artist_latitude, artist_longitude from staging_songs"


time_table_insert = "INSERT INTO time(start_time,hour,day,week,month,year,weekday) \
                        SELECT ts, \
                        date_part('hour', TIMESTAMP 'epoch'\
                        + ts/1000 * INTERVAL '1 second'), \
                        date_part('day', TIMESTAMP 'epoch'\
                        + ts/1000 * INTERVAL '1 second'), \
                        date_part('week', TIMESTAMP 'epoch' \
                        + ts/1000 * INTERVAL '1 second'), \
                        date_part('month', TIMESTAMP 'epoch'\
                        + ts/1000 * INTERVAL '1 second'), \
                        date_part('year', TIMESTAMP 'epoch'\
                        + ts/1000 * INTERVAL '1 second'), \
                        date_part('weekday', TIMESTAMP 'epoch'\
                        + ts/1000 * INTERVAL '1 second') \
                        from staging_events"

# QUERY LISTS

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create,
                        artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop,
                      artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert,
                        time_table_insert]

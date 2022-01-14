# DROP TABLES
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

drop_table_queries = [songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]

# CREATE TABLES
songplay_table_create = "CREATE TABLE IF NOT EXISTS songplays(songplay_id SERIAL PRIMARY KEY, \
                    start_time bigint NOT NULL, userId int NOT NULL, \
                    level varchar, songid varchar, \
                    artistid varchar, sessionId int, \
                    location varchar, userAgent varchar, \
                    CONSTRAINT fk_songs FOREIGN KEY(songid) \
                    REFERENCES songs(song_id), \
                    CONSTRAINT fk_users FOREIGN KEY(userId) \
                    REFERENCES users(userId), \
                    CONSTRAINT fk_artists FOREIGN KEY(artistid) \
                    REFERENCES artists(artist_id), \
                    CONSTRAINT fk_time FOREIGN KEY(start_time) \
                    REFERENCES time(start_time))"

user_table_create = "CREATE TABLE IF NOT EXISTS users(userId int PRIMARY KEY, \
                    firstName varchar, lastName varchar, \
                    gender varchar, level varchar)"


song_table_create = "CREATE TABLE IF NOT EXISTS songs(song_id varchar PRIMARY KEY, \
                    title varchar, artist_id varchar, \
                    year int, duration numeric)"

artist_table_create = "CREATE TABLE IF NOT EXISTS artists(artist_id varchar PRIMARY KEY, \
                    artist_name varchar, artist_location varchar, \
                    artist_latitude numeric, artist_longitude numeric)"

time_table_create = "CREATE TABLE IF NOT EXISTS time(start_time bigint PRIMARY KEY, \
                    hour int, day int, week int, month int, \
                    year int, weekday varchar)"

# INSERT RECORDS
songplay_table_insert = "INSERT INTO songplays(start_time,userId, \
                    level,songid,artistid,sessionID,location,userAgent) \
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ;"

user_table_insert = "INSERT INTO users(userId,firstName, \
                    lastName,gender,level) \
                    VALUES(%s,%s,%s,%s,%s) ON CONFLICT(userId) \
                    DO UPDATE SET level = EXCLUDED.level"

song_table_insert = "INSERT INTO songs(song_id,title,artist_id,year,duration) \
                    VALUES(%s,%s,%s,%s,%s) ON CONFLICT(song_id) DO NOTHING;"

artist_table_insert = "INSERT INTO artists(artist_id,artist_name,artist_location,\
                    artist_latitude,artist_longitude) \
                    VALUES(%s,%s,%s,%s,%s) ON CONFLICT(artist_id) DO NOTHING;"


time_table_insert = "INSERT INTO time(start_time,hour,day,week,month,year,weekday) \
                    VALUES(%s,%s,%s,%s,%s,%s,%s) \
                    ON CONFLICT(start_time) DO NOTHING"

# FIND SONGS
song_select = "SELECT s.song_id, a.artist_id FROM songs s INNER JOIN artists a \
                ON s.artist_id = a.artist_id WHERE s.title = %s AND \
                a.artist_name = %s AND s.duration = %s"

# QUERY LISTS
create_table_queries = [user_table_create, song_table_create,
                        artist_table_create, time_table_create,
                        songplay_table_create]

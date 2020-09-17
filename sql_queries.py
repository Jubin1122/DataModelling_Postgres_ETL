# DROP TABLES

songplay_table_drop = "Drop table if exists songplays;"
user_table_drop = "Drop table if exists users;"
song_table_drop = "Drop table if exists songs;"
artist_table_drop = "Drop table if exists artists;"
time_table_drop = "Drop table if exists time;"

# CREATE TABLES

songplay_table_create = ("""
create table if not exists songplays(
    songplay_id serial primary key, 
    start_time timestamp references time(start_time), 
    user_id integer not null references users(user_id),  
    level varchar(25), 
    song_id varchar(25) references songs(song_id),
    artist_id varchar(25) references artists(artist_id), 
    session_id integer not null, 
    location varchar(60), 
    user_agent varchar(150)
    )
""")

user_table_create = ("""
create table if not exists users(
    user_id integer primary key, 
    first_name varchar(60) not null, 
    last_name varchar(60) not null, 
    gender char(1), 
    level varchar(20) not null 
    )
""")

song_table_create = ("""
create table if not exists songs(
    song_id varchar(25) primary key, 
    title varchar(120) not null,
    artist_id varchar(30) not null references artists(artist_id), 
    year integer not null, 
    duration numeric(14,5) not null
    )
""")

artist_table_create = ("""
create table if not exists artists(
    artist_id varchar(25) primary key, 
    name varchar(120) not null, 
    location varchar(120),
    latitude numeric, 
    longitude numeric
    )
""")

time_table_create = ("""
create table if not exists time(
    start_time timestamp not null primary key, 
    hour numeric not null, 
    day numeric not null, 
    week numeric not null,
    month numeric not null,
    year numeric not null,
    weekday numeric not null
    )
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays
(
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
    INSERT INTO users(
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level
)
VALUES (%s, %s, %s, %s, %s) 
on conflict (user_id) 
do update set level = excluded.level
;
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
on conflict (song_id) do nothing;
""")

artist_table_insert = ("""
INSERT INTO artists
(
    artist_id, 
    name, 
    location, 
    latitude, 
    longitude
)
VALUES (%s, %s, %s, %s, %s) 
on conflict (artist_id) do nothing;
""")


time_table_insert = ("""
INSERT INTO time(
    start_time, 
    hour, 
    day, 
    week, 
    month, 
    year, 
    weekday
)
VALUES (%s, %s, %s, %s, %s, %s, %s) 
on conflict (start_time) do nothing;
""")

# FIND SONGS

song_select = ("""
SELECT 
    s.song_id as song_id, 
    a.artist_id as artist_id 
FROM 
    songs as s 
    JOIN artists as a on s.artist_id = a.artist_id
WHERE s.title = %s
AND a.name = %s
AND s.duration = %s
""")

# QUERY LISTS

create_table_queries = [time_table_create,user_table_create, artist_table_create, song_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
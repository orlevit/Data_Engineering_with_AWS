### Introduction
As a music streaming startup, Sparkify has experienced significant growth in its user base and song database. They are now looking to migrate their operations and data to the cloud. Currently, their data is stored in S3, with user activity logs in JSON format in one directory and song metadata in JSON format in another directory.

We develop an ETL pipeline that extracts data from S3, stages it in Redshift, and transforms it into a set of dimensional tables. This will enable the analytics team to gain deeper insights into the listening habits of Sparkify’s users.

### The tables

#### Fact Table
- songplays - records in event data associated with song plays i.e. records with page NextSong (Columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

#### Dimension Tables
- users - users in the app (Columns: user_id, first_name, last_name, gender, level).
- songs - songs in music database (Columns: song_id, title, artist_id, year, duration).
- artists - artists in music database (Columns: artist_id, name, location, latitude, longitude).
- time - timestamps of records in songplays broken down into specific units (Columns: start_time, hour, day, week, - month, year, weekday).

### The code

**etl.py** - Load data from S3 to staging tables on Redshift and then load the data from the staging tables to analytics tables.

**create_table.py** - will drop and recreate the fact and dimension tables for the star schema in Redshift.
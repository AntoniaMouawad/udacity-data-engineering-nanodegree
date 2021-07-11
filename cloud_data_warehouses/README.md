# Sparkify Data Warehouse
## Project Description
Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3 (in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app).

In this project we build an ETL pipeline that:
- Extracts their data from S3 and stages them in Redshift
- Transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

## Datasets
**Song Dataset**
A subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. 
It can be found [here](s3://udacity-dend/song_data)

**Log Dataset**
consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings. The log files in the dataset you'll be working with are partitioned by year and month.
It can be found [here](s3://udacity-dend/log_data)

## Schema for Song Play Analysis
The project follows a star schema:

**Fact Table**
`songplays` - records in event data associated with song plays i.e. records with page NextSong
```songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent```

**Dimension Tables**
`users` - users in the app
```user_id, first_name, last_name, gender, level```

`songs` - songs in music database
```song_id, title, artist_id, year, duration```

`artists` - artists in music database
```artist_id, name, location, lattitude, longitude```

`time` - timestamps of records in songplays broken down into specific units
```start_time, hour, day, week, month, year, weekday```

## Config
- Start by Creating Redshift Cluster using the AWS python SDK (like in L3 Exercise 2 - IaC)
- Fill up `dwh.cfg` with the corresponding values

## Run
Creating the tables:
`python create_tables.py`

Then:
`python etl.py`
## Analytics Requirements
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The task is to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## Project Description
In this project, we build an ETL pipeline for a data lake hosted on S3. WEe load data from S3, process the data into analytics tables using Spark, and load them back into S3. We deploy this Spark process on a cluster using AWS.

## Project Datasets
We work with two datasets that reside in S3. Here are the S3 links for each:

`Song data: s3://udacity-dend/song_data`
`Log data: s3://udacity-dend/log_data`

## ETL pipeline
- We use the JSON reading capability of Spark to load the .json files of log_data and songs_data into a DataFrame
- We then create the following tables:

`Dimension Tables`
`users` - users in the app -> `user_id, first_name, last_name, gender, level`
`songs` - songs in music database -> `song_id, title, artist_id, year, duration`
`artists` - artists in music database -> `artist_id, name, location, lattitude, longitude`
`time` - timestamps of records in songplays broken down into specific units `start_time, hour, day, week, month, year, weekday`

`Fact Table`
`songplays` - records in log data associated with song plays i.e. records with page NextSong -> `songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent`

- We then save the created tables in parquet file format, and partition some based on their attributes.
`songplays` table files are partitioned by `year` and `month`
`songs` table files are partitioned by `year` and `artist_id`. 
`time` table files are partitioned by `year` and `month`

## Example Query
`songs = spark.read.parquet(output_data + '/songs_data/songs_table.parquet')    
songs.groupBy('year').count().collect()`

which results in something like:
[Row(year=2003, count=2),
 Row(year=2007, count=1),
 Row(year=1961, count=1),
 Row(year=1997, count=2),
 Row(year=1994, count=2),
 Row(year=2004, count=4),
 Row(year=1969, count=1),
 Row(year=1982, count=1),
 Row(year=1985, count=1)]
 
## How to Run Project
You can use `etl.ipynb` to test the project locally
Or
Include your aws credentials in `dl.cfg` and then: 
`python etl.py`
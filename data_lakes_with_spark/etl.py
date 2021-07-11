import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear
from pyspark.sql import types as t
import pyspark.sql.functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['conf']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['conf']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates Spark Session and installs necessary packages
    
    @param: None
    @return: spark session
    
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Loads input data, processes the data, extracts songs_table and artists_table and saves them to parket
    
    @param spark: Spark session
    @param input_data: path to folder containing both songs and logs
    @param output_data: path to parket folder
    
    @return: None
    
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data')
    
    # read song data file
    df = spark.read.json(song_data)
    
    # extract columns to create songs table
    songs_table = df\
    .select(['song_id', 'title', 'artist_id', 'year', 'duration'])\
    .drop_duplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_parquet_dir = os.path.join(output_data, '/songs_data/songs_table.parquet') 
    songs_table.write.partitionBy('year', 'artist_id').mode('overwrite').save(songs_parquet_dir)

    # extract columns to create artists table
    artists_table = df\
    .select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'])\
    .withColumnRenamed('artist_name', 'name')\
    .withColumnRenamed('artist_location', 'location')\
    .withColumnRenamed('artist_latitude', 'latitude')\
    .withColumnRenamed('artist_longitude', 'longitude')\
    .drop_duplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_parquet_dir = os.path.join(output_data, '/artists_data/artists_table.parquet')
    artists_table.write.mode('overwrite').save(artists_parquet_dir)


def process_log_data(spark, input_data, output_data):
    """
    Loads input data, processes the data, extracts users_table and time_table and songplays_table saves them to parket
    
    @param spark: Spark session
    @param input_data: path to folder containing both songs and logs
    @param output_data: path to parket folder
    
    @return: None
    
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data')

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(col('page')=='NextSong')

    # extract columns for users table    
    users_table = df\
    .select('userId', 'firstName', 'lastName', 'gender', 'level')\
    .withColumnRenamed('userId', 'user_id')\
    .withColumnRenamed('firstName', 'first_name')\
    .withColumnRenamed('lastName', 'last_name')\
    .distinct()
    
    # write users table to parquet files
    users_parquet_dir = os.path.join(output_data, 'users_data/users_table.parquet')
    users_table.write.mode('overwrite').save(users_parquet_dir)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts / 1000), t.TimestampType())
    df = df.withColumn('timestamp', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S'), t.StringType())
    df = df.withColumn('datetime', get_datetime('ts'))
    
    with_time = df.drop_duplicates(['datetime']).withColumn("hour", hour(col('timestamp')))\
    .withColumn("day", dayofmonth(col("timestamp")))\
    .withColumn("week", weekofyear(col("timestamp")))\
    .withColumn("month", month(col("timestamp")))\
    .withColumn("year", year(col("timestamp")))
    
    # extract columns to create time table
    time_table = with_time.select('datetime', 'hour', 'day', 'week', 'month', 'year').distinct()
    
    # write time table to parquet files partitioned by year and month'
    time_parquet_dir = os.path.join(output_data, 'time_data/time_table.parquet')
    time_table.write.partitionBy(['year', 'month']).mode('overwrite').save(time_parquet_dir)

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = \
    df.join(song_df, on = 
      (df.song==song_data.title) & 
      (df.artist == song_data.artist_name) & 
      (df.length== song_data.duration))\
      .withColumn('songplay_id', F.monotonically_increasing_id())\
      .select('songplay_id', 'timestamp', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent')\
      .withColumnRenamed('timestamp', 'start_time')\
      .withColumnRenamed('userId', 'user_id')\
      .withColumnRenamed('sessionId', 'session_id')\
      .withColumnRenamed('userAgent', 'user_agent')\
      .withColumn("month", month(col("start_time")))\
      .withColumn("year", year(col("start_time")))

    # write songplays table to parquet files partitioned by year and month
    songplays_parquet_dir = os.path.join(output_data, 'songplays_data/songplays_table.parquet')
    songplays_table.write.partitionBy(['year', 'month']).mode('overwrite').save(songplays_parquet_dir)


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://antonia-udacity-bucket/data_lake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

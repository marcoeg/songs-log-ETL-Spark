from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id

"""
    Load and execute with the following command on EMR cluster:
    $ spark-submit --master yarn --driver-java-options='-Dspark.yarn.app.container.log.dir=/mnt/var/log/hadoop' ./etl.py 
"""

def create_spark_session():
    """
    Spark session configuration.  
        Note: not needed when case the script is executed on Notebook on pyspark kernel.
    Parameters:
        None
    Returns:
        spark: spark session 
    """

    spark = SparkSession \
        .builder \
        .appName("meg-sparkify-data") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    Collect the song data from the filepath in the S3 bucket and populate the songs and artists dimension tables.

    Parameters:
        spark: the spark session.
        input_path: S3 bucket containing the JSON song data.
        output_path: S3 destination bucket to store the tables in parquet format.
    Returns:
        None
    """

    print("------------------------ in process_song_data")
    # get filepath to song data file
    song_data = "song_data/*/*/*/*.json"
    
    print("------------------------ reading song data: ", input_data + song_data)
    song_dataDF = spark.read.json(input_data + song_data)

    # extract columns to create songs table
    songs_tableDF = song_dataDF['song_id', 'title', 'artist_id', 'year', 'duration']

    print("------------------------ songs_tableDF: ")
    songs_tableDF.printSchema()

    print("------------------------ writing songs_tableDF parquet")
    # write songs table to parquet files partitioned by year and artist
    songs_tableDF.write.parquet(partitionBy=["year","artist_id"], 
                                path=output_data + "song_data/songs.parquet", 
                                mode="overwrite")

    # extract columns to create artists table and drop duplicates
    artists_tableDF = song_dataDF['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artists_tableDF = artists_tableDF.drop_duplicates(subset=['artist_id'])

    print("------------------------ artists_tableDF: ")
    artists_tableDF.printSchema()

    print("------------------------ writing artists_tableDF parquet")
    # write artists table to parquet files
    artists_tableDF.write.parquet(path=output_data + "song_data/artists.parquet", 
                                  mode="overwrite")
    print("------------------------ exiting process_song_data")

def process_log_data(spark, input_data, output_data):
    """
    Collects the log data from the S3 bucket and populate the facts table songsplays and the dimension tables user and time.

    Parameters:
        spark: the spark session.
        input_path: S3 bucket containing the JSON log data.
        output_path: S3 bucket to store the tables in parquet format.
    Returns:
        None
    """

    print("------------------------ in process_log_data")
    # get filepath to log data file
    log_data = "log_data/*/*/*.json"
    # get filepath to song data file
    song_data = "song_data/*/*/*/*.json"

    print("------------------------ reading log data: ", input_data + log_data)
    log_dataDF = spark.read.json(input_data + log_data)

    print("------------------------ reading song data: ", input_data + song_data)
    song_dataDF = spark.read.json(input_data + song_data)
    print("------------------------ song_dataDF: ")
    song_dataDF.printSchema()

    # filter by actions for song plays
    log_dataDF = log_dataDF.where(col("page")=="NextSong")
    print("------------------------ log_dataDF: ")
    log_dataDF.printSchema()

    # extract columns for users table    
    users_tableDF = log_dataDF['userId', 'firstName', 'lastName', 'gender', 'level', 'ts']
    users_tableDF = users_tableDF.dropDuplicates(subset=["userId"])

    print("------------------------ users_tableDF: ")
    users_tableDF.printSchema()

    print("------------------------ writing users_tableDF parquet")
    # write artists table to parquet files
    users_tableDF.write.parquet(path=output_data + "user_data/users.parquet", 
                                  mode="overwrite")

    # ------ UDF  function
    # extract datetime based on the timestamp from log dataframe
    get_datetime_udf = udf(lambda ts: datetime.datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S'))

    # add start_time, year, month columns to log data frame (needed by songsplay and time dateframe tables)
    log_dataDF = log_dataDF.withColumn("start_time", get_datetime_udf(log_dataDF.ts)) 
 
    # extract columns to create time table
    time_tableDF  = log_dataDF['start_time', dayofmonth('start_time').alias('day'), weekofyear('start_time').alias('week'), dayofweek('start_time').alias('weekday'), hour('start_time').alias('hour'), month('start_time').alias('month'), year('start_time').alias('year')]
    time_tableDF = time_tableDF.drop_duplicates(subset=['start_time'])

    print("------------------------ time_tableDF: ")
    time_tableDF.printSchema()

    print("------------------------ writing time_tableDF parquet")
    # write time details back to parquet file partitioned by year and month
    time_tableDF.write.parquet(partitionBy=["year","month"], 
                               path=output_data + "time_data/time.parquet",
                               mode="overwrite")

    print("------------------------ creating songplays_tableDF ")

    # extract columns from joined song and log datasets to create songplays table 
    log_dataDF_J = log_dataDF.join(song_dataDF, (song_dataDF.title == log_dataDF.song) & (song_dataDF.artist_name == log_dataDF.artist))
    log_dataDF_J = log_dataDF_J.withColumn('songplay_id', monotonically_increasing_id()) 
    songplays_tableDF = log_dataDF_J['songplay_id','start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent', month('start_time').alias('month'), year('start_time').alias('year')]
   
    print("------------------------ songplays_tableDF: ")
    songplays_tableDF.printSchema()

    print("------------------------ writing songplays_tableDF parquet")
    # write songs  to parquet file partitioned by year and month
    songplays_tableDF.write.parquet(partitionBy=["year","month"], 
                                    path=output_data + "songsplay_data/songsplay.parquet", 
                                    mode="overwrite")
    print("------------------------ exiting process_log_data")

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://meg-sparkify-data/"

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()

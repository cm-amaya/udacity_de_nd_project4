import os
import configparser

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, dayofweek, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Creates a Spark session

    Returns:
        [pyspark.sql.SparkSession]: Corresponding Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark: SparkSession, input_data: str,
                      output_data: str):
    """Process Song data from the S3 path given and stores it in the output
    path

    Args:
        spark (pyspark.sql.SparkSession): Spark session
        input_data (str): Input path to data in S3
        output_data (str): Output path to data in S3
    """
    # get filepath to song data file
    song_data = os.path.join(input_data,
                             'song_data', '*', '*', '*', '*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_cols = ['song_id',
                  'title',
                  'artist_id',
                  'year',
                  'duration']
    
    songs_table = df.select(*songs_cols).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id") \
        .parquet(os.path.join(output_data,
                              'songs',
                              'songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_cols = ['artist_id',
                    'artist_name',
                    'artist_location',
                    'artist_latitude',
                    'artist_longitude']
    artists_table = df.select(*artists_cols).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data,
                                             'artists',
                                             'artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data', '*.json')
    # read log data file
    df = spark.read.json(log_data)
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")
    # extract columns for users table
    users_cols = ['userId',
                  'firstName',
                  'lastName',
                  'gender',
                  'level']
    users_table = df.select(*users_cols).dropDuplicates()
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data,
                                           'users', 'users.parquet'))
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: str(int(int(ts)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: str(datetime.fromtimestamp(int(ts))))
    df = df.withColumn('datetime', get_datetime(df.timestamp))
    # extract columns to create time table
    time_table = df.select('datetime').withColumn('start_time', df.timestamp)\
        .withColumn('hour', hour('datetime'))\
        .withColumn('day', dayofmonth('datetime'))\
        .withColumn('week', weekofyear('datetime'))\
        .withColumn('month', month('datetime'))\
        .withColumn('year', year('datetime'))\
        .withColumn('weekday', dayofweek('datetime'))\
        .dropDuplicates()
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, 'time', 'time.parquet'))
    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data,
                                           'song_data', '*',
                                           '*', '*', '*.json'))
    # extract columns from joined song and log datasets
    # to create songplays table
    df_join = df.join(song_df,
                      (df.artist == song_df.artist_name) &
                      (df.song == song_df.title), 'inner')
    songplays_select_cols = ['timestamp',
                             'userId',
                             'level',
                             'song_id',
                             'sessionId',
                             'location',
                             'userAgent',
                             'year',
                             'month']
    songplays_cols = ['start_time',
                      'user_id',
                      'level',
                      'song_id',
                      'session_id',
                      'location',
                      'user_agent',
                      'year',
                      'month']    
    songplays_table = df_join.select(songplays_select_cols)
    for old_col, new_col in zip(songplays_select_cols, songplays_cols):
        if old_col != new_col:
            songplays_table = songplays_table.withColumnRenamed(old_col,
                                                                new_col)
    songplays_table.withColumn('songplay', monotonically_increasing_id()+1)      
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month")\
        .parquet(os.path.join(output_data,
                              'songplays',
                              'songsplays.parquet'), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://datalake-detp-nano/data/"
    process_song_data(spark, input_data, output_data) 
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

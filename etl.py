from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, \
     date_format
from pyspark.sql.types import StructType as R, StructField as Fld, \
     DoubleType as Dbl, LongType as Long, StringType as Str, \
     IntegerType as Int, DecimalType as Dec, DateType as Date, \
     TimestampType as Stamp


def create_spark_session():
    """Create Spark session."""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def get_song_schema():
    """Schema for songs data."""
    song_schema = R([
        Fld("num_songs", Int()),
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dec()),
        Fld("artist_longitude", Dec()),
        Fld("artist_location", Str()),
        Fld("artist_name", Str()),
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("duration", Dbl()),
        Fld("year", Int())
    ])
    return song_schema


def process_song_data(spark, input_data, output_data):
    """
    Load data from S3, transforms it and upload to S3.
    Parameters
    ----------
    spark: session
            Spark session
    input_data: path
            Path to input data.
    output_data: path
            Path to output data.

    """

    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    print("Reading song data.")    
    df = spark.read.json(song_data, schema = get_song_schema())
    
    # extract columns to create songs table
    songs_table = df.select("song_id",
                            "title",
                            "artist_id",
                            "year",
                            "duration").dropDuplicates(["song_id"])
    
    # write songs table to parquet files partitioned by year and artist
    print("Writing songs_table.")
    songs_table.write.parquet(output_data + "songs_table.parquet",
                              partitionBy = ["year", "artist_id"],
                              mode = "overwrite") 

    # extract columns to create artists table
    artists_table = df.select("artist_id",
                              "artist_name",
                              "artist_location",
                              "artist_latitude",
                              "artist_longitude").dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    print("Writing artists_table")
    artists_table.write.parquet(output_data + "artists_table.parquet",
                                mode = "overwrite")


def get_log_schema():
    """Schema for log data."""
    log_schema = R([
        Fld("artist", Str()),
        Fld("auth", Str()),
        Fld("firstName", Str()),
        Fld("gender", Str()),
        Fld("itemInSession", Str()),
        Fld("lastName", Str()),
        Fld("length", Dbl()),
        Fld("level", Str()),
        Fld("location", Str()),
        Fld("method", Str()),
        Fld("page", Str()),
        Fld("registration", Dbl()),
        Fld("sessionId", Str()),
        Fld("song", Str()),
        Fld("status", Str()),
        Fld("ts", Long()),
        Fld("userAgent", Str()),
        Fld("userId", Str())
    ])
    return log_schema


def process_log_data(spark, input_data, output_data):
    """
    Load data from S3, transforms it and uploads to S3.
    Parameters
    ----------
    spark: session
            Spark session
    input_data: path
            Input data path
    output_data: path
            Output data path
    """

    # get filepath to log data file
    log_data = input_data + "log-data/*/*/*.json"

    # read log data file
    print("Reading log file.")
    df = spark.read.json(log_data, schema = get_log_schema())
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id",
                                "firstName as first_name",
                                "lastName as last_name",
                                "gender",
                                "level").dropDuplicates(["user_id"]) 
    
    # write users table to parquet files
    print("Writing users_table")
    users_table.write.parquet(output_data + "users_table.parquet",
                              mode = "overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp((x / 1000)), Stamp())
    df = df.withColumn("timestamp", get_timestamp(col("ts")))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp((x / 1000)), Stamp())
    df = df.withColumn("datetime", get_datetime(col("ts")))
    
    # extract columns to create time table
    time_table = df.selectExpr("timestamp as start_time",
                               "hour(timestamp) as hour",
                               "dayofmonth(timestamp) as day",
                               "weekofyear(timestamp) as week",
                               "month(timestamp) as month",
                               "year(timestamp) as year",
                               "dayofweek(timestamp) as weekday"
                               ).dropDuplicates(["start_time"])
    
    # write time table to parquet files partitioned by year and month
    print("Writing time_table.")
    time_table.write.parquet(output_data + "time_table.parquet",
                             partitionBy = ["year", "month"],
                             mode = "overwrite")

    # read in song data to use for songplays table
    song_data = input_data + "song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data, schema = get_song_schema())

    # extract columns from joined song and log datasets to create
    # songplays table
    song_df.createOrReplaceTempView("song_data")
    df.createOrReplaceTempView("log_data")
    
    songplays_table = spark.sql("""
                                SELECT monotonically_increasing_id() as songplay_id,
                                ld.timestamp as start_time,
                                year(ld.timestamp) as year,
                                month(ld.timestamp) as month,
                                ld.userId as user_id,
                                ld.level as level,
                                sd.song_id as song_id,
                                sd.artist_id as artist_id,
                                ld.sessionId as session_id,
                                ld.location as location,
                                ld.userAgent as user_agent
                                FROM log_data ld
                                JOIN song_data sd
                                ON (ld.song = sd.title
                                AND ld.length = sd.duration
                                AND ld.artist = sd.artist_name)
                                """)

    # write songplays table to parquet files partitioned by year and month
    print("Writing songplays_table")
    songplays_table.write.parquet(output_data + "songplays_table.parquet",
                                  partitionBy=["year", "month"],
                                  mode="overwrite")


def main():
    """
    The following steps are performed:
    1.) Create or get Spark session.
    2.) Load data from S3.
    3.) Transforms data.
    4.) Upload to S3 result.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-andreiliphd/"
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
    
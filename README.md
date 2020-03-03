# Songs Log ETL Spark
Sparkify mobile app events log database and ETL pipeline with Spark for songs analysis on a AWS data lake

### Abstract
The Sparkify mobile app is a music streaming application and is collecting users behavior as events in AWS S3. In preparation for analyzing these events in an efficient way, a Data Lake on AWS S3 is created.

The events from the app are stored in AWS S3 as JSON logs of user activity on the app, and metadata on songs available in the app are also available in an S3 bucket as JSON objects.

This project creates a Data Lake on S3 using Spark with a schema-on-read approach. The tables produced by the Spark ETL are stored in S3 in `parquet` format and are partitioned to optimize queries for song play analysis.

#### Events Dataset 
The activity logs from the music streaming app are in log files in JSON format.

The log files in the dataset are partitioned by year and month. For example, here are filepaths to two files in this dataset:
```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```
File format:
```
{
    "artist":"Taylor Swift",
    "auth":"Logged In",
    "firstName":"Tegan",
    "gender":"F",
    "itemInSession":4,
    "lastName":"Levine",
    "length":238.99383,
    "level":"paid",
    "location":"Portland-South Portland, ME",
    "method":"PUT",
    "page":"NextSong",
    "registration":1540794356796.0,
    "sessionId":481,
    "song":"Cold As You",
    "status":200,
    "ts":1542061558796,
    "userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
    "userId":"80"
}
```

The event logs are in a S3 bucket as specified in the `LOG_DATA` configuration variable in the `dwh.cfg` file. For convenience in populating a staging table in a single `COPY` command, the `LOG_JSONPATH` contains the URI of a file in json format containing all the log objects URIs.

### Songs Dataset
The dataset is a copy on AWS S3 of the Million Song Dataset http://millionsongdataset.com. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. 

For example, here are filepaths to two files in this dataset:
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json

Below is the format of a single song file:
```
{
    "num_songs":1,
    "artist_id":"ARJIE2Y1187B994AB7",
    "artist_latitude":null,
    "artist_longitude":null,
    "artist_location":"",
    "artist_name":"Line Renaud",
    "song_id":"SOUPIRU12A6D4FA1E1",
    "title":"Der Kleine Dompfaff",
    "duration":152.92036,
    "year":0
}
```
The songs metadata files are in the S3 bucket specified in the `SONG_DATA` configuration variable in the `dwh.cfg` file.

#### Data Lake on S3 for Song Play Analysis
The tables in the Data Lake are in the same bucket `meg-sparkify-data` partitioned to be optimized for queries on song play analysis. It includes the following tables:

##### Facts Table
`songplay_data` - records in log data associated with song plays

##### Dimension Tables
`user_data/users.parquet` - users in the app
`song_data/songs.parquet` - songs in music database
`song_data/artists.parquet` - artists in music database
`time_data/time.parquet` - timestamps of records in songplays broken down into specific units

##### Hosting
AWS S3 was choosen as the cloud technology for the Data Lake for its high availability, virtually unlimited expandibility and low cost.

#### ETL Process

The pipeline datasource are the JSON files containing the events and the songs stored in AWS S3 buckets. 

The ETL pipeline for data processing is done in AWS EMR using Spark with a Schema-On-Read approach, without a predifined schema. The resulting tables are written also in the output S3 bucket as parquet files.

The etl.py script runs on a AWS EMR Spark cluster node for elasticity and it is created on-demand, hence reducing costs. The tables created by the ETL pipeline are accessed using and queried with SQL using for instance AWS Athena or Presto. 

1. Create the EMR cluster running `emr-create_cluster.sh`
2. Upload the `etl.py` script to the master node
3. Run the ETL process to load the S3 table with Spark

Use the following command on the cluster master node:
```
 $ spark-submit --master yarn --driver-java-options='-Dspark.yarn.app.container.log.dir=/mnt/var/log/hadoop' ./etl.py 
 ```
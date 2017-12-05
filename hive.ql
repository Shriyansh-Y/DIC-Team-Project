
-- creating an external table from dynamodb table

CREATE EXTERNAL TABLE ddb_tweets 
    (item MAP<STRING, STRING>)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler' 
TBLPROPERTIES ("dynamodb.table.name" = "Tweets2", "dynamodb.region" = "us-east-2");

-- creating external table linked to hdfs
CREATE EXTERNAL TABLE hdfs_tweets 
    (item MAP<STRING, STRING>)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
LOCATION 'hdfs:///user/hadoop/tweets';


-- overwrite hdfs file table
INSERT OVERWRITE TABLE hdfs_tweets
SELECT * FROM ddb_tweets;

--#################  Write DynamoDB from HDFS

-- creating external table linked to DynamoDB
CREATE EXTERNAL TABLE ddb_features
    (hashtag      STRING,
     pos          BIGINT,
     neg          BIGINT)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "results",
    "dynamodb.region" = "us-east-2",
    "dynamodb.column.mapping"="feature_id:Id,feature_name:Name,feature_class:Class,state_alpha:State,prim_lat_dec:Latitude,prim_long_dec:Longitude,elev_in_ft:Elevation"
);

-- overwrite dynamo table
INSERT OVERWRITE TABLE ddb_features 
SELECT * FROM hive_features;

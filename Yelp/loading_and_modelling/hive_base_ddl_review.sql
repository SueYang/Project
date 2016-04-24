DROP TABLE review_raw;
CREATE EXTERNAL TABLE review_raw(
	user_id STRING,
	review_id STRING,
	text STRING,
	votes_cool STRING,
	business_id STRING,
	votes_funny STRING,
	stars STRING,
	date STRING,
	type STRING,
	votes_useful STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
 "separatorChar" = ",",
 "quoteChar" = '"',
 "escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/Yelp/review';
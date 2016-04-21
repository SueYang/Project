DROP TABLE business_raw;
CREATE EXTERNAL TABLE business_raw(	
	ambience_divey STRING,
	dietary_vegan STRING,
	happy_hour STRING,
	thursday_open STRING,
	order_at_counter STRING,
	byob STRING,
	friday_open STRING,
	good_for_latenight STRING,
	outdoor_seating	STRING,
	alcohol	STRING,
	ambience_classy STRING,
	by_appointment_only STRING,
	parking_lot STRING,
	business_id STRING,
	ambience_touristy STRING,
	corkage	STRING,
	tuesday_open STRING,
	good_for_brunch	STRING,
	categories STRING,
	waiter_service STRING,
	monday_open STRING,
	name STRING,
	parking_street STRING,
	ambience_hipster STRING,
	byob_corkage STRING,
	music_live STRING,
	dietary_dairy-free STRING,
	music_background STRING,
	price_range STRING,
	good_for_breakfast STRING,
	parking_garage STRING,
	music_karaoke STRING,
	good_for_dancing STRING,
	review_count STRING,
	state STRING,
	accepts_credit_cards STRING,
	friday_close STRING,
	good_for_lunch STRING,
	parking_valet STRING,
	take_out STRING,
	full_address STRING,
	thursday_close STRING,
	good_for_dessert STRING,
	music_video STRING,
	dietary_halal STRING,
	take_reservations STRING,
	saturday_open STRING,
	ages_allowed STRING,
	ambience_trendy	STRING,
	delivery STRING,
	wednesday_close	STRING,
	wifi STRING,
	open STRING,
	city STRING,
	wheelchair_accessible STRING,
	dietary_gluten_free STRING,
	stars STRING,
	dietary_kosher STRING,
	type STRING,
	daters STRING,
	ambience_intimate STRING,
	latitude STRING,
	good_for_dinner	STRING,
	coat_check STRING,
	longitude STRING,
	monday_close	STRING,
	tuesday_close STRING,
	saturday_close STRING,
	good_for_kids STRING,
	parking_validated STRING,
	sunday_open STRING,
	music_dj STRING,
	dietary_soy_free STRING,
	has_TV STRING,
	sunday_close	STRING,
	ambience_casual	STRING,
	dogs_allowed STRING,
	drive_thru STRING,
	dietary_vegetarian STRING,
	wednesday_open STRING,
	noise_level STRING,
	smoking	STRING,
	attire STRING,
	good_for_groups	STRING,
	neighborhoods_STRING,
	open_24_Hours STRING,
	ambience_romantic STRING,
	music_jukebox STRING,
	ambience_upscale STRING,
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
 "separatorChar" = ",",
 "quoteChar" = '"',
 "escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/Yelp/business';

DROP TABLE review_raw;
CREATE EXTERNAL TABLE review_raw(
	user_id STRING,
	review_id STRING,
	text STRING,
	votes.cool STRING,
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
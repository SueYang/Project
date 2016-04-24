--join table "business" and result(grouped by business)
DROP TABLE fullResult_byBusiness;
CREATE TABLE fullResult_byBusiness AS 
SELECT  business.business_id,
	business.name AS business_name,
	business.categories AS business_categories,
	business.stars AS business_stars,
	business.review_count AS business_review_count,
	business.city AS business_city,
	business.state AS business_state,
	business.full_address AS business_full_address,
	business.latitude AS business_latitude,
	business.longitude AS business_longitude,
	result_bybusiness.cleantext,
	result_bybusiness.totwords,
	result_bybusiness.unigrams,
	result_bybusiness.bigrams,
	result_bybusiness.trigrams
	FROM business
	INNER JOIN bybusiness_limit500k
	ON business.business_id=result_bybusiness.businessid
	WHERE business.business_id IS NOT NULL AND business.open='TRUE';
--join table "business" and result(grouped by business)
DROP TABLE fullResult_byBusiness;
CREATE TABLE fullResult2_byBusiness AS 
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
	result2_bybusiness.cleantext,
	result2_bybusiness.totwords,
	result2_bybusiness.unigrams,
	result2_bybusiness.bigrams,
	result2_bybusiness.trigrams
	FROM business
	INNER JOIN result2_bybusiness
	ON business.business_id=result2_bybusiness.businessid
	WHERE business.business_id IS NOT NULL AND business.open='TRUE';
--create simple table "review" in ER diagram
DROP TABLE cleaned_bybusiness;
CREATE TABLE cleaned_bybusiness AS
SELECT businessid,totwords,unigrams,bigrams,trigrams FROM bybusiness_limit50k;
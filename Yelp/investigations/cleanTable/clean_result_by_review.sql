--create simple table "review" in ER diagram
DROP TABLE cleaned_bybusiness_limit50k;
CREATE TABLE cleaned_bybusiness_limit50k AS
SELECT businessid,totwords,unigrams,bigrams,trigrams FROM bybusiness_limit50k;
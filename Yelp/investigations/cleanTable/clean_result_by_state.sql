--create simple table "review" in ER diagram
DROP TABLE cleaned2_bystate;
CREATE TABLE cleaned2_bystate AS
SELECT state,totwords,unigrams,bigrams,trigrams FROM result2_bystate;
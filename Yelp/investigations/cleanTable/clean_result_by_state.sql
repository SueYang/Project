--create simple table "review" in ER diagram
DROP TABLE cleaned_bystate;
CREATE TABLE cleaned_bystate AS
SELECT state,totwords,unigrams,bigrams,trigrams FROM result_bystate;
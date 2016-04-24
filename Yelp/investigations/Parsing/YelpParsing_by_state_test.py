# Import libraries
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.types import *
import numpy as np
import pandas as pd
import collections
import nltk
# nltk.download('all')
import string

from bs4 import BeautifulSoup
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
from nltk.collocations import *
from nltk.util import ngrams
from string import digits
from pandas import DataFrame, Series
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

bigram_measures = nltk.collocations.BigramAssocMeasures()
trigram_measures = nltk.collocations.TrigramAssocMeasures()

sc = SparkContext()
sqlContext = HiveContext(sc)
# Read in the train and test data
rdd_reviews = sqlContext.sql("select business_state,cleantext from fullResult_byBusiness limit 20").rdd
df_reviews = rdd_reviews.groupByKey().toDF()
pd_df_reviews = df_reviews.toPandas()
# print(rdd_reviews)

# Drop columns: Description, Resolution, and Address
#df_train.drop(['Descript', 'Resolution','Address'], axis=1, inplace=True)
#df_test.drop(['Address'], axis=1, inplace=True)
print(pd_df_reviews)
# print(rdd_reviews.toDF().toPandas())
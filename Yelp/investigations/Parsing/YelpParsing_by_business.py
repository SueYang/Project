# Import libraries
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.types import *
from pyspark import StorageLevel
import numpy as np
import pandas as pd
import collections
import nltk
import pyspark
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
rdd_reviews = sqlContext.sql("select business_id,text from review limit 500000").rdd
rdd_reviews.persist(pyspark.StorageLevel.DISK_ONLY)
df_reviews = rdd_reviews.groupByKey().toDF()
pd_df_reviews = df_reviews.toPandas()

# Drop columns: Description, Resolution, and Address
#df_train.drop(['Descript', 'Resolution','Address'], axis=1, inplace=True)
#df_test.drop(['Address'], axis=1, inplace=True)

cachedStopWords = set(nltk.corpus.stopwords.words('english'))
#add custom words
cachedStopWords.update(('and','I','A','And','So','arnt','This','When','It','many','Many','so','cant','Yes','yes','No','no','These','these',
                        'ago','also','want','always','very','absolutely','absolute','actually','finally','possible','possibly','anything','anytime',
                        'im','become','able','said','every','each','go','good','great','awesome','food','best','place','location','food','try','love','staff','pei','wei','order','ok','okay','people','hard','cook','get','ended'))
def cleanText(text):
    """
    removes punctuation, stopwords, numbers and returns lowercase text in a list of single words
    """
    text = " ".join(str(x) for x in text)
    text = text.lower()
    text = text.translate(None, string.digits)
    text = text.translate(None, string.punctuation)         #python2
    # text = text.translate({ord(k): None for k in digits})     #python3
    # text = text.translate(str.maketrans("", "", string.punctuation))
    text = BeautifulSoup(text).get_text()
    tokenizer = RegexpTokenizer(r'\w+')
    text = tokenizer.tokenize(text)
    clean = [word.strip().lower() for word in text if word not in cachedStopWords]
    return clean

def top5_words(text):
    """
    get top5 words with highest frequency
    """
    words = ' '.join(text)
    counts = collections.Counter(words.split())
    return [elem for elem, it in counts.most_common(5)]

def top5_bigram_collocations(text):
    """
    return the 5 bigrams with the highest PMI
    :param text:
    :return:
    """
    finder = BigramCollocationFinder.from_words(text)
    finder.apply_freq_filter(5)
    bigrams = finder.nbest(bigram_measures.pmi, 5)
    return bigrams

def top5_trigram_collocations(text):
    """
    return the 5 trigrams with the highest PMI
    :param text:
    :return:
    """
    finder = TrigramCollocationFinder.from_words(text)
    finder.apply_freq_filter(5)
    trigrams = finder.nbest(trigram_measures.pmi, 5)
    return trigrams

columns = ['BusinessID','CleanText', 'TotWords', 'unigrams', 'bigrams','trigrams']
index = np.arange(pd_df_reviews.shape[0])
df = pd.DataFrame(columns=columns, index = index)

i = 0
for index, row in pd_df_reviews.iterrows():
    tot = 0
    totwords = 0
    print(row)
    if pd.notnull(row['_2']):
        cleantext = cleanText(row['_2'])
        totwords = len(cleantext)
        #return the 5 n-grams with the highest PMI
        unigrams = top5_words(cleantext)
        bigrams = top5_bigram_collocations(cleantext)
        trigrams = top5_trigram_collocations(cleantext)

    df.ix[i, 'BusinessID']= row['_1']
    df.ix[i, 'CleanText']= " ".join(cleantext).encode('utf-8')
    df.ix[i, 'TotWords']= totwords
    df.ix[i, 'unigrams']= unigrams
    df.ix[i, 'bigrams']= bigrams
    df.ix[i, 'trigrams']= trigrams
    i += 1

spark_df = sqlContext.createDataFrame(df, columns)

# Save it as a table
spark_df.registerTempTable("dfBusiness500000")

sqlContext.sql("drop table if exists result2_bybusiness")
sqlContext.sql("CREATE TABLE result2_bybusiness AS SELECT * FROM dfBusiness500000")


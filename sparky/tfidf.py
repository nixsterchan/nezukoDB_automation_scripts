from pyspark.sql import SparkSession
import pyspark.sql.functions as fnc
import pyspark
from pyspark.sql.functions import col
from pyspark.sql.window import Window
import math
import time

start_time = time.time()


sc = pyspark.SparkContext('local[*]')
spark = SparkSession(sc)

# get our namenode dns url
fil = open('namenode_url', 'r')
location = fil.read()
fil.close()

# get reviews from hdfs and load into dataframe
reviews = spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(location + 'kindle_reviews.csv')
# reviews.show(5)

reviews = reviews.drop("_c0") \
            .drop("helpful") \
            .drop("overall") \
            .drop("reviewTime") \
            .drop("reviewerID") \
            .drop("reviewerName") \
            .drop("summary") \
            .drop("unixReviewTime")

# reviews.show(5)

# getting document counts, where asin is considered the document
# doc_counts = reviews.groupBy('asin').count()
# print('doc counts')
# doc_counts.show()

# now to split up our reviewText into lines
# doc_and_lines = reviews.select('asin', fnc.split('reviewText', '[\W_]+').alias('a_line'))
# print('doc and lines')
# doc_and_lines.show()

# and now further split it into words
doc_and_words = reviews \
                .select('asin', fnc.explode(fnc.split('reviewText', '[\W_]+')).alias('each_word')) \
                .filter(fnc.length('each_word') >0) \
                .select('asin', fnc.trim(fnc.lower(fnc.col('each_word'))).alias('each_word'))\

# print('doc and words')
# doc_and_words.show()

# get counts of each word
# word_counts = doc_and_words.groupBy('each_word') \
#                 .count()
# print('word counts')
# word_counts.show()

# now to get term frequency using the formula of (term in a doc)/(total num of words in that doc)

wind = Window.partitionBy(doc_and_words['asin'])

tf = doc_and_words.groupBy('asin', 'each_word')\
    .agg(fnc.count('*').alias('num_words'), 
         fnc.sum(fnc.count('*')).over(wind).alias('num_docs'),
         (fnc.count('*')/fnc.sum(fnc.count('*')).over(wind)).alias('tf')
        ) \
    .orderBy('num_words', ascending=False) \
    .drop('num_words') \
    .drop('num_docs')
    # .cache()

tf.show(truncate=5)

### get idf table using the formula of (log((total num docs) / num of docs with term in it)) ###
# we get the document counts based on each asin
total_num_docs = doc_and_words.select('asin').distinct().count()

wind = Window.partitionBy('each_word')


idf = doc_and_words.groupBy('each_word', 'asin') \
        .agg(
            fnc.lit(total_num_docs).alias('total_num_docs'),
            fnc.count('*').over(wind).alias('docs_with_term'),
            fnc.log(fnc.lit(total_num_docs)/fnc.count('*').over(wind)).alias('idf')
        ) \
        .orderBy('idf', ascending=False) \
        .drop('total_num_docs') \
        .drop('doc_with_term')
        # .cache()
    
idf.show(truncate=5)


### Combining both tables we can get calculate TFIDF ###

tfidf = tf.join(idf, ['asin', 'each_word']) \
        .withColumn('tfidf', fnc.col('tf') * fnc.col('idf')) \
        .drop
        # .cache()

tfidf.orderBy('tfidf', ascending=False).show(truncate=12)


print("start time:")
print("--- %s seconds ---" % (time.time() - start_time))
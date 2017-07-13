import time
from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext

conf = (SparkConf()
         .setAppName("MyFirstApp")
         .set("spark.executor.memory", "12g"))

sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

start_time = time.time()

text = sqlContext.read.text('really_big_text.txt').rdd
text_words = text.flatMap(lambda x: x['value'].split())
text_words.cache()

count_words = text_words\
                  .map(lambda x: (x,1))\
                  .reduceByKey(lambda x,y:x+y)\
                  .sortBy(lambda x:x[1],False)\
                  .toDF(schema=['word','count'])

print "TIME: %s"%(time.time()-start_time)
print "Top 100"
count_words.show(10)
print "Total : %s"%len(text_words.collect())
sc.stop()

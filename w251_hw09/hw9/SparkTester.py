import os
import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf

os.environ['SPARK_HOME'] = '/usr/local/spark'
sys.path.append("/usr/local/spark/python")

my_spark_master = "spark://spark-2-1:7077"
my_spark_app_name = "hw09"

conf = SparkConf()
conf.setMaster(my_spark_master)
conf.setAppName(my_spark_app_name)
sc = SparkContext(conf=conf)

rdd = sc.parallelize([("a", 1)])
print(hasattr(rdd, "toDF"))
## False

spark = SparkSession(sc)
print(hasattr(rdd, "toDF"))
## True

rdd.toDF().show()
'''
test to see if we can read a single from multiple files in hdfs

to install pyspark:
conda install -c conda-forge pyspark

to see all files:
hadoop fs -ls spark_out_data/raw

to run script
python test_pyspark_hdfs.py
'''

from pyspark.sql import SparkSession

# sets up spark
# sc = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

# conf = SparkConf()
# conf.setMaster("local[2]")
# conf.setAppName("test w251")
# sc = SparkContext(conf=conf)
# sc.setLogLevel("OFF")

spark = SparkSession \
    .builder \
    .appName("W251 test") \
    .config("spark.driver.memory", "10g") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.read.csv("hdfs://spark-2-1/spark_out_data/raw/part-00010-ef1e5516-f02e-412b-b8e1-04d14ce2fffb.csv")

df.show()
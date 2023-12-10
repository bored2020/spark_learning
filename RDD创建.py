from pyspark import SparkConf,SparkContext


import pandas as pd


conf = SparkConf().setAppName("create RDD").setMaster("local[*]")




sc = SparkContext(conf=conf)

rdd = sc.parallelize([1,2,3,4,5,6])

print(rdd.getNumPartitions())  # 默认分区数根据核心数量来确定。。



 


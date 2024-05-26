from pyspark import SparkConf,SparkContext


import pandas as pd


conf = SparkConf().setAppName("create RDD").setMaster("local[*]")



pd = pd.DataFrame()



sc = SparkContext(conf=conf)


rdd = sc.parallelize([1,2,3,4,5,6])


print(rdd.getNumPartitions())  # 默认分区数根据核心数量来确定。。


# collect 方法是将RDD中每个分区的数据，都发送到Driver中，形成一个PYTHON list 对象。
 
print(f"RDD的内容:{rdd.collect()}")


rdd.collect()



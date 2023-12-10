from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

from datetime import datetime,date

import pandas as pd 

from pyspark.sql import Row



df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])
df
df.head(2)
type(df)

df = spark.createDataFrame([
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
], schema='a long, b double, c string, d date, e timestamp')
df


df.show(1)
df.show(2)

df.collect()

df.take(2)

df.toPandas()
df.__dict__
df_pd = df.toPandas()

df_pd.head()
df_pd

df_pd.tail(2)

df_pd


data_pd  = pd.DataFrame(df_pd)

data_pd.iloc[:1,]


data_pd.to_latex()


df.head()

df.collect()


df_pdf = df.toPandas()

df_pppdf = pd.DataFrame(df_pdf)

df_pppdf.loc[1]
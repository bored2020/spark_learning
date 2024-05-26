import pandas as pd 

import pyspark
from pyspark.sql.functions import col,pandas_udf 
from pyspark.sql.types import LongType


spark = pyspark.SparkContext()


def cubed(a:pd.Series) -> pd.Series:
    return a*a*a



cubed_udf = pandas_udf(cubed,returnType=LongType())




x = pd.Series([1,2,3])

print(cubed(x))


df = spark.range(1,4)

df
type(df)




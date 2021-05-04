# Load necessary packages

from pyspark import SparkContext, SparkConf, HiveContext
import pyspark.sql.functions as F
import pyspark.sql.types as T
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.functions import *
from pyspark.sql.window import Window


# Load data from app description folder

df_app = spark.read.format('parquet').load('s3a://ada-prod-data/etl/data/ref/bundle/segment/monthly/all/all/').select('app_name','bundle','description')

# df_app.printSchema()
#root
# |-- app_name: string (nullable = true)
# |-- bundle: string (nullable = true)
# |-- description: string (nullable = true)
# |-- genres: array (nullable = true)
# |    |-- element: string (containsNull = true)
# |-- os_platform: string (nullable = true)
# |-- segment: array (nullable = true)
# |    |-- element: string (containsNull = true)
# |-- genre_segment: array (nullable = true)
# |    |-- element: struct (containsNull = true)
# |    |    |-- genres: string (nullable = true)
# |    |    |-- segment: string (nullable = true)

# Load data from the raw etl folder (MY 202101, Month of January, 2021)

path = 's3a://ada-prod-data/etl/data/brq/raw/eskimi/daily/MY/202101*'
df = spark.read.format('parquet').load(path).select('ip','bundle').distinct()

# Join app_description data and ip_addresses associated with these apps

joined_df = df.join(df_app, on='bundle', how='left').cache()

# Filter vpn apps and associated ip addresses by string match on bundle OR app_name OR app app_description

df_v1 = joined_df.select('ip','bundle','app_name','description').where("bundle like '%vpn%'" OR "app_name like '%vpn%'" OR "description like '%vpn%'")

#Tokenize

df_v2 = df_v1.select('bundle',explode('description')).alias('word'))\
   .groupBy('word').count().orderBy('count',ascending=False).show(100, truncate=False)


# words(explode(col('words')).alias('word'))\
 #  .groupBy('word').count().orderBy('count',ascending=False).show(100, truncate=False)

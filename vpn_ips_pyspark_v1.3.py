# Load necessary packages

from pyspark import SparkContext, SparkConf, HiveContext
import pyspark.sql.functions as F
import pyspark.sql.types as T
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.functions import *
from pyspark.sql.window import Window


# Load data from app description folder

df_app = spark.read.format('parquet').load('s3a://ada-prod-data/etl/data/ref/bundle/segment/monthly/all/all/')

df_app = df_app.select('app_name','bundle','description')

# Lowercasing app names and app descriptions

df_app = df_app.withColumn("desc",F.lower(F.col("description")))
df_app = df_app.withColumn("name",F.lower(F.col("app_name")))


# Count VPN appearance in app description

df_app = df_app.withColumn('vpn', lit('vpn')) # lit is the string you want to detect


app_desc = df_app.filter(df_app['desc'].contains(df_app['vpn'])).groupBy('bundle').count()
app_desc = app_desc.withColumnRenamed('count','desc_count')
app_name = df_app.filter(df_app['name'].contains(df_app['vpn'])).groupBy('bundle').count()
app_name = app_name.withColumnRenamed('count','name_count')
join_count = app_desc.join(app_name, on='bundle', how='left')


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
df = spark.read.format('parquet').load(path)
df = df.select('ip','bundle') # Add distinct later in EMR

# Join app_description data and ip_addresses associated with these apps

joined_df = df_app.join(df, on='bundle', how='left')

# Filter vpn apps and associated ip addresses by string match on bundle OR app_name OR app app_description

df_v1 = joined_df.select('ip','bundle','name','desc').where("bundle like '%vpn%' OR name like '%vpn%' OR desc like '%vpn%'")

df_vX = df_v1.join(join_count, on='bundle', how='left')

#Tokenize words in the description column to find how many times "vpn" occures
# in the description


#df_app_desc_wc = df_app.withColumn('desc_word', F.explode(F.split(F.col('desc'), ' '))).filter("desc_word == 'vpn'").groupBy('desc_word','bundle').count().sort('count', ascending=False)
#df_app_name_wc = df_app.withColumn('name_word', F.explode(F.split(F.col('name'), ' '))).filter("name_word == 'vpn'").groupBy('name_word','bundle').count().sort('count', ascending=False)







## Adding tokenized words to data DataFrame

df_v2 = df_v1.join(df_app_desc_wc,on='bundle',how ='left')
df_v3 = df_v1.join(df_app_name_wc, on = 'bundle', how = 'left')

## Write file

df_v3.write.format("parquet").option("compression", "snappy").save('s3a://ada-dev/ishti/vpn_household_X1/')

#df_v2 = df_v1.select('bundle',F.explode('description')).alias('word'))
#df_v3 = df_v2.select('bundle','word').filter("word == 'vpn'" OR "word == 'VPN'")\
#    .groupBy('word').count().orderBy('count',ascending=False).show(100, truncate=False)


#df_app.withColumn('word', F.explode(F.split(F.col('description'), ' '))).groupBy('word').count().sort('count', ascending=False)

# Tokenize app names




# words(explode(col('words')).alias('word'))\
 #  .groupBy('word').count().orderBy('count',ascending=False).show(100, truncate=False)

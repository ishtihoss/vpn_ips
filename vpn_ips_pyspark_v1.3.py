# Load necessary packages

from pyspark import SparkContext, SparkConf, HiveContext
import pyspark.sql.functions as F
import pyspark.sql.types as T
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import split, col, size, regexp_replace
from pyspark.sql.types import *
from pyspark.sql.functions import countDistinct

# Load data from app description folder

df_app = spark.read.format('parquet').load('s3a://ada-prod-data/etl/data/ref/bundle/segment/monthly/all/all/')

df_app = df_app.select('app_name','bundle','description').cache()

# Lowercasing app names and app descriptions

df_app = df_app.withColumn("desc",F.lower(F.col("description")))
df_app = df_app.withColumn("name",F.lower(F.col("app_name")))

# Isolating apps with vpn connections

df_app = df_app.select('bundle','name','desc').where("bundle like '%vpn%' OR name like '%vpn%' OR desc like '%vpn%'").cache()


# Count VPN appearance in app description

df_app = df_app.withColumn('desc_count', size(split(F.col('desc'), "vpn")) - 1)
df_app = df_app.withColumn('name_count', size(split(F.col('name'), "vpn")) - 1)


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

# Load data from the raw etl folder (ADA countries 202101, Month of January, 2021)


#Create an empty dataframe

#field = [StructField('ip', StringType(), True), StructField('bundle', StringType(), True), StructField('Country', StringType(), True)]
#schema = StructType(field)
#dfs = sqlContext.createDataFrame(sc.emptyRDD(), schema)

#countries = ["SG","KR","KH","BD","LK","TH","ID","PH"]

#countries = ["KR","LK"]
countries = ["ID"]
#countries = ["SG","KH"]
#countries = ["BD","ID","PH"]

for i in countries:
    path = 's3a://ada-prod-data/etl/data/brq/raw/eskimi/daily/{}/202101*'.format(i)
    df = spark.read.format('parquet').load(path)
    df = df.select('ip','bundle').distinct() # Add distinct later in EMR
    df = df.withColumn('Country', lit(i))
    joined_df = df_app.join(df, on='bundle', how='left') # Join app_description data and ip_addresses associated with these apps
    joined_df.write.format("parquet").option("compression", "snappy").save('s3a://ada-dev/ishti/vpn_household_c9c/{}'.format(i)) ## Write file


# Check count by Country
path = 's3a://ada-dev/ishti/vpn_household_c9c/*'
df = spark.read.parquet(path)
df.groupBy("Country").agg(countDistinct("ip")).orderBy(desc('count(DISTINCT ip)')).show(10)


# Write coalesced data

df.coalesce(4).write.format("parquet").option("compression", "snappy").save("s3a://ada-dev/ishti/master_r_vpn_parq")

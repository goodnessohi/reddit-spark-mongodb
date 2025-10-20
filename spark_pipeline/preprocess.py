from pyspark import SparkSession
from pyspark.sql.functions import col, concat_ws, lower, regexp_replace


mongo_uri = 'mongodb://localhost:27017/lost_media_db.reddit_posts'
#Initialize Spark
spark = SparkSession.builder \
        .appName('LostMediaPreprocessing') \
        .config('spark.jars', 'jars/mongo-spark-connector_2.12-10.2.0.jar') \
        .config('spark.mongodb.read.connection.uri', mongo_uri) \
        .getOrCreate()

#Reading directly from MongoDB
df =spark.read.format('mongodb').load()

#Combine and clean text
df_clean = df.withColumn('text', concat_ws('', 'title', 'body')) \
            .withColumn('text', lower(col('text'))) \
            .withColumn('text', regexp_replace(col('text'), "[^a-zA-Z0-9\\s]", "")) \
            .select('id', 'text', 'score', 'num_comments', 'created_utc')


df_clean.show(5, truncate=False)

#Write back to MongoDB
df_clean.write.format('mongodb') \
    .mode('overwrite') \
    .option("uri", "mongodb://localhost:27017/lost_media_db.cleaned_posts") \
    .save()
from pyspark.sql import SparkSession

# Updated Connection URI with the created user credentials
MONGO_URI = "mongodb://samirziani:samir5636123@mongodb:27017/sample_mflix"
DATABASE = "sample_mflix"
COLLECTION = "articles"

def get_spark_session():
    """
    Initialize and return a SparkSession with MongoDB connector.
    """
    return (
        SparkSession.builder.appName("MongoDB_Spark")
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .config("spark.mongodb.read.database", DATABASE)
        .config("spark.mongodb.read.collection", COLLECTION)
        .config("spark.jars", "/opt/spark/jars/mongo-spark-connector.jar")
        .getOrCreate()
    )

def load_articles(spark: SparkSession):
    """
    Load articles from MongoDB into a Spark DataFrame.
    """
    return spark.read.format("mongodb").load()

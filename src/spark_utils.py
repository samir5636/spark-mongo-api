from pyspark.sql import SparkSession

# Updated Connection URI with the created user credentials
MONGO_URI = "mongodb+srv://samirziani:samir5636123@cluster0.ghz8l.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
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
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
        .getOrCreate()
    )

def load_articles(spark: SparkSession):
    """
    Load articles from MongoDB into a Spark DataFrame.
    """
    try:
        return spark.read.format("mongodb").load()
        
    except Exception as e:
        raise RuntimeError(f"Failed to load data from MongoDB: {e}")

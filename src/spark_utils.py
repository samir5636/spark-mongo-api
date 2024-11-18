from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower
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
def count_publications_with_filter(spark: SparkSession, filter_column: str, filters: dict, partial_match: bool = False):
    """
    Count the number of publications by a specific filter column and apply additional filters.
    Supports partial matching for the 'keyword' field.
    """
    try:
        articles_df = load_articles(spark)

        # Apply filters from the filters dictionary
        for col_name, col_value in filters.items():
            if col_value is not None:
                if col_name == "keyword" and partial_match:
                    articles_df = articles_df.filter(lower(col(col_name)).contains(col_value.lower()))
                elif isinstance(col_value, str):
                    articles_df = articles_df.filter(lower(col(col_name)) == col_value.lower())
                else:
                    articles_df = articles_df.filter(col(col_name) == col_value)

        # Group by the filter column and count the number of publications
        count_df = (
            articles_df.groupBy(filter_column)
            .count()
            .withColumnRenamed("count", "num_pub")
            .orderBy(filter_column)
        )

        return count_df.toPandas().to_dict(orient="records")
    except Exception as e:
        raise RuntimeError(f"Failed to count publications by {filter_column} with filters: {e}")


    
def count_publications_by_column(spark: SparkSession, column_name: str):
    """
    Count the number of publications grouped by a specific column.
    """
    try:
        articles_df = load_articles(spark)
        count_df = (
            articles_df.groupBy(column_name)
            .count()
            .withColumnRenamed("count", "num_pub")
            .orderBy(column_name)
        )
        return count_df.toPandas().to_dict(orient="records")
    except Exception as e:
        raise RuntimeError(f"Failed to count publications by {column_name}: {e}")

def load_articles(spark: SparkSession):
    """
    Load articles from MongoDB into a Spark DataFrame.
    """
    try:
        return spark.read.format("mongodb").load()
        
    except Exception as e:
        raise RuntimeError(f"Failed to load data from MongoDB: {e}")

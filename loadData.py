from fastapi import FastAPI
from pyspark.sql import SparkSession
from pyspark.sql.utils import Py4JJavaError
from pydantic import BaseModel
from typing import List

# FastAPI app initialization
app = FastAPI()

# MongoDB configuration
MONGO_URI = "mongodb+srv://samirziani:samir5636123@cluster0.ghz8l.mongodb.net/"
DATABASE = "sample_mflix"
COLLECTION = "articles"

# Initialize Spark session with MongoDB connector
def get_spark_session():
    return (
        SparkSession.builder.appName("MongoDBTest")
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .config("spark.mongodb.read.database", DATABASE)
        .config("spark.mongodb.read.collection", COLLECTION)
        .getOrCreate()
    )

# Data model for the API response
class Article(BaseModel):
    title: str
    content: str

# Test MongoDB connection route
@app.get("/test-mongo", response_model=List[Article])
async def test_mongo():
    try:
        spark = get_spark_session()
        # Load data from MongoDB into a Spark DataFrame
        df = spark.read.format("mongodb").load()

        # Select specific columns and convert to Pandas for easier handling
        articles = df.select("title", "content").toPandas()

        # Convert the Pandas DataFrame to a list of dictionaries
        response = articles.to_dict(orient="records")

        # Return the response
        return response

    except Py4JJavaError as e:
        return {"error": "An error occurred while reading from MongoDB", "details": str(e)}
    except Exception as e:
        return {"error": "An unexpected error occurred", "details": str(e)}

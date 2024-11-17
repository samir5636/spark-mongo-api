from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Dict
from pyspark.sql import SparkSession
from traceback import format_exc

# Environment variables (hardcoded directly in the script)
MONGO_URI = "mongodb://samirziani:samir5636123@cluster0-shard-00-00.mongodb.net:27017,cluster0-shard-00-01.mongodb.net:27017,cluster0-shard-00-02.mongodb.net:27017/?ssl=true&replicaSet=atlas-xxx-shard-0&authSource=admin&retryWrites=true&w=majority"
DATABASE_NAME = "sample_mflix"
COLLECTION_NAME = "articles"
SPARK_MASTER = "spark://spark-master:7077"
SPARK_MONGO_URI = f"{MONGO_URI}{DATABASE_NAME}.{COLLECTION_NAME}"

# Initialize FastAPI app
app = FastAPI()

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Spark-MongoDB Integration API") \
    .master(SPARK_MASTER) \
    .config("spark.mongodb.input.uri", SPARK_MONGO_URI) \
    .config("spark.mongodb.output.uri", SPARK_MONGO_URI) \
    .config("spark.mongodb.read.connection.sslEnabled", "true") \
    .config("spark.mongodb.write.connection.sslEnabled", "true") \
    .getOrCreate()

# Pydantic models for response schemas
class TestMongoResponse(BaseModel):
    data: List[Dict]

class TestSparkResponse(BaseModel):
    result: List[Dict]

class GetDocumentsResponse(BaseModel):
    documents: List[Dict]

class RootResponse(BaseModel):
    message: str

# Root endpoint to check API status
@app.get("/", response_model=RootResponse, summary="Root Endpoint", description="Check if the FastAPI server is running.")
def root():
    return {"message": "FastAPI is running"}

# Endpoint to test MongoDB connection
@app.get("/test-mongo", response_model=TestMongoResponse, summary="Test MongoDB Connection", description="Tests the connection to MongoDB using Spark.")
def test_mongo():
    try:
        # Read data from MongoDB into Spark DataFrame
        df = spark.read.format("mongodb").load()
        data = df.limit(10).toPandas().to_dict(orient="records")
        return {"data": data}
    except Exception as e:
        return {"error": str(e), "traceback": format_exc()}

# Endpoint to test Spark functionality
@app.get("/test-spark", response_model=TestSparkResponse, summary="Test Spark Functionality", description="Runs a simple Spark job to filter rows based on age.")
def test_spark():
    try:
        # Create a sample DataFrame
        data = [("Alice", 29), ("Bob", 24), ("Charlie", 35)]
        columns = ["Name", "Age"]
        df = spark.createDataFrame(data, columns)
        
        # Filter where age > 25
        result = df.filter(df.Age > 25).collect()
        return {"result": [row.asDict() for row in result]}
    except Exception as e:
        return {"error": str(e), "traceback": format_exc()}

# Endpoint to retrieve specific documents from MongoDB
@app.get("/get-documents", response_model=GetDocumentsResponse, summary="Get MongoDB Documents", description="Retrieves documents with specific fields from the MongoDB collection.")
def get_documents():
    try:
        # Read data from MongoDB collection
        df = spark.read.format("mongodb").load()
        
        # Select specific fields
        selected_columns = ["title", "author", "university", "country", "keyword", "year", "month", "journal", "doi", "issn", "quartile"]
        df_selected = df.select(*selected_columns)
        
        # Convert to Pandas and then JSON for response
        data = df_selected.limit(10).toPandas().to_dict(orient="records")
        return {"documents": data}
    except Exception as e:
        return {"error": str(e), "traceback": format_exc()}

# Graceful shutdown of SparkSession
@app.on_event("shutdown")
def shutdown_event():
    spark.stop()

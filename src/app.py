from fastapi import FastAPI, HTTPException
from src.spark_utils import get_spark_session, load_articles
from pyspark.sql.functions import col

app = FastAPI()

# Initialize Spark session
spark = get_spark_session()

@app.get("/articles")
async def fetch_all_articles():
    """
    Fetch all articles from MongoDB via Spark.
    """
    try:
        articles_df = load_articles(spark)
        articles = articles_df.toPandas().to_dict(orient="records")  # Convert to list of dicts
        return articles
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/articles/keyword/{keyword}")
async def fetch_articles_by_keyword(keyword: str):
    """
    Fetch articles by keyword using Spark DataFrame filtering.
    """
    try:
        articles_df = load_articles(spark)
        filtered_df = articles_df.filter(col("keyword").contains(keyword))
        articles = filtered_df.toPandas().to_dict(orient="records")
        if not articles:
            raise HTTPException(status_code=404, detail="No articles found with this keyword.")
        return articles
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/articles/date")
async def fetch_articles_by_date(year: int, month: str = None):
    """
    Fetch articles by year and optional month using Spark DataFrame filtering.
    """
    try:
        articles_df = load_articles(spark)
        filtered_df = articles_df.filter(col("year") == year)
        if month:
            filtered_df = filtered_df.filter(col("month") == month)
        articles = filtered_df.toPandas().to_dict(orient="records")
        if not articles:
            raise HTTPException(status_code=404, detail="No articles found for the given date.")
        return articles
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

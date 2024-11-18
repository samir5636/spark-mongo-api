from fastapi import APIRouter, HTTPException
from src.spark_utils import get_spark_session, load_articles
from pyspark.sql.functions import col, lower

router = APIRouter()
spark = get_spark_session()

@router.get("/articles/month/{month}")
async def fetch_articles_by_month(month: str):
    """
    Fetch articles by month using a case-insensitive match.
    """
    try:
        # Convert input month to lowercase
        month = month.lower()

        # Load articles and filter by month in lowercase
        articles_df = load_articles(spark)
        filtered_df = articles_df.filter(lower(col("month")) == month)
        
        articles = filtered_df.toPandas().to_dict(orient="records")
        if not articles:
            raise HTTPException(status_code=404, detail="No articles found for the given month.")
        return articles
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

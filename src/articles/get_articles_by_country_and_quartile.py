from fastapi import APIRouter, HTTPException
from src.spark_utils import get_spark_session, load_articles
from pyspark.sql.functions import col, lower

router = APIRouter()
spark = get_spark_session()

@router.get("/articles/country/{country}/quartile/{quartile}")
async def fetch_articles_by_country_and_quartile(country: str, quartile: str):
    """
    Fetch articles by country and quartile using case-insensitive matching.
    """
    try:
        country = country.lower()
        quartile = quartile.upper()  # Assuming quartile is usually denoted as Q1, Q2, etc.

        articles_df = load_articles(spark)
        filtered_df = articles_df.filter((lower(col("country")) == country) & (col("quartile") == quartile))

        articles = filtered_df.toPandas().to_dict(orient="records")
        if not articles:
            raise HTTPException(status_code=404, detail="No articles found for the given country and quartile.")
        return articles
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

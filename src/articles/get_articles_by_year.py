from fastapi import APIRouter, HTTPException
from src.spark_utils import get_spark_session, load_articles
from pyspark.sql.functions import col

router = APIRouter()
spark = get_spark_session()

@router.get("/articles/year/{year}")
async def fetch_articles_by_year(year: int):
    try:
        articles_df = load_articles(spark)
        filtered_df = articles_df.filter(col("year") == year)
        articles = filtered_df.toPandas().to_dict(orient="records")
        if not articles:
            raise HTTPException(status_code=404, detail="No articles found for the given year.")
        return articles
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

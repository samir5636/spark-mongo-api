from fastapi import APIRouter, HTTPException
from src.spark_utils import get_spark_session, load_articles
from pyspark.sql.functions import col, lower

router = APIRouter()
spark = get_spark_session()

@router.get("/articles/keyword/{keyword}")
async def fetch_articles_by_keyword(keyword: str):
    """
    Fetch articles by keyword using a case-insensitive match.
    """
    try:
        keyword = keyword.lower()
        articles_df = load_articles(spark)
        filtered_df = articles_df.filter(lower(col("keyword")).contains(keyword))

        articles = filtered_df.toPandas().to_dict(orient="records")
        if not articles:
            raise HTTPException(status_code=404, detail="No articles found with this keyword.")
        return articles
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

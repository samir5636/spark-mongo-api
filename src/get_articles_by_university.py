from fastapi import APIRouter, HTTPException
from src.spark_utils import get_spark_session, load_articles
from pyspark.sql.functions import col, lower

router = APIRouter()
spark = get_spark_session()

@router.get("/articles/university/{university}")
async def fetch_articles_by_university(university: str):
    """
    Fetch articles by university using a case-insensitive match.
    """
    try:
        university = university.lower()
        articles_df = load_articles(spark)
        filtered_df = articles_df.filter(lower(col("university")) == university)

        articles = filtered_df.toPandas().to_dict(orient="records")
        if not articles:
            raise HTTPException(status_code=404, detail="No articles found for the given university.")
        return articles
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

from fastapi import APIRouter, HTTPException, Query
from src.spark_utils import get_spark_session, load_articles
from pyspark.sql.functions import col, lower

router = APIRouter()
spark = get_spark_session()

@router.get("/publications/search")
async def get_publications_with_filters(
    year: int = Query(None, description="Year of publication"),
    keyword: str = Query(None, description="Keyword in publication"),
    country: str = Query(None, description="Country of publication"),
    quartile: str = Query(None, description="Quartile (e.g., Q1, Q2)"),
    month: str = Query(None, description="Month of publication"),
    university: str = Query(None, description="University of author")
):
    """
    Get publications filtered by various parameters.
    """
    try:
        articles_df = load_articles(spark)

        # Apply filters if parameters are provided
        if year is not None:
            articles_df = articles_df.filter(col("year") == year)
        if keyword is not None:
            articles_df = articles_df.filter(lower(col("keyword")).contains(keyword.lower()))
        if country is not None:
            articles_df = articles_df.filter(lower(col("country")) == country.lower())
        if quartile is not None:
            articles_df = articles_df.filter(col("quartile") == quartile.upper())
        if month is not None:
            articles_df = articles_df.filter(lower(col("month")) == month.lower())
        if university is not None:
            articles_df = articles_df.filter(lower(col("university")) == university.lower())

        articles = articles_df.toPandas().to_dict(orient="records")
        if not articles:
            raise HTTPException(status_code=404, detail="No publications found for the given filters.")
        return articles
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

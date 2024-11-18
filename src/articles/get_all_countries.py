from fastapi import APIRouter, HTTPException
from src.spark_utils import get_spark_session, load_articles

router = APIRouter()
spark = get_spark_session()

@router.get("/articles/countries")
async def fetch_all_countries():
    try:
        articles_df = load_articles(spark)
        countries = articles_df.select("country").distinct().toPandas()["country"].tolist()
        if not countries:
            raise HTTPException(status_code=404, detail="No countries found.")
        return countries
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

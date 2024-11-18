from fastapi import APIRouter, HTTPException
from src.spark_utils import get_spark_session, count_publications_by_column

router = APIRouter()
spark = get_spark_session()

@router.get("/publications/quartile")
async def get_publications_by_quartile():
    """
    Get the number of publications by quartile.
    """
    try:
        result = count_publications_by_column(spark, "quartile")
        if not result:
            raise HTTPException(status_code=404, detail="No publication data found.")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

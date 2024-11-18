from fastapi import APIRouter, HTTPException
from src.spark_utils import get_spark_session, count_publications_by_column

router = APIRouter()
spark = get_spark_session()

@router.get("/publications/university")
async def get_publications_by_university():
    """
    Get the number of publications by university.
    """
    try:
        result = count_publications_by_column(spark, "university")
        if not result:
            raise HTTPException(status_code=404, detail="No publication data found.")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

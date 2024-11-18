from fastapi import APIRouter, HTTPException, Query
from src.spark_utils import get_spark_session, count_publications_with_filter

router = APIRouter()
spark = get_spark_session()

@router.get("/publications/count")
async def get_publications_count(
    year: int = Query(None, description="Filter by year"),
    keyword: str = Query(None, description="Filter by keyword (supports partial match)"),
    country: str = Query(None, description="Filter by country"),
    quartile: str = Query(None, description="Filter by quartile"),
    month: str = Query(None, description="Filter by month"),
    university: str = Query(None, description="Filter by university")
):
    """
    Get the number of publications grouped by a detected parameter with optional filters.
    Supports partial match for keyword.
    """
    try:
        # Determine which parameter to group by based on user input
        group_by = None
        if year is not None:
            group_by = "year"
        elif keyword is not None:
            group_by = "keyword"
        elif country is not None:
            group_by = "country"
        elif quartile is not None:
            group_by = "quartile"
        elif month is not None:
            group_by = "month"
        elif university is not None:
            group_by = "university"

        if group_by is None:
            raise HTTPException(status_code=400, detail="At least one parameter must be provided to group by.")

        # Collect filters as a dictionary
        filters = {
            "year": year,
            "keyword": keyword,
            "country": country,
            "quartile": quartile,
            "month": month,
            "university": university
        }

        # Enable partial match for 'keyword' filter
        partial_match = True if keyword is not None else False

        result = count_publications_with_filter(spark, group_by, filters, partial_match=partial_match)
        if not result:
            raise HTTPException(status_code=404, detail="No publication data found for the given filters.")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

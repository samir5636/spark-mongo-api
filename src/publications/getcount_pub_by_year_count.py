from fastapi import APIRouter, HTTPException
from src.spark_utils import get_spark_session, count_publications_by_column

router = APIRouter()
spark = get_spark_session()

@router.get("/publications/count_by_year")
async def get_publications_count_by_year():
    """
    Get the number of publications grouped by year.
    Returns a list of dictionaries with year and publication count.
    """
    try:
        # Use the existing count_publications_by_column function to get publication counts
        result = count_publications_by_column(spark, "year")
        
        # Rename the dictionary keys to match the desired output format
        formatted_result = [
            {
                "year": str(entry["year"]),  # Convert to string
                "nombre_publications": entry["num_pub"]
            } 
            for entry in result
        ]
        
        # Sort the results by year
        formatted_result.sort(key=lambda x: int(x["year"]))
        
        if not formatted_result:
            raise HTTPException(status_code=404, detail="No publication data found.")
        
        return formatted_result
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
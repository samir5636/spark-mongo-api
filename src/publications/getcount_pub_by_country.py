from fastapi import APIRouter, HTTPException
from src.spark_utils import get_spark_session, count_publications_by_column

router = APIRouter()
spark = get_spark_session()

@router.get("/publications/count_by_country")
async def get_publications_count_by_country():
    """
    Get the number of publications grouped by country.
    Returns a list of dictionaries with country and publication count.
    """
    try:
        # Use the existing count_publications_by_column function to get publication counts
        result = count_publications_by_column(spark, "country")
        
        # Rename the dictionary keys to match the desired output format
        formatted_result = [
            {
                "country": str(entry["country"]) if entry["country"] is not None else "Unknown",
                "nombre_publications": entry["num_pub"]
            } 
            for entry in result
        ]
        
        # Sort the results by publication count in descending order
        formatted_result.sort(key=lambda x: x["nombre_publications"], reverse=True)
        
        if not formatted_result:
            raise HTTPException(status_code=404, detail="No publication data found.")
        
        return formatted_result
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
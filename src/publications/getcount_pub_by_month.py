from fastapi import APIRouter, HTTPException
from src.spark_utils import get_spark_session, count_publications_by_column

router = APIRouter()
spark = get_spark_session()

# Define a custom month order to ensure chronological sorting
MONTH_ORDER = {
    'January': 1, 'February': 2, 'March': 3, 'April': 4, 
    'May': 5, 'June': 6, 'July': 7, 'August': 8, 
    'September': 9, 'October': 10, 'November': 11, 'December': 12
}

@router.get("/publications/count_by_month")
async def get_publications_count_by_month():
    """
    Get the number of publications grouped by month.
    Returns a list of dictionaries with month and publication count.
    """
    try:
        # Use the existing count_publications_by_column function to get publication counts
        result = count_publications_by_column(spark, "month")
        
        # Rename the dictionary keys and handle potential None values
        formatted_result = [
            {
                "month": str(entry["month"]) if entry["month"] is not None else "Unknown",
                "nombre_publications": entry["num_pub"]
            } 
            for entry in result if entry["month"] is not None
        ]
        
        # Sort the results by the predefined month order
        formatted_result.sort(key=lambda x: MONTH_ORDER.get(x["month"], 99))
        
        if not formatted_result:
            raise HTTPException(status_code=404, detail="No publication data found.")
        
        return formatted_result
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
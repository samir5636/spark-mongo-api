from fastapi import FastAPI
from src.get_articles_by_year import router as year_router
from src.get_articles_by_quartile import router as quartile_router
from src.get_articles_by_month import router as month_router
from src.get_all_countries import router as countries_router

app = FastAPI()

app.include_router(year_router, prefix="/api")
app.include_router(quartile_router, prefix="/api")
app.include_router(month_router, prefix="/api")
app.include_router(countries_router, prefix="/api")

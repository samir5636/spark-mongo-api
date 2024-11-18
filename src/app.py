from fastapi import FastAPI
from src.get_articles_by_year import router as year_router
from src.get_articles_by_quartile import router as quartile_router
from src.get_articles_by_month import router as month_router
from src.get_all_countries import router as countries_router
from src.get_articles_by_keyword import router as keyword_router
from src.get_articles_by_country import router as country_router
from src.get_articles_by_university import router as university_router
from src.get_articles_by_country_and_quartile import router as country_quartile_router

app = FastAPI()

app.include_router(year_router, prefix="/api")
app.include_router(quartile_router, prefix="/api")
app.include_router(month_router, prefix="/api")
app.include_router(countries_router, prefix="/api")
app.include_router(keyword_router, prefix="/api")
app.include_router(country_router, prefix="/api")
app.include_router(university_router, prefix="/api")
app.include_router(country_quartile_router, prefix="/api")

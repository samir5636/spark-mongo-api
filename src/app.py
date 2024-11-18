from fastapi import FastAPI
from src.articles.get_articles_by_year import router as year_router
from src.articles.get_articles_by_quartile import router as quartile_router
from src.articles.get_articles_by_month import router as month_router
from src.articles.get_all_countries import router as countries_router
from src.articles.get_articles_by_keyword import router as keyword_router
from src.articles.get_articles_by_country import router as country_router
from src.articles.get_articles_by_university import router as university_router
from src.articles.get_articles_by_country_and_quartile import router as country_quartile_router
from src.publications.get_publications_by_year import router as pub_year_router
from src.publications.get_publications_by_keyword import router as pub_keyword_router
from src.publications.get_publications_by_quartile import router as pub_quartile_router
from src.publications.get_publications_by_university import router as pub_university_router
from src.publications.get_publications_by_month import router as pub_month_router
from src.publications.get_publications_with_filters import router as pub_search_router
from src.publications.get_publications_count_by_parameter import router as pub_count_router
app = FastAPI()

app.include_router(year_router, prefix="/api")
app.include_router(quartile_router, prefix="/api")
app.include_router(month_router, prefix="/api")
app.include_router(countries_router, prefix="/api")
app.include_router(keyword_router, prefix="/api")
app.include_router(country_router, prefix="/api")
app.include_router(university_router, prefix="/api")
app.include_router(country_quartile_router, prefix="/api")
app.include_router(pub_year_router, prefix="/api")
#for get all publications count not filtred
app.include_router(pub_year_router, prefix="/api")
app.include_router(pub_keyword_router, prefix="/api")
app.include_router(pub_quartile_router, prefix="/api")
app.include_router(pub_university_router, prefix="/api")
app.include_router(pub_month_router, prefix="/api")
app.include_router(pub_search_router, prefix="/api")
#count number of publications by parameter filter
app.include_router(pub_count_router, prefix="/api")
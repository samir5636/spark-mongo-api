from pydantic import BaseModel
from typing import Optional

class Article(BaseModel):
    title: str
    author: Optional[str]
    university: Optional[str]
    country: Optional[str]
    keyword: Optional[str]
    year: Optional[int]
    month: Optional[str]
    journal: Optional[str]
    doi: Optional[str]
    issn: Optional[str]
    quartile: Optional[str]

class ErrorResponse(BaseModel):
    error: str

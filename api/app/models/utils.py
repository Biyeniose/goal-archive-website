from typing import Optional
from pydantic import BaseModel


class Country(BaseModel):
    country_id: int
    name: str
    flag_url: Optional[str] = None
    continent: Optional[str] = None
    iso_code_3: Optional[str] = None

class Manager(BaseModel):
    manager_id: int
    name: str
    country: Optional[Country] = None
    
class Referee(BaseModel):
    referee_id: int
    name: str
    country: Optional[Country] = None
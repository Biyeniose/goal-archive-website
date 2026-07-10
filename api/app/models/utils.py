from typing import List, Optional
from pydantic import BaseModel


class Country(BaseModel):
    country_id: int
    name: str
    flag_url: Optional[str] = None
    circle_url: Optional[str] = None
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


class PlayerCountries(BaseModel):
    country1: Optional[Country] = None
    country2: Optional[Country] = None


class Player(BaseModel):
    player_name: str
    player_id: int
    age: Optional[int] = None
    tfm_pic_url: Optional[str] = None
    pic_url: Optional[str] = None
    pixel_pic_url: Optional[str] = None
    position: Optional[str] = None
    other_positions: List[str] = []
    countries: PlayerCountries

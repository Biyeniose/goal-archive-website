from typing import List, Optional
from pydantic import BaseModel

from .utils import Country


class PlayerCountries(BaseModel):
    country1: Optional[Country] = None
    country2: Optional[Country] = None


class Player(BaseModel):
    player_name: str
    player_id: int
    age: Optional[int] = None
    tfm_pic_url: Optional[str] = None
    pic_url: Optional[str] = None
    position: Optional[str] = None
    other_positions: List[str] = []
    countries: PlayerCountries


class PlayerSearchResponse(BaseModel):
    data: List[Player]


class PlayerDetailResponse(BaseModel):
    data: Player

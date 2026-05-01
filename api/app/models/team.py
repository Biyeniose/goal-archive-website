from typing import List, Optional
from pydantic import BaseModel

from .utils import Country


class Team(BaseModel):
    team_id: int
    team_name: Optional[str] = None
    common_name: Optional[str] = None
    short_name: Optional[str] = None
    logo_url: Optional[str] = None
    level: Optional[str] = None
    type: Optional[str] = None
    country: Optional[Country] = None


class TeamSearchResponse(BaseModel):
    data: List[Team]


class TeamResponse(BaseModel):
    data: Team


class Transfer(BaseModel):
    buying_team: Team
    selling_team: Optional[Team] = None
    isloan: Optional[bool] = None
    fee: Optional[float] = None
    player_value: Optional[float] = None
    transfer_date: str
    season: Optional[int] = None
    season_str: Optional[str] = None



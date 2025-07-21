# app/models/league.py
from typing import List, Dict, Optional
from pydantic import BaseModel
from app.models.team import Team
    

#######################
class League(BaseModel):
    league_id: int
    league_name: str
    logo: Optional[str]

class TeamRank(BaseModel):
    team: Team
    rank: str
    info: Optional[str]
    points: int
    gp: int
    gd: int
    wins: int
    losses: int
    draws: int
    goals_f: int
    goals_a: int

class TopLeaguesInfo(BaseModel):
    comp_id: int
    league_name: str
    country_id: Optional[int]
    country_url: Optional[str]
    type: Optional[str]
    ranks: List[TeamRank]


class TopLeaguesResponse(BaseModel):
    data: List[TopLeaguesInfo]

class LeagueInfo(BaseModel):
    comp_id: int
    league_name: str
    country_id: int
    league_logo: Optional[str]
    type: str
    country_url: str
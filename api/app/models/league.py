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

class LeagueInfo(BaseModel):
    comp_id: int
    league_name: str
    country_id: Optional[int] = None
    country: Optional[str] = None
    league_logo: Optional[str] = None
    type: Optional[str] = None
    country_url: Optional[str] = None

class Comp(BaseModel):
    comp_id: int
    comp_name: str
    comp_url: Optional[str]



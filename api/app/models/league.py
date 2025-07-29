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
    rank: Optional[str]= None
    info: Optional[str] = None
    points: Optional[int] = None
    gp: Optional[int] = None
    gd: Optional[int] = None
    wins: Optional[int] = None
    losses: Optional[int] = None
    draws: Optional[int] = None
    goals_f: Optional[int] = None
    goals_a: Optional[int] = None

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



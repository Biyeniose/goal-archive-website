# app/models/league.py
from typing import List, Dict, Optional
from pydantic import BaseModel
from .match import MatchData

class TeamRank(BaseModel):
    rank: str
    team_id: int
    team_name: str
    team_logo: str
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

class LeagueData(BaseModel):
    info: LeagueInfo
    ranks: List[TeamRank]
    matches: List[MatchData]

class LeagueDataResponse(BaseModel):
    data: List[LeagueData]
    
class PastSeasonRanks(BaseModel):
    past_ranks: List[TeamRank]

class PastSeasonRanksResponse(BaseModel):
    data: PastSeasonRanks
from datetime import date
from typing import List, Optional
from pydantic import BaseModel

from .match import MatchTeamStats, Stadium
from .utils import Country, PlayerCountries

# teams
class Team(BaseModel):
    team_id: int
    name: Optional[str] = None
    common_name: Optional[str] = None
    logo_url: Optional[str] = None
    type: Optional[str] = None
    level: Optional[str] = None
    country: Optional[Country] = None

# players
class Player(BaseModel):
    player_name: str
    player_id: int
    age: Optional[int] = None
    dob: Optional[date] = None
    tfm_pic_url: Optional[str] = None
    pic_url: Optional[str] = None
    position: Optional[str] = None
    other_positions: List[str] = []
    countries: PlayerCountries

# matches
class MatchPlayer(BaseModel):
    player: Player
    match_id: int
    team_id: int
    position: Optional[str] = None
    number: Optional[int] = None
    started: Optional[bool] = None
    subbed_on: Optional[bool] = None
    subbed_off: Optional[bool] = None
    minutes: Optional[int] = None
    goals: Optional[int] = None
    assists: Optional[int] = None
    goals_assists: Optional[int] = None
    pens_made: Optional[int] = None
    pens_att: Optional[int] = None
    xg: Optional[float] = None
    xg_assist: Optional[float] = None
    xga: Optional[float] = None
    shots: Optional[int] = None
    headed_shots: Optional[int] = None
    touches: Optional[int] = None
    blocks: Optional[int] = None
    succ_dribbles: Optional[int] = None


class Match(BaseModel):
    match_id: int
    match_date: Optional[str] = None
    match_time_utc: Optional[str] = None
    home_team: Team
    away_team: Team
    win_team_id: Optional[int] = None
    loss_team_id: Optional[int] = None
    isdraw: Optional[bool] = None
    pens: Optional[bool] = None
    extra_time: Optional[bool] = None
    is_neutral: Optional[bool] = None
    isplayed: Optional[bool] = None
    is_live: Optional[bool] = None
    round: Optional[str] = None
    gameweek_number: Optional[int] = None
    stadium: Optional[Stadium] = None
    home_players: Optional[List[MatchPlayer]] = None
    home_stats: Optional[MatchTeamStats] = None
    away_players: Optional[List[MatchPlayer]] = None
    away_stats: Optional[MatchTeamStats] = None


    
# response models
class SearchTeam(BaseModel):
    data: List[Team]

class SearchPlayer(BaseModel):
    data: List[Player]

class GetTeamMatches(BaseModel):
    data: Optional[List[Match]]



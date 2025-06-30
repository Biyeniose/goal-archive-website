from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from datetime import date

class NationInfo(BaseModel):
    team_id: Optional[int]
    team_name: Optional[str]
    team_url: Optional[str]
    nation: Optional[str]
    nation_url: Optional[str]

class Transfer(BaseModel):
    transfer_id: int
    player_id: int
    player_name: str
    from_team: NationInfo  # Reusing same structure as NationInfo
    to_team: NationInfo    # Reusing same structure as NationInfo
    isLoan: bool
    fee: Optional[int]
    value: Optional[int]
    date: date
    season: str

class MatchTeam(BaseModel):
    team_name: str
    team_id: int
    team_logo: str
    goals: Optional[int]

class GoalData(BaseModel):
    event_id: int
    match_id: int
    match_date: date
    player_id: int
    player_name: str
    event_type: str
    minute: int
    add_minute: Optional[int]
    isgoalscorer: bool  # Note lowercase to match response
    home_team: MatchTeam
    away_team: MatchTeam
    total_match_goals: int
    total_match_assists: int

class PlayerStats(BaseModel):
    comp_id: int
    comp_name: str
    comp_url: Optional[str]
    player_id: int
    season_year: int
    player_name: str
    age: int
    team_id: int
    team_name: str
    team_logo: str
    ga: int
    goals: int
    assists: int
    penalty_goals: int
    gp: int
    minutes: int
    subbed_on: Optional[int]
    subbed_off: Optional[int]
    yellows: Optional[int]
    yellows2: Optional[int]
    reds: Optional[int]
    stats_id: int

class PlayerNations(BaseModel):
    nation1_id: Optional[int]
    nation2_id: Optional[int]
    nation1: Optional[str]
    nation2: Optional[str]
    nation1_url: Optional[str]
    nation2_url: Optional[str]

class PlayerInfo(BaseModel):
    player_id: int
    player_name: str
    full_name: Optional[str] = None
    pic_url: Optional[str] = None
    isRetired: bool
    curr_team_id: Optional[int]
    curr_team_name: Optional[str]
    curr_team_logo: Optional[str]
    curr_number: Optional[int]
    onLoan: Optional[bool] = None
    instagram: Optional[str]
    parent_team_id: Optional[int]
    parent_team_name: Optional[str]
    parent_team_logo: Optional[str]
    position: Optional[str]
    dob: date
    age: int
    pob: Optional[str]
    nations: Optional[PlayerNations] = None
    market_value: Optional[int]
    height: Optional[float]
    foot: Optional[str]
    date_joined: Optional[date]
    contract_end: Optional[date]
    last_extension: Optional[date]
    parent_club_exp: Optional[date]
    noClub: Optional[bool] = None

class PlayerPageData(BaseModel):
    info: PlayerInfo
    transfers: List[Transfer]
    goal_data: List[GoalData]
    stats: List[PlayerStats]

class PlayerPageDataResponse(BaseModel):
    data: PlayerPageData

class SquadPlayer(BaseModel):
    squad_id: int
    player_id: int
    player_name: str
    pic_url: str
    team_id: int
    number: Optional[int]
    age: int
    position: str
    nations: NationInfo
    contract_end: Optional[date]
    stats: PlayerStats
    season_year: int



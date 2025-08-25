# app/models/team.py
from typing import List, Dict, Optional
from pydantic import BaseModel

# Info section models
class TeamInfo(BaseModel):
    team_id: int
    team_name: str
    team_name2: Optional[str]
    logo_url: Optional[str]
    curr_league_id: int
    current_league_name: Optional[str]
    nation_id: Optional[int]
    nation_url: Optional[str]
    city: Optional[str] = None
    stadium: Optional[str] = None

class TeamCommonInfo(BaseModel):
    team_id: int
    team_name: str
    team_url: Optional[str] = None
    nation: Optional[str] = None
    nation_url: Optional[str] = None

class PlayerNations(BaseModel):
    nation1_id: Optional[int]
    nation2_id: Optional[int]
    nation1: Optional[str]
    nation2: Optional[str]
    nation1_url: Optional[str]
    nation2_url: Optional[str]

# Matches section models
class MatchTeam(BaseModel):
    team_name: str
    team_id: int
    team_logo: Optional[str]
    goals: Optional[int] = None

class MatchResult(BaseModel):
    result: Optional[str] = None
    win_team: Optional[int]
    loss_team: Optional[int]
    isDraw: Optional[bool]

class MatchDetails(BaseModel):
    extra_time: Optional[bool]
    pens: Optional[bool]
    pen_home_goals: Optional[int]
    pen_away_goals: Optional[int]

class Match(BaseModel):
    match_id: int
    comp_id: int
    comp_name: Optional[str]
    comp_url: Optional[str]
    home_team: MatchTeam
    away_team: MatchTeam
    result: MatchResult
    round: Optional[str]
    match_date: Optional[str]
    match_time: Optional[str]
    details: MatchDetails


# Transfers section models
class TransferTeam(BaseModel):
    team_id: int
    team_name: str
    team_url: Optional[str]
    nation: Optional[str]
    nation_url: Optional[str]

class Transfer(BaseModel):
    transfer_id: int
    player_id: int
    player_name: str
    from_team: TransferTeam
    to_team: TransferTeam
    isLoan: bool
    fee: Optional[float]
    value: Optional[float]
    date: Optional[str]
    season: str

class SquadStats(BaseModel):
    ga: Optional[int]
    goals: Optional[int]
    assists: Optional[int]
    penalty_goals: Optional[int]
    gp: Optional[int]
    minutes: Optional[int]
    subbed_on: Optional[int]
    subbed_off: Optional[int]
    yellows: Optional[int]
    yellows2: Optional[int]
    reds: Optional[int]

class SquadPlayer(BaseModel):
    squad_id: int
    player_id: int
    player_name: str
    pic_url: Optional[str]
    team_id: int
    number: Optional[int]
    age: Optional[int]
    position: Optional[str]
    nations: Optional[PlayerNations]
    contract_end: Optional[str]
    stats: SquadStats
    season_year: int


####################

class TeamNation(BaseModel):
    nation_id: int
    nation_name: str
    logo_url: Optional[str]
    
class TeamBasicInfo(BaseModel):
    team_id: int
    team_name: str
    team_logo: Optional[str]
    league_id: Optional[int]


class Team(BaseModel):
    team_id: int
    team_name: str
    logo: Optional[str] = None







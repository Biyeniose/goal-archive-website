from typing import List, Optional
from pydantic import BaseModel

from .player import Player

from .league import Competition, League
from .utils import Country, Manager, Referee

from .team import Team



class Match(BaseModel):
    match_id: int
    match_date: Optional[str] = None
    match_time_utc: Optional[str] = None
    home_team: Team
    home_goals: Optional[int] = None
    pen_home_goals: Optional[int] = None
    away_team: Team
    away_goals: Optional[int] = None
    away_home_goals: Optional[int] = None 
    win_team_id: Optional[int] = None
    loss_team_id: Optional[int] = None     
    isdraw: Optional[bool]
    pens: Optional[bool]
    extra_time: Optional[bool]       
    is_neutral: Optional[bool] = None
    isplayed: Optional[bool] = None
    is_live: Optional[bool] = None
    match_minute: Optional[str] = None
    round: Optional[str] = None
    gameweek_number: Optional[int] = None

class MatchesByComp(BaseModel):
    competition: Competition
    matches: List[Match]

class MatchesByDateResponse(BaseModel):
    data: List[MatchesByComp]

# match details        
class PlayerMatchStats(BaseModel):
    player: Player
    team_id: int
    position: Optional[str]
    number: Optional[int]
    started: Optional[bool]
    subbed_on: Optional[bool]
    subbed_off: Optional[bool]
    minutes: Optional[int]
    goals: Optional[int]
    assists: Optional[int]
    goals_assists: Optional[int]
    pens_made: Optional[int]
    pens_att: Optional[int]
    xg: Optional[float]
    xg_assist: Optional[float]
    xga: Optional[float]
    shots: Optional[int]
    headed_shots: Optional[int]
    touches: Optional[int]
    blocks: Optional[int]
    succ_dribbles: Optional[int]
    current_team: Optional[Team] = None # if the match is not international then leave null
    
    
class LineupDist(BaseModel):
    league_id: Optional[int] = None
    league_name: Optional[str] = None
    league_logo_url: Optional[str] = None
    country: Country
    num_players: int
    
class MatchTeamStats(BaseModel):
    goals: Optional[int] = None
    penalty_goals: Optional[int] = None
    shots: Optional[int] = None
    possesion: Optional[int] = None
    offsides: Optional[int] = None
    corners: Optional[int] = None
    xg: Optional[int] = None
    
    
    
class MatchTeam(BaseModel):
    team: Team
    team_stats: MatchTeamStats
    manager: Manager
    formation: Optional[str] = None
    lineups: List[PlayerMatchStats]
    x11_dist: Optional[List[LineupDist]]

    
        
    
        
class MatchInfo(BaseModel):
    match_id: int
    home: MatchTeam 
    away: MatchTeam
    win_team_id: Optional[int] = None
    loss_team_id: Optional[int] = None     
    isdraw: Optional[bool]
    pens: Optional[bool]
    extra_time: Optional[bool]
         
    match_date: Optional[str] = None
    match_time_utc: Optional[str] = None
    is_neutral: Optional[bool] = None
    isplayed: Optional[bool] = None
    is_live: Optional[bool] = None
    match_minute: Optional[str] = None
    round: Optional[str] = None
    gameweek_number: Optional[int] = None
    stadium_id: Optional[int] = None
    stadium_name: Optional[str] = None
    attendance: Optional[int] = None
    capacity: Optional[int] = None
    capacity_pct: Optional[float] = None
    referee: Referee
        
    
class MatchEvent(BaseModel):
    event_id: int
    team_id: int
    event_type: str
    body_part: Optional[str]
    home_goals: Optional[int]
    away_goals: Optional[int]
    minute: Optional[int]
    add_minute: Optional[int]
    active_player: Player
    passive_player: Optional[Player]
    active_notes: Optional[str]


class MatchDetails(BaseModel):
    match: MatchInfo
    events: List[MatchEvent]
    h2h: List[MatchesByComp]
    home_last5: List[MatchesByComp]
    away_last5: List[MatchesByComp]
        

class MatchDetailsResponse(BaseModel):
    data: MatchDetails
    
    
    
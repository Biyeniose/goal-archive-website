from typing import List, Optional
from pydantic import BaseModel

from .player import Player

from .league import Competition, League, TeamRank
from .utils import Country, Manager, Referee

from .team import Team

class MatchTeamStats(BaseModel):
    goals: Optional[int] = None
    penalty_goals: Optional[int] = None
    shots: Optional[int] = None
    possesion: Optional[int] = None 
    offsides: Optional[int] = None
    corners: Optional[int] = None
    xg: Optional[int] = None
    pass_succ: Optional[int] = None # succesful passes matches.home_pass_succ and .away_pass_succ

class Match(BaseModel):
    match_id: int
    match_date: Optional[str] = None
    match_time_utc: Optional[str] = None
    home_team: Team
    home_stats: MatchTeamStats
    away_team: Team
    away_stats: MatchTeamStats
    
    win_team_id: Optional[int] = None
    loss_team_id: Optional[int] = None
    isdraw: Optional[bool] = None
    pens: Optional[bool] = None
    extra_time: Optional[bool] = None
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
    tweet_mentions: Optional[int] = None
    current_team: Optional[Team] = None
    
    
    
class LineupDist(BaseModel):
    league_id: Optional[int] = None
    league_name: Optional[str] = None
    league_logo_url: Optional[str] = None
    country: Country
    num_players: int
    

    
    
    
class MatchTeam(BaseModel):
    team: Team
    team_stats: MatchTeamStats
    manager: Manager
    formation: Optional[str] = None
    lineups: List[PlayerMatchStats]
    x11_dist: Optional[List[LineupDist]] = None

    
        
    
        
class MatchInfo(BaseModel):
    match_id: int
    competition_id: int
    home: MatchTeam
    away: MatchTeam
    win_team_id: Optional[int] = None
    loss_team_id: Optional[int] = None
    isdraw: Optional[bool] = None
    pens: Optional[bool] = None
    extra_time: Optional[bool] = None
         
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
    body_part: Optional[str] = None
    home_goals: Optional[int] = None
    away_goals: Optional[int] = None
    minute: Optional[int] = None
    add_minute: Optional[int] = None
    active_player: Player
    passive_player: Optional[Player] = None
    active_notes: Optional[str] = None


class MatchDetails(BaseModel):
    match: MatchInfo
    events: List[MatchEvent]
    h2h: List[MatchesByComp]
    home_last5: List[MatchesByComp]
    away_last5: List[MatchesByComp]
    league_ranks_eod: Optional[List[TeamRank]]
        

class MatchDetailsResponse(BaseModel):
    data: MatchDetails


# /teams/{team_id}/wc2026
class LeagueStatsDist(BaseModel):
    country: str
    teams: List[Team]
    total_stat_value: int
    num_players: int


class TeamWCData(BaseModel):
    matches: Optional[List[Match]] = None
    league_dist: Optional[List[LeagueStatsDist]] = None


class TeamWCResponse(BaseModel):
    data: TeamWCData

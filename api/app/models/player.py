from typing import List, Optional, Dict, Any, TYPE_CHECKING
from pydantic import BaseModel
from datetime import date
from typing import ClassVar
from ..models.team import Team
from app.models.league import League
import importlib
#if TYPE_CHECKING:
 #  from ..models.league import League

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

class Comp(BaseModel):
    comp_id: int
    comp_name: str
    comp_url: Optional[str]

# /players/:id/allstats - player Season Stats models
class PlayerSeasonGADist(BaseModel):
    opp_team: Team
    pct: Optional[float] = None
    goals: Optional[int] = None
    assists: Optional[int] = None
    ga: Optional[int] = None
class Player(BaseModel):
    player_id: int
    player_name: str
    img: Optional[str] = None
class PlayerStats(BaseModel):
    player: Player
    comp: Comp
    team: Team
    season_year: int
    age: int
    ga: Optional[int] = None
    ga_pg: Optional[float] = None
    goals: Optional[int] = None
    goals_pg: Optional[float] = None
    assists: Optional[int] = None
    assists_pg: Optional[float] = None
    penalty_goals: Optional[int] = None
    gp: Optional[int] = None
    minutes: Optional[int] = None
    minutes_pg: Optional[float] = None
    cs: Optional[int] = None
    pass_compl_pg: Optional[float] = None
    passes_pg: Optional[float] = None
    errors_pg: Optional[float] = None
    shots_pg: Optional[float] = None
    shots_on_target_pg: Optional[float] = None
    sca_pg: Optional[float] = None
    gca_pg: Optional[float] = None
    take_ons_pg: Optional[float] = None
    take_ons_won_pg: Optional[float] = None
    goals_concede: Optional[int] = None
    yellows: Optional[int] = None
    yellows2: Optional[int] = None
    reds: Optional[int] = None
    own_goals: Optional[int] = None
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


###################
class PlayerNations(BaseModel):
    nation1_id: Optional[int] = None
    nation1: Optional[str] = None
    nation1_logo: Optional[str] = None
    nation2_id: Optional[int] = None
    nation2: Optional[str] = None
    nation2_logo: Optional[str] = None


class PlayerBasicInfo(BaseModel):
    id: Optional[int] = None    # <--- CHANGE HERE
    name: Optional[str] = None 
    current_age: Optional[int] = None
    pic_url: Optional[str] = None
    nations: Optional[PlayerNations] = None



class PlayerSeasonInfo(BaseModel):
    comp: League
    #contributions: Optional[List[PlayerSeasonGADist]]
    team: Team
    season_year: int
    age: Optional[int] = None
    gp: Optional[int] = None
    goals: Optional[int] = None
    assists: Optional[int] = None
    ga: Optional[int] = None
    penalty_goals: Optional[int] = None
    minutes: Optional[int] = None
    minutes_pg: Optional[float] = None
    goals_pg: Optional[float] = None
    shots_pg: Optional[float] = None
    shots_on_target_pg: Optional[float] = None
    passes_pg: Optional[float] = None
    pass_compl_pg: Optional[float] = None
    take_ons_pg: Optional[float] = None
    take_ons_won_pg: Optional[float] = None
    ga_pg: Optional[float] = None
    sca_pg: Optional[float] = None
    id: int





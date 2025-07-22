from typing import List, Dict, Optional
from pydantic import BaseModel
from ..models.team import TeamBasicInfo, Team
from ..models.player import PlayerBasicInfo
from ..models.people import ManagerBasicInfo

#####################
class MatchInfo(BaseModel):
    match_id: int
    match_date: str
    date_time_utc: Optional[str] = None
    round: Optional[str] = None
    season_year: int
    draw: Optional[bool] = None
    et: Optional[bool] = None
    pens: Optional[bool] = None
    result: Optional[str] = None
    comp_id: int
    comp: str
    comp_logo: Optional[str] = None


class MatchGoals(BaseModel):
    goals: Optional[int] = None
    pen_goals: Optional[int] = None
    ranking: Optional[int] = None
class MatchTeam(BaseModel):
    stats: MatchGoals
    team: Team
class MatchTeamsBasic(BaseModel):
    home: MatchTeam
    away: MatchTeam
class Match(BaseModel):
    teams: MatchTeamsBasic
    match_info: MatchInfo
    
    
class MatchTeamStats(BaseModel):
    ranking: Optional[int] = None
    goals: int
    xg: Optional[float]
    pen_goals: Optional[int]
    possesion: Optional[float]
    offsides: Optional[int]
    fouls: Optional[int]
    freekicks: Optional[int]
    corners: Optional[int]
    formation: Optional[str] = None # Assuming formation can be a string like "4-4-2"
    saves_succ: Optional[int] = None
    saves_att: Optional[int] = None
    saves_acc: Optional[float] = None # Assuming accuracy is a float/percentage
    shots_att: Optional[int] = None
    shots_succ: Optional[int] = None
    shot_acc: Optional[float] = None # Assuming accuracy is a float/percentage
    pass_att: Optional[int] = None
    pass_succ: Optional[int] = None
    pass_acc: Optional[float] = None

class BasicStats(BaseModel):
    id: int
    player_id: int
    match_id: int
    team_id: int
    minutes: Optional[int] = None
    goals: Optional[int] = None
    assists: Optional[int] = None
    goals_assists: Optional[int] = None
    pens_made: Optional[int] = None
    pens_att: Optional[int] = None
    age: Optional[int] = None
    shots: Optional[int] = None
    shots_on_target: Optional[int] = None
    cards_yellow: Optional[int] = None
    cards_red: Optional[int] = None
    touches: Optional[int] = None

class DefensiveStats(BaseModel):
    tackles: Optional[int] = None
    interceptions: Optional[int] = None
    blocks: Optional[int] = None
    tackles_won: Optional[int] = None
    tackles_def_3rd: Optional[int] = None
    tackles_mid_3rd: Optional[int] = None
    tackles_att_3rd: Optional[int] = None
    challenge_tackles: Optional[int] = None
    challenges: Optional[int] = None
    challenge_tackles_pct: Optional[float] = None
    challenges_lost: Optional[int] = None
    blocked_shots: Optional[int] = None
    blocked_passes: Optional[int] = None
    tackles_interceptions: Optional[int] = None
    clearances: Optional[int] = None
    errors: Optional[int] = None
    ball_recoveries: Optional[int] = None

class AttackingStats(BaseModel):
    xg: Optional[float] = None
    npxg: Optional[float] = None
    xg_assist: Optional[float] = None
    sca: Optional[int] = None
    gca: Optional[int] = None
    take_ons: Optional[int] = None
    take_ons_won: Optional[int] = None
    take_ons_won_pct: Optional[float] = None
    take_ons_tackled: Optional[int] = None
    take_ons_tackled_pct: Optional[float] = None
    crosses: Optional[int] = None
    own_goals: Optional[int] = None
    pens_won: Optional[int] = None
    pens_conceded: Optional[int] = None

class PassingStats(BaseModel):
    passes_completed: Optional[int] = None
    passes: Optional[int] = None
    passes_pct: Optional[float] = None
    progressive_passes: Optional[int] = None
    passes_received: Optional[int] = None
    progressive_passes_received: Optional[int] = None

class PossessionStats(BaseModel):
    carries: Optional[int] = None
    progressive_carries: Optional[int] = None
    carries_distance: Optional[int] = None
    carries_progressive_distance: Optional[int] = None
    carries_into_final_third: Optional[int] = None
    carries_into_penalty_area: Optional[int] = None
    miscontrols: Optional[int] = None
    dispossessed: Optional[int] = None
    touches_def_pen_area: Optional[int] = None
    touches_def_3rd: Optional[int] = None
    touches_mid_3rd: Optional[int] = None
    touches_att_3rd: Optional[int] = None
    touches_att_pen_area: Optional[int] = None
    touches_live_ball: Optional[int] = None

class DisciplineStats(BaseModel):
    fouls: Optional[int] = None
    fouled: Optional[int] = None
    offsides: Optional[int] = None
    cards_yellow_red: Optional[int] = None
    aerials_lost: Optional[int] = None
    aerials_won: Optional[int] = None
    aerials_won_pct: Optional[float] = None

class PlayerMatchStatsDiv(BaseModel):
    basic: BasicStats
    defensive: Optional[DefensiveStats] = None
    attacking: Optional[AttackingStats] = None
    passing: Optional[PassingStats] = None
    possession: Optional[PossessionStats] = None
    discipline: Optional[DisciplineStats] = None


class PlayerMatchStats(BaseModel):
    """
    Pydantic model representing a player's statistics for a single match,
    with most fields being optional.
    """
    id: int
    player_id: int
    match_id: int
    team_id: int
    minutes: int
    goals: int
    assists: int
    # All fields below are now optional
    goals_assists: Optional[int] = None
    pens_made: Optional[int] = None
    pens_att: Optional[int] = None
    age: Optional[int] = None
    shots: Optional[int] = None
    shots_on_target: Optional[int] = None
    cards_yellow: Optional[int] = None
    cards_red: Optional[int] = None
    touches: Optional[int] = None
    tackles: Optional[int] = None
    interceptions: Optional[int] = None
    blocks: Optional[int] = None
    xg: Optional[float] = None
    npxg: Optional[float] = None
    xg_assist: Optional[float] = None
    sca: Optional[int] = None
    gca: Optional[int] = None
    passes_completed: Optional[int] = None
    passes: Optional[int] = None
    passes_pct: Optional[float] = None
    progressive_passes: Optional[int] = None
    carries: Optional[int] = None
    progressive_carries: Optional[int] = None
    take_ons: Optional[int] = None
    take_ons_won: Optional[int] = None
    tackles_won: Optional[int] = None
    tackles_def_3rd: Optional[int] = None
    tackles_mid_3rd: Optional[int] = None
    tackles_att_3rd: Optional[int] = None
    challenge_tackles: Optional[int] = None
    challenges: Optional[int] = None
    challenge_tackles_pct: Optional[float] = None
    challenges_lost: Optional[int] = None
    blocked_shots: Optional[int] = None
    blocked_passes: Optional[int] = None
    tackles_interceptions: Optional[int] = None
    clearances: Optional[int] = None
    errors: Optional[int] = None
    touches_def_pen_area: Optional[int] = None
    touches_def_3rd: Optional[int] = None
    touches_mid_3rd: Optional[int] = None
    touches_att_3rd: Optional[int] = None
    touches_att_pen_area: Optional[int] = None
    touches_live_ball: Optional[int] = None
    take_ons_won_pct: Optional[float] = None
    take_ons_tackled: Optional[int] = None
    take_ons_tackled_pct: Optional[float] = None
    carries_distance: Optional[int] = None
    carries_progressive_distance: Optional[int] = None
    carries_into_final_third: Optional[int] = None
    carries_into_penalty_area: Optional[int] = None
    miscontrols: Optional[int] = None
    dispossessed: Optional[int] = None
    passes_received: Optional[int] = None
    progressive_passes_received: Optional[int] = None
    fouls: Optional[int] = None
    fouled: Optional[int] = None
    offsides: Optional[int] = None
    crosses: Optional[int] = None
    own_goals: Optional[int] = None
    pens_won: Optional[int] = None
    pens_conceded: Optional[int] = None
    aerials_lost: Optional[int] = None
    cards_yellow_red: Optional[int] = None
    ball_recoveries: Optional[int] = None
    aerials_won: Optional[int] = None
    aerials_won_pct: Optional[float] = None

class MatchLineups(BaseModel):
    id: int
    player: PlayerBasicInfo
    age: Optional[int]
    number: Optional[int]
    position: Optional[str]
    xi: Optional[bool]
    team_id: Optional[int]
    stats: Optional[PlayerMatchStats] = None

class MatchEvents(BaseModel):
    id: int
    event_type: str
    minute: int
    add_minute: int
    home_goals: int
    away_goals: int
    team_id: int
    active_player: Optional[PlayerBasicInfo]
    passive_player: Optional[PlayerBasicInfo]

class MatchTeamInfo(BaseModel):
    team: TeamBasicInfo
    manager: Optional[ManagerBasicInfo]

class MatchTeamData(BaseModel):
    info: Optional["MatchTeamInfo"]
    lineups: List[MatchLineups]
    team_stats: Optional[MatchTeamStats]

class MatchTeams(BaseModel):
    home: MatchTeamData
    away: MatchTeamData
    





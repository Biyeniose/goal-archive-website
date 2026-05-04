from typing import List, Optional
from pydantic import BaseModel

from .utils import Country, Player
from .team import Team, Transfer


class League(BaseModel):
    league_id: int
    league_name: str
    country: Optional[Country] = None
    tier_level: Optional[str] = None
    format: Optional[str] = None
    scope: Optional[str] = None
    competiton_level: Optional[str] = None
    logo_url: Optional[str] = None


class Competition(BaseModel):
    league: League
    competition_id: int
    name: str
    season_year: int
    stage: Optional[str] = None
    logo_url: Optional[str] = None


class TeamRank(BaseModel):
    team: Team
    rank: Optional[str] = None
    info: Optional[str] = None
    points: Optional[int] = None
    gp: Optional[int] = None
    gd: Optional[int] = None
    wins: Optional[int] = None
    losses: Optional[int] = None
    draws: Optional[int] = None
    goals_f: Optional[int] = None
    goals_a: Optional[int] = None
    


class LeagueListResponse(BaseModel):
    data: List[League]


class LeagueStandingsResponse(BaseModel):
    data: List[TeamRank]


class PlayerCompetitionStats(BaseModel):
    gp: Optional[int] = None
    minutes: Optional[int] = None
    mpg: Optional[float] = None
    goals: Optional[int] = None
    goals_p90: Optional[float] = None
    assists: Optional[int] = None
    assists_p90: Optional[float] = None
    goals_assists: Optional[int] = None
    goals_assists_p90: Optional[float] = None
    shots: Optional[int] = None
    clean_sheets: Optional[int] = None
    goals_conceded: Optional[int] = None
    goals_conceded_p90: Optional[float] = None
    penalty_goals: Optional[int] = None
    pens_att: Optional[int] = None
    cards_yellow: Optional[int] = None
    cards_red: Optional[int] = None
    competition: Optional[Competition] = None


class LeaguePlayerStats(BaseModel):
    player: Player
    teams: List[Team]
    stats: PlayerCompetitionStats


class LeagueStatsData(BaseModel):
    season_year: int
    players: List[LeaguePlayerStats]


class LeagueStatsResponse(BaseModel):
    data: LeagueStatsData


class LeaguePlayerStatsByDate(BaseModel):
    player: Player
    teams: List[Team]
    stats: PlayerCompetitionStats


class LeagueStatsByDateData(BaseModel):
    start_date: str
    end_date: str
    players: List[LeaguePlayerStatsByDate]


class LeagueStatsbyDateResponse(BaseModel):
    data: LeagueStatsByDateData


class BestLoanees(BaseModel):
    player: Player
    loan_transfer: Transfer
    return_transfer: Optional[Transfer] = None
    stats: List[PlayerCompetitionStats]


class BestLoaneesResponse(BaseModel):
    data: List[BestLoanees]


# lightweight match summary used by drought response
class DroughtLastMatch(BaseModel):
    match_id: int
    match_date: Optional[str] = None
    home_team: Team
    home_goals: Optional[int] = None
    away_team: Team
    away_goals: Optional[int] = None
    isdraw: Optional[bool] = None
    competition: Optional[Competition] = None


class LongestDroughts(BaseModel):
    player: Player
    teams: List[Team]
    competitions: List[Competition]
    days: int
    gp: int
    mpg: float
    last_match: Optional[DroughtLastMatch] = None
    last_match_ga: Optional[DroughtLastMatch] = None
    


class LongestDroughtsReponse(BaseModel):
    data: List[LongestDroughts]


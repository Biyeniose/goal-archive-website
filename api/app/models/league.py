from typing import List, Optional
from pydantic import BaseModel

from .utils import Country
from .team import Team
from .player import Player


class League(BaseModel):
    league_id: int
    league_name: str
    country: Optional[Country] = None
    tier_level: Optional[str] = None
    format: Optional[str] = None
    competiton_level: Optional[str] = None


class Competition(BaseModel):
    league: League
    competition_id: int
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


class LeaguePlayerStats(BaseModel):
    player: Player
    teams: List[Team]
    competitions: List[Competition]
    stats: PlayerCompetitionStats


class LeagueStatsData(BaseModel):
    season_year: int
    competitions: List[Competition]
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

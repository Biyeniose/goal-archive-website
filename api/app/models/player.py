from typing import List, Optional
from pydantic import BaseModel

from .match import PlayerMatchStats

from .utils import Country, Player, PlayerCountries
from .team import Team, Transfer
from .league import Competition, PlayerCompetitionStats


class PlayerSearchResponse(BaseModel):
    data: List[Player]


# /players/cv
class CompetitionStats(BaseModel):
    competition: Competition
    stats: Optional[PlayerCompetitionStats] = None
    team_finish: Optional[str] = None
    number: Optional[int] = None

class PlayerSeason(BaseModel):
    season_start_date: Optional[str] = None
    teams: Optional[List[Team]] = None
    competition_stats: Optional[List[CompetitionStats]] = None

class TransferWithStats(BaseModel):
    transfer: List[Transfer]
    seasons: List[PlayerSeason]

class PlayerCV(BaseModel):
    player: Player
    stats: List[TransferWithStats]

class PlayerCVResponse(BaseModel):
    data: PlayerCV


# /players/season-stats

class SeasonMatch(BaseModel):
    match_id: int
    match_date: Optional[str] = None
    match_time_utc: Optional[str] = None
    home_team: Team
    home_color: Optional[str] = None
    away_team: Team
    away_color: Optional[str] = None
    win_team_id: Optional[int] = None
    loss_team_id: Optional[int] = None
    isdraw: Optional[bool] = None
    pens: Optional[bool] = None
    extra_time: Optional[bool] = None
    is_neutral: Optional[bool] = None
    isplayed: Optional[bool] = None
    gameweek_number: Optional[int] = None

class PlayerCompMatches(BaseModel):
    match: SeasonMatch
    stats: Optional[PlayerMatchStats] = None

class PlayerSeasonComps(BaseModel):
    competition: Competition
    team: Team
    start_date: Optional[str] = None
    stats: Optional[PlayerCompetitionStats] = None
    matches: Optional[List[PlayerCompMatches]] = None

class PlayerSeasonStats(BaseModel):
    player: Player
    comps: Optional[List[PlayerSeasonComps]] = None

class PlayerSeasonStatsResponse(BaseModel):
    data: Optional[List[PlayerSeasonStats]]

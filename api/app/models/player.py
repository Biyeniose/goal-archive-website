from typing import List, Optional
from pydantic import BaseModel

from .utils import Country, Player, PlayerCountries
from .team import Team, Transfer
from .league import Competition, PlayerCompetitionStats


class PlayerSearchResponse(BaseModel):
    data: List[Player]


class PlayerDetailResponse(BaseModel):
    data: Player


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

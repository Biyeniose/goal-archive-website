from pydantic import BaseModel, HttpUrl
from typing import List, Optional
from datetime import date
from app.models.player import Player, PlayerBasicInfo, PlayerStats
from app.models.match import MatchInfo, MatchTeams, MatchEvents, Match, PlayerMatchStats, MatchTeamsBasic, PlayerMatchStatsDiv, BasicStats
from app.models.league import LeagueInfo, TeamRank
from app.models.team import Team, TeamInfo, Transfer, SquadPlayer


# PLAYER RESPONSES
# /players/:id/allstats
class PlayerSeasonStatsData(BaseModel):
    #player: Player
    stats: List[PlayerStats]
class PlayerSeasonStatsResponse(BaseModel):
    data: PlayerSeasonStatsData
# /players/:id/matches?season=2024
class PlayerMatch(BaseModel):
    xi: bool
    lineup_id: int
    team_id: int
    match_info: MatchInfo
    teams: MatchTeamsBasic
    player_stats: Optional[PlayerMatchStatsDiv] = None
class PlayerMatchesData(BaseModel):
    matches: List[PlayerMatch]
class PlayerMatchesResponse(BaseModel):
    data: PlayerMatchesData

# /players/:id/recent-goals
class PlayerRecentGA(BaseModel):
    teams: MatchTeamsBasic
    match_info: MatchInfo
    player_stats: Optional[BasicStats] = None
class PlayerRecentGAData(BaseModel):
    recent_ga: List[PlayerRecentGA]
class PlayerRecentGAResponse(BaseModel):
    data: PlayerRecentGAData


# TEAM RESPONSES
# /teams/:id/info
class TeamData(BaseModel):
    info: TeamInfo
    transfers: List[Transfer]
    matches: List[Match]
    stats: List[PlayerStats]
class TeamInfoResponse(BaseModel):
    data: TeamData

# /teams/:id/squads
class SquadResponse(BaseModel):
    squad: List[SquadPlayer]
class TeamSquadDataResponse(BaseModel):
    data: SquadResponse

# LEAGUE RESPONSES
# /leagues/:id/infos
class LeagueData(BaseModel):
    info: LeagueInfo
    ranks: List[TeamRank]
    matches: List[Match]
class LeagueDataResponse(BaseModel):
    data: List[LeagueData]

# /leagues/:id/stats&season=2024
class LeagueStatsData(BaseModel):
    season_year: int
    player: PlayerBasicInfo
    team: Team
    age: Optional[int] = None
    position: Optional[str] = None
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

class LeagueStatsContainer(BaseModel):
    stats: List[LeagueStatsData]
class LeagueStatsResponse(BaseModel):
    data: LeagueStatsContainer

# /leagues/:id/matches?season
class LeagueMatches(BaseModel):
    matches: List[Match]

class LeagueMatchesResponse(BaseModel):
    data: LeagueMatches

# /leagues/:id/ranks?season
class LeagueRanks(BaseModel):
    ranks: List[TeamRank]

class LeagueRanksResponse(BaseModel):
    data: LeagueRanks

# /leagues/:id/ranks?season
class LeagueFormByYear(BaseModel):
    form: List[TeamRank]
class LeagueFormResponse(BaseModel):
    data: LeagueFormByYear
# /leagues/:id/form?season
"""
end
"""

# MATCH RESPONSES
# /matches/:id
class MatchInfoData(BaseModel):
    events: List[MatchEvents]
    teams: MatchTeams
    match_info: MatchInfo

class MatchInfoResponse(BaseModel):
    data: MatchInfoData

# NICHE STATS RESPONSES




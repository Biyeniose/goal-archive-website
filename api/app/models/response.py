from pydantic import BaseModel, HttpUrl
from typing import List, Optional, Dict
from datetime import date
from app.models.player import Player, PlayerBasicInfo, PlayerStats
from app.models.match import MatchInfo, MatchTeams, MatchEvents, Match, PlayerMatchStats, MatchTeamsBasic, PlayerMatchStatsDiv, BasicStats
from app.models.league import LeagueInfo, TeamRank, Comp
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

# /players/:id/goal-dist?season
class TotalGA(BaseModel):
    goals: int
    assists: int
    ga: int
    pens: Optional[int] = None
class Pens(BaseModel):
    pen_pct: Optional[float] = None
    pens_scored: Optional[int] = None
class StatsDist(BaseModel):
    ga_against_pct: Optional[float] = None
    ga_against: Optional[int] = None
    goals_against: Optional[int] = None
    goals_against_pct: Optional[float] = None
    assists_against: Optional[int] = None
    assists_against_pct: Optional[float] = None
class TeamDist(BaseModel):
    team: Team
    stats: StatsDist
class GoalDist(BaseModel):
    teams: TeamDist
class Comp2(BaseModel):
    comp_id: int
    comp_name: str
    comp_url: Optional[str]
    season_year: int

class PlayerGADistData(BaseModel):
    info: Comp2
    total: TotalGA
    goal_dist: List[GoalDist]
    pens: Pens
class PlayerGADistResponse(BaseModel):
    data: PlayerGADistData

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

# /teams/:id/transfers?start_date=2024
class TeamTransferSum(BaseModel):
    players_in: int
    players_out: int
    total_fees_in: float
    total_fees_out: float
    net_fees: float
class TeamTransfersData(BaseModel):
    transfers: List[Transfer]
    sum: TeamTransferSum
class TeamTransfersResponse(BaseModel):
    data: TeamTransfersData
# /teams/:id/allcomps?season=2024
class TeamCompRank(BaseModel):
    rank: Optional[int] = None
    round: Optional[str] = None
    points: Optional[int] = None
    season: int
    team_id: int
    comp: Comp
class TeamSeason(BaseModel):
    ranking: TeamCompRank
    #last_match: Match
class TeamSeasonData(BaseModel):
    season_comps: List[TeamSeason]
    team: Team
class TeamSeasonResponse(BaseModel):
    data: TeamSeasonData

# /teams/:id/past-domestic
class DomesticSeason(BaseModel):
    rank: TeamRank
    comp: Comp
    season: int
class DomesticSeasonsData(BaseModel):
    seasons: List[DomesticSeason]
class DomesticSeasonsResponse(BaseModel):
    data: DomesticSeasonsData
"""
end
"""
# LEAGUE RESPONSES
# /leagues/winners
class WinTeam(BaseModel):
    team: Team
    rank: Optional[int] = None
    round: Optional[str] = None
    points: Optional[int] = None
    season: int
    rank_id: int
class TopCompsWinners(BaseModel):
    comp: LeagueInfo
    win_teams: List[WinTeam]
class TopCompsWinnersData(BaseModel):
    stats: List[TopCompsWinners]
class TopCompsWinnersResponse(BaseModel):
    data: TopCompsWinnersData

# /leagues/:id/last_winners
class LeagueWinnersData(BaseModel):
    stats: TopCompsWinners
class LeagueWinnersResponse(BaseModel):
    data: LeagueWinnersData

# /leagues/:id/highest_stats
class TeamLeagueStats(BaseModel):
    comp: LeagueInfo
    # Dynamic year keys with list of TeamRank as values
    years: Dict[str, List[TeamRank]]
class LeagueTeamStatData(BaseModel):
    stats: TeamLeagueStats
class LeagueTeamStatResponse(BaseModel):
    data: LeagueTeamStatData


# /leagues/:id/infos
class LeagueData(BaseModel):
    info: LeagueInfo
    ranks: Optional[List[TeamRank]] = None
    matches: List[Match]
class LeagueDataResponse(BaseModel):
    data: List[LeagueData]

# /leagues/:id/stats&season=2024
class LeaguePlayerStatsData(BaseModel):
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
    stats: List[LeaguePlayerStatsData]
class LeagueStatsResponse(BaseModel):
    data: LeagueStatsContainer

# /leagues/:id/past-topbystat
class LeaguePastStatsContainer(BaseModel):
    comp: LeagueInfo
    years: Dict[str, List[LeaguePlayerStatsData]]
    #stats: List[LeaguePlayerStatsData]
class LeaguePastStatsData(BaseModel):
    stats: LeaguePastStatsContainer
class LeaguePastStatsResponse(BaseModel):
    data: LeaguePastStatsData


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

# /leagues/:id/form?season
class LeagueFormByYear(BaseModel):
    form: List[TeamRank]
class LeagueFormResponse(BaseModel):
    data: LeagueFormByYear
 
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

# /matches/h2h/?team1?team2
class TeamRecord(BaseModel):
    team: Team
    gp: int
    wins: int
    win_pct: float
    losses: int
    draws: int
    goals_f: int
    goals_a: int
class H2HData(BaseModel):
    matches: List[Match]
    record: List[TeamRecord]
class H2HResponse(BaseModel):
    data: H2HData

# NICHE STATS RESPONSES




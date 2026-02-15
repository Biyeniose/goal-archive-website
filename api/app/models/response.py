#from turtle import home
from pydantic import BaseModel, HttpUrl, AwareDatetime, Field
from typing import List, Optional, Dict, Any
from decimal import Decimal
from datetime import date, datetime

import strenum
from app.models.player import Player, PlayerBasicInfo, PlayerStats, PlayerInfo, Transfer
from app.models.match import MatchInfo, MatchTeams, MatchEvents, Match, PlayerMatchStats, MatchTeamsBasic, PlayerMatchStatsDiv, BasicStats
from app.models.league import LeagueInfo, TeamRank, Comp
from app.models.team import Team, TeamInfo, Transfer, SquadPlayer, TeamCommonInfo, PlayerNations

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

# PLAYER SEARCH
class PlayerSearchData(BaseModel):
    search: List[PlayerInfo]

class PlayerSearchResponse(BaseModel):
    data: PlayerSearchData

# /players/:id/career-teams
class PlayerCommonInfo(BaseModel):
    player_id: Optional[int] = None
    player_name: Optional[str] = None
    age: Optional[int] = None
    pic_url: Optional[str] = None
    nation1_id: Optional[int] = None
    nation1: Optional[str] = None
    nation1_logo: Optional[str] = None
    nation2_id: Optional[int] = None
    nation2: Optional[str] = None
    nation2_logo: Optional[str] = None
class PlayerCareerTeamsData(BaseModel):
    player: PlayerCommonInfo
    teams: List[TeamCommonInfo]
class PlayerCareerTeamsResponse(BaseModel):
    data: PlayerCareerTeamsData

# /players/rand-transfer
class RandomTranfer(BaseModel):
    transfer: Transfer
class RandomTransferResponse(BaseModel):
    data: RandomTranfer

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


class PlayerWeeklyStats(BaseModel):
    # Basic info
    started: Optional[bool] = None  # Change from bool to Optional[bool]
    position: Optional[str] = None
    number: Optional[int] = None
    age: Optional[int] = None
    #value: Optional[Decimal] = None
    
    # Playing time
    minutes: Optional[int] = None
    
    # Goals and assists
    goals: Optional[int] = 0
    assists: Optional[int] = 0
    goals_assists: Optional[int] = 0
    
    # Expected stats
    xg: Optional[Decimal] = None
    xg_assist: Optional[Decimal] = None
    xga: Optional[Decimal] = None
    npxg: Optional[Decimal] = None
    
    # Penalties
    pens_made: Optional[int] = 0
    pens_att: Optional[int] = 0
    pens_won: Optional[int] = 0
    pens_conceded: Optional[int] = 0
    
    # Shooting
    shots: Optional[int] = 0
    shots_on_target: Optional[int] = 0
    
    # Touches
    touches: Optional[int] = None
    touches_def_pen_area: Optional[int] = None
    touches_def_3rd: Optional[int] = None
    touches_mid_3rd: Optional[int] = None
    touches_att_3rd: Optional[int] = None
    touches_att_pen_area: Optional[int] = None
    
    # Tackles
    tackles: Optional[int] = 0
    tackles_won: Optional[int] = 0
    tackles_def_3rd: Optional[int] = 0
    tackles_mid_3rd: Optional[int] = 0
    tackles_att_3rd: Optional[int] = 0
    
    # Challenges
    challenges: Optional[int] = 0
    challenges_lost: Optional[int] = 0
    
    # Blocks
    blocks: Optional[int] = 0
    blocked_shots: Optional[int] = 0
    blocked_passes: Optional[int] = 0
    
    # Defensive actions
    interceptions: Optional[int] = 0
    clearances: Optional[int] = 0
    errors: Optional[int] = 0
    
    # Shot/Goal creating actions
    sca: Optional[int] = 0
    gca: Optional[int] = 0
    
    # Passing
    passes_completed: Optional[int] = None
    passes: Optional[int] = None
    passes_pct: Optional[Decimal] = None
    progressive_passes: Optional[int] = None
    
    # Carries
    carries: Optional[int] = None
    progressive_carries: Optional[int] = None
    carries_distance: Optional[int] = None
    carries_progressive_distance: Optional[int] = None
    carries_into_final_third: Optional[int] = None
    carries_into_penalty_area: Optional[int] = None
    
    # Ball control
    miscontrols: Optional[int] = 0
    dispossessed: Optional[int] = 0
    passes_received: Optional[int] = None
    progressive_passes_received: Optional[int] = None
    
    # Take-ons
    take_ons: Optional[int] = 0
    take_ons_won: Optional[int] = 0
    take_ons_won_pct: Optional[Decimal] = None
    take_ons_tackled: Optional[int] = 0
    take_ons_tackled_pct: Optional[Decimal] = None
    
    # Pass distances
    passes_total_distance: Optional[int] = None
    passes_progressive_distance: Optional[int] = None
    
    # Pass types
    passes_long: Optional[int] = None
    passes_completed_long: Optional[int] = None
    passes_medium: Optional[int] = None
    passes_completed_medium: Optional[int] = None
    passes_short: Optional[int] = None
    passes_completed_short: Optional[int] = None
    
    # Pass targets
    assisted_shots: Optional[int] = None
    passes_into_final_third: Optional[int] = None
    passes_into_penalty_area: Optional[int] = None
    crosses_into_penalty_area: Optional[int] = None
    
    # Pass categories
    passes_live: Optional[int] = None
    passes_dead: Optional[int] = None
    through_balls: Optional[int] = None
    passes_switches: Optional[int] = None
    passes_offsides: Optional[int] = None
    passes_blocked: Optional[int] = None
    crosses: Optional[int] = None
    throw_ins: Optional[int] = None
    corner_kicks: Optional[int] = None
    
    # Discipline
    cards_yellow: Optional[int] = 0
    cards_red: Optional[int] = 0
    cards_yellow_red: Optional[int] = 0
    fouls: Optional[int] = 0
    fouled: Optional[int] = 0
    
    # Other
    offsides: Optional[int] = 0
    own_goals: Optional[int] = 0
    ball_recoveries: Optional[int] = None
    
    # Aerials
    aerials_won: Optional[int] = 0
    aerials_lost: Optional[int] = 0
    aerials_won_pct: Optional[Decimal] = None
    
    # Goalkeeper stats
    gk_shots_on_target_against: Optional[int] = None
    gk_goals_against: Optional[int] = None
    gk_saves: Optional[int] = None
    gk_save_pct: Optional[Decimal] = None
    gk_psxg: Optional[Decimal] = None
    gk_passes_completed_launched: Optional[int] = None
    gk_passes_launched: Optional[int] = None
    gk_passes_pct_launched: Optional[Decimal] = None
    gk_passes: Optional[int] = None
    gk_passes_throws: Optional[int] = None
    gk_pct_passes_launched: Optional[Decimal] = None
    gk_passes_length_avg: Optional[Decimal] = None
    gk_goal_kicks: Optional[int] = None
    gk_pct_goal_kicks_launched: Optional[Decimal] = None
    gk_goal_kick_length_avg: Optional[Decimal] = None
    gk_crosses: Optional[int] = None
    gk_crosses_stopped: Optional[int] = None
    gk_crosses_stopped_pct: Optional[Decimal] = None
    gk_def_actions_outside_pen_area: Optional[int] = None
    

class PlayerPerformance(BaseModel):
    player_id: int
    player_name: str
    pixel_pic_url: Optional[str] = None    
    age: Optional[int] = None
    position: Optional[str] = None
    nations: PlayerNations
    team: Team
    stats: PlayerWeeklyStats

class InfoMatch(BaseModel):
    match_id: int
    comp_id: int
    match_date: str
    round: Optional[str] = None
    season_year: int
    result_string: Optional[str] = None
    comp_name: str
    comp_logo: Optional[str] = None
    home_team: Team
    home_goals: Optional[int] = None
    pen_home_goals: Optional[int] = None
    away_team: Team
    away_goals: Optional[int] = None
    pen_away_goals: Optional[int] = None


class PlayerPerformanceData(BaseModel):
    player: PlayerPerformance
    match_info: InfoMatch

class PlayerPerformanceResponse(BaseModel):
    data: List[PlayerPerformanceData]



class PlayerPerformanceResponse2(BaseModel):
    player_id: int
    player_country: str
    player_name: str
    position: Optional[str]
    age: Optional[int]
    sca: Optional[int]
    xg: Optional[Decimal]
    goals: Optional[int]
    minutes: Optional[int]
    player_team_name: str  # Added
    player_team_id: Optional[int]  # Added
    home_team_name: str
    away_team_name: str
    home_team_id: int
    away_team_id: int
    result_string: Optional[str]
    match_date: Optional[date]
    comp_id: int
    competition_name: str
    season_year: int

    data: List[PlayerPerformance]


# NICHE STATS RESPONSES
# player records against a team

class MatchStats(BaseModel):
    """Individual match statistics for a player"""
    xg: Optional[float] = None
    age: Optional[int] = None
    gca: Optional[int] = None
    sca: Optional[int] = None
    xga: Optional[float] = None
    npxg: Optional[float] = None
    fouls: Optional[int] = None
    goals: Optional[int] = None
    shots: Optional[int] = None
    value: Optional[float] = None
    blocks: Optional[int] = None
    errors: Optional[int] = None
    fouled: Optional[int] = None
    passes: Optional[int] = None
    assists: Optional[int] = None
    carries: Optional[int] = None
    crosses: Optional[int] = None
    gk_psxg: Optional[float] = None
    minutes: Optional[int] = None
    started: Optional[bool] = None
    tackles: Optional[int] = None
    touches: Optional[int] = None
    gk_saves: Optional[int] = None
    offsides: Optional[int] = None
    pens_att: Optional[int] = None
    pens_won: Optional[int] = None
    position: Optional[str] = None
    take_ons: Optional[int] = None
    cards_red: Optional[int] = None
    gk_passes: Optional[int] = None
    own_goals: Optional[int] = None
    pens_made: Optional[int] = None
    subbed_on: Optional[bool] = None
    throw_ins: Optional[int] = None
    xg_assist: Optional[float] = None
    challenges: Optional[int] = None
    clearances: Optional[int] = None
    gk_crosses: Optional[int] = None
    passes_pct: Optional[float] = None
    subbed_off: Optional[bool] = None
    aerials_won: Optional[int] = None
    gk_save_pct: Optional[float] = None
    miscontrols: Optional[int] = None
    passes_dead: Optional[int] = None
    passes_live: Optional[int] = None
    passes_long: Optional[int] = None
    tackles_won: Optional[int] = None
    aerials_lost: Optional[int] = None
    cards_yellow: Optional[int] = None
    corner_kicks: Optional[int] = None
    dispossessed: Optional[int] = None
    passes_short: Optional[int] = None
    take_ons_won: Optional[int] = None
    blocked_shots: Optional[int] = None
    gk_goal_kicks: Optional[int] = None
    goals_assists: Optional[int] = None
    interceptions: Optional[int] = None
    passes_medium: Optional[int] = None
    pens_conceded: Optional[int] = None
    through_balls: Optional[int] = None
    assisted_shots: Optional[int] = None
    blocked_passes: Optional[int] = None
    passes_blocked: Optional[int] = None
    aerials_won_pct: Optional[float] = None
    ball_recoveries: Optional[int] = None
    challenges_lost: Optional[int] = None
    passes_offsides: Optional[int] = None
    passes_received: Optional[int] = None
    passes_switches: Optional[int] = None
    shots_on_target: Optional[int] = None
    tackles_att_3rd: Optional[int] = None
    tackles_def_3rd: Optional[int] = None
    tackles_mid_3rd: Optional[int] = None
    touches_att_3rd: Optional[int] = None
    touches_def_3rd: Optional[int] = None
    touches_mid_3rd: Optional[int] = None
    cards_yellow_red: Optional[int] = None
    carries_distance: Optional[int] = None
    gk_goals_against: Optional[int] = None
    gk_passes_throws: Optional[int] = None
    passes_completed: Optional[int] = None
    take_ons_tackled: Optional[int] = None
    take_ons_won_pct: Optional[float] = None
    gk_crosses_stopped: Optional[int] = None
    gk_passes_launched: Optional[int] = None
    progressive_passes: Optional[int] = None
    progressive_carries: Optional[int] = None
    gk_passes_length_avg: Optional[float] = None
    take_ons_tackled_pct: Optional[float] = None
    touches_att_pen_area: Optional[int] = None
    touches_def_pen_area: Optional[int] = None
    passes_completed_long: Optional[int] = None
    passes_total_distance: Optional[int] = None
    gk_crosses_stopped_pct: Optional[float] = None
    gk_passes_pct_launched: Optional[float] = None
    gk_pct_passes_launched: Optional[float] = None
    passes_completed_short: Optional[int] = None
    gk_goal_kick_length_avg: Optional[float] = None
    passes_completed_medium: Optional[int] = None
    passes_into_final_third: Optional[int] = None
    carries_into_final_third: Optional[int] = None
    passes_into_penalty_area: Optional[int] = None
    carries_into_penalty_area: Optional[int] = None
    crosses_into_penalty_area: Optional[int] = None
    gk_pct_goal_kicks_launched: Optional[float] = None
    gk_shots_on_target_against: Optional[int] = None
    passes_progressive_distance: Optional[int] = None
    progressive_passes_received: Optional[int] = None
    carries_progressive_distance: Optional[int] = None
    gk_passes_completed_launched: Optional[int] = None
    gk_def_actions_outside_pen_area: Optional[int] = None


class MatchData(BaseModel):
    """Match information with player stats"""
    match_id: int
    match_time_utc: Optional[str] = None
    comp_name: str
    season_year: int
    result_string: str
    home_id: int
    home_name: str
    home_logo: str
    away_id: int
    away_name: str
    away_logo: str
    stats: MatchStats


class TotalStats(BaseModel):
    """Aggregated total statistics across all matches"""
    matches_played: int
    total_minutes: int
    goals: Optional[int] = None
    assists: Optional[int] = None
    goals_assists: Optional[int] = None
    xg: Optional[float] = None
    xg_assist: Optional[float] = None
    xga: Optional[float] = None
    npxg: Optional[float] = None
    pens_made: Optional[int] = None
    pens_att: Optional[int] = None
    shots: Optional[int] = None
    shots_on_target: Optional[int] = None
    touches: Optional[int] = None
    touches_def_pen_area: Optional[int] = None
    touches_def_3rd: Optional[int] = None
    touches_mid_3rd: Optional[int] = None
    touches_att_3rd: Optional[int] = None
    touches_att_pen_area: Optional[int] = None
    tackles: Optional[int] = None
    tackles_won: Optional[int] = None
    tackles_def_3rd: Optional[int] = None
    tackles_mid_3rd: Optional[int] = None
    tackles_att_3rd: Optional[int] = None
    challenges: Optional[int] = None
    challenges_lost: Optional[int] = None
    blocks: Optional[int] = None
    blocked_shots: Optional[int] = None
    blocked_passes: Optional[int] = None
    interceptions: Optional[int] = None
    clearances: Optional[int] = None
    errors: Optional[int] = None
    sca: Optional[int] = None
    gca: Optional[int] = None
    passes_completed: Optional[int] = None
    passes: Optional[int] = None
    passes_pct: Optional[float] = None
    progressive_passes: Optional[int] = None
    carries: Optional[int] = None
    progressive_carries: Optional[int] = None
    carries_distance: Optional[int] = None
    carries_progressive_distance: Optional[int] = None
    carries_into_final_third: Optional[int] = None
    carries_into_penalty_area: Optional[int] = None
    miscontrols: Optional[int] = None
    dispossessed: Optional[int] = None
    passes_received: Optional[int] = None
    progressive_passes_received: Optional[int] = None
    take_ons: Optional[int] = None
    take_ons_won: Optional[int] = None
    take_ons_won_pct: Optional[float] = None
    take_ons_tackled: Optional[int] = None
    take_ons_tackled_pct: Optional[float] = None
    passes_total_distance: Optional[int] = None
    passes_progressive_distance: Optional[int] = None
    passes_long: Optional[int] = None
    passes_completed_long: Optional[int] = None
    passes_medium: Optional[int] = None
    passes_completed_medium: Optional[int] = None
    passes_short: Optional[int] = None
    passes_completed_short: Optional[int] = None
    assisted_shots: Optional[int] = None
    passes_into_final_third: Optional[int] = None
    passes_into_penalty_area: Optional[int] = None
    crosses_into_penalty_area: Optional[int] = None
    passes_live: Optional[int] = None
    passes_dead: Optional[int] = None
    through_balls: Optional[int] = None
    passes_switches: Optional[int] = None
    passes_offsides: Optional[int] = None
    passes_blocked: Optional[int] = None
    crosses: Optional[int] = None
    throw_ins: Optional[int] = None
    corner_kicks: Optional[int] = None
    cards_yellow: Optional[int] = None
    cards_red: Optional[int] = None
    cards_yellow_red: Optional[int] = None
    fouls: Optional[int] = None
    fouled: Optional[int] = None
    offsides: Optional[int] = None
    pens_won: Optional[int] = None
    pens_conceded: Optional[int] = None
    own_goals: Optional[int] = None
    ball_recoveries: Optional[int] = None
    aerials_won: Optional[int] = None
    aerials_lost: Optional[int] = None
    aerials_won_pct: Optional[float] = None
    gk_shots_on_target_against: Optional[int] = None
    gk_goals_against: Optional[int] = None
    gk_saves: Optional[int] = None
    gk_save_pct: Optional[float] = None
    gk_psxg: Optional[float] = None


class P90Stats(BaseModel):
    """Per 90 minute statistics"""
    goals_p90: Optional[float] = None
    assists_p90: Optional[float] = None
    goals_assists_p90: Optional[float] = None
    xg_p90: Optional[float] = None
    xg_assist_p90: Optional[float] = None
    xga_p90: Optional[float] = None
    npxg_p90: Optional[float] = None
    pens_made_p90: Optional[float] = None
    pens_att_p90: Optional[float] = None
    shots_p90: Optional[float] = None
    shots_on_target_p90: Optional[float] = None
    touches_p90: Optional[float] = None
    touches_def_pen_area_p90: Optional[float] = None
    touches_def_3rd_p90: Optional[float] = None
    touches_mid_3rd_p90: Optional[float] = None
    touches_att_3rd_p90: Optional[float] = None
    touches_att_pen_area_p90: Optional[float] = None
    tackles_p90: Optional[float] = None
    tackles_won_p90: Optional[float] = None
    tackles_def_3rd_p90: Optional[float] = None
    tackles_mid_3rd_p90: Optional[float] = None
    tackles_att_3rd_p90: Optional[float] = None
    challenges_p90: Optional[float] = None
    challenges_lost_p90: Optional[float] = None
    blocks_p90: Optional[float] = None
    blocked_shots_p90: Optional[float] = None
    blocked_passes_p90: Optional[float] = None
    interceptions_p90: Optional[float] = None
    clearances_p90: Optional[float] = None
    errors_p90: Optional[float] = None
    sca_p90: Optional[float] = None
    gca_p90: Optional[float] = None
    passes_completed_p90: Optional[float] = None
    passes_p90: Optional[float] = None
    progressive_passes_p90: Optional[float] = None
    carries_p90: Optional[float] = None
    progressive_carries_p90: Optional[float] = None
    carries_distance_p90: Optional[float] = None
    carries_progressive_distance_p90: Optional[float] = None
    carries_into_final_third_p90: Optional[float] = None
    carries_into_penalty_area_p90: Optional[float] = None
    miscontrols_p90: Optional[float] = None
    dispossessed_p90: Optional[float] = None
    passes_received_p90: Optional[float] = None
    progressive_passes_received_p90: Optional[float] = None
    take_ons_p90: Optional[float] = None
    take_ons_won_p90: Optional[float] = None
    take_ons_tackled_p90: Optional[float] = None
    passes_total_distance_p90: Optional[float] = None
    passes_progressive_distance_p90: Optional[float] = None
    passes_long_p90: Optional[float] = None
    passes_completed_long_p90: Optional[float] = None
    passes_medium_p90: Optional[float] = None
    passes_completed_medium_p90: Optional[float] = None
    passes_short_p90: Optional[float] = None
    passes_completed_short_p90: Optional[float] = None
    assisted_shots_p90: Optional[float] = None
    passes_into_final_third_p90: Optional[float] = None
    passes_into_penalty_area_p90: Optional[float] = None
    crosses_into_penalty_area_p90: Optional[float] = None
    passes_live_p90: Optional[float] = None
    passes_dead_p90: Optional[float] = None
    through_balls_p90: Optional[float] = None
    passes_switches_p90: Optional[float] = None
    passes_offsides_p90: Optional[float] = None
    passes_blocked_p90: Optional[float] = None
    crosses_p90: Optional[float] = None
    throw_ins_p90: Optional[float] = None
    corner_kicks_p90: Optional[float] = None
    cards_yellow_p90: Optional[float] = None
    cards_red_p90: Optional[float] = None
    cards_yellow_red_p90: Optional[float] = None
    fouls_p90: Optional[float] = None
    fouled_p90: Optional[float] = None
    offsides_p90: Optional[float] = None
    pens_won_p90: Optional[float] = None
    pens_conceded_p90: Optional[float] = None
    own_goals_p90: Optional[float] = None
    ball_recoveries_p90: Optional[float] = None
    aerials_won_p90: Optional[float] = None
    aerials_lost_p90: Optional[float] = None
    gk_shots_on_target_against_p90: Optional[float] = None
    gk_goals_against_p90: Optional[float] = None
    gk_saves_p90: Optional[float] = None
    gk_psxg_p90: Optional[float] = None

class PlayerData(BaseModel):
    """Complete player statistics data"""
    player_name: str
    player_id: int
    tfm_pic_url: str
    matches_stats: List[MatchData]
    total: TotalStats
    p90_stats: P90Stats


class PlayerRecordResponse(BaseModel):
    """Root response model"""
    data: PlayerData



# Match Info Models
class MatchInformation(BaseModel):
    """Match information"""
    match_id: int
    match_time_utc: str  # Changed from strenum to str
    result_string: str
    competition_name: str
    season_year: int
    home_team_name: str
    home_team_id: int
    home_team_logo: str
    away_team_name: str
    away_team_id: int
    away_team_logo: str


class MatchStatistics(BaseModel):
    """Match statistics"""
    home_goals: Optional[int] = None
    away_goals: Optional[int] = None
    home_poss: Optional[float] = None
    away_poss: Optional[float] = None
    home_fouls: Optional[int] = None
    away_fouls: Optional[int] = None
    home_corners: Optional[int] = None
    away_corners: Optional[int] = None
    home_offsides: Optional[int] = None
    away_offsides: Optional[int] = None
    home_xg: Optional[float] = None
    away_xg: Optional[float] = None
    home_touches: Optional[int] = None
    away_touches: Optional[int] = None
    home_clearances: Optional[int] = None
    away_clearances: Optional[int] = None
    home_interceptions: Optional[int] = None
    away_interceptions: Optional[int] = None
    home_tackles: Optional[int] = None
    away_tackles: Optional[int] = None
    home_saves_succ: Optional[int] = None
    home_saves_att: Optional[int] = None
    away_saves_succ: Optional[int] = None
    away_saves_att: Optional[int] = None
    home_shots_att: Optional[int] = None
    home_shots_succ: Optional[int] = None
    away_shots_succ: Optional[int] = None
    away_shots_att: Optional[int] = None
    home_pass_att: Optional[int] = None
    away_pass_att: Optional[int] = None
    home_pass_succ: Optional[int] = None
    away_pass_succ: Optional[int] = None


class MatchObject(BaseModel):
    """Single match with info and stats"""
    match_info: MatchInformation
    stats: MatchStatistics


# Team Stats Models
class TeamTotalStats(BaseModel):
    """Total statistics for a team"""
    team_id: int
    team_name: str
    logo_url: str
    matches_played: int
    goals: Optional[int] = None
    goals_conceded: Optional[int] = None
    poss: Optional[float] = None
    fouls: Optional[int] = None
    corners: Optional[int] = None
    offsides: Optional[int] = None
    xg: Optional[float] = None
    xg_conceded: Optional[float] = None
    touches: Optional[int] = None
    clearances: Optional[int] = None
    interceptions: Optional[int] = None
    tackles: Optional[int] = None
    saves_succ: Optional[int] = None
    saves_att: Optional[int] = None
    shots_att: Optional[int] = None
    shots_succ: Optional[int] = None
    pass_att: Optional[int] = None
    pass_succ: Optional[int] = None
    pass_pct: Optional[float] = None


class TeamPerMatchAverages(BaseModel):
    """Per match averages for a team"""
    team_id: int
    team_name: str
    logo_url: str
    matches_played: int
    goals_per_match: Optional[float] = None
    goals_conceded_per_match: Optional[float] = None
    avg_poss: Optional[float] = None
    fouls_per_match: Optional[float] = None
    corners_per_match: Optional[float] = None
    offsides_per_match: Optional[float] = None
    xg_per_match: Optional[float] = None
    xg_conceded_per_match: Optional[float] = None
    touches_per_match: Optional[float] = None
    clearances_per_match: Optional[float] = None
    interceptions_per_match: Optional[float] = None
    tackles_per_match: Optional[float] = None
    saves_succ_per_match: Optional[float] = None
    saves_att_per_match: Optional[float] = None
    shots_att_per_match: Optional[float] = None
    shots_succ_per_match: Optional[float] = None
    pass_att_per_match: Optional[float] = None
    pass_succ_per_match: Optional[float] = None
    pass_pct: Optional[float] = None


class CombinedAverages(BaseModel):
    """Combined averages for both teams"""
    matches_played: int
    goals_per_match: Optional[float] = None
    corners_per_match: Optional[float] = None
    fouls_per_match: Optional[float] = None
    offsides_per_match: Optional[float] = None
    xg_per_match: Optional[float] = None
    shots_att_per_match: Optional[float] = None
    shots_succ_per_match: Optional[float] = None


class TeamH2HData(BaseModel):
    """Head to head data"""
    matches: List[MatchObject]  # Changed from Match to MatchObject
    total_stats: Dict[str, Any] = Field(default_factory=dict)
    per_match_averages: Dict[str, Any] = Field(default_factory=dict)
    combined_averages: CombinedAverages


class TeamH2HResponse(BaseModel):
    """Root response model"""
    data: TeamH2HData

class CompStats(BaseModel):
    games_played: Optional[int] = None
    minutes: Optional[int] = None
    minutes_per_game: Optional[float] = None
    goals: Optional[int] = None
    goals_p90: Optional[float] = None
    assists: Optional[int] = None
    assists_p90: Optional[float] = None
    goals_assists: Optional[int] = None
    goals_assists_p90: Optional[float] = None
    clean_sheets: Optional[int] = None
    clean_sheets_p90: Optional[float] = None
    goals_conceded: Optional[int] = None
    goals_conceded_p90: Optional[float] = None
    cards_yellow: Optional[int] = None
    cards_red: Optional[int] = None
    second_yellows: Optional[int] = None
    
    
    
    
# /stats/nation-dist
class PlayerLeagueInfo(BaseModel):
    """Individual player information in a league"""
    player_name: str
    player_id: int
    tfm_pic_url: Optional[str] = None
    country: Optional[str] = None
    country_id: Optional[int] = None
    flag_url: Optional[str] = None
    country2: Optional[str] = None
    country2_id: Optional[int] = None
    flag2_url: Optional[str] = None
    team_name: Optional[str] = None
    common_name: Optional[str] = None
    logo_url: Optional[str] = None
    pixel_pic_url: Optional[str] = None
    position: Optional[str] = None
    other_positions: Optional[List[str]] = Field(default_factory=list)
    age: Optional[int] = None
    team_id: int
    stats: CompStats
    


class LeagueDistribution(BaseModel):
    """League distribution with players"""
    league_id: int
    comp_name: Optional[str] = None
    tier_level: Optional[str] = None
    country: Optional[str] = None
    flag_url: Optional[str] = None
    player_count: int
    players: List[PlayerLeagueInfo]


class NationDistData(BaseModel):
    """Nation distribution data"""
    league_dist: List[LeagueDistribution]


class NationDistResponse(BaseModel):
    """Root response model"""
    data: NationDistData




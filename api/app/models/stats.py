from pydantic import BaseModel, HttpUrl, AwareDatetime, Field
from typing import List, Optional, Dict, Any, Union
from decimal import Decimal
from datetime import date, datetime

from ..models.player import PlayerBaseInfo, PlayerSearchResult

class TeamInfo(BaseModel):
    team_id: int
    team_name: str
    logo_url: Optional[str] = None

class GAStats(BaseModel):
    goals: int
    assists: int
    goals_assists: int

class GAAgainst(BaseModel):
    team: TeamInfo
    stats: GAStats

class PlayerMatchStatsInfo(BaseModel):
    team: TeamInfo
    position: Optional[str] = None
    goals: Optional[int] = None
    assists: Optional[int] = None
    shots: Optional[int] = None
    sca: Optional[int] = None
    xg: Optional[float] = None
    xg_assist: Optional[float] = None

class OpponentDefender(BaseModel):
    player: PlayerBaseInfo
    position: Optional[str] = None
    age: Optional[int] = None
    minutes: Optional[int] = None

class TeamInfoWithFormation(BaseModel):
    team_id: int
    team_name: str
    logo_url: Optional[str] = None
    formation: Optional[str] = None

class MatchData(BaseModel):
    match_id: int
    comp_id: int
    match_date: date
    round: str
    season_year: int
    result_string: Optional[str] = None
    #total_goals: int
    comp_name: str
    #comp_logo: Optional[str] = None
    home_team: TeamInfoWithFormation
    away_team: TeamInfoWithFormation

class MatchWithOpponents(BaseModel):
    match_info: MatchData
    stats: PlayerMatchStatsInfo
    opp_defenders: List[OpponentDefender] = None

class MatchInfo(BaseModel):
    match_id: int
    comp_id: int
    match_date: date
    round: str
    season_year: int
    result_string: str
    total_goals: int
    comp_name: str
    #comp_logo: Optional[str] = None
    home_team: TeamInfo
    away_team: TeamInfo

class InfoMatch(BaseModel):
    match_id: int
    comp_id: int
    match_date: str
    round: Optional[str] = None
    season_year: int
    result_string: Optional[str] = None
    comp_name: str
    comp_logo: Optional[str] = None
    home_team: TeamInfo
    away_team: TeamInfo
    
    
# /stats/players-leaders/{league_id}
class SeasonStatsLeader(BaseModel):
    player_name: str
    player_id: int
    position: Optional[str] = None
    height: Optional[Union[int, float]] = None
    age: Optional[int] = None
    mpg: Optional[float] = None
    mins: Optional[int] = None
    games: Optional[int] = None
    
    # Goals and assists
    goals: Optional[int] = None
    goals_p90: Optional[float] = None
    assists: Optional[int] = None
    assists_p90: Optional[float] = None
    goals_assists: Optional[int] = None
    goals_assists_p90: Optional[float] = None
    
    # Passing
    passes_completed: Optional[int] = None
    passes_completed_p90: Optional[float] = None
    
    # Carries
    progressive_carries: Optional[int] = None
    progressive_carries_p90: Optional[float] = None
    
    # Shooting
    shots: Optional[int] = None
    shots_p90: Optional[float] = None
    
    # Defending
    tackles: Optional[int] = None
    tackles_p90: Optional[float] = None
    blocks: Optional[int] = None
    blocks_p90: Optional[float] = None
    
    # Dribbling
    take_ons_won: Optional[int] = None
    take_ons_won_p90: Optional[float] = None
    
    # Teams
    team: Optional[str] = None
    team_id: Optional[int] = None
    team_logo: Optional[str] = None
    team2: Optional[str] = None
    team2_id: Optional[int] = None
    team2_logo: Optional[str] = None
    
    # Countries
    country: Optional[str] = None
    country_flag: Optional[str] = None
    country2: Optional[str] = None
    country2_flag: Optional[str] = None

    class Config:
        extra = "allow"

class PlayerMatch(BaseModel):
    match_info: InfoMatch
    stats: PlayerMatchStatsInfo

class SeasonStatsLeaderEnhanced(BaseModel):
    player_name: str
    player_id: int
    position: Optional[str] = None
    height: Optional[Union[int, float]] = None
    age: Optional[int] = None
    mpg: Optional[float] = None
    mins: Optional[int] = None
    games: Optional[int] = None
    
    # Goals and assists
    goals: Optional[int] = None
    goals_p90: Optional[float] = None
    assists: Optional[int] = None
    assists_p90: Optional[float] = None
    goals_assists: Optional[int] = None
    goals_assists_p90: Optional[float] = None
    
    # Passing
    passes_completed: Optional[int] = None
    passes_completed_p90: Optional[float] = None
    
    # Carries
    progressive_carries: Optional[int] = None
    progressive_carries_p90: Optional[float] = None
    
    # Shooting
    shots: Optional[int] = None
    shots_p90: Optional[float] = None
    
    # Defending
    tackles: Optional[int] = None
    tackles_p90: Optional[float] = None
    blocks: Optional[int] = None
    blocks_p90: Optional[float] = None
    
    # Dribbling
    take_ons_won: Optional[int] = None
    take_ons_won_p90: Optional[float] = None
    
    # Teams
    teams: List[TeamInfo] = []
    ga_against: List[GAAgainst] = []
    # new
    matches: List[PlayerMatch] = []
    
    # Countries
    country: Optional[str] = None
    country_flag: Optional[str] = None
    country2: Optional[str] = None
    country2_flag: Optional[str] = None

    class Config:
        extra = "allow"


class SeasonStatsLeadersResponse(BaseModel):
    data: List[SeasonStatsLeader]

class SeasonStatsLeadersEnhancedResponse(BaseModel):
    data: List[SeasonStatsLeaderEnhanced]

# /stats/players/search
class PlayerSearchData(BaseModel):
    """Player search results data"""
    players: List[PlayerSearchResult]

class PlayerSearchResponse(BaseModel):
    """Root response model"""
    data: PlayerSearchData

# /stats/teams/search
class TeamSearchResult(BaseModel):
    """Single team search result"""
    team_name: str
    common_name: Optional[str] = None
    team_id: int
    logo_url: Optional[str] = None

class TeamSearchData(BaseModel):
    """Team search results data"""
    teams: List[TeamSearchResult]

class TeamSearchResponse(BaseModel):
    """Root response model"""
    data: TeamSearchData

# /stats/best-games/{league_id}
# best highscoring games by dates




class BestGamesData(BaseModel):
    matches: List[MatchInfo]

class BestGamesResponse(BaseModel):
    data: BestGamesData

# /stats/player-stats-detailed/{player_id}/{season_year}
class PlayerInfo(BaseModel):
    player_name: str
    player_id: int
    tfm_pic_url: Optional[str] = None

class CompInfo(BaseModel):
    comp_name: str
    season_year: int

class TotalStats(BaseModel):
    minutes: Optional[int] = 0
    minutes_per_game: Optional[float] = 0.0
    games: Optional[int] = 0
    goals: Optional[int] = 0
    assists: Optional[int] = 0
    goals_assists: Optional[int] = 0

class CompStats(BaseModel):
    comp: CompInfo
    total_stats: TotalStats
    ga_against: List[GAAgainst] = None

class CompStats2(BaseModel):
    comp: CompInfo
    total_stats: TotalStats
    #ga_against: List[GAAgainst] = None
    all_matches: List[MatchWithOpponents] = None

class PlayerStatsDetailedData(BaseModel):
    player: Optional[PlayerInfo] = None
    teams: List[dict]
    stats: List[CompStats]

class PlayerStatsDetailedResponse(BaseModel):
    data: PlayerStatsDetailedData

# loan watch version
class LoanWatchData(BaseModel):
    player: Optional[PlayerInfo] = None
    teams: List[dict]
    stats: List[CompStats2]
class LoanWatchResponse(BaseModel):
    data: List[LoanWatchData]

# /stats/player-stats-table/{player_id}/{league_id}/{season_year}
class TeamRankWithPlayerStats(BaseModel):
    team: TeamInfo
    rank: int
    points: int
    wins: int
    draws: int
    losses: int
    gd: int
    gp: int
    player_goals: int
    player_assists: int
    minutes: int
    minutes_per_game: float
    cards_yellow: int
    cards_red: int
    cards_yellow_red: int

class PlayerStatsTableData(BaseModel):
    player: PlayerInfo
    teams: List[TeamInfo]
    ranks: List[TeamRankWithPlayerStats]

class PlayerStatsTableResponse(BaseModel):
    data: PlayerStatsTableData

# /followers decrease
class PlayerFollowerDecrease(BaseModel):
    player_name: str
    player_id: int
    tfm_pic_url: Optional[str] = None
    followers_decrease: int
    followers_before: int
    followers_now: int

class InstaFollowersDecreaseData(BaseModel):
    start_date: datetime
    end_date: datetime
    players: List[PlayerFollowerDecrease]

class InstaFollowersDecreaseResponse(BaseModel):
    data: InstaFollowersDecreaseData

# /followers increase
class PlayerFollowerIncrease(BaseModel):
    player_name: str
    player_id: int
    tfm_pic_url: Optional[str] = None
    followers_increase: int
    followers_before: int
    followers_now: int

#class InstaFollowersResponse(BaseModel):
    #data: dict = Field(default_factory=lambda: {"players": []})
class InstaFollowersData(BaseModel):
    start_date: datetime
    end_date: datetime
    players: List[PlayerFollowerIncrease]

class InstaFollowersResponse(BaseModel):
    data: InstaFollowersData

# get followers and difference by each entry
class FollowerEntry(BaseModel):
    num_followers: int
    updated_at: datetime
    difference: Optional[int] = None

class PlayerFollowerHistory(BaseModel):
    player_name: str
    player_id: int
    tfm_pic_url: Optional[str] = None
    follower_entries: List[FollowerEntry]

class InstaFollowersHistoryData(BaseModel):
    start_date: datetime
    end_date: datetime
    players: List[PlayerFollowerHistory]

class InstaFollowersHistoryResponse(BaseModel):
    data: InstaFollowersHistoryData


# followers count
class FollowersMatchInfo(BaseModel):
    match_id: int
    comp_id: int
    match_date: date
    match_time_utc: Optional[datetime] = None
    round: Optional[str] = None
    season_year: int
    result_string: Optional[str] = None
    outcome: Optional[str] = None
    comp_name: str
    home_team: TeamInfo
    away_team: TeamInfo

class FollowersPlayerMatchStats(BaseModel):
    team: TeamInfo
    minutes: Optional[int] = None
    xi: Optional[bool] = None
    goals: Optional[int] = None
    assists: Optional[int] = None
    yellows: Optional[int] = None
    reds: Optional[int] = None

class MatchPlayed(BaseModel):
    match: FollowersMatchInfo
    stats: FollowersPlayerMatchStats

class PlayerFollowerHistoryWithGames(BaseModel):
    player_name: str
    player_id: int
    tfm_pic_url: Optional[str] = None
    follower_entries: List[FollowerEntry]
    matches_played: List[MatchPlayed]

class InstaFollowersHistoryWithGamesData(BaseModel):
    start_date: datetime
    end_date: datetime
    players: List[PlayerFollowerHistoryWithGames]

class InstaFollowersHistoryWithGamesResponse(BaseModel):
    data: InstaFollowersHistoryWithGamesData

# detailed match stats with defenders


class CompData(BaseModel):
    comp: dict
    total_stats: dict
    all_matches: List[MatchWithOpponents]

class PlayerStatsDetailedWithOppData(BaseModel):
    player: Optional[PlayerInfo] = None
    teams: List[dict]
    stats: List[CompData]

class PlayerStatsDetailedWithOppResponse(BaseModel):
    data: PlayerStatsDetailedWithOppData


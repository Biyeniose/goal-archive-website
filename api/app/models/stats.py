from pydantic import BaseModel, HttpUrl, AwareDatetime, Field
from typing import List, Optional, Dict, Any, Union
from decimal import Decimal
from datetime import date, datetime


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

class SeasonStatsLeadersResponse(BaseModel):
    data: List[SeasonStatsLeader]

# /stats/players/search
class PlayerSearchResult(BaseModel):
    """Single player search result"""
    player_name: str
    player_id: int
    tfm_pic_url: Optional[str] = None
    country: Optional[str] = None
    country_id: Optional[int] = None
    country_flag: Optional[str] = None

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
class TeamInfo(BaseModel):
    team_id: int
    team_name: str
    logo_url: Optional[str] = None

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
    minutes: int
    minutes_per_game: float
    games: int
    goals: int
    assists: int
    goals_assists: int

class GAStats(BaseModel):
    goals: int
    assists: int
    goals_assists: int

class GAAgainst(BaseModel):
    team: TeamInfo
    stats: GAStats

class CompStats(BaseModel):
    comp: CompInfo
    total_stats: TotalStats
    ga_against: List[GAAgainst]

class PlayerStatsDetailedResponse(BaseModel):
    data: dict

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


from typing import List, Optional

from pydantic import BaseModel

from .league import Competition, League, TeamRank
from .team import Team
from .utils import Country, Manager, Player, Referee


class Stadium(BaseModel):
    stadium_id: Optional[int] = None
    stadium_name: Optional[str] = None
    attendance: Optional[int] = None
    capacity: Optional[int] = None
    capacity_pct: Optional[float] = None
    address: Optional[str] = None


class MatchTeamStats(BaseModel):
    goals: Optional[int] = None
    penalty_goals: Optional[int] = None
    shots: Optional[int] = None
    possesion: Optional[int] = None
    offsides: Optional[int] = None
    corners: Optional[int] = None
    xg: Optional[float] = None
    pass_att: Optional[int] = (
        None  # passes attempted: matches.home_pass_att and .away_pass_att
    )
    pass_succ: Optional[int] = (
        None  # succesful passes matches.home_pass_succ and .away_pass_succ
    )
    league_rank: Optional[int] = None


class MarketStats(BaseModel):
    market_ticker: str
    probability: float
    volume: Optional[float] = None
    dollar_volume: Optional[float] = None
    open_interest: Optional[float] = None
    open_interest_dollar: Optional[float] = None


class MarketMatchPrediction(BaseModel):
    event_ticker: str
    winner_id: Optional[int] = None
    loser_id: Optional[int] = None
    is_draw: bool
    winner_stats: Optional[MarketStats] = None
    loser_stats: Optional[MarketStats] = None
    draw_stats: Optional[MarketStats] = None
    is_correct: Optional[bool] = None
    time_saved_utc: Optional[str] = None
    total_volume: Optional[float] = None


class Match(BaseModel):
    match_id: int
    match_date: Optional[str] = None
    match_time_utc: Optional[str] = None
    home_team: Team
    home_stats: Optional[MatchTeamStats] = None
    home_color: Optional[str] = None
    away_team: Team
    away_stats: Optional[MatchTeamStats] = None
    away_color: Optional[str] = None
    win_team_id: Optional[int] = None
    loss_team_id: Optional[int] = None
    isdraw: Optional[bool] = None
    pens: Optional[bool] = None
    extra_time: Optional[bool] = None
    is_neutral: Optional[bool] = None
    isplayed: Optional[bool] = None
    is_live: Optional[bool] = None
    match_minute: Optional[str] = None
    round: Optional[str] = None
    gameweek_number: Optional[int] = None
    stadium: Optional[Stadium] = None
    kalshi_prediction: Optional[MarketMatchPrediction] = None
    kalshi_prematch_prediction: Optional[MarketMatchPrediction] = None
    polymarket_prediction: Optional[MarketMatchPrediction] = None
    polymarket_prematch_prediction: Optional[MarketMatchPrediction] = None


class TeamInfo(BaseModel):
    team: Team
    league: League
    color_hex: Optional[str] = None
    twitter_handle: Optional[str] = None
    twitter_follower_count: Optional[int] = None
    ig_handle: Optional[str] = None
    ig_follower_count: Optional[int] = None
    fixtures: Optional[List[Match]] = None
    results: Optional[List[Match]] = None


class TeamDataResponse(BaseModel):
    data: TeamInfo


class MatchesByComp(BaseModel):
    competition: Competition
    matches: List[Match]


class MatchesByDateResponse(BaseModel):
    data: List[MatchesByComp]


class LeagueMatchesByDate(BaseModel):
    matches: List[Match]


class LeagueMatchesResponse(BaseModel):
    data: List[MatchesByComp]


# match details
class PlayerMatchStats(BaseModel):
    player: Player
    match_id: int
    # player_id: int
    team_id: int
    position: Optional[str] = None
    number: Optional[int] = None
    started: Optional[bool] = None
    subbed_on: Optional[bool] = None
    subbed_off: Optional[bool] = None
    minutes: Optional[int] = None
    goals: Optional[int] = None
    assists: Optional[int] = None
    goals_assists: Optional[int] = None
    pens_made: Optional[int] = None
    pens_att: Optional[int] = None
    xg: Optional[float] = None
    xg_assist: Optional[float] = None
    xga: Optional[float] = None
    shots: Optional[int] = None
    headed_shots: Optional[int] = None
    touches: Optional[int] = None
    blocks: Optional[int] = None
    succ_dribbles: Optional[int] = None
    tweet_mentions: Optional[int] = None
    current_team: Optional[Team] = None


class LineupDist(BaseModel):
    league_id: Optional[int] = None
    league_name: Optional[str] = None
    league_logo_url: Optional[str] = None
    country: Country
    num_players: int


class MatchTeam(BaseModel):
    team: Team
    team_stats: MatchTeamStats
    manager: Optional[Manager] = None
    formation: Optional[str] = None
    lineups: Optional[List[PlayerMatchStats]] = None
    x11_dist: Optional[List[LineupDist]] = None


class MatchInfo(BaseModel):
    match_id: int
    competition_id: int
    home: MatchTeam
    away: MatchTeam
    win_team_id: Optional[int] = None
    loss_team_id: Optional[int] = None
    isdraw: Optional[bool] = None
    pens: Optional[bool] = None
    extra_time: Optional[bool] = None

    match_date: Optional[str] = None
    match_time_utc: Optional[str] = None
    match_half_time_utc: Optional[str] = None
    match_end_time_utc: Optional[str] = None

    is_neutral: Optional[bool] = None
    isplayed: Optional[bool] = None
    is_live: Optional[bool] = None
    match_minute: Optional[str] = None
    round: Optional[str] = None
    gameweek_number: Optional[int] = None
    stadium: Optional[Stadium] = None
    referee: Optional[Referee] = None


class MatchEvent(BaseModel):
    event_id: int
    team_id: int
    event_type: str
    body_part: Optional[str] = None
    home_goals: Optional[int] = None
    away_goals: Optional[int] = None
    minute: Optional[int] = None
    add_minute: Optional[int] = None
    active_player: Player
    passive_player: Optional[Player] = None
    active_notes: Optional[str] = None


class EndOfDayTable(BaseModel):
    rank: TeamRank
    rank_difference: int
    points_difference: int
    gd_difference: int


class MatchStatsTimeStamp(BaseModel):
    minute: int
    add_minute: int
    home_stats: Optional[MatchTeamStats] = None
    away_stats: Optional[MatchTeamStats] = None


class MatchDetails(BaseModel):
    match: MatchInfo
    events: List[MatchEvent]
    h2h: List[MatchesByComp]
    home_last5: List[MatchesByComp]
    away_last5: List[MatchesByComp]
    league_ranks_eod: Optional[List[EndOfDayTable]]
    possesion_sequence: Optional[List[MatchStatsTimeStamp]]


class MatchDetailsResponse(BaseModel):
    data: MatchDetails


# /teams/{team_id}/wc2026
class PlayersFromLeague(BaseModel):
    team_id: List[int]
    player_id: int
    stat_value: int


class PlayerStats(BaseModel):
    player: Player
    stat_value: int


class TeamDist(BaseModel):
    team_id: int
    players: List[PlayerStats]
    stat_value: int


class LeagueStatsDist(BaseModel):
    country: str
    league_logos: Optional[List[str]] = None
    teams: List[Team]
    total_stat_value: int
    num_players: int
    top_players: Optional[List[PlayersFromLeague]]
    teams_player_dist: Optional[List[TeamDist]]


class WCMatch(BaseModel):
    competition: Competition
    match: Match


class NationDistData(BaseModel):
    matches: Optional[List[WCMatch]] = None
    league_dist: Optional[List[LeagueStatsDist]] = None


class NationDistResponse(BaseModel):
    data: NationDistData


# competition squads
class SquadPlayer(BaseModel):
    player: Player
    number: Optional[int] = None
    club_team: Optional[Team] = None


class CompetitionSquad(BaseModel):
    team: Team
    competition: Competition
    matches: Optional[List[Match]] = None
    players: Optional[List[SquadPlayer]] = None
    manager: Optional[Manager] = None
    date_announced: Optional[str] = None
    type: Optional[str] = None


class CompetitionSquadsResponse(BaseModel):
    data: CompetitionSquad


# kalshi markets
class KalshiForecastHistory(BaseModel):
    market_ticker: str
    end_period_ts: str
    probability: float  # column is called raw_numerical_forecast


class KalshiMarket(BaseModel):
    market_id: str
    ticker: str  # ex: KXPGATOUR-THPC26-ANOR
    title: str
    name: str
    team_id: Optional[int]
    forecast_history: Optional[List[KalshiForecastHistory]]


class KalshiForecastData(BaseModel):
    match_id: int
    kalshi_event_ticker: str
    markets: List[KalshiMarket]
    pre_match_prediction: Optional[MarketMatchPrediction] = None
    latest_prediction: Optional[MarketMatchPrediction] = None


class KalshiForecastResponse(BaseModel):
    data: KalshiForecastData


# polym markets
class PolymForecastHistory(BaseModel):
    token_id: int
    end_period_ts: str
    probability: float  # column is probability


class PolymTokens(BaseModel):
    market_id: int
    token_id: int
    outcome: str
    forecast_history: Optional[List[PolymForecastHistory]]


class PolymMarket(BaseModel):
    id: int
    condition_id: str
    question: str
    slug: str
    outcomes: List[str]
    team_id: Optional[int]
    tokens: Optional[List[PolymTokens]]
    # forecast_history: Optional[List[PolymForecastHistory]]


class PolymForecastData(BaseModel):
    match_id: int
    polym_event_ticker: str
    prediction: Optional[MarketMatchPrediction] = None
    markets: List[PolymMarket]


class PolymForecastResponse(BaseModel):
    data: PolymForecastData

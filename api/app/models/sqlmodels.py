from datetime import date, datetime, time, timezone
from typing import Optional, List, Dict, Any
from decimal import Decimal
from sqlalchemy import ARRAY, DECIMAL, Index, String, UniqueConstraint, func, TIMESTAMP, Text, ForeignKey, Numeric, SmallInteger, BigInteger, Boolean, Column, CheckConstraint
from sqlalchemy.dialects.postgresql import TSVECTOR, JSONB
from sqlmodel import Field, Relationship, SQLModel, Column, Enum as SAEnum
import enum


# players table
class Player(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}
    __tablename__ = "players"
    
    player_id: int = Field(primary_key=True)
    player_name: Optional[str] = Field(default=None, index=True)
    full_name: Optional[str] = Field(default=None)
    position: Optional[str] = Field(default=None)
    dob: Optional[date] = Field(default=None)
    other_positions: Optional[List[str]] = Field(
        default=None, 
        sa_column=Column(ARRAY(String))
    )
    age: Optional[int] = Field(default=None)  # smallint maps to int
    tfm_id: Optional[int] = Field(default=None, unique=True)
    tfm_url: Optional[str] = Field(default=None)
    curr_team_id: Optional[int] = Field(default=None, foreign_key="teams.team_id", index=True)
    fbref_id: Optional[str] = Field(default=None, unique=True)
    fbref_url: Optional[str] = Field(default=None)
    country_id: Optional[int] = Field(default=None, foreign_key="countries.country_id")
    country2_id: Optional[int] = Field(default=None, foreign_key="countries.country_id")
    height: Optional[float] = Field(default=None)  # real maps to float
    foot: Optional[str] = Field(default=None)
    isRetired: Optional[bool] = Field(default=None)
    pob_country_id: Optional[int] = Field(default=None, foreign_key="countries.country_id")
    pob_city: Optional[str] = Field(default=None)
    instagram: Optional[str] = Field(default=None)
    twitter: Optional[str] = Field(default=None)
    onLoan: Optional[bool] = Field(default=None)
    noClub: Optional[bool] = Field(default=None)
    player_slug: Optional[str] = Field(default=None)
    market_value: Optional[Decimal] = Field(default=None)
    full_name2: Optional[str] = Field(default=None)
    updated_at: Optional[datetime] = Field(default=None)
    team_joined_date: Optional[date] = Field(default=None)
    team_contract_exp: Optional[date] = Field(default=None)
    parent_team_id: Optional[int] = Field(default=None, foreign_key="teams.team_id")
    parent_team_exp: Optional[date] = Field(default=None)
    last_extension: Optional[date] = Field(default=None)
    curr_number: Optional[int] = Field(default=None)
    tfm_pic_url: Optional[str] = Field(default=None)
    pic_url: Optional[str] = Field(default=None)
    pixel_pic_url: Optional[str] = Field(default=None)
    track_followers: Optional[bool] = Field(default=None)
    wikipedia_url: Optional[str] = Field(default=None)
    kalshi_id: Optional[str] = Field(default=None, max_length=255)
    espn_id: Optional[str] = Field(default=None, unique=True)
    livescore_id: Optional[int] = Field(default=None, unique=True)
    livescore_url: Optional[str] = Field(default=None)
    flashscore_id: Optional[str] = Field(default=None, unique=True)
    flashscore_url: Optional[str] = Field(default=None)

    
    


# transfers
class Transfer(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}
    __tablename__ = "transfers"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    from_team_id: Optional[int] = Field(default=None, foreign_key="teams.team_id", index=True)
    to_team_id: Optional[int] = Field(default=None, foreign_key="teams.team_id", index=True)
    player_id: Optional[int] = Field(default=None, foreign_key="players.player_id", index=True)
    isloan: Optional[bool] = Field(default=False)
    fee: Optional[Decimal] = Field(default=None, max_digits=15, decimal_places=2)
    player_value: Optional[Decimal] = Field(default=None, max_digits=15, decimal_places=2)
    transfer_date: Optional[date] = Field(default=None)
    season: Optional[int] = Field(default=None, index=True)
    season_str: Optional[str] = Field(default=None)

# groups
class Group(SQLModel, table=True):
    __tablename__ = "groups"
    
    group_id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    comp_id: Optional[int] = Field(default=None, foreign_key="competitions.competition_id")
    
    __table_args__ = (
        UniqueConstraint("name", "comp_id"),
        {"extend_existing": True}
    )

# ig followers
class IgFollowers(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}
    __tablename__ = "ig_followers"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    player_id: Optional[int] = Field(default=None, foreign_key="players.player_id", index=True)
    num_followers: Optional[int] = Field(default=None)
    updated_at: Optional[datetime] = Field(default=None)
    

# countries table
class Country(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}
    __tablename__ = "countries"
    
    country_id: int = Field(primary_key=True)
    name: str = Field(unique=True)
    iso_code_2: Optional[str] = Field(default=None, max_length=2)
    iso_code_3: Optional[str] = Field(default=None, max_length=3)
    continent: Optional[str] = Field(default=None)
    population: Optional[int] = Field(default=None)  # bigint maps to int
    updated_at: Optional[datetime] = Field(default=None)
    second_name: Optional[str] = Field(default=None)
    flag_url: Optional[str] = Field(default=None)

# leagues tables
class League(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}
    __tablename__ = "leagues"
    
    league_id: int = Field(primary_key=True)
    name: Optional[str] = Field(default=None, unique=True)
    country_id: Optional[int] = Field(default=None, foreign_key="countries.country_id")
    confederation: Optional[str] = Field(default=None)
    scope: Optional[str] = Field(default=None)
    tier_level: Optional[str] = Field(default=None)
    competition_level: Optional[str] = Field(default=None)
    format: Optional[str] = Field(default=None)
    kalshi_series_ticker:    Optional[str] = Field(default=None, foreign_key="kalshi_series.ticker", index=True)
    polymarket_series_ticker: Optional[str] = Field(default=None, foreign_key="polymarket_series.ticker", index=True)

# competitions table
class Competition(SQLModel, table=True):
    __table_args__ = (
        UniqueConstraint("league_id", "season_year", "scrape_league_id",
                         name="uq_competitions_league_season_scrape_league"),
        {"extend_existing": True},
    )
    __tablename__ = "competitions"
    
    competition_id: int = Field(primary_key=True)
    league_id: Optional[int] = Field(default=None, foreign_key="leagues.league_id")
    name: Optional[str] = Field(default=None)
    stage: Optional[str] = Field(default=None)
    season_year: Optional[int] = Field(default=None)  # smallint maps to int
    stage_order: Optional[int] = Field(default=None)  # smallint maps to int
    scrape_league_id: Optional[int] = Field(default=None, foreign_key="scraping_leagues.id")
    logo_url: Optional[str] = Field(default=None)
    pixel_logo_url: Optional[str] = Field(default=None)
    trophy_url: Optional[str] = Field(default=None)
    

# scraping_leagues tables
class ScrapingLeague(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}
    __tablename__ = "scraping_leagues"
    
    id: int = Field(primary_key=True)
    name: Optional[str] = Field(default=None, unique=True)
    tfm_name: Optional[str] = Field(default=None)
    tfm_url: Optional[str] = Field(default=None)
    fbref_id: Optional[str] = Field(default=None)
    fbref_url: Optional[str] = Field(default=None)
    fotmob_id: Optional[int] = Field(default=None)
    fotmob_url: Optional[str] = Field(default=None)
    espn_id: Optional[str] = Field(default=None)
    espn_url: Optional[str] = Field(default=None)
    playerstats_url: Optional[str] = Field(default=None, unique=True)
    livescore_id: Optional[str] = Field(default=None)
    flashscore_id: Optional[str] = Field(default=None)
    livescore_url: Optional[str] = Field(default=None, unique=True)
    flashscore_url: Optional[str] = Field(default=None, unique=True)



# ranks table
class Rank(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}
    __tablename__ = "ranks"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    competition_id: Optional[int] = Field(default=None, foreign_key="competitions.competition_id", index=True)
    team_id: int = Field(foreign_key="teams.team_id", index=True)
    rank: Optional[int] = Field(default=None, index=True)
    points: Optional[int] = Field(default=None)
    wins: Optional[int] = Field(default=None)
    losses: Optional[int] = Field(default=None)
    draws: Optional[int] = Field(default=None)
    gd: Optional[int] = Field(default=None)
    goals_f: Optional[int] = Field(default=None)
    goals_a: Optional[int] = Field(default=None)
    gp: Optional[int] = Field(default=None)
    info: Optional[str] = Field(default=None)
    group_id: Optional[int] = Field(default=None, foreign_key="groups.group_id", index=True)
    

# teams table
class Team(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}
    __tablename__ = "teams"
    
    team_id: int = Field(primary_key=True)
    name: str
    country_id: Optional[int] = Field(default=None, foreign_key="countries.country_id")
    league_id: Optional[int] = Field(default=None, foreign_key="leagues.league_id")
    tfm_id: Optional[int] = Field(default=None, index=True)
    fbref_id: Optional[str] = Field(default=None)
    type: str
    parent_team_id: Optional[int] = Field(default=None, foreign_key="teams.team_id")
    stadium_id: Optional[int] = Field(default=None, foreign_key="stadiums.id")
    manager_id: Optional[int] = Field(default=None, foreign_key="managers.id")
    tfm_url: Optional[str] = Field(default=None)
    fbref_url: Optional[str] = Field(default=None)
    city: Optional[str] = Field(default=None)
    color_hex: Optional[str] = Field(default=None, max_length=7)  # #FFFFFF format
    level: Optional[str] = Field(default=None)
    founded_year: Optional[int] = Field(default=None)  # smallint maps to int
    gender: Optional[str] = Field(default=None, max_length=10)
    common_name: Optional[str] = Field(default=None, index=True)
    short_name: Optional[str] = Field(default=None)
    slug: Optional[str] = Field(default=None)
    logo_url: Optional[str] = Field(default=None)
    fotmob_id: Optional[int] = Field(default=None)
    fotmob_url: Optional[str] = Field(default=None)
    twitter_handle: Optional[str] = Field(default=None)
    instagram_handle: Optional[str] = Field(default=None)
    twitter_user_rest_id: Optional[str] = Field(
        default=None,
        foreign_key="twitter_users.rest_id",
        index=True,
        max_length=255
    )
    instagram_user_id: Optional[str] = Field(
        default=None,
        foreign_key="instagram_users.user_id",
        index=True,
        max_length=255
    )
    kalshi_id: Optional[str] = Field(default=None, max_length=255)
    espn_id: Optional[int] = Field(default=None, index=True)
    livescore_id: Optional[int] = Field(default=None, unique=True)
    livescore_url: Optional[str] = Field(default=None)
    livescore_name: Optional[str] = Field(default=None)
    flashscore_name: Optional[str] = Field(default=None)
    flashscore_id: Optional[str] = Field(default=None, unique=True)
    flashscore_url: Optional[str] = Field(default=None)
    wikipedia_url: Optional[str] = Field(default=None)


    
# squads table
class Squad(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}
    __tablename__ = "squads"
    
    id: int = Field(primary_key=True)
    player_id: int = Field(foreign_key="players.player_id")
    team_id: int = Field(foreign_key="teams.team_id")
    number: Optional[int] = Field(default=None)
    value: Optional[Decimal] = Field(default=None)
    season_year: int
    position: Optional[str] = Field(default=None)

# managers table
class Manager(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}
    __tablename__ = "managers"
    
    id: int = Field(primary_key=True)
    name: str = Field(index=True)
    country_id: Optional[int] = Field(default=None, foreign_key="countries.country_id", index=True)
    active: Optional[bool] = Field(default=None)
    dob: Optional[date] = Field(default=None)
    country2_id: Optional[int] = Field(default=None, foreign_key="countries.country_id")
    age: Optional[int] = Field(default=None)
    tfm_id: Optional[int] = Field(default=None, unique=True)
    tfm_url: Optional[str] = Field(default=None)
    livescore_id: Optional[int] = Field(default=None, unique=True)

# stadiums table
class Stadium(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}
    __tablename__ = "stadiums"
    
    id: int = Field(primary_key=True)
    name: str = Field(index=True, unique=True)
    city: Optional[str] = Field(default=None)
    country_id: Optional[int] = Field(default=None, foreign_key="countries.country_id", index=True)
    capacity: Optional[int] = Field(default=None)
    tfm_id: Optional[int] = Field(default=None)
    tfm_url: Optional[str] = Field(default=None)

class MatchEventType(str, enum.Enum):
    goal                    = "goal"
    own_goal                = "own goal"
    penalty_goal            = "penalty goal"
    penalty_miss            = "penalty miss"
    penalty_shootout_goal   = "penalty shoot-out goal"
    penalty_shootout_miss   = "penalty shoot-out miss"
    yellow_card             = "yellow card"
    red_card                = "red card"
    second_yellow_card      = "second yellow card"
    shot_blocked            = "shot blocked"
    shot_off_target         = "shot off target"
    shot_on_target          = "shot on target"
    substitute_in           = "substitute in"
    woodwork                = "woodwork"
    ht_end                  = "ht-end"
    ft_end                  = "ft-end"
    aet_end                 = "aet-end"
    var_disallowed_goal = "var disallowed goal"

# match events
class MatchEvent(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}
    __tablename__ = "match_events"
    
    event_id: Optional[int] = Field(default=None, primary_key=True)
    match_id: int = Field(foreign_key="matches.match_id", index=True)
    team_id: int = Field(foreign_key="teams.team_id", index=True)
    event_type: MatchEventType = Field(sa_column=Column(SAEnum(MatchEventType, name="match_event_type"), nullable=False))
    minute: int
    add_minute: int = Field(default=0)
    active_player_id: int = Field(foreign_key="players.player_id", index=True)
    passive_player_id: Optional[int] = Field(default=None, foreign_key="players.player_id", index=True)
    home_goals: Optional[int] = None
    away_goals: Optional[int] = None
    opp_team_id: Optional[int] = Field(default=None, foreign_key="teams.team_id")
    body_part: Optional[str] = None
    active_notes: Optional[str] = None
    xg_shot: Optional[Decimal] = None
    psxg_shot: Optional[Decimal] = None
    passive_notes: Optional[str] = None
    distance: Optional[int] = None
    second_passive_player_id: Optional[int] = Field(default=None, foreign_key="players.player_id")
    second_passive_notes: Optional[str] = None
    created_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(TIMESTAMP(timezone=True), server_default=func.now()),
    )
    updated_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(TIMESTAMP(timezone=True), onupdate=func.now(), server_default=func.now()),
    )
    inserted_by: Optional[str] = Field(default=None)
    last_updated_by: Optional[str] = Field(default=None)

# discarded match events
class DiscardedMatchEvent(SQLModel, table=True):
    __tablename__ = "discarded_match_events"

    id:                       Optional[int] = Field(default=None, primary_key=True)
    match_id:                 Optional[int] = Field(default=None, foreign_key="matches.match_id", index=True)
    team_id:                  Optional[int] = Field(default=None, foreign_key="teams.team_id", index=True)
    event_type:               Optional[MatchEventType] = Field(sa_column=Column(SAEnum(MatchEventType, name="match_event_type"), nullable=True))
    minute:                   Optional[int] = None
    add_minute:               int           = Field(default=0)
    active_player_id:         Optional[int] = Field(default=None, foreign_key="players.player_id", index=True)
    passive_player_id:        Optional[int] = Field(default=None, foreign_key="players.player_id")
    home_goals:               Optional[int] = None
    away_goals:               Optional[int] = None
    opp_team_id:              Optional[int] = Field(default=None, foreign_key="teams.team_id")
    body_part:                Optional[str] = None
    active_notes:             Optional[str] = None
    xg_shot:                  Optional[float] = None
    psxg_shot:                Optional[float] = None
    passive_notes:            Optional[str] = None
    distance:                 Optional[int] = None
    second_passive_player_id: Optional[int] = Field(default=None, foreign_key="players.player_id")
    second_passive_notes:     Optional[str] = None
    discard_reason:           Optional[str] = None
    created_at:               Optional[datetime] = Field(default_factory=lambda: datetime.now(timezone.utc))
    inserted_by:              Optional[str] = None
    last_updated_by:          Optional[str] = None
    
# matches table
class Match(SQLModel, table=True):
    __table_args__ = (
        CheckConstraint("home_id <> away_id", name="chk_matches_home_away_different"),
        {"extend_existing": True},
    )
    __tablename__ = "matches"
    
    # Primary key
    match_id: Optional[int] = Field(default=None, primary_key=True)
    
    # IDs and URLs
    fbref_id: Optional[str] = Field(default=None, unique=True)
    fbref_url: Optional[str] = None
    tfm_id: Optional[int] = Field(default=None, unique=True)
    tfm_url: Optional[str] = None
    
    # Competition and date
    comp_id: Optional[int] = Field(default=None, foreign_key="competitions.competition_id", index=True)
    match_date: Optional[date] = Field(default=None, index=True)
    match_time_utc: Optional[datetime] = None
    round: Optional[str] = None
    gameweek_number: Optional[int] = None
    
    # Teams
    home_id: Optional[int] = Field(default=None, foreign_key="teams.team_id", index=True)
    away_id: Optional[int] = Field(default=None, foreign_key="teams.team_id", index=True)
    
    # Score
    home_goals: Optional[int] = None
    away_goals: Optional[int] = None
    home_xg: Optional[Decimal] = None
    away_xg: Optional[Decimal] = None
    result_string: Optional[str] = None
    
    # Match outcome
    isdraw: Optional[bool] = None
    extra_time: Optional[bool] = None
    pens: Optional[bool] = None
    pen_home_goals: Optional[int] = None
    pen_away_goals: Optional[int] = None
    win_team: Optional[int] = Field(default=None, foreign_key="teams.team_id")
    loss_team: Optional[int] = Field(default=None, foreign_key="teams.team_id")
    
    # Formations
    home_formation: Optional[str] = None
    away_formation: Optional[str] = None
    
    # Managers
    home_manager_id: Optional[int] = Field(default=None, foreign_key="managers.id")
    away_manager_id: Optional[int] = Field(default=None, foreign_key="managers.id")
    
    # Rankings
    home_ranking: Optional[int] = None
    away_ranking: Optional[int] = None
    
    # Captains
    home_captain_id: Optional[int] = Field(default=None, foreign_key="players.player_id")
    away_captain_id: Optional[int] = Field(default=None, foreign_key="players.player_id")
    
    # Possession
    home_poss: Optional[Decimal] = None
    away_poss: Optional[Decimal] = None
    
    # Offsides
    home_offsides: Optional[int] = None
    away_offsides: Optional[int] = None
    
    # Fouls
    home_fouls: Optional[int] = None
    away_fouls: Optional[int] = None
    
    # Free kicks
    home_freekicks: Optional[int] = None
    away_freekicks: Optional[int] = None
    
    # Corners
    home_corners: Optional[int] = None
    away_corners: Optional[int] = None
    
    # Match officials and venue
    ref_id: Optional[int] = Field(default=None, foreign_key="referees.id")
    stadium_id: Optional[int] = Field(default=None, foreign_key="stadiums.id")
    attendance: Optional[int] = None
    
    # Match status
    isplayed: Optional[bool] = None
    
    # Shooting stats
    home_shots: Optional[int] = None
    away_shots: Optional[int] = None
    home_shots_ontg: Optional[int] = None
    away_shots_ontg: Optional[int] = None
    home_shots_oftg: Optional[int] = None
    away_shots_oftg: Optional[int] = None
    home_shots_blocked: Optional[int] = None
    away_shots_blocked: Optional[int] = None
    home_shots_out_box: Optional[int] = None
    away_shots_out_box: Optional[int] = None
    home_shots_in_box: Optional[int] = None
    away_shots_in_box: Optional[int] = None
    home_shots_acc: Optional[Decimal] = None
    away_shots_acc: Optional[Decimal] = None

    # Crosses
    home_crosses: Optional[int] = None
    away_crosses: Optional[int] = None
    
    # Touches
    home_touches: Optional[int] = None
    away_touches: Optional[int] = None
    
    # Tackles
    home_tackles: Optional[int] = None
    away_tackles: Optional[int] = None
    
    # Interceptions
    home_interceptions: Optional[int] = None
    away_interceptions: Optional[int] = None
    
    # Aerials
    home_aerials_won: Optional[int] = None
    away_aerials_won: Optional[int] = None
    
    # Clearances
    home_clearances: Optional[int] = None
    away_clearances: Optional[int] = None
    
    # Goal kicks
    home_goal_kicks: Optional[int] = None
    away_goal_kicks: Optional[int] = None
    
    # Throw ins
    home_throw_ins: Optional[int] = None
    away_throw_ins: Optional[int] = None
    
    # Long balls
    home_long_balls: Optional[int] = None
    away_long_balls: Optional[int] = None
    
    # Passing stats
    home_pass_acc: Optional[Decimal] = None
    away_pass_acc: Optional[Decimal] = None
    home_pass_att: Optional[int] = None
    away_pass_att: Optional[int] = None
    home_pass_succ: Optional[int] = None
    away_pass_succ: Optional[int] = None
    
    # Saves stats
    home_saves_acc: Optional[Decimal] = None
    away_saves_acc: Optional[Decimal] = None
    home_saves: Optional[int] = None
    away_saves: Optional[int] = None
    analyze_tweets: Optional[bool] = Field(default=False)
    home_insta_keywords: Optional[Dict[str, Any]] = Field(
        default=None,
        sa_column=Column(JSONB)
    )
    away_insta_keywords: Optional[Dict[str, Any]] = Field(
        default=None,
        sa_column=Column(JSONB)
    )
    home_twitter_keywords: Optional[Dict[str, Any]] = Field(
        default=None,
        sa_column=Column(JSONB)
    )
    away_twitter_keywords: Optional[Dict[str, Any]] = Field(
        default=None,
        sa_column=Column(JSONB)
    )
    match_end_time_utc: Optional[datetime] = None
    polymarket_event_ticker: Optional[str] = Field(
        default=None,
        foreign_key="polymarket_events.ticker",
        index=True,
        max_length=300,
    )
    kalshi_event_ticker: Optional[str] = Field(
        default=None,
        foreign_key="kalshi_events.event_ticker",
        index=True,
        max_length=150,
    )
    espn_match_id: Optional[str] = Field(default=None, unique=True)
    espn_url: Optional[str] = Field(default=None)
    playerstats_url: Optional[str] = Field(default=None, unique=True)
    # extra refs
    assistant_ref_id:   Optional[int] = Field(default=None, foreign_key="referees.id", index=True)
    assistant_ref2_id:  Optional[int] = Field(default=None, foreign_key="referees.id", index=True)
    fourth_official_id: Optional[int] = Field(default=None, foreign_key="referees.id", index=True)
    var_id:             Optional[int] = Field(default=None, foreign_key="referees.id", index=True)
    var2_id:            Optional[int] = Field(default=None, foreign_key="referees.id", index=True)

    home_agg: Optional[int] = None
    away_agg: Optional[int] = None
    half_time_home_goals: Optional[int] = None
    half_time_away_goals: Optional[int] = None
    aet_home_goals: Optional[int] = None
    aet_away_goals: Optional[int] = None
    ft_home_goals: Optional[int] = None
    ft_away_goals: Optional[int] = None
    youtube_url: Optional[str] = None
    youtube_url2: Optional[str] = None
    livescore_url: Optional[str] = Field(default=None, unique=True)
    flashscore_url: Optional[str] = Field(default=None, unique=True)
    home_yellow_cards: Optional[int] = None
    away_yellow_cards: Optional[int] = None
    home_red_cards: Optional[int] = None
    away_red_cards: Optional[int] = None
    home_red_cards_h1: Optional[int] = Field(default=None)
    away_red_cards_h1: Optional[int] = Field(default=None)
    home_red_cards_h2: Optional[int] = Field(default=None)
    away_red_cards_h2: Optional[int] = Field(default=None)
    home_red_cards_et: Optional[int] = Field(default=None)
    away_red_cards_et: Optional[int] = Field(default=None)
    is_neutral: Optional[bool] = None
    created_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(TIMESTAMP(timezone=True), server_default=func.now()),
    )
    updated_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(TIMESTAMP(timezone=True), onupdate=func.now(), server_default=func.now()),
    )
    inserted_by: Optional[str] = Field(default=None)
    last_updated_by: Optional[str] = Field(default=None)

    # xG by period
    home_xg_h1: Optional[float] = None
    away_xg_h1: Optional[float] = None
    home_xg_h2: Optional[float] = None
    away_xg_h2: Optional[float] = None
    home_xg_et: Optional[float] = None
    away_xg_et: Optional[float] = None

    # xG on target
    home_xg_ontg: Optional[float] = None
    away_xg_ontg: Optional[float] = None
    home_xg_ontg_h1: Optional[float] = None
    away_xg_ontg_h1: Optional[float] = None
    home_xg_ontg_h2: Optional[float] = None
    away_xg_ontg_h2: Optional[float] = None
    home_xg_ontg_et: Optional[float] = None
    away_xg_ontg_et: Optional[float] = None

    # xA (expected assists)
    home_xa: Optional[float] = None
    away_xa: Optional[float] = None
    home_xa_h1: Optional[float] = None
    away_xa_h1: Optional[float] = None
    home_xa_h2: Optional[float] = None
    away_xa_h2: Optional[float] = None
    home_xa_et: Optional[float] = None
    away_xa_et: Optional[float] = None

    # Possession by period
    home_poss_h1: Optional[float] = None
    away_poss_h1: Optional[float] = None
    home_poss_h2: Optional[float] = None
    away_poss_h2: Optional[float] = None
    home_poss_et: Optional[float] = None
    away_poss_et: Optional[float] = None

    # Shots by period
    home_shots_h1: Optional[int] = None
    away_shots_h1: Optional[int] = None
    home_shots_h2: Optional[int] = None
    away_shots_h2: Optional[int] = None
    home_shots_et: Optional[int] = None
    away_shots_et: Optional[int] = None

    # Shots on target by period
    home_shots_ontg_h1: Optional[int] = None
    away_shots_ontg_h1: Optional[int] = None
    home_shots_ontg_h2: Optional[int] = None
    away_shots_ontg_h2: Optional[int] = None
    home_shots_ontg_et: Optional[int] = None
    away_shots_ontg_et: Optional[int] = None

    # Shots in/out of box by period
    home_shots_in_box_h1: Optional[int] = None
    away_shots_in_box_h1: Optional[int] = None
    home_shots_in_box_h2: Optional[int] = None
    away_shots_in_box_h2: Optional[int] = None
    home_shots_in_box_et: Optional[int] = None
    away_shots_in_box_et: Optional[int] = None
    home_shots_out_box_h1: Optional[int] = None
    away_shots_out_box_h1: Optional[int] = None
    home_shots_out_box_h2: Optional[int] = None
    away_shots_out_box_h2: Optional[int] = None
    home_shots_out_box_et: Optional[int] = None
    away_shots_out_box_et: Optional[int] = None

    # Big chances
    home_big_chances: Optional[int] = None
    away_big_chances: Optional[int] = None
    home_big_chances_h1: Optional[int] = None
    away_big_chances_h1: Optional[int] = None
    home_big_chances_h2: Optional[int] = None
    away_big_chances_h2: Optional[int] = None
    home_big_chances_et: Optional[int] = None
    away_big_chances_et: Optional[int] = None

    # Corners by period
    home_corners_h1: Optional[int] = None
    away_corners_h1: Optional[int] = None
    home_corners_h2: Optional[int] = None
    away_corners_h2: Optional[int] = None
    home_corners_et: Optional[int] = None
    away_corners_et: Optional[int] = None

    # Passes accurate by period
    home_pass_acc_h1: Optional[float] = None
    away_pass_acc_h1: Optional[float] = None
    home_pass_acc_h2: Optional[float] = None
    away_pass_acc_h2: Optional[float] = None
    home_pass_acc_et: Optional[float] = None
    away_pass_acc_et: Optional[float] = None

    # Pass attempts by period
    home_pass_att_h1: Optional[int] = None
    away_pass_att_h1: Optional[int] = None
    home_pass_att_h2: Optional[int] = None
    away_pass_att_h2: Optional[int] = None
    home_pass_att_et: Optional[int] = None
    away_pass_att_et: Optional[int] = None

    # Passes successful by period
    home_pass_succ_h1: Optional[int] = None
    away_pass_succ_h1: Optional[int] = None
    home_pass_succ_h2: Optional[int] = None
    away_pass_succ_h2: Optional[int] = None
    home_pass_succ_et: Optional[int] = None
    away_pass_succ_et: Optional[int] = None

    # Touches in opposition box
    home_touches_opp_box: Optional[int] = None
    away_touches_opp_box: Optional[int] = None
    home_touches_opp_box_h1: Optional[int] = None
    away_touches_opp_box_h1: Optional[int] = None
    home_touches_opp_box_h2: Optional[int] = None
    away_touches_opp_box_h2: Optional[int] = None
    home_touches_opp_box_et: Optional[int] = None
    away_touches_opp_box_et: Optional[int] = None

    # Yellow cards by period
    home_yellow_cards_h1: Optional[int] = None
    away_yellow_cards_h1: Optional[int] = None
    home_yellow_cards_h2: Optional[int] = None
    away_yellow_cards_h2: Optional[int] = None
    home_yellow_cards_et: Optional[int] = None
    away_yellow_cards_et: Optional[int] = None

    # Fouls by period
    home_fouls_h1: Optional[int] = None
    away_fouls_h1: Optional[int] = None
    home_fouls_h2: Optional[int] = None
    away_fouls_h2: Optional[int] = None
    home_fouls_et: Optional[int] = None
    away_fouls_et: Optional[int] = None

    # Tackles by period
    home_tackles_h1: Optional[int] = None
    away_tackles_h1: Optional[int] = None
    home_tackles_h2: Optional[int] = None
    away_tackles_h2: Optional[int] = None
    home_tackles_et: Optional[int] = None
    away_tackles_et: Optional[int] = None

    # Duels won
    home_duels_won: Optional[int] = None
    away_duels_won: Optional[int] = None
    home_duels_won_h1: Optional[int] = None
    away_duels_won_h1: Optional[int] = None
    home_duels_won_h2: Optional[int] = None
    away_duels_won_h2: Optional[int] = None
    home_duels_won_et: Optional[int] = None
    away_duels_won_et: Optional[int] = None

    # Interceptions by period
    home_interceptions_h1: Optional[int] = None
    away_interceptions_h1: Optional[int] = None
    home_interceptions_h2: Optional[int] = None
    away_interceptions_h2: Optional[int] = None
    home_interceptions_et: Optional[int] = None
    away_interceptions_et: Optional[int] = None

    # Clearances by period
    home_clearances_h1: Optional[int] = None
    away_clearances_h1: Optional[int] = None
    home_clearances_h2: Optional[int] = None
    away_clearances_h2: Optional[int] = None
    home_clearances_et: Optional[int] = None
    away_clearances_et: Optional[int] = None

    # Errors leading to shot
    home_errors_to_shot: Optional[int] = None
    away_errors_to_shot: Optional[int] = None
    home_errors_to_shot_h1: Optional[int] = None
    away_errors_to_shot_h1: Optional[int] = None
    home_errors_to_shot_h2: Optional[int] = None
    away_errors_to_shot_h2: Optional[int] = None
    home_errors_to_shot_et: Optional[int] = None
    away_errors_to_shot_et: Optional[int] = None

    # Errors leading to goal
    home_errors_to_goal: Optional[int] = None
    away_errors_to_goal: Optional[int] = None
    home_errors_to_goal_h1: Optional[int] = None
    away_errors_to_goal_h1: Optional[int] = None
    home_errors_to_goal_h2: Optional[int] = None
    away_errors_to_goal_h2: Optional[int] = None
    home_errors_to_goal_et: Optional[int] = None
    away_errors_to_goal_et: Optional[int] = None

    # Saves by period
    home_saves_h1: Optional[int] = None
    away_saves_h1: Optional[int] = None
    home_saves_h2: Optional[int] = None
    away_saves_h2: Optional[int] = None
    home_saves_et: Optional[int] = None
    away_saves_et: Optional[int] = None

    # xG on target faced (GK)
    home_xgot_faced: Optional[float] = None
    away_xgot_faced: Optional[float] = None
    home_xgot_faced_h1: Optional[float] = None
    away_xgot_faced_h1: Optional[float] = None
    home_xgot_faced_h2: Optional[float] = None
    away_xgot_faced_h2: Optional[float] = None
    home_xgot_faced_et: Optional[float] = None
    away_xgot_faced_et: Optional[float] = None

    # Goals prevented (GK)
    home_goals_prevented: Optional[float] = None
    away_goals_prevented: Optional[float] = None
    home_goals_prevented_h1: Optional[float] = None
    away_goals_prevented_h1: Optional[float] = None
    home_goals_prevented_h2: Optional[float] = None
    away_goals_prevented_h2: Optional[float] = None
    home_goals_prevented_et: Optional[float] = None
    away_goals_prevented_et: Optional[float] = None
    is_live: Optional[bool] = None
    match_minute: Optional[str] = Field(default=None, max_length=50)
    home_logo_id:   Optional[int] = Field(default=None, foreign_key="team_logos.id", index=True)
    away_logo_id:   Optional[int] = Field(default=None, foreign_key="team_logos.id", index=True)
    home_jersey: Optional[str] = None
    away_jersey: Optional[str] = None

    
    


# discarded matches
class DiscardedMatch(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}
    __tablename__ = "discarded_matches"

    id: Optional[int] = Field(default=None, primary_key=True)
    home_id: Optional[int] = Field(default=None, foreign_key="teams.team_id", index=True)
    away_id: Optional[int] = Field(default=None, foreign_key="teams.team_id", index=True)
    comp_id: Optional[int] = Field(default=None, foreign_key="competitions.competition_id", index=True)
    match_date: Optional[date] = Field(default=None, index=True)
    match_time_utc: Optional[datetime] = None
    round: Optional[str] = Field(default=None, max_length=50)
    gameweek_number: Optional[int] = None
    result_string: Optional[str] = Field(default=None, max_length=20)
    tfm_url: Optional[str] = None
    livescore_url: Optional[str] = None
    flashcore_url: Optional[str] = None
    espn_url: Optional[str] = None
    playerstats_url: Optional[str] = None
    conflict_match_id: Optional[int] = Field(default=None, foreign_key="matches.match_id", index=True)
    created_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(TIMESTAMP(timezone=True), server_default=func.now()),
    )

# matches snapshots
class MatchSnapshot(SQLModel, table=True):
    __tablename__ = "match_snapshots"
    __table_args__ = (
        UniqueConstraint("match_id", "minute", "add_minute", name="uq_match_snapshots"),
    )

    id:             Optional[int] = Field(default=None, primary_key=True)
    match_id:       int           = Field(foreign_key="matches.match_id", nullable=False, index=True)
    match_minute:   Optional[str]   = Field(default=None, max_length=50)
    minute:         Optional[int]   = Field(default=None, index=True)
    add_minute:     int             = Field(default=0, nullable=False)
    snapshot_time:  Optional[datetime] = Field(default_factory=lambda: datetime.now(timezone.utc))

    home_goals:     Optional[int]   = None
    away_goals:     Optional[int]   = None

    home_poss:      Optional[float] = None
    away_poss:      Optional[float] = None

    home_shots:         Optional[int] = None
    away_shots:         Optional[int] = None
    home_shots_ontg:    Optional[int] = None
    away_shots_ontg:    Optional[int] = None
    home_shots_oftg:    Optional[int] = None
    away_shots_oftg:    Optional[int] = None
    home_shots_blocked: Optional[int] = None
    away_shots_blocked: Optional[int] = None
    home_shots_in_box:  Optional[int] = None
    away_shots_in_box:  Optional[int] = None
    home_shots_out_box: Optional[int] = None
    away_shots_out_box: Optional[int] = None

    home_xg:        Optional[float] = None
    away_xg:        Optional[float] = None
    home_xg_ontg:   Optional[float] = None
    away_xg_ontg:   Optional[float] = None
    home_xa:        Optional[float] = None
    away_xa:        Optional[float] = None

    home_corners:   Optional[int] = None
    away_corners:   Optional[int] = None

    home_yellow_cards:  Optional[int] = None
    away_yellow_cards:  Optional[int] = None
    home_red_cards:     Optional[int] = None
    away_red_cards:     Optional[int] = None

    home_fouls:     Optional[int] = None
    away_fouls:     Optional[int] = None

    home_pass_att:  Optional[int]   = None
    away_pass_att:  Optional[int]   = None
    home_pass_succ: Optional[int]   = None
    away_pass_succ: Optional[int]   = None
    home_pass_acc:  Optional[float] = None
    away_pass_acc:  Optional[float] = None

    home_tackles:   Optional[int] = None
    away_tackles:   Optional[int] = None

    home_interceptions: Optional[int] = None
    away_interceptions: Optional[int] = None

    home_clearances:    Optional[int] = None
    away_clearances:    Optional[int] = None

    home_duels_won: Optional[int] = None
    away_duels_won: Optional[int] = None

    home_saves:     Optional[int] = None
    away_saves:     Optional[int] = None

    home_big_chances:   Optional[int] = None
    away_big_chances:   Optional[int] = None

    home_touches:           Optional[int] = None
    away_touches:           Optional[int] = None
    home_touches_opp_box:   Optional[int] = None
    away_touches_opp_box:   Optional[int] = None

    home_errors_to_shot:    Optional[int] = None
    away_errors_to_shot:    Optional[int] = None
    home_errors_to_goal:    Optional[int] = None
    away_errors_to_goal:    Optional[int] = None

    home_xgot_faced:        Optional[float] = None
    away_xgot_faced:        Optional[float] = None
    home_goals_prevented:   Optional[float] = None
    away_goals_prevented:   Optional[float] = None
    home_throw_ins: Optional[int] = None
    away_throw_ins: Optional[int] = None
    home_crosses: Optional[int] = None
    away_crosses: Optional[int] = None
    home_goal_kicks: Optional[int] = None
    away_goal_kicks: Optional[int] = None
    last_updated_by: Optional[str] = None

    created_at: Optional[datetime] = Field(default_factory=lambda: datetime.now(timezone.utc))

# player match stats
class PlayerMatchStat(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}
    __tablename__ = "player_match_stats"
    
    # Identity
    id: int = Field(primary_key=True)
    player_id: int = Field(foreign_key="players.player_id", index=True)
    match_id: int = Field(foreign_key="matches.match_id", index=True)
    team_id: Optional[int] = Field(default=None, foreign_key="teams.team_id")
    
    # Basic match participation
    in_squad: Optional[bool] = Field(default=None)
    started: Optional[bool] = Field(default=None)
    subbed_on: Optional[bool] = Field(default=None)
    subbed_off: Optional[bool] = Field(default=None)
    minutes: Optional[int] = Field(default=None, index=True)
    
    # Player info at time of match
    position: Optional[str] = Field(default=None)
    number: Optional[int] = Field(default=None)
    age: Optional[int] = Field(default=None)
    value: Optional[Decimal] = Field(default=None)
    
    # Goals and assists
    goals: Optional[int] = Field(default=0, index=True)
    assists: Optional[int] = Field(default=0, index=True)
    goals_assists: Optional[int] = Field(default=0)
    
    # Expected stats
    xg: Optional[Decimal] = Field(default=None, max_digits=5, decimal_places=3)
    xg_assist: Optional[Decimal] = Field(default=None, max_digits=5, decimal_places=3)
    xga: Optional[Decimal] = Field(default=None, max_digits=5, decimal_places=3)
    npxg: Optional[Decimal] = Field(default=None, max_digits=5, decimal_places=3)
    xg_ontg: Optional[Decimal] = Field(default=None, max_digits=5, decimal_places=3)
    
    # Penalties
    pens_made: Optional[int] = Field(default=0)
    pens_att: Optional[int] = Field(default=0)
    
    # Shooting
    shots: Optional[int] = Field(default=0)
    shots_on_target: Optional[int] = Field(default=0)
    shots_off_target: Optional[int] = Field(default=None)
    shots_blocked: Optional[int] = Field(default=None)
    shots_in_box: Optional[int] = Field(default=None)
    shots_out_box: Optional[int] = Field(default=None)
    headed_shots: Optional[int] = Field(default=None)
    big_chances: Optional[int] = Field(default=None)
    big_chances_missed: Optional[int] = Field(default=None)
    
    # Touches
    touches: Optional[int] = Field(default=None)
    touches_def_pen_area: Optional[int] = Field(default=None)
    touches_def_3rd: Optional[int] = Field(default=None)
    touches_mid_3rd: Optional[int] = Field(default=None)
    touches_att_3rd: Optional[int] = Field(default=None)
    touches_att_pen_area: Optional[int] = Field(default=None)
    
    # Tackles
    tackles: Optional[int] = Field(default=0)
    tackles_won: Optional[int] = Field(default=0)
    tackles_def_3rd: Optional[int] = Field(default=0)
    tackles_mid_3rd: Optional[int] = Field(default=0)
    tackles_att_3rd: Optional[int] = Field(default=0)
    
    # Challenges
    challenges: Optional[int] = Field(default=0)
    challenges_lost: Optional[int] = Field(default=0)
    
    # Blocks
    blocks: Optional[int] = Field(default=0)
    blocked_shots: Optional[int] = Field(default=0)
    blocked_passes: Optional[int] = Field(default=0)
    
    # Defensive actions
    interceptions: Optional[int] = Field(default=0)
    clearances: Optional[int] = Field(default=0)
    errors_to_shot: Optional[int] = Field(default=0)
    errors_to_goal: Optional[int] = Field(default=0)
    
    # Shot/Goal creating actions
    sca: Optional[int] = Field(default=0)
    gca: Optional[int] = Field(default=0)
    
    # Passing
    passes_completed: Optional[int] = Field(default=None)
    passes: Optional[int] = Field(default=None)
    passes_pct: Optional[Decimal] = Field(default=None, max_digits=5, decimal_places=2)
    progressive_passes: Optional[int] = Field(default=None)
    
    # Carries
    carries: Optional[int] = Field(default=None)
    progressive_carries: Optional[int] = Field(default=None)
    carries_distance: Optional[int] = Field(default=None)
    carries_progressive_distance: Optional[int] = Field(default=None)
    carries_into_final_third: Optional[int] = Field(default=None)
    carries_into_penalty_area: Optional[int] = Field(default=None)
    
    # Ball control
    miscontrols: Optional[int] = Field(default=0)
    dispossessed: Optional[int] = Field(default=0)
    passes_received: Optional[int] = Field(default=None)
    progressive_passes_received: Optional[int] = Field(default=None)
    
    # Take-ons / dribbles
    take_ons: Optional[int] = Field(default=0)
    take_ons_won: Optional[int] = Field(default=0)
    succ_dribbles: Optional[int] = Field(default=None)
    take_ons_won_pct: Optional[Decimal] = Field(default=None, max_digits=5, decimal_places=2)
    take_ons_tackled: Optional[int] = Field(default=0)
    take_ons_tackled_pct: Optional[Decimal] = Field(default=None, max_digits=5, decimal_places=2)
    
    # Pass distances
    passes_total_distance: Optional[int] = Field(default=None)
    passes_progressive_distance: Optional[int] = Field(default=None)
    
    # Pass types (long/medium/short)
    passes_long: Optional[int] = Field(default=None)
    passes_completed_long: Optional[int] = Field(default=None)
    passes_medium: Optional[int] = Field(default=None)
    passes_completed_medium: Optional[int] = Field(default=None)
    passes_short: Optional[int] = Field(default=None)
    passes_completed_short: Optional[int] = Field(default=None)
    
    # Pass targets
    assisted_shots: Optional[int] = Field(default=None)
    passes_into_final_third: Optional[int] = Field(default=None)
    passes_into_penalty_area: Optional[int] = Field(default=None)
    crosses_into_penalty_area: Optional[int] = Field(default=None)
    
    # Pass categories
    passes_live: Optional[int] = Field(default=None)
    passes_dead: Optional[int] = Field(default=None)
    through_balls: Optional[int] = Field(default=None)
    passes_switches: Optional[int] = Field(default=None)
    passes_offsides: Optional[int] = Field(default=None)
    passes_blocked: Optional[int] = Field(default=None)
    crosses: Optional[int] = Field(default=None)
    throw_ins: Optional[int] = Field(default=None)
    corner_kicks: Optional[int] = Field(default=None)
    
    # Discipline
    cards_yellow: Optional[int] = Field(default=0)
    cards_red: Optional[int] = Field(default=0)
    cards_yellow_red: Optional[int] = Field(default=0)
    fouls: Optional[int] = Field(default=0)
    fouled: Optional[int] = Field(default=0)
    
    # Other
    offsides: Optional[int] = Field(default=0)
    pens_won: Optional[int] = Field(default=0)
    pens_conceded: Optional[int] = Field(default=0)
    own_goals: Optional[int] = Field(default=0)
    ball_recoveries: Optional[int] = Field(default=None)
    
    # Aerials / duels
    aerials_duels_won: Optional[int] = Field(default=0)
    aerials_duels_lost: Optional[int] = Field(default=0)
    aerials_won_pct: Optional[Decimal] = Field(default=None, max_digits=5, decimal_places=2)
    ground_duels_won: Optional[int] = Field(default=None)
    
    # Goalkeeper stats
    gk_shots_on_target_against: Optional[int] = Field(default=None)
    gk_goals_against: Optional[int] = Field(default=None)
    gk_saves: Optional[int] = Field(default=None)
    gk_save_pct: Optional[Decimal] = Field(default=None, max_digits=5, decimal_places=2)
    gk_psxg: Optional[Decimal] = Field(default=None, max_digits=6, decimal_places=2)
    gk_passes_completed_launched: Optional[int] = Field(default=None)
    gk_passes_launched: Optional[int] = Field(default=None)
    gk_passes_pct_launched: Optional[Decimal] = Field(default=None, max_digits=5, decimal_places=2)
    gk_passes: Optional[int] = Field(default=None)
    gk_passes_throws: Optional[int] = Field(default=None)
    gk_pct_passes_launched: Optional[Decimal] = Field(default=None, max_digits=5, decimal_places=2)
    gk_passes_length_avg: Optional[Decimal] = Field(default=None, max_digits=5, decimal_places=2)
    gk_goal_kicks: Optional[int] = Field(default=None)
    gk_pct_goal_kicks_launched: Optional[Decimal] = Field(default=None, max_digits=5, decimal_places=2)
    gk_goal_kick_length_avg: Optional[Decimal] = Field(default=None, max_digits=5, decimal_places=2)
    gk_crosses: Optional[int] = Field(default=None)
    gk_crosses_stopped: Optional[int] = Field(default=None)
    gk_crosses_stopped_pct: Optional[Decimal] = Field(default=None, max_digits=5, decimal_places=2)
    gk_def_actions_outside_pen_area: Optional[int] = Field(default=None)
    order: Optional[int] = Field(default=None)
    created_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(TIMESTAMP(timezone=True), server_default=func.now()),
    )
    updated_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(TIMESTAMP(timezone=True), onupdate=func.now(), server_default=func.now()),
    )
    inserted_by: Optional[str] = Field(default=None)
    last_updated_by: Optional[str] = Field(default=None)
    gk_sweeper: Optional[int] = Field(default=None)
    gk_punches: Optional[int] = Field(default=None)
    gk_xgot_faced: Optional[Decimal] = Field(default=None, max_digits=5, decimal_places=3)
    gk_goals_prevented: Optional[Decimal] = Field(default=None, max_digits=5, decimal_places=3)
    ga_rating: Optional[float] = None

# dsicard player match stats
class DiscardedPlayerMatchStat(SQLModel, table=True):
    __tablename__ = "discarded_player_match_stats"
    __table_args__ = (
        UniqueConstraint("discarded_match_id", "player_id", name="uq_discarded_pms"),
    )

    id:                              Optional[int]   = Field(default=None, primary_key=True)
    discarded_match_id:              Optional[int]   = Field(default=None, foreign_key="discarded_matches.id", index=True)
    player_id:                       Optional[int]   = Field(default=None, foreign_key="players.player_id", index=True)
    team_id:                         Optional[int]   = Field(default=None, foreign_key="teams.team_id", index=True)
    discard_reason:                  Optional[str]   = None

    in_squad:                        Optional[bool]  = None
    started:                         Optional[bool]  = None
    subbed_on:                       Optional[bool]  = None
    subbed_off:                      Optional[bool]  = None
    minutes:                         Optional[int]   = None
    goals:                           Optional[int]   = Field(default=0)
    assists:                         Optional[int]   = Field(default=0)
    goals_assists:                   Optional[int]   = Field(default=0)
    xg:                              Optional[float] = None
    xg_assist:                       Optional[float] = None
    xga:                             Optional[float] = None
    npxg:                            Optional[float] = None
    pens_made:                       Optional[int]   = Field(default=0)
    pens_att:                        Optional[int]   = Field(default=0)
    shots:                           Optional[int]   = Field(default=0)
    shots_on_target:                 Optional[int]   = Field(default=0)
    shots_off_target:                Optional[int]   = None
    shots_blocked:                   Optional[int]   = None
    shots_in_box:                    Optional[int]   = None
    shots_out_box:                   Optional[int]   = None
    headed_shots:                    Optional[int]   = None
    big_chances:                     Optional[int]   = None
    big_chances_missed:              Optional[int]   = None
    touches:                         Optional[int]   = None
    touches_def_pen_area:            Optional[int]   = None
    touches_def_3rd:                 Optional[int]   = None
    touches_mid_3rd:                 Optional[int]   = None
    touches_att_3rd:                 Optional[int]   = None
    touches_att_pen_area:            Optional[int]   = None
    tackles:                         Optional[int]   = Field(default=0)
    tackles_won:                     Optional[int]   = Field(default=0)
    tackles_def_3rd:                 Optional[int]   = Field(default=0)
    tackles_mid_3rd:                 Optional[int]   = Field(default=0)
    tackles_att_3rd:                 Optional[int]   = Field(default=0)
    challenges:                      Optional[int]   = Field(default=0)
    challenges_lost:                 Optional[int]   = Field(default=0)
    blocks:                          Optional[int]   = Field(default=0)
    blocked_shots:                   Optional[int]   = Field(default=0)
    blocked_passes:                  Optional[int]   = Field(default=0)
    interceptions:                   Optional[int]   = Field(default=0)
    clearances:                      Optional[int]   = Field(default=0)
    errors_to_shot:                  Optional[int]   = Field(default=0)
    errors_to_goal:                  Optional[int]   = None
    sca:                             Optional[int]   = Field(default=0)
    gca:                             Optional[int]   = Field(default=0)
    passes_completed:                Optional[int]   = None
    passes:                          Optional[int]   = None
    passes_pct:                      Optional[float] = None
    progressive_passes:              Optional[int]   = None
    carries:                         Optional[int]   = None
    progressive_carries:             Optional[int]   = None
    carries_distance:                Optional[int]   = None
    carries_progressive_distance:    Optional[int]   = None
    carries_into_final_third:        Optional[int]   = None
    carries_into_penalty_area:       Optional[int]   = None
    miscontrols:                     Optional[int]   = Field(default=0)
    dispossessed:                    Optional[int]   = Field(default=0)
    passes_received:                 Optional[int]   = None
    progressive_passes_received:     Optional[int]   = None
    take_ons:                        Optional[int]   = Field(default=0)
    take_ons_won:                    Optional[int]   = Field(default=0)
    take_ons_won_pct:                Optional[float] = None
    take_ons_tackled:                Optional[int]   = Field(default=0)
    take_ons_tackled_pct:            Optional[float] = None
    passes_total_distance:           Optional[int]   = None
    passes_progressive_distance:     Optional[int]   = None
    passes_long:                     Optional[int]   = None
    passes_completed_long:           Optional[int]   = None
    passes_medium:                   Optional[int]   = None
    passes_completed_medium:         Optional[int]   = None
    passes_short:                    Optional[int]   = None
    passes_completed_short:          Optional[int]   = None
    assisted_shots:                  Optional[int]   = None
    passes_into_final_third:         Optional[int]   = None
    passes_into_penalty_area:        Optional[int]   = None
    crosses_into_penalty_area:       Optional[int]   = None
    passes_live:                     Optional[int]   = None
    passes_dead:                     Optional[int]   = None
    through_balls:                   Optional[int]   = None
    passes_switches:                 Optional[int]   = None
    passes_offsides:                 Optional[int]   = None
    passes_blocked:                  Optional[int]   = None
    crosses:                         Optional[int]   = None
    throw_ins:                       Optional[int]   = None
    corner_kicks:                    Optional[int]   = None
    cards_yellow:                    Optional[int]   = Field(default=0)
    cards_red:                       Optional[int]   = Field(default=0)
    cards_yellow_red:                Optional[int]   = Field(default=0)
    fouls:                           Optional[int]   = Field(default=0)
    fouled:                          Optional[int]   = Field(default=0)
    offsides:                        Optional[int]   = Field(default=0)
    pens_won:                        Optional[int]   = Field(default=0)
    pens_conceded:                   Optional[int]   = Field(default=0)
    own_goals:                       Optional[int]   = Field(default=0)
    ball_recoveries:                 Optional[int]   = None
    aerials_duels_won:               Optional[int]   = Field(default=0)
    aerials_duels_lost:              Optional[int]   = Field(default=0)
    aerials_won_pct:                 Optional[float] = None
    succ_dribbles:                   Optional[int]   = None
    ground_duels_won:                Optional[int]   = None
    position:                        Optional[str]   = None
    number:                          Optional[int]   = None
    age:                             Optional[int]   = None
    value:                           Optional[float] = None
    ga_rating:                       Optional[float] = None
    order:                           Optional[int]   = None
    gk_shots_on_target_against:      Optional[int]   = None
    gk_goals_against:                Optional[int]   = None
    gk_saves:                        Optional[int]   = None
    gk_save_pct:                     Optional[float] = None
    gk_psxg:                         Optional[float] = None
    gk_passes_completed_launched:    Optional[int]   = None
    gk_passes_launched:              Optional[int]   = None
    gk_passes_pct_launched:          Optional[float] = None
    gk_passes:                       Optional[int]   = None
    gk_passes_throws:                Optional[int]   = None
    gk_pct_passes_launched:          Optional[float] = None
    gk_passes_length_avg:            Optional[float] = None
    gk_goal_kicks:                   Optional[int]   = None
    gk_pct_goal_kicks_launched:      Optional[float] = None
    gk_goal_kick_length_avg:         Optional[float] = None
    gk_crosses:                      Optional[int]   = None
    gk_crosses_stopped:              Optional[int]   = None
    gk_crosses_stopped_pct:          Optional[float] = None
    gk_def_actions_outside_pen_area: Optional[int]   = None
    gk_sweeper:                      Optional[int]   = None
    gk_punches:                      Optional[int]   = None
    gk_xgot_faced:                   Optional[float] = None
    gk_goals_prevented:              Optional[float] = None
    xg_ontg:                         Optional[float] = None

    inserted_by:                     Optional[str]      = None
    last_updated_by:                 Optional[str]      = None
    created_at:                      Optional[datetime] = Field(default_factory=lambda: datetime.now(timezone.utc))

# player match stats snapshots
class PlayerMatchStatSnapshot(SQLModel, table=True):
    __tablename__ = "player_match_stats_snapshots"
    __table_args__ = (
        UniqueConstraint("player_id", "match_id", "minute", "add_minute", name="uq_player_match_stats_snapshots"),
    )

    id:                              Optional[int]   = Field(default=None, primary_key=True)
    player_id:                       int             = Field(foreign_key="players.player_id", nullable=False, index=True)
    match_id:                        int             = Field(foreign_key="matches.match_id", nullable=False, index=True)
    team_id:                         Optional[int]   = Field(default=None, foreign_key="teams.team_id", index=True)
    minute:                          int             = Field(nullable=False, index=True)
    add_minute:                      int             = Field(default=0, nullable=False)
    last_updated_by: Optional[str] = None

    goals:                           Optional[int]   = Field(default=0)
    assists:                         Optional[int]   = Field(default=0)
    goals_assists:                   Optional[int]   = Field(default=0)
    xg:                              Optional[float] = None
    xg_assist:                       Optional[float] = None
    xga:                             Optional[float] = None
    npxg:                            Optional[float] = None
    xg_ontg:                         Optional[float] = None
    pens_made:                       Optional[int]   = Field(default=0)
    pens_att:                        Optional[int]   = Field(default=0)

    shots:                           Optional[int]   = Field(default=0)
    shots_on_target:                 Optional[int]   = Field(default=0)
    shots_off_target:                Optional[int]   = None
    shots_blocked:                   Optional[int]   = None
    shots_in_box:                    Optional[int]   = None
    shots_out_box:                   Optional[int]   = None
    headed_shots:                    Optional[int]   = None
    big_chances:                     Optional[int]   = None
    big_chances_missed:              Optional[int]   = None

    touches:                         Optional[int]   = None
    touches_def_pen_area:            Optional[int]   = None
    touches_def_3rd:                 Optional[int]   = None
    touches_mid_3rd:                 Optional[int]   = None
    touches_att_3rd:                 Optional[int]   = None
    touches_att_pen_area:            Optional[int]   = None

    tackles:                         Optional[int]   = Field(default=0)
    tackles_won:                     Optional[int]   = Field(default=0)
    tackles_def_3rd:                 Optional[int]   = Field(default=0)
    tackles_mid_3rd:                 Optional[int]   = Field(default=0)
    tackles_att_3rd:                 Optional[int]   = Field(default=0)
    challenges:                      Optional[int]   = Field(default=0)
    challenges_lost:                 Optional[int]   = Field(default=0)
    blocks:                          Optional[int]   = Field(default=0)
    blocked_shots:                   Optional[int]   = Field(default=0)
    blocked_passes:                  Optional[int]   = Field(default=0)
    interceptions:                   Optional[int]   = Field(default=0)
    clearances:                      Optional[int]   = Field(default=0)
    errors_to_shot:                  Optional[int]   = Field(default=0)
    errors_to_goal:                  Optional[int]   = None
    ball_recoveries:                 Optional[int]   = None

    sca:                             Optional[int]   = Field(default=0)
    gca:                             Optional[int]   = Field(default=0)

    passes_completed:                Optional[int]   = None
    passes:                          Optional[int]   = None
    passes_pct:                      Optional[float] = None
    progressive_passes:              Optional[int]   = None
    passes_total_distance:           Optional[int]   = None
    passes_progressive_distance:     Optional[int]   = None
    passes_long:                     Optional[int]   = None
    passes_completed_long:           Optional[int]   = None
    passes_medium:                   Optional[int]   = None
    passes_completed_medium:         Optional[int]   = None
    passes_short:                    Optional[int]   = None
    passes_completed_short:          Optional[int]   = None
    assisted_shots:                  Optional[int]   = None
    passes_into_final_third:         Optional[int]   = None
    passes_into_penalty_area:        Optional[int]   = None
    crosses_into_penalty_area:       Optional[int]   = None
    passes_live:                     Optional[int]   = None
    passes_dead:                     Optional[int]   = None
    through_balls:                   Optional[int]   = None
    passes_switches:                 Optional[int]   = None
    passes_offsides:                 Optional[int]   = None
    passes_blocked:                  Optional[int]   = None
    crosses:                         Optional[int]   = None
    throw_ins:                       Optional[int]   = None
    corner_kicks:                    Optional[int]   = None

    carries:                         Optional[int]   = None
    progressive_carries:             Optional[int]   = None
    carries_distance:                Optional[int]   = None
    carries_progressive_distance:    Optional[int]   = None
    carries_into_final_third:        Optional[int]   = None
    carries_into_penalty_area:       Optional[int]   = None
    miscontrols:                     Optional[int]   = Field(default=0)
    dispossessed:                    Optional[int]   = Field(default=0)

    passes_received:                 Optional[int]   = None
    progressive_passes_received:     Optional[int]   = None

    take_ons:                        Optional[int]   = Field(default=0)
    take_ons_won:                    Optional[int]   = Field(default=0)
    take_ons_won_pct:                Optional[float] = None
    take_ons_tackled:                Optional[int]   = Field(default=0)
    take_ons_tackled_pct:            Optional[float] = None

    aerials_duels_won:               Optional[int]   = Field(default=0)
    aerials_duels_lost:              Optional[int]   = Field(default=0)
    aerials_won_pct:                 Optional[float] = None
    ground_duels_won:                Optional[int]   = None
    succ_dribbles:                   Optional[int]   = None

    cards_yellow:                    Optional[int]   = Field(default=0)
    cards_red:                       Optional[int]   = Field(default=0)
    cards_yellow_red:                Optional[int]   = Field(default=0)
    fouls:                           Optional[int]   = Field(default=0)
    fouled:                          Optional[int]   = Field(default=0)
    offsides:                        Optional[int]   = Field(default=0)
    pens_won:                        Optional[int]   = Field(default=0)
    pens_conceded:                   Optional[int]   = Field(default=0)
    own_goals:                       Optional[int]   = Field(default=0)

    gk_shots_on_target_against:      Optional[int]   = None
    gk_goals_against:                Optional[int]   = None
    gk_saves:                        Optional[int]   = None
    gk_save_pct:                     Optional[float] = None
    gk_psxg:                         Optional[float] = None
    gk_passes_completed_launched:    Optional[int]   = None
    gk_passes_launched:              Optional[int]   = None
    gk_passes_pct_launched:          Optional[float] = None
    gk_passes:                       Optional[int]   = None
    gk_passes_throws:                Optional[int]   = None
    gk_pct_passes_launched:          Optional[float] = None
    gk_passes_length_avg:            Optional[float] = None
    gk_goal_kicks:                   Optional[int]   = None
    gk_pct_goal_kicks_launched:      Optional[float] = None
    gk_goal_kick_length_avg:         Optional[float] = None
    gk_crosses:                      Optional[int]   = None
    gk_crosses_stopped:              Optional[int]   = None
    gk_crosses_stopped_pct:          Optional[float] = None
    gk_def_actions_outside_pen_area: Optional[int]   = None
    gk_sweeper:                      Optional[int]   = None
    gk_punches:                      Optional[int]   = None
    gk_xgot_faced:                   Optional[float] = None
    gk_goals_prevented:              Optional[float] = None

    ga_rating:                       Optional[float] = None
    minutes:                         Optional[int]   = None
    created_at:                      Optional[datetime] = Field(default_factory=lambda: datetime.now(timezone.utc))
    match_time: Optional[str] = Field(default=None)
   
# player comp stats
class PlayerCompStats(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}
    __tablename__ = "player_comp_stats"
    
    # Primary key
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Foreign keys (unique constraint on player_id, competition_id, team_id)
    player_id: int = Field(foreign_key="players.player_id", index=True)
    competition_id: int = Field(foreign_key="competitions.competition_id", index=True)
    team_id: Optional[int] = Field(default=None, foreign_key="teams.team_id")
    
    # Squad and playing time
    in_squad: Optional[int] = None
    games_played: Optional[int] = None
    subbed_on: Optional[int] = None
    subbed_off: Optional[int] = None
    minutes: Optional[int] = None
    minutes_per_game: Optional[Decimal] = None
    age: Optional[int] = None
    
    # Goals and assists
    goals: Optional[int] = Field(default=None, index=True)
    goals_p90: Optional[Decimal] = None
    assists: Optional[int] = Field(default=None, index=True)
    assists_p90: Optional[Decimal] = None
    goals_assists: Optional[int] = None
    goals_assists_p90: Optional[Decimal] = None
    penalty_goals: Optional[int] = None
    
    # Expected stats
    xg: Optional[Decimal] = None
    xg_p90: Optional[Decimal] = None
    xg_assist: Optional[Decimal] = None
    xg_assist_p90: Optional[Decimal] = None
    xga: Optional[Decimal] = None
    xga_p90: Optional[Decimal] = None
    npxg: Optional[Decimal] = None
    npxg_p90: Optional[Decimal] = None
    
    # Penalties
    pens_made: Optional[int] = None
    pens_att: Optional[int] = None
    pens_won: Optional[int] = None
    pens_conceded: Optional[int] = None
    
    # Shooting
    shots: Optional[int] = None
    shots_p90: Optional[Decimal] = None
    shots_on_target: Optional[int] = None
    shots_on_target_p90: Optional[Decimal] = None
    
    # Touches
    touches: Optional[int] = None
    touches_p90: Optional[Decimal] = None
    touches_def_pen_area: Optional[int] = None
    touches_def_pen_area_p90: Optional[Decimal] = None
    touches_def_3rd: Optional[int] = None
    touches_def_3rd_p90: Optional[Decimal] = None
    touches_mid_3rd: Optional[int] = None
    touches_mid_3rd_p90: Optional[Decimal] = None
    touches_att_3rd: Optional[int] = None
    touches_att_3rd_p90: Optional[Decimal] = None
    touches_att_pen_area: Optional[int] = None
    touches_att_pen_area_p90: Optional[Decimal] = None
    
    # Tackles
    tackles: Optional[int] = None
    tackles_p90: Optional[Decimal] = None
    tackles_won: Optional[int] = None
    tackles_won_p90: Optional[Decimal] = None
    tackles_won_pct: Optional[Decimal] = None
    tackles_def_3rd: Optional[int] = None
    tackles_def_3rd_p90: Optional[Decimal] = None
    tackles_mid_3rd: Optional[int] = None
    tackles_mid_3rd_p90: Optional[Decimal] = None
    tackles_att_3rd: Optional[int] = None
    tackles_att_3rd_p90: Optional[Decimal] = None
    
    # Challenges
    challenges: Optional[int] = None
    challenges_p90: Optional[Decimal] = None
    challenges_lost: Optional[int] = None
    challenges_lost_p90: Optional[Decimal] = None
    
    # Blocks
    blocks: Optional[int] = None
    blocks_p90: Optional[Decimal] = None
    blocked_shots: Optional[int] = None
    blocked_shots_p90: Optional[Decimal] = None
    blocked_passes: Optional[int] = None
    blocked_passes_p90: Optional[Decimal] = None
    
    # Defensive actions
    interceptions: Optional[int] = None
    interceptions_p90: Optional[Decimal] = None
    clearances: Optional[int] = None
    clearances_p90: Optional[Decimal] = None
    errors: Optional[int] = None
    errors_p90: Optional[Decimal] = None
    
    # Shot/Goal creating actions
    sca: Optional[int] = None
    sca_p90: Optional[Decimal] = None
    gca: Optional[int] = None
    gca_p90: Optional[Decimal] = None
    
    # Passing
    passes_completed: Optional[int] = None
    passes_completed_p90: Optional[Decimal] = None
    passes: Optional[int] = None
    passes_p90: Optional[Decimal] = None
    passes_pct: Optional[Decimal] = None
    progressive_passes: Optional[int] = None
    progressive_passes_p90: Optional[Decimal] = None
    
    # Carries
    carries: Optional[int] = None
    carries_p90: Optional[Decimal] = None
    progressive_carries: Optional[int] = None
    progressive_carries_p90: Optional[Decimal] = None
    carries_distance: Optional[int] = None
    carries_distance_p90: Optional[Decimal] = None
    carries_progressive_distance: Optional[int] = None
    carries_progressive_distance_p90: Optional[Decimal] = None
    carries_into_final_third: Optional[int] = None
    carries_into_final_third_p90: Optional[Decimal] = None
    carries_into_penalty_area: Optional[int] = None
    carries_into_penalty_area_p90: Optional[Decimal] = None
    
    # Ball control
    miscontrols: Optional[int] = None
    miscontrols_p90: Optional[Decimal] = None
    dispossessed: Optional[int] = None
    dispossessed_p90: Optional[Decimal] = None
    passes_received: Optional[int] = None
    passes_received_p90: Optional[Decimal] = None
    progressive_passes_received: Optional[int] = None
    progressive_passes_received_p90: Optional[Decimal] = None
    # Take-ons
    take_ons: Optional[int] = None
    take_ons_p90: Optional[Decimal] = None
    take_ons_won: Optional[int] = None
    take_ons_won_p90: Optional[Decimal] = None
    take_ons_won_pct: Optional[Decimal] = None
    take_ons_tackled: Optional[int] = None
    take_ons_tackled_pct: Optional[Decimal] = None
    
    # Pass distances
    passes_total_distance: Optional[int] = None
    passes_total_distance_p90: Optional[Decimal] = None
    passes_progressive_distance: Optional[int] = None
    passes_progressive_distance_p90: Optional[Decimal] = None
    progressive_passes_pct: Optional[Decimal] = None
    
    # Pass types
    passes_long: Optional[int] = None
    passes_long_p90: Optional[Decimal] = None
    passes_completed_long: Optional[int] = None
    passes_completed_long_p90: Optional[Decimal] = None
    passes_medium: Optional[int] = None
    passes_medium_p90: Optional[Decimal] = None
    passes_completed_medium: Optional[int] = None
    passes_completed_medium_p90: Optional[Decimal] = None
    passes_short: Optional[int] = None
    passes_short_p90: Optional[Decimal] = None
    passes_completed_short: Optional[int] = None
    passes_completed_short_p90: Optional[Decimal] = None
    
    # Pass targets
    assisted_shots: Optional[int] = None
    assisted_shots_p90: Optional[Decimal] = None
    passes_into_final_third: Optional[int] = None
    passes_into_final_third_p90: Optional[Decimal] = None
    passes_into_penalty_area: Optional[int] = None
    passes_into_penalty_area_p90: Optional[Decimal] = None
    crosses_into_penalty_area: Optional[int] = None
    crosses_into_penalty_area_p90: Optional[Decimal] = None
    
    # Pass categories
    passes_live: Optional[int] = None
    passes_live_p90: Optional[Decimal] = None
    passes_dead: Optional[int] = None
    passes_dead_p90: Optional[Decimal] = None
    through_balls: Optional[int] = None
    through_balls_p90: Optional[Decimal] = None
    passes_switches: Optional[int] = None
    passes_switches_p90: Optional[Decimal] = None
    passes_offsides: Optional[int] = None
    passes_offsides_p90: Optional[Decimal] = None
    passes_offside_pct: Optional[Decimal] = None
    passes_blocked: Optional[int] = None
    passes_blocked_p90: Optional[Decimal] = None
    crosses: Optional[int] = None
    crosses_p90: Optional[Decimal] = None
    throw_ins: Optional[int] = None
    corner_kicks: Optional[int] = None
    
    # Discipline
    cards_yellow: Optional[int] = None
    cards_red: Optional[int] = None
    cards_yellow_red: Optional[int] = None
    fouls: Optional[int] = None
    fouls_p90: Optional[Decimal] = None
    fouled: Optional[int] = None
    fouled_p90: Optional[Decimal] = None
    
    # Other
    offsides: Optional[int] = None
    offsides_p90: Optional[Decimal] = None
    own_goals: Optional[int] = None
    ball_recoveries: Optional[int] = None
    ball_recoveries_p90: Optional[Decimal] = None
    
    # Aerials
    aerials_won: Optional[int] = None
    aerials_won_p90: Optional[Decimal] = None
    aerials_lost: Optional[int] = None
    aerials_lost_p90: Optional[Decimal] = None
    aerials_won_pct: Optional[Decimal] = None
    
    # Goalkeeper stats
    gk_shots_on_target_against: Optional[int] = None
    gk_shots_on_target_against_p90: Optional[Decimal] = None
    gk_goals_against: Optional[int] = None
    gk_goals_against_p90: Optional[Decimal] = None
    gk_saves: Optional[int] = None
    gk_saves_p90: Optional[Decimal] = None
    gk_saves_pct: Optional[Decimal] = None
    gk_psxg: Optional[Decimal] = None
    gk_passes_completed_launched: Optional[int] = None
    gk_passes_completed_launched_p90: Optional[Decimal] = None
    gk_passes_launched: Optional[int] = None
    gk_passes_launched_p90: Optional[Decimal] = None
    gk_passes_pct_launched: Optional[Decimal] = None
    gk_passes: Optional[int] = None
    gk_passes_p90: Optional[Decimal] = None
    gk_passes_throws: Optional[int] = None
    gk_passes_throws_p90: Optional[Decimal] = None
    gk_pct_passes_launched: Optional[Decimal] = None
    gk_passes_length_avg: Optional[Decimal] = None
    gk_goal_kicks: Optional[int] = None
    gk_pct_goal_kicks_launched: Optional[Decimal] = None
    gk_goal_kick_length_avg: Optional[Decimal] = None
    gk_crosses: Optional[int] = None
    gk_crosses_p90: Optional[Decimal] = None
    gk_crosses_stopped: Optional[int] = None
    gk_crosses_stopped_p90: Optional[Decimal] = None
    gk_crosses_stopped_pct: Optional[Decimal] = None
    gk_def_actions_outside_pen_area: Optional[int] = None
    gk_def_actions_outside_pen_area_p90: Optional[Decimal] = None
    gk_psxg_p90: Optional[Decimal] = None
    
    # Defensive stats
    goals_conceded: Optional[int] = None
    goals_conceded_p90: Optional[Decimal] = None
    clean_sheets: Optional[int] = None
    
    # Calculated metrics
    touches_per_goal: Optional[Decimal] = None
    minutes_per_goal: Optional[Decimal] = None
    touches_per_ga: Optional[Decimal] = None
    touches_per_assists: Optional[Decimal] = None
    minutes_per_assists: Optional[Decimal] = None
    minutes_per_ga: Optional[Decimal] = None
    touches_per_shot: Optional[Decimal] = None
    shots_per_goal: Optional[Decimal] = None
    xg_per_shot: Optional[Decimal] = None

    touches_per_pass: Optional[Decimal] = None
    prog_carries_distance_per_touch: Optional[Decimal] = None
    carries_distance_per_touch: Optional[Decimal] = None
    prog_carries_pct: Optional[Decimal] = None
    prog_passes_pct: Optional[Decimal] = None
    pass_accuracy: Optional[Decimal] = None
    shot_accuracy: Optional[Decimal] = None
    shot_conversion_rate: Optional[Decimal] = None
    challenges_won: Optional[Decimal] = None
    challenges_won_p90: Optional[Decimal] = None
    challenges_won_pct: Optional[Decimal] = None
    distance_per_carry: Optional[Decimal] = None
    prog_distance_per_carry: Optional[Decimal] = None
    prog_distance_per_prog_carry: Optional[Decimal] = None
    progressive_carries_pct: Optional[Decimal] = None
    passes_short_accuracy: Optional[Decimal] = None
    passes_medium_accuracy: Optional[Decimal] = None
    passes_long_accuracy: Optional[Decimal] = None

    pens_won_p90: Optional[Decimal] = None
    pens_conceded_p90: Optional[Decimal] = None

# referees
class Referee(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}
    __tablename__ = "referees"
    
    id: int = Field(primary_key=True)
    name: Optional[str] = Field(default=None)
    tfm_id: int = Field(unique=True)
    tfm_url: Optional[str] = Field(default=None)
    country_id: Optional[int] = Field(default=None, foreign_key="countries.country_id")
    age: Optional[int] = Field(default=None)
    

# team_cups
class TeamCup(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}
    __tablename__ = "team_cups"
    # Primary key
    id: Optional[int] = Field(default=None, primary_key=True)
    # Foreign keys
    competition_id: int = Field(foreign_key="competitions.competition_id", index=True)
    team_id: int = Field(foreign_key="teams.team_id", index=True)
    # Other fields
    round: Optional[str] = None
    rank: Optional[int] = None
    
    
# wikipedia
class WikipediaData(SQLModel, table=True):
    __tablename__ = "wikipedia_data"
    __table_args__ = (
        UniqueConstraint('raw_data', 'wikipedia_url', name='uq_wikipedia_data_raw_data_url'),
        {"extend_existing": True},
    )

    # Primary key
    id: Optional[int] = Field(default=None, primary_key=True)

    # Plain text paragraph (readable, searchable via raw_data_tsv)
    raw_data: Optional[str] = Field(
        default=None,
        sa_column=Column(Text)
    )
    # Generated tsvector column (GENERATED ALWAYS AS ... STORED in DB — read-only here)
    raw_data_tsv: Optional[str] = Field(
        default=None,
        sa_column=Column(TSVECTOR)
    )

    # Foreign keys
    player_id: Optional[int] = Field(
        default=None,
        foreign_key="players.player_id",
        index=True
    )
    team_id: Optional[int] = Field(
        default=None,
        foreign_key="teams.team_id"
    )
    wikipedia_url: Optional[str] = None

    # JSONB sources referenced in the paragraph
    sources: Optional[dict] = Field(
        default=None,
        sa_column=Column(JSONB)
    )
    parsed_data: Optional[str] = None

    updated_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(
            TIMESTAMP(timezone=True),
            server_default=func.current_timestamp()
        )
    )

# twitter
class TwitterUser(SQLModel, table=True):
    __tablename__ = "twitter_users"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    rest_id: str = Field(unique=True, index=True, max_length=255)
    handle: str = Field(index=True, max_length=255)
    name: str = Field(max_length=255)
    description: Optional[str] = Field(default=None, sa_column=Column(Text))
    location: Optional[str] = Field(default=None, max_length=255)
    urls: Optional[dict] = Field(default=None, sa_column=Column(JSONB))
    verified: bool = Field(default=False)
    is_blue_verified: bool = Field(default=False)
    verified_type: Optional[str] = Field(default=None, max_length=50)
    followers_count: int = Field(default=0)
    friends_count: int = Field(default=0)
    statuses_count: int = Field(default=0)
    favourites_count: int = Field(default=0)
    media_count: int = Field(default=0)
    profile_image_url: Optional[str] = Field(default=None, sa_column=Column(Text))
    profile_banner_url: Optional[str] = Field(default=None, sa_column=Column(Text))
    created_at: Optional[str] = Field(default=None, max_length=255)
    first_collected_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(TIMESTAMP(timezone=True))
    )
    last_updated_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(
            TIMESTAMP(timezone=True),
            server_default=func.current_timestamp()
        )
    )


class TwitterUserSnapshot(SQLModel, table=True):
    __tablename__ = "twitter_user_snapshots"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    user_rest_id: str = Field(
        foreign_key="twitter_users.rest_id",
        index=True,
        max_length=255
    )
    handle: str = Field(max_length=255)
    name: str = Field(max_length=255)
    followers_count: int = Field(default=0)
    friends_count: int = Field(default=0)
    statuses_count: int = Field(default=0)
    favourites_count: int = Field(default=0)
    media_count: int = Field(default=0)
    description: Optional[str] = Field(default=None, sa_column=Column(Text))
    location: Optional[str] = Field(default=None, max_length=255)
    urls: Optional[dict] = Field(default=None, sa_column=Column(JSONB))
    verified: bool = Field(default=False)
    is_blue_verified: bool = Field(default=False)
    profile_image_url: Optional[str] = Field(default=None, sa_column=Column(Text))
    profile_banner_url: Optional[str] = Field(default=None, sa_column=Column(Text))
    snapshot_at: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            server_default=func.current_timestamp(),
            nullable=False
        )
    )
    
    __table_args__ = (
        UniqueConstraint('user_rest_id', 'snapshot_at', name='uq_user_snapshot_time'),
        Index('idx_user_snapshot_time', 'user_rest_id', 'snapshot_at'),
    )


class Tweet(SQLModel, table=True):
    __tablename__ = "tweets"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    rest_id: str = Field(unique=True, index=True, max_length=255)
    user_rest_id: str = Field(
        foreign_key="twitter_users.rest_id",
        index=True,
        max_length=255
    )
    text: str = Field(sa_column=Column(Text))
    tweet_url: str = Field(sa_column=Column(Text))
    language: Optional[str] = Field(default=None, max_length=10)
    favorites: int = Field(default=0)
    retweets: int = Field(default=0)
    replies: int = Field(default=0)
    quotes: int = Field(default=0)
    views: Optional[int] = None
    bookmarks: int = Field(default=0)
    created_at: str = Field(max_length=255)
    conversation_id: Optional[str] = Field(default=None, index=True, max_length=255)
    in_reply_to_status_id: Optional[str] = Field(default=None, max_length=255)
    in_reply_to_user_id: Optional[str] = Field(default=None, max_length=255)
    is_reply: bool = Field(default=False)
    is_retweet: bool = Field(default=False)
    is_quote: bool = Field(default=False)
    is_quote_status: bool = Field(default=False)
    possibly_sensitive: Optional[bool] = None
    hashtags: Optional[List[str]] = Field(
        default=None,
        sa_column=Column(ARRAY(String))
    )
    mentions: Optional[List[str]] = Field(
        default=None,
        sa_column=Column(ARRAY(String))
    )
    first_collected_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(TIMESTAMP(timezone=True))
    )
    last_updated_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(
            TIMESTAMP(timezone=True),
            server_default=func.current_timestamp()
        )
    )
    search_value: Optional[str] = Field(default=None, sa_column=Column(Text))
    sentiment_value: Optional[str] = Field(default=None, max_length=255)
    sentiment_probs: Optional[Dict[str, Any]] = Field(
        default=None,
        sa_column=Column(JSONB)
    )


class TweetSnapshot(SQLModel, table=True):
    __tablename__ = "tweet_snapshots"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    tweet_rest_id: str = Field(
        foreign_key="tweets.rest_id",
        index=True,
        max_length=255
    )
    text: str = Field(sa_column=Column(Text))
    hashtags: Optional[List[str]] = Field(
        default=None,
        sa_column=Column(ARRAY(String))
    )
    mentions: Optional[List[str]] = Field(
        default=None,
        sa_column=Column(ARRAY(String))
    )
    favorites: int = Field(default=0)
    retweets: int = Field(default=0)
    replies: int = Field(default=0)
    quotes: int = Field(default=0)
    views: Optional[int] = None
    bookmarks: int = Field(default=0)
    snapshot_at: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            server_default=func.current_timestamp(),
            nullable=False
        )
    )
    
    __table_args__ = (
        UniqueConstraint('tweet_rest_id', 'snapshot_at', name='uq_tweet_snapshot_time'),
        Index('idx_tweet_snapshot_time', 'tweet_rest_id', 'snapshot_at'),
    )


class TweetMedia(SQLModel, table=True):
    __tablename__ = "tweet_media"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    tweet_rest_id: str = Field(
        foreign_key="tweets.rest_id",
        index=True,
        max_length=255
    )
    media_type: str = Field(max_length=50)
    media_url: str = Field(sa_column=Column(Text))
    
    __table_args__ = (
        UniqueConstraint('tweet_rest_id', 'media_url', name='uq_tweet_media_url'),
    )
    
    


# db types for Instagram data
class InstagramUser(SQLModel, table=True):
    __tablename__ = "instagram_users"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: str = Field(unique=True, index=True, max_length=255)
    fbid_v2: Optional[str] = Field(default=None, index=True, max_length=255)
    username: str = Field(index=True, max_length=255)
    full_name: Optional[str] = Field(default=None, max_length=255)
    biography: Optional[str] = Field(default=None, sa_column=Column(Text))
    external_url: Optional[str] = Field(default=None, sa_column=Column(Text))
    follower_count: int = Field(default=0)
    following_count: int = Field(default=0)
    posts_count: int = Field(default=0)
    is_private: bool = Field(default=False)
    is_verified: bool = Field(default=False)
    is_business_account: bool = Field(default=False)
    profile_pic_url: Optional[str] = Field(default=None, sa_column=Column(Text))
    profile_pic_url_hd: Optional[str] = Field(default=None, sa_column=Column(Text))
    category: Optional[str] = Field(default=None, max_length=255)
    first_collected_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(TIMESTAMP(timezone=True))
    )
    last_updated_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(
            TIMESTAMP(timezone=True),
            server_default=func.current_timestamp()
        )
    )


class InstagramUserSnapshot(SQLModel, table=True):
    __tablename__ = "instagram_user_snapshots"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: str = Field(
        foreign_key="instagram_users.user_id",
        index=True,
        max_length=255
    )
    username: str = Field(max_length=255)
    full_name: Optional[str] = Field(default=None, max_length=255)
    biography: Optional[str] = Field(default=None, sa_column=Column(Text))
    follower_count: int = Field(default=0)
    following_count: int = Field(default=0)
    posts_count: int = Field(default=0)
    profile_pic_url: Optional[str] = Field(default=None, sa_column=Column(Text))
    snapshot_at: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            server_default=func.current_timestamp(),
            nullable=False
        )
    )
    
    __table_args__ = (
        UniqueConstraint('user_id', 'snapshot_at'),
        Index('idx_ig_user_snap_time', 'user_id', 'snapshot_at'),
    )


class InstagramPost(SQLModel, table=True):
    __tablename__ = "instagram_posts"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    post_id: str = Field(unique=True, index=True, max_length=255)
    shortcode: str = Field(unique=True, index=True, max_length=255)
    user_id: str = Field(
        foreign_key="instagram_users.user_id",
        index=True,
        max_length=255
    )
    type: str = Field(index=True, max_length=50)
    caption: Optional[str] = Field(default=None, sa_column=Column(Text))
    hashtags: Optional[List[str]] = Field(
        default=None,
        sa_column=Column(ARRAY(String))
    )
    mentions: Optional[List[str]] = Field(
        default=None,
        sa_column=Column(ARRAY(String))
    )
    url: str = Field(sa_column=Column(Text))
    display_url: Optional[str] = Field(default=None, sa_column=Column(Text))
    media: Optional[dict] = Field(default=None, sa_column=Column(JSONB))
    tagged_users: Optional[dict] = Field(default=None, sa_column=Column(JSONB))
    likes_count: int = Field(default=0, index=True)
    comments_count: int = Field(default=0)
    video_views_count: Optional[int] = None
    video_play_count: Optional[int] = None
    is_comments_disabled: bool = Field(default=False)
    dimensions_height: Optional[int] = None
    dimensions_width: Optional[int] = None
    alt_text: Optional[str] = Field(default=None, sa_column=Column(Text))
    posted_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(TIMESTAMP(timezone=True), index=True),
    )
    is_parent_post: bool = Field(default=True)
    parent_post_id: Optional[str] = Field(
        default=None,
        foreign_key="instagram_posts.post_id",
        index=True,
        max_length=255
    )
    is_pinned: bool = Field(default=False)
    first_comment: Optional[str] = Field(default=None, sa_column=Column(Text))
    product_type: Optional[str] = Field(default=None, max_length=50)
    video_url: Optional[str] = Field(default=None, sa_column=Column(Text))
    audio_url: Optional[str] = Field(default=None, sa_column=Column(Text))
    video_duration: Optional[float] = Field(
        default=None,
        sa_column=Column(DECIMAL(10, 3))
    )
    music_info: Optional[dict] = Field(default=None, sa_column=Column(JSONB))
    coauthor_producers: Optional[dict] = Field(default=None, sa_column=Column(JSONB))
    first_collected_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(TIMESTAMP(timezone=True))
    )
    last_updated_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(
            TIMESTAMP(timezone=True),
            server_default=func.current_timestamp()
        )
    )
    after_match: bool = Field(default=False)
    
    
    __table_args__ = (
        Index('idx_instagram_posts_pinned', 'is_pinned', postgresql_where=Column('is_pinned') == True),
    )


class InstagramPostSnapshot(SQLModel, table=True):
    __tablename__ = "instagram_post_snapshots"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    post_id: str = Field(
        foreign_key="instagram_posts.post_id",
        index=True,
        max_length=255
    )
    caption: Optional[str] = Field(default=None, sa_column=Column(Text))
    hashtags: Optional[List[str]] = Field(
        default=None,
        sa_column=Column(ARRAY(String))
    )
    mentions: Optional[List[str]] = Field(
        default=None,
        sa_column=Column(ARRAY(String))
    )
    media: Optional[dict] = Field(default=None, sa_column=Column(JSONB))
    tagged_users: Optional[dict] = Field(default=None, sa_column=Column(JSONB))
    likes_count: int = Field(default=0)
    comments_count: int = Field(default=0)
    video_views_count: Optional[int] = None
    video_play_count: Optional[int] = None
    first_comment: Optional[str] = Field(default=None, sa_column=Column(Text))
    is_pinned: bool = Field(default=False)
    snapshot_at: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            server_default=func.current_timestamp(),
            nullable=False
        )
    )
    
    __table_args__ = (
        UniqueConstraint('post_id', 'snapshot_at'),
        Index('idx_post_snapshot_time', 'post_id', 'snapshot_at'),
    )


class InstagramComment(SQLModel, table=True):
    __tablename__ = "instagram_comments"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    comment_id: str = Field(unique=True, index=True, max_length=255)
    post_id: str = Field(
        foreign_key="instagram_posts.post_id",
        index=True,
        max_length=255
    )
    parent_comment_id: Optional[str] = Field(
        default=None,
        foreign_key="instagram_comments.comment_id",
        index=True,
        max_length=255
    )
    post_url: Optional[str] = Field(default=None, sa_column=Column(Text))
    comment_url: Optional[str] = Field(default=None, sa_column=Column(Text))
    text: str = Field(sa_column=Column(Text))
    likes_count: int = Field(default=0)
    replies_count: int = Field(default=0)
    commented_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(TIMESTAMP(timezone=True), index=True),
    )
    collected_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(
            TIMESTAMP(timezone=True),
            server_default=func.current_timestamp()
        )
    )
    commenter_db_id: Optional[int] = Field(
        default=None,
        foreign_key="instagram_users.id",
        index=True
    )
    owner_user_id: Optional[str] = Field(
        default=None,
        index=True,
        max_length=255
    )
    owner_username: str = Field(max_length=255)
    owner_full_name: Optional[str] = Field(default=None, max_length=255)
    owner_profile_pic_url: Optional[str] = Field(default=None, sa_column=Column(Text))
    owner_is_verified: bool = Field(default=False)
    owner_is_private: bool = Field(default=False)
    has_media: bool = Field(default=False)
    media: Optional[Dict[str, Any]] = Field(
        default=None,
        sa_column=Column(JSONB)
    )
    sentiment_value: Optional[str] = Field(default=None, max_length=255)
    sentiment_probs: Optional[Dict[str, Any]] = Field(
        default=None,
        sa_column=Column(JSONB)
    )
    


# kalshi models
class KalshiSeries(SQLModel, table=True):
    __tablename__ = "kalshi_series"
    __table_args__ = {"extend_existing": True}
 
    ticker: str = Field(primary_key=True, max_length=100)
    title: str = Field(max_length=255)
    category: Optional[str] = Field(default=None, max_length=100)
    frequency: Optional[str] = Field(default=None, max_length=50)
    fee_multiplier: Optional[float] = Field(default=None)
    fee_type: Optional[str] = Field(default=None, max_length=100)
    contract_terms_url: Optional[str] = Field(default=None, sa_column=Column(Text))
    contract_url: Optional[str] = Field(default=None, sa_column=Column(Text))
    tags: Optional[list] = Field(default=None, sa_column=Column(ARRAY(String)))
    settlement_sources: Optional[dict] = Field(default=None, sa_column=Column(JSONB))
    product_metadata: Optional[dict] = Field(default=None, sa_column=Column(JSONB))
    last_updated_ts: Optional[datetime] = Field(default=None)
    created_at: Optional[datetime] = Field(default=None)
 
 
class KalshiEvent(SQLModel, table=True):
    __tablename__ = "kalshi_events"
    __table_args__ = {"extend_existing": True}
 
    event_ticker: str = Field(primary_key=True, max_length=150)
    series_ticker: str = Field(max_length=100, foreign_key="kalshi_series.ticker", index=True)
    title: Optional[str] = Field(default=None, max_length=255)
    sub_title: Optional[str] = Field(default=None, max_length=255)
    category: Optional[str] = Field(default=None, max_length=100, index=True)
    target_datetime: Optional[datetime] = Field(default=None, index=True)
    mutually_exclusive: Optional[bool] = Field(default=True)
    settle_details: Optional[str] = Field(default=None, sa_column=Column(Text))
    description_context: Optional[str] = Field(default=None, sa_column=Column(Text))
    tags: Optional[list] = Field(default=None, sa_column=Column(ARRAY(String)))
    keywords: Optional[list] = Field(default=None, sa_column=Column(ARRAY(String)))
    min_tick_size: Optional[str] = Field(default=None, max_length=20)
    settlement_sources: Optional[dict] = Field(default=None, sa_column=Column(JSONB))
    product_metadata: Optional[dict] = Field(default=None, sa_column=Column(JSONB))
    created_at: Optional[datetime] = Field(default=None)
 
 
class KalshiMarket(SQLModel, table=True):
    __tablename__ = "kalshi_markets"
    __table_args__ = {"extend_existing": True}
 
    market_id: str = Field(primary_key=True, max_length=36)  # UUID as string
    ticker: str = Field(max_length=200, unique=True, index=True)
    event_ticker: str = Field(max_length=150, foreign_key="kalshi_events.event_ticker", index=True)
    name: Optional[str] = Field(default=None, max_length=100)
    title: Optional[str] = Field(default=None, max_length=255)
    subtitle: Optional[str] = Field(default=None, max_length=255)
    status: Optional[str] = Field(default=None, max_length=50, index=True)
    result: Optional[str] = Field(default=None, max_length=10, index=True)
    expiration_value: Optional[str] = Field(default=None, max_length=100)
 
    # Dates
    open_time: Optional[datetime] = Field(default=None, index=True)
    close_time: Optional[datetime] = Field(default=None, index=True)
    expiration_time: Optional[datetime] = Field(default=None)
    expected_expiration_time: Optional[datetime] = Field(default=None)
    created_time: Optional[datetime] = Field(default=None)
    updated_time: Optional[datetime] = Field(default=None)
 
    # Pricing
    yes_bid_dollars: Optional[float] = Field(default=None)
    yes_ask_dollars: Optional[float] = Field(default=None)
    no_bid_dollars: Optional[float] = Field(default=None)
    no_ask_dollars: Optional[float] = Field(default=None)
    last_price_dollars: Optional[float] = Field(default=None)
    prev_period_price_dollars: Optional[float] = Field(default=None)
    notional_value_dollars: Optional[float] = Field(default=None)
 
    # Volume & interest
    volume: Optional[int] = Field(default=None)
    volume_24h: Optional[int] = Field(default=None)
    recent_volume: Optional[int] = Field(default=None)
    open_interest: Optional[int] = Field(default=None)
    dollar_volume: Optional[int] = Field(default=None)
    dollar_open_interest: Optional[int] = Field(default=None)
 
    # Config
    market_type: Optional[str] = Field(default="binary", max_length=50)
    tick_size: Optional[int] = Field(default=None)
    settlement_timer_seconds: Optional[int] = Field(default=None)
    can_close_early: Optional[bool] = Field(default=None)
    fractional_trading_enabled: Optional[bool] = Field(default=None)
    close_unconfirmed: Optional[bool] = Field(default=None)
 
    # Metadata
    custom_strike: Optional[dict] = Field(default=None, sa_column=Column(JSONB))
    rulebook_variables: Optional[dict] = Field(default=None, sa_column=Column(JSONB))
    rules_primary: Optional[str] = Field(default=None, sa_column=Column(Text))
    rules_secondary: Optional[str] = Field(default=None, sa_column=Column(Text))

    created_at: Optional[datetime] = Field(default=None)
    player_id: Optional[int] = Field(
        default=None,
        foreign_key="players.player_id",
        index=True,
    )
    team_id: Optional[int] = Field(
        default=None,
        foreign_key="teams.team_id",
        index=True,
    )
    competition_id: Optional[int] = Field(
        default=None,
        foreign_key="competitions.competition_id",
        index=True,
    )


class KalshiForecastHistory(SQLModel, table=True):
    __tablename__ = "kalshi_forecast_history"
    __table_args__ = {"extend_existing": True}
 
    id: Optional[int] = Field(default=None, primary_key=True)
    market_ticker: str = Field(max_length=200, foreign_key="kalshi_markets.ticker", index=True)
    event_ticker: str = Field(max_length=150, index=True)
    end_period_ts: datetime = Field(index=True)
    period_interval_seconds: Optional[int] = Field(default=None)
    raw_numerical_forecast: Optional[float] = Field(default=None)
    numerical_forecast: Optional[float] = Field(default=None)
    formatted_forecast: Optional[str] = Field(default=None, max_length=20)
    created_at: Optional[datetime] = Field(default=None)


class KalshiMarketSnapshot(SQLModel, table=True):
    __tablename__ = "kalshi_market_snapshots"
    __table_args__ = (
        UniqueConstraint("market_ticker", "snapshotted_at", name="uq_kalshi_market_snapshot_ticker_ts"),
        {"extend_existing": True},
    )

    id: Optional[int] = Field(default=None, sa_column=Column(BigInteger, primary_key=True, autoincrement=True))
    market_ticker: str = Field(max_length=200, foreign_key="kalshi_markets.ticker", index=True)
    snapshotted_at: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            server_default=func.current_timestamp(),
            nullable=False,
            index=True,
        )
    )
    status: Optional[str] = Field(default=None, max_length=50)
    result: Optional[str] = Field(default=None, max_length=10)
    yes_bid_dollars: Optional[float] = Field(default=None)
    yes_ask_dollars: Optional[float] = Field(default=None)
    no_bid_dollars: Optional[float] = Field(default=None)
    no_ask_dollars: Optional[float] = Field(default=None)
    last_price_dollars: Optional[float] = Field(default=None)
    prev_period_price_dollars: Optional[float] = Field(default=None)
    notional_value_dollars: Optional[float] = Field(default=None)
    volume: Optional[int] = Field(default=None)
    volume_24h: Optional[int] = Field(default=None)
    recent_volume: Optional[int] = Field(default=None)
    open_interest: Optional[int] = Field(default=None)
    dollar_volume: Optional[int] = Field(default=None)
    dollar_open_interest: Optional[int] = Field(default=None)


class KalshiTrade(SQLModel, table=True):
    __tablename__ = "kalshi_trades"
    __table_args__ = (
        UniqueConstraint("trade_id", name="uq_kalshi_trade_id"),
        {"extend_existing": True},
    )

    id: Optional[int] = Field(default=None, sa_column=Column(BigInteger, primary_key=True, autoincrement=True))
    trade_id: str = Field(max_length=36, index=True)
    ticker: str = Field(max_length=200, foreign_key="kalshi_markets.ticker", index=True)
    created_time: Optional[datetime] = Field(
        default=None,
        sa_column=Column(TIMESTAMP(timezone=True), nullable=True, index=True),
    )
    taker_side: Optional[str] = Field(default=None, max_length=10)
    count: Optional[float] = Field(default=None)
    yes_price_dollars: Optional[float] = Field(default=None)
    no_price_dollars: Optional[float] = Field(default=None)
    inserted_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(TIMESTAMP(timezone=True), server_default=func.current_timestamp()),
    )


#Polymarket types
class PolymarketSeries(SQLModel, table=True):
    __tablename__ = "polymarket_series"
 
    id: str = Field(primary_key=True, sa_type=String(50))
    ticker: str = Field(sa_type=String(200), unique=True, index=True)
    slug: str = Field(sa_type=String(200), index=True)
    title: str = Field(sa_type=String(500))
    series_type: Optional[str] = Field(default=None, sa_type=String(50))
    recurrence: Optional[str] = Field(default=None, sa_type=String(50))
    active: bool = Field(default=True, index=True)
    closed: bool = Field(default=False)
    archived: bool = Field(default=False)
    new: bool = Field(default=False)
    featured: bool = Field(default=False)
    restricted: bool = Field(default=False)
    liquidity: Optional[Decimal] = Field(default=None, sa_type=Numeric(18, 4))
    volume: Optional[Decimal] = Field(default=None, sa_type=Numeric(18, 4))
    volume_24hr: Optional[Decimal] = Field(default=None, sa_type=Numeric(18, 4))
    comment_count: int = Field(default=0)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
 
    events: List["PolymarketEvent"] = Relationship(back_populates="series")
 
 
class PolymarketEvent(SQLModel, table=True):
    __tablename__ = "polymarket_events"
 
    id: str = Field(primary_key=True, sa_type=String(50))
    ticker: str = Field(sa_type=String(300), unique=True, index=True)
    slug: str = Field(sa_type=String(300), index=True)
    series_id: Optional[str] = Field(
        default=None,
        sa_column=Column(String(50), ForeignKey("polymarket_series.id", ondelete="SET NULL"), index=True),
    )
    series_slug: Optional[str] = Field(default=None, sa_type=String(200), index=True)
    title: str = Field(sa_type=String(1000))
    description: Optional[str] = Field(default=None, sa_type=Text)
    resolution_source: Optional[str] = Field(default=None, sa_type=Text)
    image: Optional[str] = Field(default=None, sa_type=Text)
    icon: Optional[str] = Field(default=None, sa_type=Text)
    tags: Optional[Any] = Field(default="[]", sa_column=Column(JSONB, nullable=False, server_default="'[]'"))
    start_date: Optional[datetime] = Field(default=None)
    creation_date: Optional[datetime] = Field(default=None)
    end_date: Optional[datetime] = Field(default=None, index=True)
    closed_time: Optional[datetime] = Field(default=None)
    finished_timestamp: Optional[datetime] = Field(default=None)
    event_date: Optional[date] = Field(default=None, index=True)
    start_time: Optional[datetime] = Field(default=None)
    active: bool = Field(default=True, index=True)
    closed: bool = Field(default=False, index=True)
    archived: bool = Field(default=False)
    new: bool = Field(default=False)
    featured: bool = Field(default=False)
    restricted: bool = Field(default=False)
    live: bool = Field(default=False)
    ended: bool = Field(default=False)
    cyom: bool = Field(default=False)
    automatically_active: bool = Field(default=False)
    automatically_resolved: bool = Field(default=False)
    neg_risk: bool = Field(default=False)
    neg_risk_augmented: bool = Field(default=False)
    enable_neg_risk: bool = Field(default=False)
    enable_order_book: bool = Field(default=False)
    show_all_outcomes: bool = Field(default=False)
    show_market_images: bool = Field(default=False)
    neg_risk_market_id: Optional[str] = Field(default=None, sa_type=String(100))
    liquidity: Decimal = Field(default=Decimal(0), sa_type=Numeric(18, 4))
    liquidity_clob: Decimal = Field(default=Decimal(0), sa_type=Numeric(18, 4))
    volume: Decimal = Field(default=Decimal(0), sa_type=Numeric(18, 4))
    open_interest: Decimal = Field(default=Decimal(0), sa_type=Numeric(18, 4))
    volume_24hr: Decimal = Field(default=Decimal(0), sa_type=Numeric(18, 4))
    volume_1wk: Decimal = Field(default=Decimal(0), sa_type=Numeric(18, 4))
    volume_1mo: Decimal = Field(default=Decimal(0), sa_type=Numeric(18, 4))
    volume_1yr: Decimal = Field(default=Decimal(0), sa_type=Numeric(18, 4))
    competitive: Optional[Decimal] = Field(default=None, sa_type=Numeric(10, 8))
    event_week: Optional[int] = Field(default=None)
    score: Optional[str] = Field(default=None, sa_type=String(50))
    elapsed: Optional[str] = Field(default=None, sa_type=String(50))
    period: Optional[str] = Field(default=None, sa_type=String(50))
    game_id: Optional[int] = Field(default=None, sa_column=Column(BigInteger, index=True))
    comment_count: int = Field(default=0)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
 
    series: Optional[PolymarketSeries] = Relationship(back_populates="events")
    markets: List["PolymarketMarket"] = Relationship(back_populates="event")
 
 
class PolymarketMarket(SQLModel, table=True):
    __tablename__ = "polymarket_markets"
 
    id: str = Field(primary_key=True, sa_type=String(50))
    event_id: str = Field(
        sa_column=Column(String(50), ForeignKey("polymarket_events.id", ondelete="RESTRICT"), nullable=False, index=True)
    )
    question: str = Field(sa_type=Text)
    condition_id: str = Field(sa_type=String(100), unique=True, index=True)
    slug: str = Field(sa_type=String(300), index=True)
    resolution_source: Optional[str] = Field(default=None, sa_type=Text)
    image: Optional[str] = Field(default=None, sa_type=Text)
    icon: Optional[str] = Field(default=None, sa_type=Text)
    description: Optional[str] = Field(default=None, sa_type=Text)
    tags: Optional[Any] = Field(default="[]", sa_column=Column(JSONB, nullable=False, server_default="'[]'"))
    outcomes: Optional[List[str]] = Field(
        default=None,
        sa_column=Column(ARRAY(String), nullable=False, server_default="ARRAY['Yes','No']"),
    )
    outcome_prices: Optional[List[float]] = Field(default=None, sa_column=Column(ARRAY(Numeric(10, 6))))
    start_date: Optional[datetime] = Field(default=None)
    end_date: Optional[datetime] = Field(default=None, index=True)
    end_date_iso: Optional[date] = Field(default=None)
    start_date_iso: Optional[date] = Field(default=None)
    closed_time: Optional[datetime] = Field(default=None)
    accepting_orders_timestamp: Optional[datetime] = Field(default=None)
    uma_end_date: Optional[datetime] = Field(default=None)
    game_start_time: Optional[datetime] = Field(default=None)
    question_id: Optional[str] = Field(default=None, sa_type=String(100), index=True)
    resolved_by: Optional[str] = Field(default=None, sa_type=String(100))
    submitted_by: Optional[str] = Field(default=None, sa_type=String(100))
    uma_resolution_status: Optional[str] = Field(default=None, sa_type=String(50))
    uma_resolution_statuses: Optional[Any] = Field(default=None, sa_column=Column(JSONB))
    uma_bond: Optional[Decimal] = Field(default=None, sa_type=Numeric(10, 2))
    uma_reward: Optional[Decimal] = Field(default=None, sa_type=Numeric(10, 2))
    active: bool = Field(default=True, index=True)
    closed: bool = Field(default=False, index=True)
    archived: bool = Field(default=False)
    new: bool = Field(default=False)
    featured: bool = Field(default=False)
    restricted: bool = Field(default=False)
    ready: bool = Field(default=False)
    funded: bool = Field(default=False)
    accepting_orders: bool = Field(default=True)
    enable_order_book: bool = Field(default=True)
    cyom: bool = Field(default=False)
    automatically_active: bool = Field(default=False)
    automatically_resolved: bool = Field(default=False)
    neg_risk: bool = Field(default=False)
    neg_risk_other: bool = Field(default=False)
    rfq_enabled: bool = Field(default=False)
    fees_enabled: bool = Field(default=False)
    holding_rewards_enabled: bool = Field(default=False)
    neg_risk_market_id: Optional[str] = Field(default=None, sa_type=String(100), index=True)
    neg_risk_request_id: Optional[str] = Field(default=None, sa_type=String(100))
    order_price_min_tick_size: Optional[Decimal] = Field(default=None, sa_type=Numeric(10, 6))
    order_min_size: Optional[int] = Field(default=None)
    seconds_delay: Optional[int] = Field(default=None)
    group_item_title: Optional[str] = Field(default=None, sa_type=String(500))
    group_item_threshold: Optional[int] = Field(default=None)
    sports_market_type: Optional[str] = Field(default=None, sa_type=String(50))
    fee_type: Optional[str] = Field(default=None, sa_type=String(50))
    volume: Decimal = Field(default=Decimal(0), sa_type=Numeric(18, 4))
    liquidity: Decimal = Field(default=Decimal(0), sa_type=Numeric(18, 4))
    open_interest: Decimal = Field(default=Decimal(0), sa_type=Numeric(18, 4))
    volume_24hr: Decimal = Field(default=Decimal(0), sa_type=Numeric(18, 4))
    volume_1wk: Decimal = Field(default=Decimal(0), sa_type=Numeric(18, 4))
    volume_1mo: Decimal = Field(default=Decimal(0), sa_type=Numeric(18, 4))
    volume_1yr: Decimal = Field(default=Decimal(0), sa_type=Numeric(18, 4))
    last_trade_price: Optional[Decimal] = Field(default=None, sa_type=Numeric(10, 6))
    best_bid: Optional[Decimal] = Field(default=None, sa_type=Numeric(10, 6))
    best_ask: Optional[Decimal] = Field(default=None, sa_type=Numeric(10, 6))
    spread: Optional[Decimal] = Field(default=None, sa_type=Numeric(10, 6))
    competitive: Optional[Decimal] = Field(default=None, sa_type=Numeric(10, 8))
    one_hour_price_change: Optional[Decimal] = Field(default=None, sa_type=Numeric(10, 6))
    one_day_price_change: Optional[Decimal] = Field(default=None, sa_type=Numeric(10, 6))
    one_week_price_change: Optional[Decimal] = Field(default=None, sa_type=Numeric(10, 6))
    one_month_price_change: Optional[Decimal] = Field(default=None, sa_type=Numeric(10, 6))
    one_year_price_change: Optional[Decimal] = Field(default=None, sa_type=Numeric(10, 6))
    rewards_min_size: Optional[Decimal] = Field(default=None, sa_type=Numeric(10, 4))
    rewards_max_spread: Optional[Decimal] = Field(default=None, sa_type=Numeric(10, 4))
    clob_rewards: Optional[Any] = Field(default=None, sa_column=Column(JSONB))
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    player_id: Optional[int] = Field(
        default=None,
        foreign_key="players.player_id",
        index=True,
    )
    team_id: Optional[int] = Field(
        default=None,
        foreign_key="teams.team_id",
        index=True,
    )
    competition_id: Optional[int] = Field(
        default=None,
        foreign_key="competitions.competition_id",
        index=True,
    )

    event: Optional[PolymarketEvent] = Relationship(back_populates="markets")
    tokens: List["PolymarketToken"] = Relationship(back_populates="market")
    snapshots: List["PolymarketMarketSnapshot"] = Relationship(back_populates="market")
 
 
class PolymarketMarketSnapshot(SQLModel, table=True):
    __tablename__ = "polymarket_market_snapshots"
 
    id: Optional[int] = Field(default=None, sa_column=Column(BigInteger, primary_key=True, autoincrement=True))
    market_id: str = Field(
        sa_column=Column(String(50), ForeignKey("polymarket_markets.id", ondelete="CASCADE"), nullable=False, index=True)
    )
    snapshotted_at: datetime = Field(
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False, index=True,
                         server_default=func.current_timestamp())
    )
    active: bool = Field()
    closed: bool = Field()
    accepting_orders: bool = Field()
    outcome_prices: Optional[List[float]] = Field(default=None, sa_column=Column(ARRAY(Numeric(10, 6))))
    volume: Decimal = Field(default=Decimal(0), sa_type=Numeric(18, 4))
    liquidity: Decimal = Field(default=Decimal(0), sa_type=Numeric(18, 4))
    open_interest: Decimal = Field(default=Decimal(0), sa_type=Numeric(18, 4))
    volume_24hr: Decimal = Field(default=Decimal(0), sa_type=Numeric(18, 4))
    last_trade_price: Optional[Decimal] = Field(default=None, sa_type=Numeric(10, 6))
    best_bid: Optional[Decimal] = Field(default=None, sa_type=Numeric(10, 6))
    best_ask: Optional[Decimal] = Field(default=None, sa_type=Numeric(10, 6))
    spread: Optional[Decimal] = Field(default=None, sa_type=Numeric(10, 6))
    one_hour_price_change: Optional[Decimal] = Field(default=None, sa_type=Numeric(10, 6))
    one_day_price_change: Optional[Decimal] = Field(default=None, sa_type=Numeric(10, 6))
    one_week_price_change: Optional[Decimal] = Field(default=None, sa_type=Numeric(10, 6))
 
    market: Optional[PolymarketMarket] = Relationship(back_populates="snapshots")
 
 
class PolymarketToken(SQLModel, table=True):
    __tablename__ = "polymarket_tokens"
    __table_args__ = (
        UniqueConstraint("market_id", "outcome", name="uq_pm_tokens_market_outcome"),
    )
 
    id: Optional[int] = Field(default=None, sa_column=Column(BigInteger, primary_key=True, autoincrement=True))
    market_id: str = Field(
        sa_column=Column(String(50), ForeignKey("polymarket_markets.id", ondelete="CASCADE"), nullable=False, index=True)
    )
    token_id: str = Field(sa_type=String(100), unique=True, index=True)
    outcome: str = Field(sa_type=String(200))        # "Yes", "No", or team name
    outcome_index: int = Field(sa_column=Column(SmallInteger, nullable=False))  # 0=Yes, 1=No
 
    market: Optional[PolymarketMarket] = Relationship(back_populates="tokens")
    price_history: List["PolymarketPriceHistory"] = Relationship(back_populates="token")
 
 
class PolymarketPriceHistory(SQLModel, table=True):
    __tablename__ = "polymarket_price_history"
    __table_args__ = (
        UniqueConstraint("token_id", "ts", name="uq_pm_price_history_token_ts"),
    )

    id: Optional[int] = Field(default=None, sa_column=Column(BigInteger, primary_key=True, autoincrement=True))
    token_id: str = Field(
        sa_column=Column(String(100), ForeignKey("polymarket_tokens.token_id", ondelete="CASCADE"), nullable=False, index=True)
    )
    ts: int = Field(sa_column=Column(BigInteger, nullable=False, index=True))
    probability: Decimal = Field(sa_type=Numeric(10, 6))

    token: Optional[PolymarketToken] = Relationship(back_populates="price_history")


class PolymarketTrade(SQLModel, table=True):
    __tablename__ = "polymarket_trades"
    __table_args__ = (
        UniqueConstraint("transaction_hash", "condition_id", "outcome_index", name="uq_pm_trade"),
        Index("ix_pm_trades_condition_id", "condition_id"),
        Index("ix_pm_trades_ts", "ts"),
        Index("ix_pm_trades_proxy_wallet", "proxy_wallet"),
    )

    id:               Optional[int] = Field(default=None, sa_column=Column(BigInteger, primary_key=True, autoincrement=True))
    transaction_hash: str           = Field(sa_column=Column(String(100), nullable=False))
    condition_id:     str           = Field(sa_column=Column(String(100), ForeignKey("polymarket_markets.condition_id", ondelete="CASCADE"), nullable=False))
    proxy_wallet:     str           = Field(sa_column=Column(String(100), nullable=False))
    side:             str           = Field(sa_column=Column(String(10), nullable=False))
    asset:            str           = Field(sa_column=Column(String(200), nullable=False))
    size:             Decimal       = Field(sa_type=Numeric(20, 6))
    price:            Decimal       = Field(sa_type=Numeric(10, 6))
    ts:               int           = Field(sa_column=Column(BigInteger, nullable=False))
    outcome:          Optional[str] = Field(default=None, sa_column=Column(String(100)))
    outcome_index:    Optional[int] = Field(default=None, sa_column=Column(SmallInteger))
    trader_name:      Optional[str] = Field(default=None, sa_column=Column(String(200)))
    pseudonym:        Optional[str] = Field(default=None, sa_column=Column(String(200)))

# var decision
class VarDecision(SQLModel, table=True):
    __tablename__ = "var_decisions"
    __table_args__ = (
        UniqueConstraint("referee_final_decision", "match_id", name="uq_var_final_match"),
        UniqueConstraint("team_id_in_favour", "match_id", name="uq_var_team_match"),
        UniqueConstraint("source_url", name="uq_var_source_url"),
        {"extend_existing": True},
    )
    id: Optional[int] = Field(default=None, primary_key=True)
    match_id: Optional[int] = Field(default=None, foreign_key="matches.match_id", index=True)

    gif_url: Optional[str] = None
    raw_article_text: str = Field(unique=True)
    referee_original_decision: Optional[str] = None
    referee_final_decision: Optional[str] = None
    reason: Optional[str] = None
    var_decision: Optional[str] = None
    var_review: Optional[str] = None

    team_id_in_favour: Optional[int] = Field(default=None, foreign_key="teams.team_id", index=True)
    team_id_against: Optional[int] = Field(default=None, foreign_key="teams.team_id", index=True)

    team_id_original_in_favour: Optional[int] = Field(default=None, foreign_key="teams.team_id", index=True)
    team_id_original_against: Optional[int] = Field(default=None, foreign_key="teams.team_id", index=True)

    match_time: Optional[str] = Field(default=None, max_length=10)
    created_at: Optional[datetime] = Field(default_factory=lambda: datetime.now(timezone.utc))
    source_url: str

# espn commentary
class EspnCommentary(SQLModel, table=True):
    __tablename__ = "espn_commentary"
    __table_args__ = (
        UniqueConstraint("espn_match_id", "text", name="uq_espn_commentary"),
        UniqueConstraint("source_url", "text", name="uq_espn_commentary_source"),
        {"extend_existing": True},
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    match_id: Optional[int] = Field(default=None, foreign_key="matches.match_id", index=True)

    text: Optional[str] = None
    minute: Optional[int] = None
    add_minute: int = Field(default=0)
    source_url: Optional[str] = None
    espn_match_id: Optional[int] = Field(default=None, index=True)
    team_id: Optional[int] = Field(default=None, foreign_key="teams.team_id", index=True)
    player_id: Optional[int] = Field(default=None, foreign_key="players.player_id", index=True)

    created_at: Optional[datetime] = Field(default_factory=lambda: datetime.now(timezone.utc))


# player absences
from typing import Optional
from datetime import datetime, date, timezone
from sqlmodel import SQLModel, Field
from sqlalchemy import UniqueConstraint


class PlayerAbsence(SQLModel, table=True):
    __tablename__ = "player_absences"
    __table_args__ = (
        UniqueConstraint("player_id", "team_id", "from_date", "season", "absence_reason", name="uq_player_absence"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    player_id: int = Field(foreign_key="players.player_id", nullable=False, index=True)
    team_id: Optional[int] = Field(default=None, foreign_key="teams.team_id", index=True)

    season_str: Optional[str] = Field(default=None, max_length=20)
    season: Optional[int] = Field(default=None)
    absence_reason: Optional[str] = None
    competition: Optional[str] = Field(default=None, max_length=100)
    from_date: Optional[date] = None
    to_date: Optional[date] = None
    days: Optional[int] = None
    games_missed: Optional[int] = None

    created_at: Optional[datetime] = Field(default_factory=lambda: datetime.now(timezone.utc))


# social media trasnfer news
class TransferEventType(str, enum.Enum):
    transfer_talk                      = "transfer talk"
    transfer_talk_bad                  = "transfer talk bad"
    bid_rejected                       = "bid rejected"
    bid_accepted                       = "bid accepted"
    bid_sent                           = "bid sent"
    teams_agreement                    = "teams agreement"
    teams_discussions                  = "teams discussions"
    teams_discussions_bad              = "teams discussions bad"
    person_going_to_medical            = "person going to medical"
    person_arrived_for_medical         = "person arrived for medical"
    medical_good                       = "medical good"
    medical_bad                        = "medical bad"
    person_team_agreement              = "person team agreement"
    person_team_discussions            = "person team discussions"
    person_team_discussions_bad        = "person team discussions bad"
    transfer_confirmed                 = "transfer confirmed"
    transfer_confirmed_by_selling_team = "transfer confirmed by selling team"
    transfer_confirmed_by_buying_team  = "transfer confirmed by buying team"
    transfer_photo_confirmation        = "transfer photo confirmation"
    transfer_called_off                = "transfer called off"
    team_transfer_talk                 = "team transfer talk"
    team_transfer_talk_bad             = "team transfer talk bad"
    person_transfer_talk               = "person transfer talk"
    person_transfer_talk_bad = "person transfer talk bad"
    not_applicable           = "not applicable"
    transfer_confirmed_by_person = "transfer confirmed by person"


class TransferTypeEnum(str, enum.Enum):
    transfer             = "transfer"
    contract_termination = "contract termination"
    contract_renewal     = "contract renewal"
    loan                 = "loan"
    contract_expiration  = "contract expiration"
    not_applicable       = "not applicable"
    
    
    


class SocialMediaTransferNews(SQLModel, table=True):
    __tablename__ = "social_media_transfer_news"
    __table_args__ = (
        UniqueConstraint(
        "post_raw_content", "player_id", "event_type", "from_team_id", "to_team_id", 
        "instagram_post_id", "twitter_post_id",
        name="uq_smtn_player"
    ),
        UniqueConstraint(
        "post_raw_content", "manager_id", "event_type", "from_team_id", "to_team_id",
        "instagram_post_id", "twitter_post_id",
        name="uq_smtn_manager"
    ),
    )
    id: Optional[int] = Field(default=None, primary_key=True)

    player_id:  int           = Field(foreign_key="players.player_id", nullable=False, index=True)
    manager_id: Optional[int] = Field(default=None, foreign_key="managers.id", index=True)

    from_team_id: Optional[int] = Field(default=None, foreign_key="teams.team_id", index=True)
    to_team_id:   Optional[int] = Field(default=None, foreign_key="teams.team_id", index=True)
    to_team2_id:  Optional[int] = Field(default=None, foreign_key="teams.team_id")
    to_team3_id:  Optional[int] = Field(default=None, foreign_key="teams.team_id")

    is_source_verified: Optional[bool]    = Field(default=None)
    is_rumour:          Optional[bool]    = Field(default=None)
    event_type:         TransferEventType = Field(sa_column=Column(SAEnum(TransferEventType, name="transfer_event_type"), nullable=False, index=True))
    transfer_type:      TransferTypeEnum  = Field(sa_column=Column(SAEnum(TransferTypeEnum, name="transfer_type_enum"), nullable=False, index=True))

    post_raw_content: str = Field(nullable=False)

    instagram_post_id:      Optional[str]      = Field(default=None, max_length=100, foreign_key="instagram_posts.post_id", index=True)
    twitter_post_id:        Optional[str]      = Field(default=None, max_length=100, foreign_key="tweets.rest_id", index=True)
    social_media_posted_at: Optional[datetime] = Field(default=None, index=True)

    kalshi_event_ticker:     Optional[str] = Field(default=None, max_length=100, foreign_key="kalshi_events.event_ticker")
    polymarket_event_ticker: Optional[str] = Field(default=None, max_length=100, foreign_key="polymarket_events.ticker")

    transfer_fee_reported_usd: Optional[Decimal] = Field(default=None, max_digits=15, decimal_places=2)
    transfer_fee_reported_eur: Optional[Decimal] = Field(default=None, max_digits=15, decimal_places=2)
    transfer_fee_reported_gbp: Optional[Decimal] = Field(default=None, max_digits=15, decimal_places=2)
    salary_reported_usd: Optional[float] = None
    salary_reported_eur: Optional[float] = None
    salary_reported_gbp: Optional[float] = None

    created_at: Optional[datetime] = Field(default_factory=lambda: datetime.now(timezone.utc))    



class TeamLogo(SQLModel, table=True):
    __tablename__ = "team_logos"
    __table_args__ = (
        UniqueConstraint("team_id", "season", name="uq_team_logos"),
    )

    id:         Optional[int] = Field(default=None, primary_key=True)
    team_id:    int           = Field(foreign_key="teams.team_id", nullable=False, index=True)
    logo_url:   Optional[str] = None
    season:     Optional[int] = None
    created_at: Optional[datetime] = Field(default_factory=lambda: datetime.now(timezone.utc))


class PlayerPicture(SQLModel, table=True):
    __tablename__ = "player_pictures"
    __table_args__ = (
        UniqueConstraint("player_id", "season", name="uq_player_pictures"),
    )

    id:          Optional[int] = Field(default=None, primary_key=True)
    player_id:   int           = Field(foreign_key="players.player_id", nullable=False, index=True)
    picture_url: Optional[str] = None
    season:      Optional[int] = None
    created_at:  Optional[datetime] = Field(default_factory=lambda: datetime.now(timezone.utc))

class KitType(str, enum.Enum):
    home = "home"
    away = "away"
    third = "third"
    goalkeeper = "goalkeeper"
    special = "special"
    fourth = "fourth"
    special_2 = "special 2"  # Variable name uses underscore, string value uses space

class TeamKit(SQLModel, table=True):
    __tablename__ = "team_kits"
    __table_args__ = (
        UniqueConstraint("team_id", "type", "season", name="uq_team_kits"),
    )
    
    id:          Optional[int] = Field(default=None, primary_key=True)
    team_id:     int           = Field(foreign_key="teams.team_id", nullable=False, index=True)
    picture_url: Optional[str] = None
    season:      Optional[int] = None
    
    # Links the Python Enum to the PostgreSQL ENUM type
    type: KitType = Field(
        sa_column=Column(SAEnum(KitType), nullable=False)
    )
    
    created_at: Optional[datetime] = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

from datetime import date, datetime, time
from typing import Optional, List
from decimal import Decimal
from sqlalchemy import ARRAY, String

from sqlmodel import Field, SQLModel, Column

# players table
class Player(SQLModel, table=True):
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

# ig followers
class IgFollowers(SQLModel, table=True):
    __tablename__ = "ig_followers"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    player_id: Optional[int] = Field(default=None, foreign_key="players.player_id", index=True)
    num_followers: Optional[int] = Field(default=None)
    updated_at: Optional[datetime] = Field(default=None)



# countries table
class Country(SQLModel, table=True):
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
    __tablename__ = "leagues"
    
    league_id: int = Field(primary_key=True)
    name: Optional[str] = Field(default=None, unique=True)
    country_id: Optional[int] = Field(default=None, foreign_key="countries.country_id")
    confederation: Optional[str] = Field(default=None)
    scope: Optional[str] = Field(default=None)
    tier_level: Optional[str] = Field(default=None)
    competition_level: Optional[str] = Field(default=None)
    format: Optional[str] = Field(default=None)

# competitions table
class Competition(SQLModel, table=True):
    __tablename__ = "competitions"
    
    competition_id: int = Field(primary_key=True)
    league_id: Optional[int] = Field(default=None, foreign_key="leagues.league_id")
    name: Optional[str] = Field(default=None)
    stage: Optional[str] = Field(default=None)
    season_year: Optional[int] = Field(default=None)  # smallint maps to int
    stage_order: Optional[int] = Field(default=None)  # smallint maps to int
    scrape_league_id: Optional[int] = Field(default=None, foreign_key="scraping_leagues.id")
    logo_url: Optional[str] = Field(default=None)


# scraping_leagues tables
class ScrapingLeague(SQLModel, table=True):
    __tablename__ = "scraping_leagues"
    
    id: int = Field(primary_key=True)
    name: Optional[str] = Field(default=None)
    tfm_name: Optional[str] = Field(default=None)
    tfm_url: Optional[str] = Field(default=None)
    fbref_id: Optional[str] = Field(default=None)
    fbref_url: Optional[str] = Field(default=None)
    fotmob_id: Optional[int] = Field(default=None)
    fotmob_url: Optional[str] = Field(default=None)

# ranks table
class Rank(SQLModel, table=True):
    __tablename__ = "ranks"
    
    id: int = Field(primary_key=True)  # bigint maps to int
    competition_id: int = Field(foreign_key="competitions.competition_id", index=True)
    team_id: int = Field(foreign_key="teams.team_id", index=True)
    rank: int = Field(index=True)  # smallint maps to int
    points: Optional[int] = Field(default=None)
    wins: Optional[int] = Field(default=None)
    losses: Optional[int] = Field(default=None)
    draws: Optional[int] = Field(default=None)
    gd: Optional[int] = Field(default=None)  # goal difference
    goals_f: Optional[int] = Field(default=None)  # goals for
    goals_a: Optional[int] = Field(default=None)  # goals against
    gp: Optional[int] = Field(default=None)  # games played
    info: Optional[str] = Field(default=None)

# teams table
class Team(SQLModel, table=True):
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

# squads table
class Squad(SQLModel, table=True):
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

# stadiums table
class Stadium(SQLModel, table=True):
    __tablename__ = "stadiums"
    
    id: int = Field(primary_key=True)
    name: str = Field(index=True, unique=True)
    city: Optional[str] = Field(default=None)
    country_id: Optional[int] = Field(default=None, foreign_key="countries.country_id", index=True)
    capacity: Optional[int] = Field(default=None)
    tfm_id: Optional[int] = Field(default=None)
    tfm_url: Optional[str] = Field(default=None)

# match events
class MatchEvent(SQLModel, table=True):
    __tablename__ = "match_events"
    
    event_id: Optional[int] = Field(default=None, primary_key=True)
    match_id: int = Field(foreign_key="matches.match_id", index=True)
    team_id: int = Field(foreign_key="teams.team_id", index=True)
    event_type: str = Field(index=True)
    minute: int
    add_minute: Optional[int] = None
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

# matches table
class Match(SQLModel, table=True):
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
    home_shots_att: Optional[int] = None
    away_shots_att: Optional[int] = None
    home_shots_acc: Optional[int] = None
    away_shots_acc: Optional[int] = None
    home_shots_succ: Optional[int] = None
    away_shots_succ: Optional[int] = None
    
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
    home_saves_succ: Optional[int] = None
    away_saves_succ: Optional[int] = None
    home_saves_att: Optional[int] = None
    away_saves_att: Optional[int] = None

# player match stats
class PlayerMatchStat(SQLModel, table=True):
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
    
    # Penalties
    pens_made: Optional[int] = Field(default=0)
    pens_att: Optional[int] = Field(default=0)
    
    # Shooting
    shots: Optional[int] = Field(default=0)
    shots_on_target: Optional[int] = Field(default=0)
    
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
    errors: Optional[int] = Field(default=0)
    
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
    
    # Take-ons
    take_ons: Optional[int] = Field(default=0)
    take_ons_won: Optional[int] = Field(default=0)
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
    
    # Aerials
    aerials_won: Optional[int] = Field(default=0)
    aerials_lost: Optional[int] = Field(default=0)
    aerials_won_pct: Optional[Decimal] = Field(default=None, max_digits=5, decimal_places=2)
    
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

# player comp stats
class PlayerCompStats(SQLModel, table=True):
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
    __tablename__ = "referees"
    
    id: int = Field(primary_key=True)
    name: Optional[str] = Field(default=None)
    tfm_id: int = Field(unique=True)
    tfm_url: Optional[str] = Field(default=None)
    country_id: Optional[int] = Field(default=None, foreign_key="countries.country_id")
    age: Optional[int] = Field(default=None)

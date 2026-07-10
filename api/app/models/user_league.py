from typing import Optional
from datetime import date, datetime
from pydantic import BaseModel, Field


class UserLeague(BaseModel):
    id: int
    name: str
    league_code: str
    creator_id: str
    start_date: date
    end_date: date
    # The scoring window — which public.matches (by date) count toward
    # rankings. Defaults to start_date/end_date at creation, but admins can
    # move it independently of the league's own lifecycle dates.
    matches_start_date: date
    matches_end_date: date
    status: str
    rankings_updated_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class UserLeagueSummary(UserLeague):
    member_count: int
    is_admin: bool


class UserLeagueMember(BaseModel):
    profile_id: str
    is_admin: bool
    joined_at: Optional[datetime] = None
    username: Optional[str] = None
    display_name: Optional[str] = None
    avatar_url: Optional[str] = None


class UserLeagueRanking(BaseModel):
    profile_id: str
    points: int
    matches_scored: int
    username: Optional[str] = None
    display_name: Optional[str] = None
    avatar_url: Optional[str] = None
    country_flag_url: Optional[str] = None
    favourite_team_logo_url: Optional[str] = None


class UserLeagueDetail(UserLeague):
    # Overrides the base (required) league_code — null unless the requester
    # is a member. Anyone can view a league's details; the join code stays
    # member-only.
    league_code: Optional[str] = None
    league_ids: list[int]
    is_member: bool
    is_admin: bool
    members: list[UserLeagueMember]


class UserLeagueResponse(BaseModel):
    data: UserLeague


class UserLeagueListResponse(BaseModel):
    data: list[UserLeagueSummary]


class UserLeagueDetailResponse(BaseModel):
    data: UserLeagueDetail


class UserLeagueRankingsResponse(BaseModel):
    data: list[UserLeagueRanking]


class ScoredMatchTeam(BaseModel):
    team_id: int
    name: str
    logo_url: Optional[str] = None


class UserLeagueScoredMatch(BaseModel):
    match_id: int
    match_date: date
    home_goals: int
    away_goals: int
    predicted_home_goals: int
    predicted_away_goals: int
    outcome_correct: bool
    points: int
    home_team: ScoredMatchTeam
    away_team: ScoredMatchTeam


class UserLeagueScoredMatchesResponse(BaseModel):
    data: list[UserLeagueScoredMatch]


class CreateUserLeagueRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    start_date: date
    end_date: date
    league_ids: list[int] = Field(min_length=1)


class UpdateUserLeagueRequest(BaseModel):
    # start_date is intentionally not editable — only when the league itself
    # finishes (end_date) and which matches count toward scoring
    # (matches_start_date/matches_end_date) can change after creation.
    end_date: Optional[date] = None
    matches_start_date: Optional[date] = None
    matches_end_date: Optional[date] = None
    league_ids: Optional[list[int]] = None


class JoinUserLeagueRequest(BaseModel):
    league_code: str = Field(min_length=1, max_length=20)


class SetMemberAdminRequest(BaseModel):
    is_admin: bool

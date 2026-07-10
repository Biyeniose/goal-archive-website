from typing import Optional
from datetime import datetime
from pydantic import BaseModel, Field


class MatchPrediction(BaseModel):
    id: int
    profile_id: str
    match_id: int
    home_goals: int
    away_goals: int
    win_team: Optional[int] = None
    loss_team: Optional[int] = None
    isdraw: Optional[bool] = None
    pens_winner: Optional[int] = None
    is_settled: bool = False
    correct: Optional[bool] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class MatchPredictionResponse(BaseModel):
    data: Optional[MatchPrediction] = None


class MatchPredictionRequest(BaseModel):
    home_goals: int = Field(ge=0, le=99)
    away_goals: int = Field(ge=0, le=99)
    # Only meaningful (and only accepted by the DB trigger) when home_goals ==
    # away_goals and the match's round makes it pens-eligible — who the user
    # thinks wins the shootout, not a score.
    pens_winner: Optional[int] = None


class PredictionMatchTeam(BaseModel):
    team_id: int
    name: str
    logo_url: Optional[str] = None


class PredictionMatchInfo(BaseModel):
    match_id: int
    match_date: Optional[str] = None
    match_time_utc: Optional[datetime] = None
    isplayed: Optional[bool] = None
    is_live: Optional[bool] = None
    home_goals: Optional[int] = None
    away_goals: Optional[int] = None
    # win_team/isdraw are the actual result (win_team reflects the real pens
    # winner when the match went to a shootout — home_goals/away_goals stay
    # the drawn regulation score in that case).
    win_team: Optional[int] = None
    isdraw: Optional[bool] = None
    pen_home_goals: Optional[int] = None
    pen_away_goals: Optional[int] = None
    round: Optional[str] = None
    can_go_to_pens: bool = False
    home_team: PredictionMatchTeam
    away_team: PredictionMatchTeam


class MatchPredictionWithMatch(MatchPrediction):
    match: PredictionMatchInfo


class MatchPredictionListResponse(BaseModel):
    data: list[MatchPredictionWithMatch]


class UserPredictionsResponse(BaseModel):
    pending: list[MatchPredictionWithMatch]
    settled: list[MatchPredictionWithMatch]

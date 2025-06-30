from typing import List, Dict, Optional
from pydantic import BaseModel


class MatchData(BaseModel):
    match_id: int
    comp_id: int
    home_team_name: str
    home_id: int
    home_logo: str
    home_color: Optional[str]
    home_formation: Optional[str]
    home_ranking: Optional[int]
    home_goals: Optional[int]
    away_team_name: str
    away_id: int
    away_logo: str
    away_color: Optional[str]
    away_formation: Optional[str]
    away_ranking: Optional[int]
    away_goals: Optional[int]
    result: Optional[str]
    round: Optional[str]
    match_date: Optional[str]
    season_year: int
    win_team: Optional[int]
    loss_team: Optional[int]
    isDraw: Optional[bool]
    extra_time: Optional[bool]
    pens: Optional[bool]
    pen_home_goals: Optional[int]
    pen_away_goals: Optional[int]
    match_time: Optional[str]

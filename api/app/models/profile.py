from typing import Optional
from datetime import datetime
from pydantic import BaseModel, field_validator


class Profile(BaseModel):
    id: str
    username: str
    display_name: Optional[str] = None
    bio: Optional[str] = None
    avatar_url: Optional[str] = None
    country_id: Optional[int] = None
    favourite_team_id: Optional[int] = None
    total_predictions: int = 0
    correct_predictions: int = 0
    overall_rank: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class ProfileResponse(BaseModel):
    data: Profile


class ProfileUpdateRequest(BaseModel):
    username: Optional[str] = None
    display_name: Optional[str] = None
    bio: Optional[str] = None
    avatar_url: Optional[str] = None
    country_id: Optional[int] = None
    favourite_team_id: Optional[int] = None

    @field_validator("username")
    @classmethod
    def validate_username(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v = v.strip()
        if not v:
            raise ValueError("Username cannot be empty")
        if len(v) > 30:
            raise ValueError("Username must be 30 characters or fewer")
        return v

    @field_validator("display_name")
    @classmethod
    def validate_display_name(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if len(v) > 100:
            raise ValueError("Display name must be 100 characters or fewer")
        return v

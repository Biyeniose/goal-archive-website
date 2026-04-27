from sqlalchemy import text
from fastapi import APIRouter, Query, HTTPException

from ..dependencies import DBSession, AppLoggerDep
from ..models.team import TeamResponse, TeamSearchResponse
from ..models.utils import Country

router = APIRouter(
    prefix="/v1/teams",
    tags=["teams"],
)


@router.get("/search", response_model=TeamSearchResponse)
async def search_teams(
    session: DBSession,
    logger: AppLoggerDep,
    q: str = Query(..., description="Team name search query"),
    limit: int = Query(15, description="Maximum number of results"),
):
    logger.info(f"Searching teams with query: {q}")

    query = text("""
        SELECT json_build_object('data', coalesce(json_agg(d), '[]'::json))
        FROM (
            SELECT json_build_object(
                'team_id',     t.team_id,
                'team_name',   t.name,
                'common_name', t.common_name,
                'short_name',  t.short_name,
                'logo_url',    t.logo_url,
                'level',       t.level,
                'type',        t.type,
                'country', json_build_object(
                    'country_id', c.country_id,
                    'name',       c.name,
                    'flag_url',   c.flag_url,
                    'continent',  c.continent,
                    'iso_code_3', c.iso_code_3
                )
            ) AS d
            FROM teams t
            LEFT JOIN countries c ON c.country_id = t.country_id
            WHERE t.name ILIKE :q OR t.common_name ILIKE :q
            ORDER BY t.name
            LIMIT :limit
        ) sub
    """)

    result = session.exec(query, params={"q": f"%{q}%", "limit": limit}).first()
    return result[0] if result else {"data": []}


@router.get("/countries", response_model=list[Country])
async def get_countries(
    session: DBSession,
    logger: AppLoggerDep,
):
    logger.info("Fetching all countries")

    query = text("""
        SELECT coalesce(json_agg(d ORDER BY sort_name), '[]'::json)
        FROM (
            SELECT json_build_object(
                'country_id', c.country_id,
                'name',       c.name,
                'flag_url',   c.flag_url,
                'continent',  c.continent,
                'iso_code_3', c.iso_code_3
            ) AS d,
            c.name AS sort_name
            FROM countries c
        ) sub
    """)

    result = session.exec(query).first()
    return result[0] if result else []


@router.get("/country/{country_id}", response_model=TeamResponse)
async def get_national_team_by_country(
    country_id: int,
    session: DBSession,
    logger: AppLoggerDep,
):
    logger.info(f"Fetching national team for country_id: {country_id}")

    query = text("""
        SELECT json_build_object(
            'data', json_build_object(
                'team_id',     t.team_id,
                'team_name',   t.name,
                'common_name', t.common_name,
                'short_name',  t.short_name,
                'logo_url',    t.logo_url,
                'level',       t.level,
                'type',        t.type,
                'country', json_build_object(
                    'country_id', c.country_id,
                    'name',       c.name,
                    'flag_url',   c.flag_url,
                    'continent',  c.continent,
                    'iso_code_3', c.iso_code_3
                )
            )
        )
        FROM teams t
        LEFT JOIN countries c ON c.country_id = t.country_id
        WHERE t.country_id = :country_id
          AND t.type = 'national'
        LIMIT 1
    """)

    result = session.exec(query, params={"country_id": country_id}).first()
    if not result or result[0].get("data") is None:
        raise HTTPException(status_code=404, detail="National team not found")
    return result[0]


@router.get("/{team_id}", response_model=TeamResponse)
async def get_team(
    team_id: int,
    session: DBSession,
    logger: AppLoggerDep,
):
    logger.info(f"Fetching team detail for team_id: {team_id}")

    query = text("""
        SELECT json_build_object(
            'data', json_build_object(
                'team_id',     t.team_id,
                'team_name',   t.name,
                'common_name', t.common_name,
                'short_name',  t.short_name,
                'logo_url',    t.logo_url,
                'level',       t.level,
                'type',        t.type,
                'country', json_build_object(
                    'country_id', c.country_id,
                    'name',       c.name,
                    'flag_url',   c.flag_url,
                    'continent',  c.continent,
                    'iso_code_3', c.iso_code_3
                )
            )
        )
        FROM teams t
        LEFT JOIN countries c ON c.country_id = t.country_id
        WHERE t.team_id = :team_id
    """)

    result = session.exec(query, params={"team_id": team_id}).first()
    if not result or result[0].get("data") is None:
        raise HTTPException(status_code=404, detail="Team not found")
    return result[0]

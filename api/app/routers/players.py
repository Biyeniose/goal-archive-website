from sqlalchemy import text
from fastapi import APIRouter, Query, HTTPException

from ..dependencies import DBSession, AppLoggerDep
from ..models.player import PlayerSearchResponse

router = APIRouter(
    prefix="/v1/players",
    tags=["players"],
)


@router.get("/search", response_model=PlayerSearchResponse)
async def search_players(
    session: DBSession,
    logger: AppLoggerDep,
    q: str = Query(..., description="Player name search query"),
    limit: int = Query(15, description="Maximum number of results"),
):
    logger.info(f"Searching players with query: {q}")

    query = text("""
        SELECT json_build_object('data', coalesce(json_agg(d), '[]'::json))
        FROM (
            SELECT json_build_object(
                'player_name',    p.player_name,
                'player_id',      p.player_id,
                'age',            p.age,
                'tfm_pic_url',    p.tfm_pic_url,
                'pic_url',  p.pic_url,
                'position',       p.position,
                'other_positions', COALESCE(p.other_positions, ARRAY[]::text[]),
                'countries', json_build_object(
                    'country1', CASE
                        WHEN c1.country_id IS NULL THEN NULL
                        ELSE json_build_object(
                            'country_id', c1.country_id,
                            'name',       c1.name,
                            'flag_url',   c1.flag_url,
                            'continent',  c1.continent,
                            'iso_code_3', c1.iso_code_3
                        )
                    END,
                    'country2', CASE
                        WHEN c2.country_id IS NULL THEN NULL
                        ELSE json_build_object(
                            'country_id', c2.country_id,
                            'name',       c2.name,
                            'flag_url',   c2.flag_url,
                            'continent',  c2.continent,
                            'iso_code_3', c2.iso_code_3
                        )
                    END
                )
            ) AS d
            FROM players p
            LEFT JOIN countries c1 ON c1.country_id = p.country_id
            LEFT JOIN countries c2 ON c2.country_id = p.country2_id
            WHERE p.player_name ILIKE :q
            ORDER BY p.player_name
            LIMIT :limit
        ) sub
    """)

    result = session.exec(query, params={"q": f"%{q}%", "limit": limit}).first()
    return result[0] if result else {"data": []}

# get basic player info
@router.get("/{player_id}", response_model=PlayerSearchResponse)
async def get_player(
    player_id: int,
    session: DBSession,
    logger: AppLoggerDep,
):
    logger.info(f"Fetching player detail for player_id: {player_id}")

    query = text("""
        SELECT json_build_object('data', json_agg(d))
        FROM (
            SELECT json_build_object(
                'player_name',    p.player_name,
                'player_id',      p.player_id,
                'age',            p.age,
                'tfm_pic_url',    p.tfm_pic_url,
                'pic_url',  p.pic_url,
                'position',       p.position,
                'other_positions', COALESCE(p.other_positions, ARRAY[]::text[]),
                'countries', json_build_object(
                    'country1', CASE
                        WHEN c1.country_id IS NULL THEN NULL
                        ELSE json_build_object(
                            'country_id', c1.country_id,
                            'name',       c1.name,
                            'flag_url',   c1.flag_url,
                            'continent',  c1.continent,
                            'iso_code_3', c1.iso_code_3
                        )
                    END,
                    'country2', CASE
                        WHEN c2.country_id IS NULL THEN NULL
                        ELSE json_build_object(
                            'country_id', c2.country_id,
                            'name',       c2.name,
                            'flag_url',   c2.flag_url,
                            'continent',  c2.continent,
                            'iso_code_3', c2.iso_code_3
                        )
                    END
                )
            ) AS d
            FROM players p
            LEFT JOIN countries c1 ON c1.country_id = p.country_id
            LEFT JOIN countries c2 ON c2.country_id = p.country2_id
            WHERE p.player_id = :player_id
        ) sub
    """)

    result = session.exec(query, params={"player_id": player_id}).first()
    if not result or result[0].get("data") is None:
        raise HTTPException(status_code=404, detail="Player not found")
    return result[0]




# get player career cv



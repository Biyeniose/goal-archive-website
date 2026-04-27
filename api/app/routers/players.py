from sqlalchemy import text
from fastapi import APIRouter, Query, HTTPException

from ..dependencies import DBSession, AppLoggerDep
from ..models.player import PlayerSearchResponse, PlayerCVResponse

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
@router.get("/{player_id}/cv", response_model=PlayerCVResponse)
async def get_player_cv(
    player_id: int,
    session: DBSession,
    logger: AppLoggerDep,
):
    logger.info(f"Fetching CV for player_id={player_id}")

    query = text("""
        WITH
        -- Non-loan-return transfers, ordered chronologically
        tb AS (
            SELECT
                t.id, t.from_team_id, t.to_team_id, t.isloan,
                t.fee::float, t.player_value::float,
                t.transfer_date, t.season, t.season_str,
                LEAD(t.transfer_date) OVER (ORDER BY t.transfer_date) AS next_transfer_date
            FROM transfers t
            WHERE t.player_id = :player_id
            AND NOT EXISTS (
                SELECT 1 FROM transfers lo
                WHERE lo.player_id      = t.player_id
                AND   lo.from_team_id   = t.to_team_id
                AND   lo.to_team_id     = t.from_team_id
                AND   lo.isloan         = true
                AND   lo.transfer_date  < t.transfer_date
            )
        ),

        -- Player info (single row)
        pd AS (
            SELECT
                p.player_name, p.player_id, p.age,
                p.tfm_pic_url, p.pic_url, p.position,
                COALESCE(p.other_positions, ARRAY[]::text[]) AS other_positions,
                c1.country_id AS c1_id, c1.name AS c1_name, c1.flag_url AS c1_flag,
                c1.continent  AS c1_cont, c1.iso_code_3 AS c1_iso,
                c2.country_id AS c2_id, c2.name AS c2_name, c2.flag_url AS c2_flag,
                c2.continent  AS c2_cont, c2.iso_code_3 AS c2_iso
            FROM players p
            LEFT JOIN countries c1 ON c1.country_id = p.country_id
            LEFT JOIN countries c2 ON c2.country_id = p.country2_id
            WHERE p.player_id = :player_id
        ),

        -- All match-comp participations per transfer period
        -- (club team + any national team during that period)
        -- Skips Without Club / Retired destinations
        tmc AS (
            SELECT
                tb.id          AS transfer_id,
                pms.team_id,
                comp.season_year,
                m.comp_id      AS competition_id,
                MIN(m.match_date)::text AS season_start_date,
                (
                    SELECT pms2.number
                    FROM player_match_stats pms2
                    JOIN matches m2 ON m2.match_id = pms2.match_id
                    WHERE pms2.player_id = :player_id
                    AND   pms2.team_id   = pms.team_id
                    AND   m2.comp_id     = m.comp_id
                    AND   m2.match_date >= tb.transfer_date
                    AND   (tb.next_transfer_date IS NULL OR m2.match_date < tb.next_transfer_date)
                    AND   pms2.number IS NOT NULL
                    GROUP BY pms2.number
                    ORDER BY COUNT(*) DESC LIMIT 1
                ) AS number
            FROM tb
            JOIN teams dest ON dest.team_id = tb.to_team_id
                AND dest.name NOT ILIKE '%without club%'
                AND dest.name NOT ILIKE '%retired%'
            JOIN player_match_stats pms ON pms.player_id = :player_id
            JOIN teams pt  ON pt.team_id   = pms.team_id
            JOIN matches m ON m.match_id   = pms.match_id
                AND m.match_date >= tb.transfer_date
                AND (tb.next_transfer_date IS NULL OR m.match_date < tb.next_transfer_date)
            JOIN competitions comp ON comp.competition_id = m.comp_id
            JOIN leagues lf ON lf.league_id = comp.league_id
            WHERE (
                pms.team_id = tb.to_team_id
                AND (lf.competition_level IS NULL OR lf.competition_level != 'national')
            )
            OR (
                pt.type = 'national'
                AND lf.competition_level = 'national'
                AND pt.country_id IN (
                    SELECT p2.country_id  FROM players p2 WHERE p2.player_id = :player_id AND p2.country_id  IS NOT NULL
                    UNION
                    SELECT p2.country2_id FROM players p2 WHERE p2.player_id = :player_id AND p2.country2_id IS NOT NULL
                )
            )
            GROUP BY tb.id, pms.team_id, comp.season_year, m.comp_id,
                     tb.transfer_date, tb.next_transfer_date
        ),

        -- Competition details + player_comp_stats + team_cups per row
        cs AS (
            SELECT
                tmc.transfer_id,
                tmc.team_id,
                tmc.season_year,
                tmc.competition_id,
                tmc.season_start_date,
                tmc.number,
                comp.season_year        AS c_season_year,
                comp.stage,
                comp.logo_url           AS c_logo,
                l.league_id,
                l.name                  AS l_name,
                l.tier_level,
                l.format,
                l.competition_level,
                lc.country_id           AS lc_id,
                lc.name                 AS lc_name,
                lc.flag_url             AS lc_flag,
                lc.continent            AS lc_cont,
                lc.iso_code_3           AS lc_iso,
                pcs.games_played,
                pcs.minutes,
                pcs.minutes_per_game::float    AS mpg,
                pcs.goals,              pcs.goals_p90::float,
                pcs.assists,            pcs.assists_p90::float,
                pcs.goals_assists,      pcs.goals_assists_p90::float,
                pcs.shots,
                pcs.clean_sheets,
                pcs.goals_conceded,     pcs.goals_conceded_p90::float,
                pcs.penalty_goals,
                pcs.pens_att,
                pcs.cards_yellow,
                pcs.cards_red,
                COALESCE(tc.round, tc.rank::text) AS team_finish
            FROM tmc
            JOIN competitions comp ON comp.competition_id = tmc.competition_id
            JOIN leagues l         ON l.league_id         = comp.league_id
            LEFT JOIN countries lc ON lc.country_id       = l.country_id
            LEFT JOIN player_comp_stats pcs
                ON  pcs.player_id      = :player_id
                AND pcs.competition_id = tmc.competition_id
                AND pcs.team_id        = tmc.team_id
            LEFT JOIN team_cups tc
                ON  tc.competition_id  = tmc.competition_id
                AND tc.team_id         = tmc.team_id
        ),

        -- Aggregate competition_stats per (transfer_id, season_year)
        comp_agg AS (
            SELECT
                transfer_id,
                season_year,
                MIN(season_start_date) AS season_start_date,
                COALESCE(json_agg(json_build_object(
                    'competition', json_build_object(
                        'league', json_build_object(
                            'league_id',        league_id,
                            'league_name',      l_name,
                            'tier_level',       tier_level,
                            'format',           format,
                            'competiton_level', competition_level,
                            'country', CASE WHEN lc_id IS NULL THEN NULL ELSE json_build_object(
                                'country_id', lc_id, 'name', lc_name,
                                'flag_url', lc_flag, 'continent', lc_cont,
                                'iso_code_3', lc_iso) END
                        ),
                        'competition_id', competition_id,
                        'season_year',    c_season_year,
                        'stage',          stage,
                        'logo_url',       c_logo
                    ),
                    'stats', CASE WHEN games_played IS NULL THEN NULL ELSE json_build_object(
                        'gp',                games_played,
                        'minutes',           minutes,
                        'mpg',               mpg,
                        'goals',             goals,
                        'goals_p90',         goals_p90,
                        'assists',           assists,
                        'assists_p90',       assists_p90,
                        'goals_assists',     goals_assists,
                        'goals_assists_p90', goals_assists_p90,
                        'shots',             shots,
                        'clean_sheets',      clean_sheets,
                        'goals_conceded',    goals_conceded,
                        'goals_conceded_p90', goals_conceded_p90,
                        'penalty_goals',     penalty_goals,
                        'pens_att',          pens_att,
                        'cards_yellow',      cards_yellow,
                        'cards_red',         cards_red
                    ) END,
                    'team_finish', team_finish,
                    'number',      number
                ) ORDER BY competition_id), '[]'::json) AS competition_stats
            FROM cs
            GROUP BY transfer_id, season_year
        ),

        -- Distinct teams played for per (transfer_id, season_year)
        season_teams AS (
            SELECT
                tg.transfer_id,
                tg.season_year,
                COALESCE((
                    SELECT json_agg(j ORDER BY (j->>'team_id')::int)
                    FROM (
                        SELECT DISTINCT ON (t2.team_id) json_build_object(
                            'team_id',     t2.team_id,
                            'team_name',   t2.name,
                            'common_name', t2.common_name,
                            'short_name',  t2.short_name,
                            'logo_url',    t2.logo_url,
                            'level',       t2.level,
                            'type',        t2.type,
                            'country', CASE WHEN tc2.country_id IS NULL THEN NULL ELSE json_build_object(
                                'country_id', tc2.country_id, 'name', tc2.name,
                                'flag_url', tc2.flag_url, 'continent', tc2.continent,
                                'iso_code_3', tc2.iso_code_3) END
                        ) AS j
                        FROM tmc tm3
                        JOIN teams t2        ON t2.team_id        = tm3.team_id
                        LEFT JOIN countries tc2 ON tc2.country_id = t2.country_id
                        WHERE tm3.transfer_id = tg.transfer_id
                        AND   tm3.season_year = tg.season_year
                        ORDER BY t2.team_id
                    ) _
                ), '[]'::json) AS teams
            FROM (SELECT DISTINCT transfer_id, season_year FROM tmc) tg
        ),

        -- Seasons aggregated per transfer
        season_agg AS (
            SELECT
                ca.transfer_id,
                COALESCE(json_agg(json_build_object(
                    'season_start_date', ca.season_start_date,
                    'teams',             st.teams,
                    'competition_stats', ca.competition_stats
                ) ORDER BY ca.season_year), '[]'::json) AS seasons
            FROM comp_agg ca
            JOIN season_teams st
                ON  st.transfer_id = ca.transfer_id
                AND st.season_year = ca.season_year
            GROUP BY ca.transfer_id
        )

        SELECT json_build_object(
            'data', json_build_object(
                'player', (
                    SELECT json_build_object(
                        'player_name',     pd.player_name,
                        'player_id',       pd.player_id,
                        'age',             pd.age,
                        'tfm_pic_url',     pd.tfm_pic_url,
                        'pic_url',         pd.pic_url,
                        'position',        pd.position,
                        'other_positions', pd.other_positions,
                        'countries', json_build_object(
                            'country1', CASE WHEN pd.c1_id IS NULL THEN NULL ELSE json_build_object(
                                'country_id', pd.c1_id, 'name', pd.c1_name,
                                'flag_url', pd.c1_flag, 'continent', pd.c1_cont,
                                'iso_code_3', pd.c1_iso) END,
                            'country2', CASE WHEN pd.c2_id IS NULL THEN NULL ELSE json_build_object(
                                'country_id', pd.c2_id, 'name', pd.c2_name,
                                'flag_url', pd.c2_flag, 'continent', pd.c2_cont,
                                'iso_code_3', pd.c2_iso) END
                        )
                    ) FROM pd
                ),
                'stats', COALESCE(json_agg(json_build_object(
                    'transfer', json_build_array(json_build_object(
                        'buying_team', CASE WHEN bt.team_id IS NULL THEN NULL ELSE json_build_object(
                            'team_id', bt.team_id, 'team_name', bt.name,
                            'common_name', bt.common_name, 'short_name', bt.short_name,
                            'logo_url', bt.logo_url, 'level', bt.level, 'type', bt.type,
                            'country', CASE WHEN btc.country_id IS NULL THEN NULL ELSE json_build_object(
                                'country_id', btc.country_id, 'name', btc.name,
                                'flag_url', btc.flag_url, 'continent', btc.continent,
                                'iso_code_3', btc.iso_code_3) END
                        ) END,
                        'selling_team', CASE WHEN sell.team_id IS NULL THEN NULL ELSE json_build_object(
                            'team_id', sell.team_id, 'team_name', sell.name,
                            'common_name', sell.common_name, 'short_name', sell.short_name,
                            'logo_url', sell.logo_url, 'level', sell.level, 'type', sell.type,
                            'country', CASE WHEN sc.country_id IS NULL THEN NULL ELSE json_build_object(
                                'country_id', sc.country_id, 'name', sc.name,
                                'flag_url', sc.flag_url, 'continent', sc.continent,
                                'iso_code_3', sc.iso_code_3) END
                        ) END,
                        'isloan',        tb.isloan,
                        'fee',           tb.fee,
                        'player_value',  tb.player_value,
                        'transfer_date', tb.transfer_date::text,
                        'season',        tb.season,
                        'season_str',    tb.season_str
                    )),
                    'seasons', COALESCE(sa.seasons, '[]'::json)
                ) ORDER BY tb.transfer_date DESC), '[]'::json)
            )
        )
        FROM tb
        LEFT JOIN teams bt      ON bt.team_id    = tb.to_team_id
        LEFT JOIN countries btc ON btc.country_id = bt.country_id
        LEFT JOIN teams sell    ON sell.team_id   = tb.from_team_id
        LEFT JOIN countries sc  ON sc.country_id  = sell.country_id
        LEFT JOIN season_agg sa ON sa.transfer_id = tb.id
    """)

    result = session.exec(query, params={"player_id": player_id}).first()
    if not result or not result[0]:
        raise HTTPException(status_code=404, detail="Player not found")
    return result[0]

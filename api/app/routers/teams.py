from typing import Optional

from sqlalchemy import text
from fastapi import APIRouter, Query, HTTPException

from ..dependencies import DBSession, AppLoggerDep
from ..models.team import TeamResponse, TeamSearchResponse
from ..models.match import TeamWCResponse
from ..models.utils import Country

_WC_STAT_COLS = {"goals", "assists", "minutes"}

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


@router.get("/national-team/{country_id}", response_model=TeamResponse)
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


@router.get("/{team_id}/wc2026", response_model=TeamWCResponse)
async def get_team_wc_preview(
    team_id: int,
    session: DBSession,
    logger: AppLoggerDep,
    stat: str = Query("goals", description="Stat to search for: goals, assists, minutes"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD), default 2025-08-01"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD), default 2026-06-01"),
):
    stat_col = "assists" if stat == "assist" else stat
    if stat_col not in _WC_STAT_COLS:
        raise HTTPException(status_code=400, detail=f"Invalid stat. Valid options: {', '.join(sorted(_WC_STAT_COLS))}")

    resolved_start = start_date or "2025-08-01"
    resolved_end = end_date or "2026-06-01"

    logger.info(f"WC2026 preview for team_id={team_id}, stat={stat_col}, {resolved_start} to {resolved_end}")

    query = text(f"""
        WITH
        -- The team's nationality
        team_country AS (
            SELECT t.country_id
            FROM teams t
            WHERE t.team_id = :team_id
        ),

        -- WC 2026 matches involving this team (league_id=81, season_year=2026)
        wc_matches AS (
            SELECT
                m.match_id, m.match_date, m.match_time_utc,
                m.home_id, m.away_id,
                m.home_goals, m.pen_home_goals, m.away_goals,
                m.home_pass_succ, m.away_pass_succ,
                m.win_team, m.loss_team,
                m.isdraw, m.pens, m.extra_time, m.isplayed,
                m.round, m.gameweek_number
            FROM matches m
            JOIN competitions comp ON comp.competition_id = m.comp_id
            WHERE comp.league_id = 81
              AND comp.season_year = 2026
              AND (m.home_id = :team_id OR m.away_id = :team_id)
        ),

        -- All players from that country (primary or secondary nationality)
        country_players AS (
            SELECT DISTINCT p.player_id
            FROM players p
            JOIN team_country tc ON p.country_id = tc.country_id
                                 OR p.country2_id = tc.country_id
        ),

        -- Stat contributions by those players within the date window,
        -- keyed by the club team they played for
        stat_events AS (
            SELECT
                pms.player_id,
                pms.{stat_col}               AS stat_value,
                t.team_id                    AS club_team_id,
                t.name                       AS club_team_name,
                t.common_name                AS club_common,
                t.short_name                 AS club_short,
                t.logo_url                   AS club_logo,
                t.level                      AS club_level,
                t.type                       AS club_type,
                tc.country_id                AS club_tc_id,
                tc.name                      AS club_tc_name,
                tc.flag_url                  AS club_tc_flag,
                tc.continent                 AS club_tc_cont,
                tc.iso_code_3                AS club_tc_iso,
                COALESCE(tc.name, 'Unknown') AS club_country_name
            FROM player_match_stats pms
            JOIN country_players cp ON cp.player_id       = pms.player_id
            JOIN matches m          ON m.match_id          = pms.match_id
            JOIN competitions comp  ON comp.competition_id  = m.comp_id
            JOIN leagues l          ON l.league_id          = comp.league_id
            JOIN teams t            ON t.team_id            = pms.team_id
            LEFT JOIN countries tc  ON tc.country_id        = t.country_id
            WHERE m.match_date BETWEEN :start_date AND :end_date
              AND pms.{stat_col} > 0
              AND LOWER(COALESCE(comp.stage, '')) NOT IN ('qualification', 'qualifiers')
        ),

        -- Total stat + distinct players per club country
        dist_totals AS (
            SELECT
                club_country_name,
                SUM(stat_value)           AS total_stat_value,
                COUNT(DISTINCT player_id) AS num_players
            FROM stat_events
            GROUP BY club_country_name
        ),

        -- Distinct club teams per club country
        teams_agg AS (
            SELECT
                club_country_name,
                json_agg(DISTINCT jsonb_build_object(
                    'team_id',     club_team_id,
                    'team_name',   club_team_name,
                    'common_name', club_common,
                    'short_name',  club_short,
                    'logo_url',    club_logo,
                    'level',       club_level,
                    'type',        club_type,
                    'country', CASE WHEN club_tc_id IS NULL THEN NULL ELSE jsonb_build_object(
                        'country_id', club_tc_id,
                        'name',       club_tc_name,
                        'flag_url',   club_tc_flag,
                        'continent',  club_tc_cont,
                        'iso_code_3', club_tc_iso
                    ) END
                )) AS teams_json
            FROM (
                SELECT DISTINCT ON (club_country_name, club_team_id)
                    club_country_name, club_team_id, club_team_name, club_common,
                    club_short, club_logo, club_level, club_type,
                    club_tc_id, club_tc_name, club_tc_flag, club_tc_cont, club_tc_iso
                FROM stat_events
                ORDER BY club_country_name, club_team_id
            ) unique_teams
            GROUP BY club_country_name
        ),

        -- Build the matches JSON array
        match_list AS (
            SELECT COALESCE(json_agg(json_build_object(
                'match_id',        wm.match_id,
                'match_date',      wm.match_date::text,
                'match_time_utc',  wm.match_time_utc::text,
                'home_team', json_build_object(
                    'team_id',     ht.team_id,
                    'team_name',   ht.name,
                    'common_name', ht.common_name,
                    'short_name',  ht.short_name,
                    'logo_url',    ht.logo_url,
                    'level',       ht.level,
                    'type',        ht.type,
                    'country', CASE WHEN htc.country_id IS NULL THEN NULL ELSE json_build_object(
                        'country_id', htc.country_id, 'name', htc.name,
                        'flag_url', htc.flag_url, 'continent', htc.continent,
                        'iso_code_3', htc.iso_code_3) END
                ),
                'home_stats', json_build_object(
                    'goals',         wm.home_goals,
                    'penalty_goals', wm.pen_home_goals,
                    'shots',         NULL,
                    'possesion',     NULL,
                    'offsides',      NULL,
                    'corners',       NULL,
                    'xg',            NULL,
                    'pass_succ',     wm.home_pass_succ
                ),
                'away_team', json_build_object(
                    'team_id',     awt.team_id,
                    'team_name',   awt.name,
                    'common_name', awt.common_name,
                    'short_name',  awt.short_name,
                    'logo_url',    awt.logo_url,
                    'level',       awt.level,
                    'type',        awt.type,
                    'country', CASE WHEN atc.country_id IS NULL THEN NULL ELSE json_build_object(
                        'country_id', atc.country_id, 'name', atc.name,
                        'flag_url', atc.flag_url, 'continent', atc.continent,
                        'iso_code_3', atc.iso_code_3) END
                ),
                'away_stats', json_build_object(
                    'goals',         wm.away_goals,
                    'penalty_goals', NULL,
                    'shots',         NULL,
                    'possesion',     NULL,
                    'offsides',      NULL,
                    'corners',       NULL,
                    'xg',            NULL,
                    'pass_succ',     wm.away_pass_succ
                ),
                'win_team_id',     wm.win_team,
                'loss_team_id',    wm.loss_team,
                'isdraw',          wm.isdraw,
                'pens',            wm.pens,
                'extra_time',      wm.extra_time,
                'isplayed',        wm.isplayed,
                'round',           wm.round,
                'gameweek_number', wm.gameweek_number
            ) ORDER BY wm.match_date), '[]'::json) AS matches_json
            FROM wc_matches wm
            JOIN teams ht           ON ht.team_id    = wm.home_id
            LEFT JOIN countries htc ON htc.country_id = ht.country_id
            JOIN teams awt          ON awt.team_id   = wm.away_id
            LEFT JOIN countries atc ON atc.country_id = awt.country_id
        ),

        -- Build the league_dist JSON array — one entry per club country
        dist_list AS (
            SELECT COALESCE(json_agg(json_build_object(
                'country',          dt.club_country_name,
                'teams',            ta.teams_json,
                'total_stat_value', dt.total_stat_value,
                'num_players',      dt.num_players
            ) ORDER BY dt.total_stat_value DESC), '[]'::json) AS dist_json
            FROM dist_totals dt
            JOIN teams_agg ta ON ta.club_country_name = dt.club_country_name
        )

        SELECT json_build_object(
            'data', json_build_object(
                'matches',     ml.matches_json,
                'league_dist', dl.dist_json
            )
        )
        FROM match_list ml, dist_list dl
    """)

    result = session.exec(query, params={
        "team_id": team_id,
        "start_date": resolved_start,
        "end_date": resolved_end,
    }).first()
    return result[0] if result else {"data": {"matches": [], "league_dist": []}}


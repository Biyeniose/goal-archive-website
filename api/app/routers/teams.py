from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import text

from ..dependencies import AppLoggerDep, DBSession
from ..models.match import NationDistResponse, TeamDataResponse
from ..models.team import TeamResponse, TeamSearchResponse
from ..models.utils import Country

_WC_STAT_COLS = {"goals", "assists", "minutes", "goals_assists"}

router = APIRouter(
    prefix="/v1/teams",
    tags=["teams"],
)


@router.get("/search", response_model=TeamSearchResponse)
def search_teams(
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
                'country', CASE WHEN c.country_id IS NULL THEN NULL ELSE json_build_object(
                    'country_id', c.country_id,
                    'name',       c.name,
                    'flag_url',   c.flag_url,
                    'continent',  c.continent,
                    'iso_code_3', c.iso_code_3
                ) END
            ) AS d
            FROM teams t
            LEFT JOIN countries c ON c.country_id = t.country_id
            WHERE (t.name ILIKE :q OR t.common_name ILIKE :q)
              AND t.level IN ('senior')
            ORDER BY t.name
            LIMIT :limit
        ) sub
    """)

    result = session.exec(query, params={"q": f"%{q}%", "limit": limit}).first()
    return result[0] if result else {"data": []}


@router.get("/countries", response_model=list[Country])
def get_countries(
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
                'circle_url', c.circle_url,
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
def get_national_team_by_country(
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


@router.get("/nation-dist", response_model=NationDistResponse)
def get_nation_dist(
    session: DBSession,
    logger: AppLoggerDep,
    season_year: int = Query(..., description="Season year (e.g. 2026)"),
    stat: str = Query(
        "goals",
        description="Stat to sort league_dist by: goals, assists, minutes, goals_assists",
    ),
    country_id: Optional[int] = Query(None, description="Primary country ID"),
    country2_id: Optional[int] = Query(
        None, description="Secondary country ID (optional)"
    ),
):
    if country_id is None and country2_id is None:
        raise HTTPException(
            status_code=400,
            detail="At least one of country_id or country2_id is required",
        )

    stat_col = "assists" if stat == "assist" else stat
    if stat_col not in _WC_STAT_COLS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid stat. Valid options: {', '.join(sorted(_WC_STAT_COLS))}",
        )

    if stat_col == "goals_assists":
        stat_expr = "COALESCE(pms.goals, 0) + COALESCE(pms.assists, 0)"
        stat_pos_filter = "(COALESCE(pms.goals, 0) + COALESCE(pms.assists, 0)) > 0"
    else:
        stat_expr = f"pms.{stat_col}"
        stat_pos_filter = f"pms.{stat_col} > 0"

    # Player nationality filter
    if country_id is not None and country2_id is not None:
        player_filter = "p.country_id = :country_id OR p.country2_id = :country2_id"
    elif country_id is not None:
        player_filter = "p.country_id = :country_id"
    else:
        player_filter = "p.country2_id = :country2_id"

    # Matches: only when country_id is provided (look up national team by country_id)
    if country_id is not None:
        matches_section = """
        national_team AS (
            SELECT t.team_id
            FROM teams t
            WHERE t.type = 'national'
              AND t.country_id = :country_id
            LIMIT 1
        ),
        comp_matches AS (
            SELECT
                m.match_id, m.comp_id, m.match_date, m.match_time_utc,
                m.home_id, m.away_id,
                m.home_goals, m.pen_home_goals, m.away_goals,
                m.home_pass_succ, m.away_pass_succ,
                m.win_team, m.loss_team,
                m.isdraw, m.pens, m.extra_time, m.isplayed,
                m.round, m.gameweek_number
            FROM matches m
            WHERE m.comp_id = 881445421
              AND (m.home_id = (SELECT team_id FROM national_team)
                   OR m.away_id = (SELECT team_id FROM national_team))
        ),
        recent_matches AS (
            SELECT
                m.match_id, m.comp_id, m.match_date, m.match_time_utc,
                m.home_id, m.away_id,
                m.home_goals, m.pen_home_goals, m.away_goals,
                m.home_pass_succ, m.away_pass_succ,
                m.win_team, m.loss_team,
                m.isdraw, m.pens, m.extra_time, m.isplayed,
                m.round, m.gameweek_number
            FROM matches m
            WHERE (m.home_id = (SELECT team_id FROM national_team)
                   OR m.away_id = (SELECT team_id FROM national_team))
              AND m.isplayed = true
            ORDER BY m.match_date DESC
            LIMIT 4
        ),
        wc_matches AS (
            SELECT * FROM comp_matches
            UNION
            SELECT * FROM recent_matches
        ),"""
    else:
        matches_section = """
        wc_matches AS (
            SELECT
                NULL::int  AS match_id,   NULL::int  AS comp_id,
                NULL::date AS match_date, NULL::text AS match_time_utc,
                NULL::int  AS home_id,    NULL::int  AS away_id,
                NULL::int  AS home_goals, NULL::int  AS pen_home_goals,
                NULL::int  AS away_goals, NULL::int  AS home_pass_succ,
                NULL::int  AS away_pass_succ,
                NULL::int  AS win_team,   NULL::int  AS loss_team,
                NULL::bool AS isdraw,     NULL::bool AS pens,
                NULL::bool AS extra_time, NULL::bool AS isplayed,
                NULL::text AS round,      NULL::int  AS gameweek_number
            WHERE false
        ),"""

    params: dict = {"season_year": season_year}
    if country_id is not None:
        params["country_id"] = country_id
    if country2_id is not None:
        params["country2_id"] = country2_id

    logger.info(
        f"nation-dist country_id={country_id}, country2_id={country2_id}, stat={stat_col}, season_year={season_year}"
    )

    query = text(f"""
        WITH
        {matches_section}

        -- All players matching nationality criteria
        country_players AS (
            SELECT DISTINCT p.player_id
            FROM players p
            WHERE {player_filter}
        ),

        -- Competitions that contain at least one match involving a national team
        national_comps AS (
            SELECT DISTINCT m.comp_id
            FROM matches m
            JOIN teams t ON (t.team_id = m.home_id OR t.team_id = m.away_id)
                        AND t.type = 'national'
        ),

        -- All match appearances (minutes > 0) in the season — used for teams_player_dist
        all_country_events AS (
            SELECT
                pms.player_id,
                pms.age                      AS player_age,
                t.team_id                    AS club_team_id,
                COALESCE(tc.name, 'Unknown') AS club_country_name
            FROM player_match_stats pms
            JOIN country_players cp ON cp.player_id      = pms.player_id
            JOIN matches m          ON m.match_id         = pms.match_id
            JOIN competitions comp  ON comp.competition_id = m.comp_id
            JOIN leagues l          ON l.league_id         = comp.league_id
            JOIN teams t            ON t.team_id           = pms.team_id
            LEFT JOIN countries tc  ON tc.country_id       = t.country_id
            WHERE comp.season_year = :season_year
              AND pms.minutes > 0
              AND m.comp_id NOT IN (SELECT comp_id FROM national_comps)
              AND LOWER(COALESCE(comp.stage, '')) NOT IN ('qualification', 'qualifiers')
        ),

        -- Stat contributions by those players in the given season,
        -- keyed by the country of the club team they played for
        stat_events AS (
            SELECT
                pms.player_id,
                pms.age                      AS player_age,
                {stat_expr}                  AS stat_value,
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
            WHERE comp.season_year = :season_year
              AND {stat_pos_filter}
              AND m.comp_id NOT IN (SELECT comp_id FROM national_comps)
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

        -- Per-team stat totals (dedup + enable ordering within country)
        team_totals AS (
            SELECT
                club_country_name,
                club_team_id, club_team_name, club_common, club_short,
                club_logo, club_level, club_type,
                club_tc_id, club_tc_name, club_tc_flag, club_tc_cont, club_tc_iso,
                SUM(stat_value) AS team_stat_total
            FROM stat_events
            GROUP BY
                club_country_name, club_team_id, club_team_name, club_common,
                club_short, club_logo, club_level, club_type,
                club_tc_id, club_tc_name, club_tc_flag, club_tc_cont, club_tc_iso
        ),
        -- Teams per country, ordered by highest stat team first
        teams_agg AS (
            SELECT
                club_country_name,
                json_agg(json_build_object(
                    'team_id',     club_team_id,
                    'team_name',   club_team_name,
                    'common_name', club_common,
                    'short_name',  club_short,
                    'logo_url',    club_logo,
                    'level',       club_level,
                    'type',        club_type,
                    'country', CASE WHEN club_tc_id IS NULL THEN NULL ELSE json_build_object(
                        'country_id', club_tc_id,
                        'name',       club_tc_name,
                        'flag_url',   club_tc_flag,
                        'continent',  club_tc_cont,
                        'iso_code_3', club_tc_iso
                    ) END
                ) ORDER BY team_stat_total DESC) AS teams_json
            FROM team_totals
            GROUP BY club_country_name
        ),

        -- Top 5 players per country by stat total
        player_country_stats AS (
            SELECT
                club_country_name,
                player_id,
                SUM(stat_value)                         AS player_stat_total,
                array_agg(DISTINCT club_team_id)::int[] AS player_team_ids
            FROM stat_events
            GROUP BY club_country_name, player_id
        ),
        top_players_agg AS (
            SELECT
                club_country_name,
                COALESCE(json_agg(json_build_object(
                    'player_id',  player_id,
                    'team_id',    player_team_ids,
                    'stat_value', player_stat_total
                ) ORDER BY player_stat_total DESC) FILTER (WHERE rn <= 5), '[]'::json) AS top_players_json
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY club_country_name ORDER BY player_stat_total DESC) AS rn
                FROM player_country_stats
            ) ranked
            GROUP BY club_country_name
        ),

        -- All players per (country, team) with avg match age
        player_team_agg AS (
            SELECT
                club_country_name,
                club_team_id,
                player_id,
                ROUND(AVG(player_age)::numeric, 1) AS avg_age
            FROM all_country_events
            GROUP BY club_country_name, club_team_id, player_id
        ),
        -- Per-player stat total at each team (for ordering and display)
        player_team_stat_totals AS (
            SELECT
                club_country_name,
                club_team_id,
                player_id,
                SUM(stat_value) AS player_stat_total
            FROM stat_events
            GROUP BY club_country_name, club_team_id, player_id
        ),
        -- Teams with their full player lists, ordered by team stat DESC
        team_player_dist_agg AS (
            SELECT
                ts.club_country_name,
                COALESCE(json_agg(json_build_object(
                    'team_id',    ts.club_team_id,
                    'stat_value', ts.team_stat_total,
                    'players',    ts.players_json
                ) ORDER BY ts.team_stat_total DESC NULLS LAST), '[]'::json) AS team_dist_json
            FROM (
                SELECT
                    pta.club_country_name,
                    pta.club_team_id,
                    COALESCE(tt.team_stat_total, 0) AS team_stat_total,
                    json_agg(json_build_object(
                        'player', json_build_object(
                            'player_id',       p.player_id,
                            'player_name',     p.player_name,
                            'age',             pta.avg_age::int,
                            'tfm_pic_url',     p.tfm_pic_url,
                            'pic_url',         p.pic_url,
                            'pixel_pic_url',   p.pixel_pic_url,
                            'position',        p.position,
                            'other_positions', COALESCE(p.other_positions, ARRAY[]::text[]),
                            'countries', json_build_object(
                                'country1', CASE WHEN spc1.country_id IS NULL THEN NULL ELSE json_build_object(
                                    'country_id', spc1.country_id, 'name', spc1.name,
                                    'flag_url', spc1.flag_url, 'continent', spc1.continent,
                                    'iso_code_3', spc1.iso_code_3) END,
                                'country2', CASE WHEN spc2.country_id IS NULL THEN NULL ELSE json_build_object(
                                    'country_id', spc2.country_id, 'name', spc2.name,
                                    'flag_url', spc2.flag_url, 'continent', spc2.continent,
                                    'iso_code_3', spc2.iso_code_3) END
                            )
                        ),
                        'stat_value', COALESCE(ptst.player_stat_total, 0)
                    ) ORDER BY COALESCE(ptst.player_stat_total, 0) DESC NULLS LAST) AS players_json
                FROM player_team_agg pta
                JOIN players p ON p.player_id = pta.player_id
                LEFT JOIN countries spc1 ON spc1.country_id = p.country_id
                LEFT JOIN countries spc2 ON spc2.country_id = p.country2_id
                LEFT JOIN player_team_stat_totals ptst
                       ON ptst.club_country_name = pta.club_country_name
                      AND ptst.club_team_id      = pta.club_team_id
                      AND ptst.player_id         = pta.player_id
                LEFT JOIN team_totals tt
                       ON tt.club_country_name = pta.club_country_name
                      AND tt.club_team_id      = pta.club_team_id
                GROUP BY pta.club_country_name, pta.club_team_id, tt.team_stat_total
            ) ts
            GROUP BY ts.club_country_name
        ),

        -- Domestic league logos per country (competitions where scope=domestic, format=league)
        league_logos_agg AS (
            SELECT
                COALESCE(tc.name, 'Unknown') AS club_country_name,
                array_agg(DISTINCT comp.logo_url) FILTER (WHERE comp.logo_url IS NOT NULL) AS league_logos
            FROM player_match_stats pms
            JOIN country_players cp ON cp.player_id       = pms.player_id
            JOIN matches m          ON m.match_id          = pms.match_id
            JOIN competitions comp  ON comp.competition_id  = m.comp_id
            JOIN leagues l          ON l.league_id          = comp.league_id
            JOIN teams t            ON t.team_id            = pms.team_id
            LEFT JOIN countries tc  ON tc.country_id        = t.country_id
            WHERE comp.season_year = :season_year
              AND m.comp_id NOT IN (SELECT comp_id FROM national_comps)
              AND LOWER(COALESCE(comp.stage, '')) NOT IN ('qualification', 'qualifiers')
              AND LOWER(COALESCE(l.scope, '')) = 'domestic'
              AND LOWER(COALESCE(l.format, '')) = 'league'
            GROUP BY COALESCE(tc.name, 'Unknown')
        ),

        -- Build the matches JSON array
        match_list AS (
            SELECT COALESCE(json_agg(json_build_object(
                'competition', json_build_object(
                    'competition_id', comp.competition_id,
                    'name',           comp.name,
                    'season_year',    comp.season_year,
                    'stage',          comp.stage,
                    'logo_url',       comp.logo_url,
                    'league', json_build_object(
                        'league_id',        l.league_id,
                        'league_name',      l.name,
                        'tier_level',       l.tier_level,
                        'format',           l.format,
                        'competiton_level', l.competition_level,
                        'scope',            l.scope,
                        'logo_url',         (SELECT c2.logo_url FROM competitions c2 WHERE c2.league_id = l.league_id ORDER BY c2.season_year DESC LIMIT 1),
                        'country', CASE WHEN lc.country_id IS NULL THEN NULL ELSE json_build_object(
                            'country_id', lc.country_id, 'name', lc.name,
                            'flag_url', lc.flag_url, 'continent', lc.continent,
                            'iso_code_3', lc.iso_code_3) END
                    )
                ),
                'match', json_build_object(
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
                    'gameweek_number', wm.gameweek_number,
                    'stadium',         NULL
                )
            ) ORDER BY wm.match_date), '[]'::json) AS matches_json
            FROM wc_matches wm
            JOIN competitions comp  ON comp.competition_id = wm.comp_id
            JOIN leagues l          ON l.league_id         = comp.league_id
            LEFT JOIN countries lc  ON lc.country_id       = l.country_id
            JOIN teams ht           ON ht.team_id           = wm.home_id
            LEFT JOIN countries htc ON htc.country_id       = ht.country_id
            JOIN teams awt          ON awt.team_id          = wm.away_id
            LEFT JOIN countries atc ON atc.country_id       = awt.country_id
        ),

        -- Build the league_dist JSON array — one entry per club country
        dist_list AS (
            SELECT COALESCE(json_agg(json_build_object(
                'country',           dt.club_country_name,
                'league_logos',      COALESCE(to_json(lla.league_logos), '[]'::json),
                'teams',             ta.teams_json,
                'total_stat_value',  dt.total_stat_value,
                'num_players',       dt.num_players,
                'top_players',       COALESCE(tpa.top_players_json, '[]'::json),
                'teams_player_dist', COALESCE(tpda.team_dist_json, '[]'::json)
            ) ORDER BY dt.total_stat_value DESC), '[]'::json) AS dist_json
            FROM dist_totals dt
            JOIN teams_agg ta ON ta.club_country_name = dt.club_country_name
            LEFT JOIN top_players_agg tpa       ON tpa.club_country_name  = dt.club_country_name
            LEFT JOIN team_player_dist_agg tpda ON tpda.club_country_name = dt.club_country_name
            LEFT JOIN league_logos_agg lla      ON lla.club_country_name  = dt.club_country_name
        )

        SELECT json_build_object(
            'data', json_build_object(
                'matches',     ml.matches_json,
                'league_dist', dl.dist_json
            )
        )
        FROM match_list ml, dist_list dl
    """)

    result = session.exec(query, params=params).first()
    return result[0] if result else {"data": {"matches": [], "league_dist": []}}


# get team data
_TEAM_MATCH_OBJ = """json_build_object(
    'match_id',        match_id,
    'match_date',      match_date::text,
    'match_time_utc',  match_time_utc::text,
    'home_team', json_build_object(
        'team_id',     ht_id,     'team_name',   ht_name,
        'common_name', ht_common, 'short_name',  ht_short,
        'logo_url',    ht_logo,   'level',       ht_level,
        'type',        ht_type,
        'country', CASE WHEN htc_id IS NULL THEN NULL ELSE json_build_object(
            'country_id', htc_id, 'name', htc_name, 'flag_url', htc_flag,
            'continent', htc_continent, 'iso_code_3', htc_iso) END
    ),
    'home_stats', json_build_object('goals', home_goals, 'penalty_goals', pen_home_goals),
    'away_team', json_build_object(
        'team_id',     at_id,     'team_name',   at_name,
        'common_name', at_common, 'short_name',  at_short,
        'logo_url',    at_logo,   'level',       at_level,
        'type',        at_type,
        'country', CASE WHEN atc_id IS NULL THEN NULL ELSE json_build_object(
            'country_id', atc_id, 'name', atc_name, 'flag_url', atc_flag,
            'continent', atc_continent, 'iso_code_3', atc_iso) END
    ),
    'away_stats', json_build_object('goals', away_goals, 'penalty_goals', pen_away_goals),
    'win_team_id',     win_team,
    'loss_team_id',    loss_team,
    'isdraw',          isdraw,
    'pens',            pens,
    'extra_time',      extra_time,
    'isplayed',        isplayed,
    'round',           round,
    'gameweek_number', gameweek_number
)"""


# team season data
@router.get("/{team_id}", response_model=TeamDataResponse)
def get_team(
    team_id: int,
    session: DBSession,
    logger: AppLoggerDep,
):
    logger.info(f"Fetching team detail for team_id: {team_id}")

    query = text(f"""
        WITH
        team_base AS (
            SELECT
                t.team_id, t.name, t.common_name, t.short_name, t.logo_url, t.level, t.type,
                t.color_hex,
                c.country_id  AS tc_id, c.name  AS tc_name, c.flag_url  AS tc_flag,
                c.continent   AS tc_continent, c.iso_code_3  AS tc_iso,
                l.league_id, l.name AS league_name, l.tier_level, l.format,
                l.competition_level, l.scope,
                (SELECT comp2.logo_url FROM competitions comp2
                 WHERE comp2.league_id = l.league_id
                 ORDER BY comp2.season_year DESC LIMIT 1) AS league_logo_url,
                lc.country_id AS lc_id, lc.name AS lc_name, lc.flag_url AS lc_flag,
                lc.continent  AS lc_continent, lc.iso_code_3 AS lc_iso,
                iu.username AS ig_handle, iu.follower_count AS ig_follower_count,
                tu.handle   AS twitter_handle, tu.followers_count AS twitter_follower_count
            FROM teams t
            LEFT JOIN countries c ON c.country_id = t.country_id
            LEFT JOIN leagues l ON l.league_id = t.league_id
            LEFT JOIN countries lc ON lc.country_id = l.country_id
            LEFT JOIN instagram_users iu ON iu.username = t.instagram_user_id
            LEFT JOIN twitter_users tu ON tu.rest_id = t.twitter_user_rest_id
            WHERE t.team_id = :team_id
        ),
        match_rows AS (
            SELECT
                m.match_id, m.match_date, m.match_time_utc,
                m.home_goals, m.away_goals, m.pen_home_goals, m.pen_away_goals,
                m.win_team, m.loss_team, m.isdraw, m.pens, m.extra_time, m.isplayed,
                m.round, m.gameweek_number,
                ht.team_id AS ht_id, ht.name AS ht_name, ht.common_name AS ht_common,
                ht.short_name AS ht_short, ht.logo_url AS ht_logo, ht.level AS ht_level, ht.type AS ht_type,
                htc.country_id AS htc_id, htc.name AS htc_name, htc.flag_url AS htc_flag,
                htc.continent AS htc_continent, htc.iso_code_3 AS htc_iso,
                awt.team_id AS at_id, awt.name AS at_name, awt.common_name AS at_common,
                awt.short_name AS at_short, awt.logo_url AS at_logo, awt.level AS at_level, awt.type AS at_type,
                atc.country_id AS atc_id, atc.name AS atc_name, atc.flag_url AS atc_flag,
                atc.continent AS atc_continent, atc.iso_code_3 AS atc_iso,
                COALESCE(m.match_time_utc, m.match_date::timestamp) AS sort_ts
            FROM matches m
            JOIN teams ht ON ht.team_id = m.home_id
            LEFT JOIN countries htc ON htc.country_id = ht.country_id
            JOIN teams awt ON awt.team_id = m.away_id
            LEFT JOIN countries atc ON atc.country_id = awt.country_id
            WHERE m.home_id = :team_id OR m.away_id = :team_id
        ),
        fixtures AS (
            SELECT * FROM match_rows
            WHERE sort_ts > NOW() AT TIME ZONE 'UTC'
            ORDER BY sort_ts ASC
            LIMIT 5
        ),
        results AS (
            SELECT * FROM match_rows
            WHERE sort_ts <= NOW() AT TIME ZONE 'UTC'
            ORDER BY sort_ts DESC
            LIMIT 5
        ),
        fixtures_agg AS (
            SELECT COALESCE(json_agg({_TEAM_MATCH_OBJ} ORDER BY sort_ts ASC), '[]'::json) AS matches_json
            FROM fixtures
        ),
        results_agg AS (
            SELECT COALESCE(json_agg({_TEAM_MATCH_OBJ} ORDER BY sort_ts DESC), '[]'::json) AS matches_json
            FROM results
        )
        SELECT json_build_object(
            'data', json_build_object(
                'team', json_build_object(
                    'team_id',     tb.team_id,
                    'team_name',   tb.name,
                    'common_name', tb.common_name,
                    'short_name',  tb.short_name,
                    'logo_url',    tb.logo_url,
                    'level',       tb.level,
                    'type',        tb.type,
                    'country', CASE WHEN tb.tc_id IS NULL THEN NULL ELSE json_build_object(
                        'country_id', tb.tc_id, 'name', tb.tc_name, 'flag_url', tb.tc_flag,
                        'continent', tb.tc_continent, 'iso_code_3', tb.tc_iso) END
                ),
                'league', CASE WHEN tb.league_id IS NULL THEN NULL ELSE json_build_object(
                    'league_id',        tb.league_id,
                    'league_name',      tb.league_name,
                    'tier_level',       tb.tier_level,
                    'format',           tb.format,
                    'competiton_level', tb.competition_level,
                    'scope',            tb.scope,
                    'logo_url',         tb.league_logo_url,
                    'country', CASE WHEN tb.lc_id IS NULL THEN NULL ELSE json_build_object(
                        'country_id', tb.lc_id, 'name', tb.lc_name, 'flag_url', tb.lc_flag,
                        'continent', tb.lc_continent, 'iso_code_3', tb.lc_iso) END
                ) END,
                'color_hex',              tb.color_hex,
                'twitter_handle',         tb.twitter_handle,
                'twitter_follower_count', tb.twitter_follower_count,
                'ig_handle',              tb.ig_handle,
                'ig_follower_count',      tb.ig_follower_count,
                'fixtures', fa.matches_json,
                'results',  ra.matches_json
            )
        )
        FROM team_base tb, fixtures_agg fa, results_agg ra
    """)

    result = session.exec(query, params={"team_id": team_id}).first()
    if not result or result[0].get("data") is None:
        raise HTTPException(status_code=404, detail="Team not found")
    return result[0]


# team season summary


# get tournament squad
@router.get("/{team_id}/squad/{league_id}", response_model=TeamResponse)
def get_comp_squad(
    team_id: int,
    league_id: int,
    session: DBSession,
    logger: AppLoggerDep,
):
    logger.info(f"Fetching squad for team_id={team_id} league_id={league_id}")
    raise HTTPException(status_code=501, detail="Not implemented")

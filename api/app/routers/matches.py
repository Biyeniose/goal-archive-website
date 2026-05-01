from datetime import date
from typing import List, Optional

from sqlalchemy import text
from fastapi import APIRouter, Query, HTTPException

from ..dependencies import DBSession, AppLoggerDep
from ..models.match import MatchesByDateResponse, MatchDetailsResponse
from ..constants import DEFAULT_LEAGUE_IDS


router = APIRouter(
    prefix="/v1/matches",
    tags=["matches"],
)


# ─── shared SQL pieces ───────────────────────────────────────────────────────

_BASE_COLS = """
    l.league_id,
    l.name              AS league_name,
    l.tier_level,
    l.format,
    l.competition_level,
    l.scope,
    (SELECT c2.logo_url FROM competitions c2 WHERE c2.league_id = l.league_id ORDER BY c2.season_year DESC LIMIT 1) AS league_logo_url,
    lc.country_id       AS lc_id,
    lc.name             AS lc_name,
    lc.flag_url         AS lc_flag,
    lc.continent        AS lc_continent,
    lc.iso_code_3       AS lc_iso,
    comp.competition_id,
    comp.name           AS comp_name,
    comp.season_year,
    comp.stage,
    comp.logo_url       AS comp_logo,
    m.match_id,
    m.match_date,
    m.match_time_utc,
    m.home_goals,
    m.pen_home_goals,
    m.away_goals,
    m.home_pass_succ,
    m.away_pass_succ,
    m.win_team          AS win_team_id,
    m.loss_team         AS loss_team_id,
    m.isdraw,
    m.pens,
    m.extra_time,
    m.isplayed,
    m.round,
    m.gameweek_number,
    ht.team_id          AS ht_id,
    ht.name             AS ht_name,
    ht.common_name      AS ht_common,
    ht.short_name       AS ht_short,
    ht.logo_url         AS ht_logo,
    ht.level            AS ht_level,
    ht.type             AS ht_type,
    htc.country_id      AS htc_id,
    htc.name            AS htc_name,
    htc.flag_url        AS htc_flag,
    htc.continent       AS htc_continent,
    htc.iso_code_3      AS htc_iso,
    awt.team_id         AS at_id,
    awt.name            AS at_name,
    awt.common_name     AS at_common,
    awt.short_name      AS at_short,
    awt.logo_url        AS at_logo,
    awt.level           AS at_level,
    awt.type            AS at_type,
    atc.country_id      AS atc_id,
    atc.name            AS atc_name,
    atc.flag_url        AS atc_flag,
    atc.continent       AS atc_continent,
    atc.iso_code_3      AS atc_iso"""

_BASE_JOINS = """
    FROM matches m
    JOIN competitions comp ON comp.competition_id = m.comp_id
    JOIN leagues l ON l.league_id = comp.league_id
    JOIN teams ht ON ht.team_id = m.home_id
    LEFT JOIN countries htc ON htc.country_id = ht.country_id
    JOIN teams awt ON awt.team_id = m.away_id
    LEFT JOIN countries atc ON atc.country_id = awt.country_id
    LEFT JOIN countries lc ON lc.country_id = l.country_id"""

_COMP_OBJ_EXPR = """json_build_object(
    'league', json_build_object(
        'league_id',        league_id,
        'league_name',      league_name,
        'tier_level',       tier_level,
        'format',           format,
        'competiton_level', competition_level,
        'scope',            scope,
        'logo_url',         league_logo_url,
        'country', CASE WHEN lc_id IS NULL THEN NULL ELSE json_build_object(
            'country_id', lc_id, 'name', lc_name,
            'flag_url', lc_flag, 'continent', lc_continent, 'iso_code_3', lc_iso
        ) END
    ),
    'competition_id', competition_id,
    'name',           comp_name,
    'season_year',    season_year,
    'stage',          stage,
    'logo_url',       comp_logo
)"""

_COMP_DATA_CTE = f"""
    comp_data AS (
        SELECT DISTINCT ON (competition_id)
            competition_id,
            {_COMP_OBJ_EXPR} AS competition_obj
        FROM base
        ORDER BY competition_id
    )"""

_MATCH_OBJ = """json_build_object(
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
    'home_stats', json_build_object(
        'goals',         home_goals,
        'penalty_goals', pen_home_goals,
        'shots',         NULL,
        'possesion',     NULL,
        'offsides',      NULL,
        'corners',       NULL,
        'xg',            NULL,
        'pass_succ',     home_pass_succ
    ),
    'away_team', json_build_object(
        'team_id',     at_id,     'team_name',   at_name,
        'common_name', at_common, 'short_name',  at_short,
        'logo_url',    at_logo,   'level',       at_level,
        'type',        at_type,
        'country', CASE WHEN atc_id IS NULL THEN NULL ELSE json_build_object(
            'country_id', atc_id, 'name', atc_name, 'flag_url', atc_flag,
            'continent', atc_continent, 'iso_code_3', atc_iso) END
    ),
    'away_stats', json_build_object(
        'goals',         away_goals,
        'penalty_goals', NULL,
        'shots',         NULL,
        'possesion',     NULL,
        'offsides',      NULL,
        'corners',       NULL,
        'xg',            NULL,
        'pass_succ',     away_pass_succ
    ),
    'win_team_id',     win_team_id,
    'loss_team_id',    loss_team_id,
    'isdraw',          isdraw,
    'pens',            pens,
    'extra_time',      extra_time,
    'isplayed',        isplayed,
    'round',           round,
    'gameweek_number', gameweek_number
)"""

_MATCH_OBJS_CTE = f"""
    match_objs AS (
        SELECT competition_id,
            json_agg({_MATCH_OBJ} ORDER BY match_time_utc NULLS LAST) AS matches
        FROM base
        GROUP BY competition_id
    )"""

_AGG_EXPR = """COALESCE(json_agg(json_build_object(
        'competition', cd.competition_obj,
        'matches',     mo.matches
    )), '[]'::json)"""

_AGG_FROM = """FROM comp_data cd
    JOIN match_objs mo ON mo.competition_id = cd.competition_id"""



# ─── bydate route ────────────────────────────────────────────────────────────

@router.get("/bydate", response_model=MatchesByDateResponse)
async def get_matches_bydate(
    session: DBSession,
    logger: AppLoggerDep,
    match_date: Optional[str] = Query(default=None, description="Date in YYYY-MM-DD format, defaults to today"),
    league_ids: List[int] = Query(default=DEFAULT_LEAGUE_IDS),
):
    if match_date is None:
        match_date = date.today().isoformat()
    logger.info(f"Fetching matches for date={match_date} league_ids={league_ids}")

    query = text(f"""
        WITH base AS (
            SELECT {_BASE_COLS}
            {_BASE_JOINS}
            WHERE m.match_date = :match_date
            AND comp.league_id = ANY(:league_ids)
        ),
        {_COMP_DATA_CTE},
        {_MATCH_OBJS_CTE}
        SELECT json_build_object('data', {_AGG_EXPR})
        {_AGG_FROM}
    """)

    result = session.exec(query, params={"match_date": match_date, "league_ids": league_ids}).first()
    return result[0] if result else {"data": []}


# ─── match data route (single query) ─────────────────────────────────────────

@router.get("/{match_id}/data", response_model=MatchDetailsResponse)
async def get_match_data(
    match_id: int,
    session: DBSession,
    logger: AppLoggerDep,
):
    logger.info(f"Fetching match data for match_id={match_id}")

    query = text(f"""
        WITH
        -- ── flags ──────────────────────────────────────────────────────────
        match_flags AS (
            SELECT m.home_id, m.away_id, m.match_date, m.comp_id,
                   (ht.type = 'national' AND awt.type = 'national') AS is_intl
            FROM matches m
            JOIN teams ht  ON ht.team_id  = m.home_id
            JOIN teams awt ON awt.team_id = m.away_id
            WHERE m.match_id = :match_id
        ),

        -- ── player stats with club team ─────────────────────────────────────
        pms_base AS (
            SELECT
                pms.player_id, pms.team_id, pms.started, pms.position, pms.number,
                pms.subbed_on, pms.subbed_off, pms.minutes,
                pms.goals, pms.assists, pms.goals_assists,
                pms.pens_made, pms.pens_att,
                pms.xg::float AS xg, pms.xg_assist::float AS xg_assist, pms.xga::float AS xga,
                pms.shots, pms.touches, pms.blocks, pms.take_ons_won, pms.tweet_mentions,
                pms.age AS match_age,
                club.club_id, club.club_name, club.club_common, club.club_short,
                club.club_logo, club.club_level, club.club_type,
                club.club_tc_id, club.club_tc_name,
                club.club_tc_flag, club.club_tc_cont, club.club_tc_iso,
                club.dom_league_id, club.dom_comp_name, club.dom_comp_logo
            FROM player_match_stats pms
            CROSS JOIN match_flags mf
            LEFT JOIN LATERAL (
                SELECT
                    t2.team_id       AS club_id,
                    t2.name          AS club_name,
                    t2.common_name   AS club_common,
                    t2.short_name    AS club_short,
                    t2.logo_url      AS club_logo,
                    t2.level         AS club_level,
                    t2.type          AS club_type,
                    tc.country_id    AS club_tc_id,
                    tc.name          AS club_tc_name,
                    tc.flag_url      AS club_tc_flag,
                    tc.continent     AS club_tc_cont,
                    tc.iso_code_3    AS club_tc_iso,
                    dc.league_id     AS dom_league_id,
                    dc.comp_name     AS dom_comp_name,
                    dc.comp_logo     AS dom_comp_logo
                FROM player_match_stats pm2
                JOIN matches m2 ON m2.match_id = pm2.match_id
                JOIN teams t2   ON t2.team_id  = pm2.team_id
                LEFT JOIN countries tc ON tc.country_id = t2.country_id
                LEFT JOIN LATERAL (
                    SELECT l3.league_id, comp3.name AS comp_name, comp3.logo_url AS comp_logo
                    FROM player_match_stats pm3
                    JOIN matches m3      ON m3.match_id      = pm3.match_id
                    JOIN competitions comp3 ON comp3.competition_id = m3.comp_id
                    JOIN leagues l3       ON l3.league_id    = comp3.league_id
                    WHERE pm3.player_id = pm2.player_id
                    AND   pm3.team_id   = pm2.team_id
                    AND   l3.format = 'league' AND l3.scope = 'domestic'
                    ORDER BY m3.match_date DESC LIMIT 1
                ) dc ON mf.is_intl
                WHERE pm2.player_id = pms.player_id
                AND   t2.type != 'national'
                ORDER BY m2.match_date DESC LIMIT 1
            ) club ON mf.is_intl
            WHERE pms.match_id = :match_id
        ),

        -- ── distribution base ────────────────────────────────────────────────
        player_dist AS (
            SELECT
                pb.team_id, pb.started,
                CASE WHEN mf.is_intl THEN pb.club_tc_id   ELSE p.country_id END AS dist_cid,
                CASE WHEN mf.is_intl THEN pb.dom_league_id ELSE NULL::int   END AS dist_lid,
                CASE WHEN mf.is_intl THEN pb.dom_comp_name ELSE NULL        END AS dist_lname,
                CASE WHEN mf.is_intl THEN pb.dom_comp_logo ELSE NULL        END AS dist_llogo
            FROM pms_base pb
            JOIN players p ON p.player_id = pb.player_id
            CROSS JOIN match_flags mf
            WHERE CASE WHEN mf.is_intl THEN pb.club_tc_id ELSE p.country_id END IS NOT NULL
        ),

        -- ── lineups ──────────────────────────────────────────────────────────
        lineup_agg AS (
            SELECT pb.team_id,
                COALESCE(json_agg(json_build_object(
                    'player', json_build_object(
                        'player_name',     p.player_name,
                        'player_id',       p.player_id,
                        'age',             pb.match_age,
                        'tfm_pic_url',     p.tfm_pic_url,
                        'pic_url',         p.pic_url,
                        'pixel_pic_url',   p.pixel_pic_url,
                        'position',        p.position,
                        'other_positions', COALESCE(p.other_positions, ARRAY[]::text[]),
                        'countries', json_build_object(
                            'country1', CASE WHEN c1.country_id IS NULL THEN NULL ELSE json_build_object(
                                'country_id', c1.country_id, 'name', c1.name,
                                'flag_url', c1.flag_url, 'continent', c1.continent,
                                'iso_code_3', c1.iso_code_3) END,
                            'country2', CASE WHEN c2.country_id IS NULL THEN NULL ELSE json_build_object(
                                'country_id', c2.country_id, 'name', c2.name,
                                'flag_url', c2.flag_url, 'continent', c2.continent,
                                'iso_code_3', c2.iso_code_3) END
                        )
                    ),
                    'team_id',       pb.team_id,
                    'position',      pb.position,
                    'number',        pb.number,
                    'started',       pb.started,
                    'subbed_on',     pb.subbed_on,
                    'subbed_off',    pb.subbed_off,
                    'minutes',       pb.minutes,
                    'goals',         pb.goals,
                    'assists',       pb.assists,
                    'goals_assists', pb.goals_assists,
                    'pens_made',     pb.pens_made,
                    'pens_att',      pb.pens_att,
                    'xg',            pb.xg,
                    'xg_assist',     pb.xg_assist,
                    'xga',           pb.xga,
                    'shots',         pb.shots,
                    'headed_shots',  NULL,
                    'touches',       pb.touches,
                    'blocks',         pb.blocks,
                    'succ_dribbles',  pb.take_ons_won,
                    'tweet_mentions', pb.tweet_mentions,
                    'current_team', CASE WHEN mf.is_intl AND pb.club_id IS NOT NULL
                        THEN json_build_object(
                            'team_id',     pb.club_id,   'team_name',   pb.club_name,
                            'common_name', pb.club_common, 'short_name', pb.club_short,
                            'logo_url',    pb.club_logo,  'level',      pb.club_level,
                            'type',        pb.club_type,
                            'country', CASE WHEN pb.club_tc_id IS NULL THEN NULL ELSE json_build_object(
                                'country_id', pb.club_tc_id,  'name',      pb.club_tc_name,
                                'flag_url',   pb.club_tc_flag, 'continent', pb.club_tc_cont,
                                'iso_code_3', pb.club_tc_iso) END)
                        ELSE NULL END
                ) ORDER BY pb.started DESC NULLS LAST, pb.minutes DESC NULLS LAST),
                '[]'::json) AS lineups
            FROM pms_base pb
            JOIN players p ON p.player_id = pb.player_id
            LEFT JOIN countries c1 ON c1.country_id = p.country_id
            LEFT JOIN countries c2 ON c2.country_id = p.country2_id
            CROSS JOIN match_flags mf
            GROUP BY pb.team_id
        ),

        -- ── x11 distribution (starters only) ────────────────────────────────
        x11_dist_agg AS (
            SELECT team_id,
                COALESCE(json_agg(json_build_object(
                    'country', json_build_object(
                        'country_id', c.country_id, 'name', c.name,
                        'flag_url', c.flag_url, 'continent', c.continent,
                        'iso_code_3', c.iso_code_3),
                    'num_players', cnt,
                    'league_id',       dist_lid,
                    'league_name',     dist_lname,
                    'league_logo_url', dist_llogo
                )), '[]'::json) AS dist
            FROM (
                SELECT team_id, dist_cid, dist_lid, dist_lname, dist_llogo, COUNT(*) AS cnt
                FROM player_dist WHERE started = true
                GROUP BY team_id, dist_cid, dist_lid, dist_lname, dist_llogo
            ) g
            JOIN countries c ON c.country_id = g.dist_cid
            GROUP BY team_id
        ),

        -- ── events ──────────────────────────────────────────────────────────
        events_agg AS (
            SELECT COALESCE(json_agg(json_build_object(
                'event_id',   e.event_id,
                'team_id',    e.team_id,
                'event_type', e.event_type,
                'body_part',  e.body_part,
                'home_goals', e.home_goals,
                'away_goals', e.away_goals,
                'minute',     e.minute,
                'add_minute', e.add_minute,
                'active_player', json_build_object(
                    'player_name',     ap.player_name,
                    'player_id',       ap.player_id,
                    'age',             ap.age,
                    'tfm_pic_url',     ap.tfm_pic_url,
                    'pic_url',         ap.pic_url,
                    'pixel_pic_url',   ap.pixel_pic_url,
                    'position',        ap.position,
                    'other_positions', COALESCE(ap.other_positions, ARRAY[]::text[]),
                    'countries', json_build_object(
                        'country1', CASE WHEN ac1.country_id IS NULL THEN NULL ELSE json_build_object(
                            'country_id', ac1.country_id, 'name', ac1.name,
                            'flag_url', ac1.flag_url, 'continent', ac1.continent,
                            'iso_code_3', ac1.iso_code_3) END,
                        'country2', CASE WHEN ac2.country_id IS NULL THEN NULL ELSE json_build_object(
                            'country_id', ac2.country_id, 'name', ac2.name,
                            'flag_url', ac2.flag_url, 'continent', ac2.continent,
                            'iso_code_3', ac2.iso_code_3) END
                    )
                ),
                'passive_player', CASE WHEN pp.player_id IS NULL THEN NULL ELSE json_build_object(
                    'player_name',     pp.player_name,
                    'player_id',       pp.player_id,
                    'age',             pp.age,
                    'tfm_pic_url',     pp.tfm_pic_url,
                    'pic_url',         pp.pic_url,
                    'pixel_pic_url',   pp.pixel_pic_url,
                    'position',        pp.position,
                    'other_positions', COALESCE(pp.other_positions, ARRAY[]::text[]),
                    'countries', json_build_object(
                        'country1', CASE WHEN pc1.country_id IS NULL THEN NULL ELSE json_build_object(
                            'country_id', pc1.country_id, 'name', pc1.name,
                            'flag_url', pc1.flag_url, 'continent', pc1.continent,
                            'iso_code_3', pc1.iso_code_3) END,
                        'country2', CASE WHEN pc2.country_id IS NULL THEN NULL ELSE json_build_object(
                            'country_id', pc2.country_id, 'name', pc2.name,
                            'flag_url', pc2.flag_url, 'continent', pc2.continent,
                            'iso_code_3', pc2.iso_code_3) END
                    )
                ) END,
                'active_notes', e.active_notes
            ) ORDER BY e.minute, e.add_minute NULLS LAST), '[]'::json) AS events
            FROM match_events e
            JOIN players ap ON ap.player_id = e.active_player_id
            LEFT JOIN countries ac1 ON ac1.country_id = ap.country_id
            LEFT JOIN countries ac2 ON ac2.country_id = ap.country2_id
            LEFT JOIN players pp ON pp.player_id = e.passive_player_id
            LEFT JOIN countries pc1 ON pc1.country_id = pp.country_id
            LEFT JOIN countries pc2 ON pc2.country_id = pp.country2_id
            WHERE e.match_id = :match_id
        ),

        -- ── h2h (last 5 between the two teams) ──────────────────────────────
        h2h_recent AS (
            SELECT m.match_id
            FROM matches m, match_flags mf
            WHERE ((m.home_id = mf.home_id AND m.away_id = mf.away_id)
                OR (m.home_id = mf.away_id AND m.away_id = mf.home_id))
            AND m.isplayed = true AND m.match_id != :match_id
            AND m.match_date < mf.match_date
            ORDER BY m.match_date DESC LIMIT 6
        ),
        h2h_base AS (
            SELECT {_BASE_COLS}
            {_BASE_JOINS}
            WHERE m.match_id IN (SELECT match_id FROM h2h_recent)
        ),
        h2h_comp_data AS (
            SELECT DISTINCT ON (competition_id)
                competition_id, {_COMP_OBJ_EXPR} AS competition_obj
            FROM h2h_base ORDER BY competition_id
        ),
        h2h_match_objs AS (
            SELECT competition_id,
                json_agg({_MATCH_OBJ} ORDER BY match_time_utc NULLS LAST) AS matches
            FROM h2h_base GROUP BY competition_id
        ),
        h2h_agg AS (
            SELECT COALESCE(json_agg(json_build_object(
                'competition', cd.competition_obj, 'matches', mo.matches
            )), '[]'::json) AS result
            FROM h2h_comp_data cd
            JOIN h2h_match_objs mo ON mo.competition_id = cd.competition_id
        ),

        -- ── home last 5 ──────────────────────────────────────────────────────
        home_last5_recent AS (
            SELECT m.match_id
            FROM matches m, match_flags mf
            WHERE (m.home_id = mf.home_id OR m.away_id = mf.home_id)
            AND m.isplayed = true AND m.match_id != :match_id
            AND m.match_date < mf.match_date
            ORDER BY m.match_date DESC LIMIT 6
        ),
        home_last5_base AS (
            SELECT {_BASE_COLS}
            {_BASE_JOINS}
            WHERE m.match_id IN (SELECT match_id FROM home_last5_recent)
        ),
        home_last5_comp_data AS (
            SELECT DISTINCT ON (competition_id)
                competition_id, {_COMP_OBJ_EXPR} AS competition_obj
            FROM home_last5_base ORDER BY competition_id
        ),
        home_last5_match_objs AS (
            SELECT competition_id,
                json_agg({_MATCH_OBJ} ORDER BY match_time_utc NULLS LAST) AS matches
            FROM home_last5_base GROUP BY competition_id
        ),
        home_last5_agg AS (
            SELECT COALESCE(json_agg(json_build_object(
                'competition', cd.competition_obj, 'matches', mo.matches
            )), '[]'::json) AS result
            FROM home_last5_comp_data cd
            JOIN home_last5_match_objs mo ON mo.competition_id = cd.competition_id
        ),

        -- ── away last 5 ──────────────────────────────────────────────────────
        away_last5_recent AS (
            SELECT m.match_id
            FROM matches m, match_flags mf
            WHERE (m.home_id = mf.away_id OR m.away_id = mf.away_id)
            AND m.isplayed = true AND m.match_id != :match_id
            AND m.match_date < mf.match_date
            ORDER BY m.match_date DESC LIMIT 6
        ),
        away_last5_base AS (
            SELECT {_BASE_COLS}
            {_BASE_JOINS}
            WHERE m.match_id IN (SELECT match_id FROM away_last5_recent)
        ),
        away_last5_comp_data AS (
            SELECT DISTINCT ON (competition_id)
                competition_id, {_COMP_OBJ_EXPR} AS competition_obj
            FROM away_last5_base ORDER BY competition_id
        ),
        away_last5_match_objs AS (
            SELECT competition_id,
                json_agg({_MATCH_OBJ} ORDER BY match_time_utc NULLS LAST) AS matches
            FROM away_last5_base GROUP BY competition_id
        ),
        away_last5_agg AS (
            SELECT COALESCE(json_agg(json_build_object(
                'competition', cd.competition_obj, 'matches', mo.matches
            )), '[]'::json) AS result
            FROM away_last5_comp_data cd
            JOIN away_last5_match_objs mo ON mo.competition_id = cd.competition_id
        ),

        -- ── league table end-of-day ──────────────────────────────────────────
        is_league_comp AS (
            SELECT 1
            FROM competitions comp, match_flags mf
            WHERE comp.competition_id = mf.comp_id
              AND comp.stage = 'league'
              AND comp.stage_order = 1
        ),
        comp_matches_eod AS (
            SELECT m.home_id, m.away_id, m.home_goals, m.away_goals
            FROM matches m, match_flags mf
            WHERE m.comp_id = mf.comp_id
              AND m.match_date <= mf.match_date
              AND m.isplayed = true
        ),
        team_match_stats_eod AS (
            SELECT home_id AS team_id, 1 AS played,
                   CASE WHEN home_goals > away_goals THEN 1 ELSE 0 END AS wins,
                   CASE WHEN home_goals = away_goals THEN 1 ELSE 0 END AS draws,
                   CASE WHEN home_goals < away_goals THEN 1 ELSE 0 END AS losses,
                   COALESCE(home_goals, 0) AS goals_for,
                   COALESCE(away_goals, 0) AS goals_against
            FROM comp_matches_eod
            UNION ALL
            SELECT away_id AS team_id, 1 AS played,
                   CASE WHEN away_goals > home_goals THEN 1 ELSE 0 END AS wins,
                   CASE WHEN away_goals = home_goals THEN 1 ELSE 0 END AS draws,
                   CASE WHEN away_goals < home_goals THEN 1 ELSE 0 END AS losses,
                   COALESCE(away_goals, 0) AS goals_for,
                   COALESCE(home_goals, 0) AS goals_against
            FROM comp_matches_eod
        ),
        team_totals_eod AS (
            SELECT
                team_id,
                SUM(played)::int                              AS gp,
                SUM(wins)::int                               AS wins,
                SUM(draws)::int                              AS draws,
                SUM(losses)::int                             AS losses,
                SUM(goals_for)::int                          AS goals_f,
                SUM(goals_against)::int                      AS goals_a,
                (SUM(goals_for) - SUM(goals_against))::int   AS gd,
                (SUM(wins) * 3 + SUM(draws))::int            AS points
            FROM team_match_stats_eod
            GROUP BY team_id
        ),
        ranked_teams_eod AS (
            SELECT
                ROW_NUMBER() OVER (ORDER BY tt.points DESC, tt.gd DESC, tt.goals_f DESC)::text AS rank,
                tt.team_id, tt.gp, tt.wins, tt.draws, tt.losses,
                tt.goals_f, tt.goals_a, tt.gd, tt.points,
                t.name AS team_name, t.common_name, t.short_name, t.logo_url, t.level, t.type,
                tc.country_id AS tc_id, tc.name AS tc_name, tc.flag_url AS tc_flag,
                tc.continent AS tc_cont, tc.iso_code_3 AS tc_iso
            FROM team_totals_eod tt
            JOIN teams t ON t.team_id = tt.team_id
            LEFT JOIN countries tc ON tc.country_id = t.country_id
        ),
        league_ranks_eod_agg AS (
            SELECT
                CASE WHEN EXISTS (SELECT 1 FROM is_league_comp)
                    THEN (
                        SELECT COALESCE(json_agg(json_build_object(
                            'rank',    rt.rank,
                            'team', json_build_object(
                                'team_id',     rt.team_id,
                                'team_name',   rt.team_name,
                                'common_name', rt.common_name,
                                'short_name',  rt.short_name,
                                'logo_url',    rt.logo_url,
                                'level',       rt.level,
                                'type',        rt.type,
                                'country', CASE WHEN rt.tc_id IS NULL THEN NULL ELSE json_build_object(
                                    'country_id', rt.tc_id,  'name',      rt.tc_name,
                                    'flag_url',   rt.tc_flag, 'continent', rt.tc_cont,
                                    'iso_code_3', rt.tc_iso) END
                            ),
                            'gp',      rt.gp,
                            'wins',    rt.wins,
                            'draws',   rt.draws,
                            'losses',  rt.losses,
                            'goals_f', rt.goals_f,
                            'goals_a', rt.goals_a,
                            'gd',      rt.gd,
                            'points',  rt.points
                        ) ORDER BY rt.rank::int), '[]'::json)
                        FROM ranked_teams_eod rt
                    )
                    ELSE NULL
                END AS rankings
        )

        -- ── final assembly ───────────────────────────────────────────────────
        SELECT json_build_object(
            'data', json_build_object(
                'match', json_build_object(
                    'match_id',        m.match_id,
                    'competition_id',  m.comp_id,
                    'match_date',      m.match_date::text,
                    'match_time_utc',  m.match_time_utc::text,
                    'win_team_id',     m.win_team,
                    'loss_team_id',    m.loss_team,
                    'isdraw',          m.isdraw,
                    'pens',            m.pens,
                    'extra_time',      m.extra_time,
                    'is_neutral',      NULL,
                    'isplayed',        m.isplayed,
                    'is_live',         NULL,
                    'match_minute',    NULL,
                    'round',           m.round,
                    'gameweek_number', m.gameweek_number,
                    'stadium_id',      m.stadium_id,
                    'stadium_name',    s.name,
                    'attendance',      m.attendance,
                    'capacity',        s.capacity,
                    'capacity_pct', CASE
                        WHEN s.capacity > 0 AND m.attendance IS NOT NULL
                        THEN ROUND((m.attendance::numeric / s.capacity * 100), 1)::float
                        ELSE NULL END,
                    'referee', CASE WHEN r.id IS NULL THEN NULL ELSE json_build_object(
                        'referee_id', r.id, 'name', r.name,
                        'country', CASE WHEN rc.country_id IS NULL THEN NULL ELSE json_build_object(
                            'country_id', rc.country_id, 'name', rc.name,
                            'flag_url', rc.flag_url, 'continent', rc.continent,
                            'iso_code_3', rc.iso_code_3) END
                    ) END,
                    'home', json_build_object(
                        'team', json_build_object(
                            'team_id', ht.team_id, 'team_name', ht.name,
                            'common_name', ht.common_name, 'short_name', ht.short_name,
                            'logo_url', ht.logo_url, 'level', ht.level, 'type', ht.type,
                            'country', CASE WHEN htc.country_id IS NULL THEN NULL ELSE json_build_object(
                                'country_id', htc.country_id, 'name', htc.name,
                                'flag_url', htc.flag_url, 'continent', htc.continent,
                                'iso_code_3', htc.iso_code_3) END
                        ),
                        'team_stats', json_build_object(
                            'goals',         m.home_goals,
                            'penalty_goals', m.pen_home_goals,
                            'shots',         m.home_shots,
                            'possesion',     m.home_poss::int,
                            'offsides',      m.home_offsides,
                            'corners',       m.home_corners,
                            'xg',            m.home_xg::int
                        ),
                        'manager', CASE WHEN hm.id IS NULL THEN NULL ELSE json_build_object(
                            'manager_id', hm.id, 'name', hm.name,
                            'country', CASE WHEN hmc.country_id IS NULL THEN NULL ELSE json_build_object(
                                'country_id', hmc.country_id, 'name', hmc.name,
                                'flag_url', hmc.flag_url, 'continent', hmc.continent,
                                'iso_code_3', hmc.iso_code_3) END
                        ) END,
                        'formation', m.home_formation,
                        'lineups',    (SELECT lineups FROM lineup_agg   WHERE team_id = ht.team_id),
                        'x11_dist',   (SELECT dist   FROM x11_dist_agg  WHERE team_id = ht.team_id)
                    ),
                    'away', json_build_object(
                        'team', json_build_object(
                            'team_id', awt.team_id, 'team_name', awt.name,
                            'common_name', awt.common_name, 'short_name', awt.short_name,
                            'logo_url', awt.logo_url, 'level', awt.level, 'type', awt.type,
                            'country', CASE WHEN atc.country_id IS NULL THEN NULL ELSE json_build_object(
                                'country_id', atc.country_id, 'name', atc.name,
                                'flag_url', atc.flag_url, 'continent', atc.continent,
                                'iso_code_3', atc.iso_code_3) END
                        ),
                        'team_stats', json_build_object(
                            'goals',         m.away_goals,
                            'penalty_goals', m.pen_away_goals,
                            'shots',         m.away_shots,
                            'possesion',     m.away_poss::int,
                            'offsides',      m.away_offsides,
                            'corners',       m.away_corners,
                            'xg',            m.away_xg::int
                        ),
                        'manager', CASE WHEN am.id IS NULL THEN NULL ELSE json_build_object(
                            'manager_id', am.id, 'name', am.name,
                            'country', CASE WHEN amc.country_id IS NULL THEN NULL ELSE json_build_object(
                                'country_id', amc.country_id, 'name', amc.name,
                                'flag_url', amc.flag_url, 'continent', amc.continent,
                                'iso_code_3', amc.iso_code_3) END
                        ) END,
                        'formation', m.away_formation,
                        'lineups',    (SELECT lineups FROM lineup_agg    WHERE team_id = awt.team_id),
                        'x11_dist',   (SELECT dist   FROM x11_dist_agg   WHERE team_id = awt.team_id)
                    )
                ),
                'events',           (SELECT events FROM events_agg),
                'h2h',              (SELECT result FROM h2h_agg),
                'home_last5',       (SELECT result FROM home_last5_agg),
                'away_last5',       (SELECT result FROM away_last5_agg),
                'league_ranks_eod', (SELECT rankings FROM league_ranks_eod_agg)
            )
        )
        FROM matches m
        JOIN teams ht ON ht.team_id = m.home_id
        LEFT JOIN countries htc ON htc.country_id = ht.country_id
        LEFT JOIN managers hm   ON hm.id  = m.home_manager_id
        LEFT JOIN countries hmc ON hmc.country_id = hm.country_id
        JOIN teams awt ON awt.team_id = m.away_id
        LEFT JOIN countries atc ON atc.country_id = awt.country_id
        LEFT JOIN managers am   ON am.id  = m.away_manager_id
        LEFT JOIN countries amc ON amc.country_id = am.country_id
        LEFT JOIN stadiums s ON s.id = m.stadium_id
        LEFT JOIN referees r ON r.id = m.ref_id
        LEFT JOIN countries rc ON rc.country_id = r.country_id
        WHERE m.match_id = :match_id
    """)

    result = session.exec(query, params={"match_id": match_id}).first()
    if not result or not result[0]:
        raise HTTPException(status_code=404, detail="Match not found")
    return result[0]



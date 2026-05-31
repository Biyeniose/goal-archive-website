from datetime import date
from typing import List, Optional

from sqlalchemy import text
from fastapi import APIRouter, Query, HTTPException

from ..dependencies import DBSession, AppLoggerDep
from ..models.match import MatchesByDateResponse, MatchDetailsResponse, KalshiForecastResponse, PolymForecastResponse
from ..constants import DEFAULT_LEAGUE_IDS, TEAM_NAME_ALIASES


def _alias_or_clauses(market_col: str, team_name_col: str) -> str:
    """Return extra SQL OR clauses for known team name aliases, ready to append after existing ILIKE conditions."""
    parts = []
    for team_name, aliases in TEAM_NAME_ALIASES.items():
        safe_name = team_name.replace("'", "''")
        for alias in aliases:
            safe_alias = alias.replace("'", "''")
            parts.append(
                f"OR ({team_name_col} = '{safe_name}' AND {market_col} ILIKE '%{safe_alias}%')"
            )
    return ("\n                              " + "\n                              ".join(parts)) if parts else ""

_K_HT = _alias_or_clauses("km.name", "ht.name")
_K_AT = _alias_or_clauses("km.name", "awt.name")
_P_HT = _alias_or_clauses("pm.question", "ht.name")
_P_AT = _alias_or_clauses("pm.question", "awt.name")
_P_GIT_HT = _alias_or_clauses("pm.group_item_title", "ht.name")
_P_GIT_AT = _alias_or_clauses("pm.group_item_title", "awt.name")
# endpoint variants: team names come from match_info CTE (mi.home_name / mi.away_name)
_K_MI_HT = _alias_or_clauses("km.name", "mi.home_name")
_K_MI_AT = _alias_or_clauses("km.name", "mi.away_name")
_P_MI_HT = _alias_or_clauses("pm.question", "mi.home_name")
_P_MI_AT = _alias_or_clauses("pm.question", "mi.away_name")
_P_GIT_MI_HT = _alias_or_clauses("pm.group_item_title", "mi.home_name")
_P_GIT_MI_AT = _alias_or_clauses("pm.group_item_title", "mi.away_name")
# endpoint variants for deduction subqueries (pm2, km2)
_K2_MI_HT = _alias_or_clauses("km2.name", "mi.home_name")
_K2_MI_AT = _alias_or_clauses("km2.name", "mi.away_name")
_P2_MI_HT = _alias_or_clauses("pm2.question", "mi.home_name")
_P2_MI_AT = _alias_or_clauses("pm2.question", "mi.away_name")
_P2_GIT_MI_HT = _alias_or_clauses("pm2.group_item_title", "mi.home_name")
_P2_GIT_MI_AT = _alias_or_clauses("pm2.group_item_title", "mi.away_name")
# _BASE_JOINS LATERAL variants: team names come from ht.name / awt.name
_K2_HT = _alias_or_clauses("km2.name", "ht.name")
_K2_AT = _alias_or_clauses("km2.name", "awt.name")
_P2_HT = _alias_or_clauses("pm2.question", "ht.name")
_P2_AT = _alias_or_clauses("pm2.question", "awt.name")
_P2_GIT_HT = _alias_or_clauses("pm2.group_item_title", "ht.name")
_P2_GIT_AT = _alias_or_clauses("pm2.group_item_title", "awt.name")


router = APIRouter(
    prefix="/v1/matches",
    tags=["matches"],
)


# ─── shared SQL pieces ───────────────────────────────────────────────────────

_BASE_COLS = f"""
    l.league_id,
    l.name              AS league_name,
    l.tier_level,
    l.format,
    l.competition_level,
    l.scope,
    comp.logo_url AS league_logo_url,
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
    m.pen_away_goals,
    m.home_shots,
    m.away_shots,
    m.home_poss,
    m.away_poss,
    m.home_offsides,
    m.away_offsides,
    m.home_corners,
    m.away_corners,
    m.home_xg,
    m.away_xg,
    m.home_pass_att,
    m.away_pass_att,
    m.home_pass_succ,
    m.away_pass_succ,
    m.home_ranking,
    m.away_ranking,
    m.win_team          AS win_team_id,
    m.loss_team         AS loss_team_id,
    m.isdraw,
    m.pens,
    m.extra_time,
    m.isplayed,
    m.round,
    m.gameweek_number,
    m.stadium_id,
    s.name              AS stadium_name,
    m.attendance,
    s.capacity          AS s_capacity,
    s.address           AS stadium_address,
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
    atc.iso_code_3      AS atc_iso,
    COALESCE(
        m.home_color,
        (SELECT m2.home_color FROM matches m2
         WHERE m2.home_id = m.home_id
           AND m2.match_id != m.match_id
           AND m2.match_date <= m.match_date
           AND m2.match_date >= m.match_date - INTERVAL '2 months'
           AND m2.home_color IS NOT NULL
         ORDER BY m2.match_date DESC LIMIT 1)
    ) AS home_color,
    COALESCE(
        m.away_color,
        (SELECT m2.away_color FROM matches m2
         WHERE m2.away_id = m.away_id
           AND m2.match_id != m.match_id
           AND m2.match_date <= m.match_date
           AND m2.match_date >= m.match_date - INTERVAL '2 months'
           AND m2.away_color IS NOT NULL
         ORDER BY m2.match_date DESC LIMIT 1)
    ) AS away_color,
    -- kalshi latest prediction (null if match hasn't started yet)
    CASE WHEN m.match_time_utc IS NULL OR m.match_time_utc > NOW() AT TIME ZONE 'UTC'
              OR (kpred.w_prob_lat IS NULL AND kpred.d_prob_lat IS NULL)
         THEN NULL
    ELSE json_build_object(
        'event_ticker', kpred.event_ticker,
        'winner_id', kpred.w_id_lat, 'loser_id', kpred.l_id_lat,
        'is_draw', COALESCE(kpred.d_prob_lat,0) > COALESCE(kpred.w_prob_lat,0),
        'winner_stats', CASE WHEN kpred.w_ticker_lat IS NULL OR kpred.w_prob_lat IS NULL THEN NULL ELSE json_build_object(
            'market_ticker', kpred.w_ticker_lat, 'probability', kpred.w_prob_lat,
            'volume', kpred.w_vol, 'dollar_volume', kpred.w_dvol,
            'open_interest', kpred.w_oi, 'open_interest_dollar', kpred.w_oi_dollar
        ) END,
        'winner_prematch_stats', NULL, 'loser_prematch_stats', NULL, 'draw_prematch_stats', NULL,
        'loser_stats', CASE WHEN kpred.l_ticker_lat IS NULL OR kpred.l_prob_lat IS NULL THEN NULL ELSE json_build_object(
            'market_ticker', kpred.l_ticker_lat, 'probability', kpred.l_prob_lat,
            'volume', kpred.l_vol, 'dollar_volume', kpred.l_dvol,
            'open_interest', kpred.l_oi, 'open_interest_dollar', kpred.l_oi_dollar
        ) END,
        'draw_stats', CASE WHEN kpred.d_ticker IS NULL OR kpred.d_prob_lat IS NULL THEN NULL ELSE json_build_object(
            'market_ticker', kpred.d_ticker, 'probability', kpred.d_prob_lat,
            'volume', kpred.d_vol, 'dollar_volume', kpred.d_dvol,
            'open_interest', kpred.d_oi, 'open_interest_dollar', kpred.d_oi_dollar
        ) END,
        'time_saved_utc', kpred.time_saved,
        'total_volume', NULLIF(kpred.total_dvol_lat, 0),
        'is_correct', CASE
            WHEN m.isplayed = true THEN
                CASE
                    WHEN COALESCE(kpred.d_prob_lat,0) > COALESCE(kpred.w_prob_lat,0) AND m.isdraw = true THEN true
                    WHEN COALESCE(kpred.d_prob_lat,0) <= COALESCE(kpred.w_prob_lat,0) AND kpred.w_id_lat IS NOT NULL
                         AND kpred.w_id_lat = m.win_team THEN true
                    WHEN COALESCE(kpred.d_prob_lat,0) <= COALESCE(kpred.w_prob_lat,0) AND kpred.w_id_lat IS NULL THEN NULL
                    ELSE false
                END
            WHEN m.home_goals IS NOT NULL AND m.away_goals IS NOT NULL THEN
                CASE
                    WHEN COALESCE(kpred.d_prob_lat,0) > COALESCE(kpred.w_prob_lat,0) AND m.home_goals = m.away_goals THEN true
                    WHEN COALESCE(kpred.d_prob_lat,0) <= COALESCE(kpred.w_prob_lat,0) AND kpred.w_id_lat IS NOT NULL
                         AND m.home_goals > m.away_goals AND kpred.w_id_lat = m.home_id THEN true
                    WHEN COALESCE(kpred.d_prob_lat,0) <= COALESCE(kpred.w_prob_lat,0) AND kpred.w_id_lat IS NOT NULL
                         AND m.away_goals > m.home_goals AND kpred.w_id_lat = m.away_id THEN true
                    WHEN COALESCE(kpred.d_prob_lat,0) <= COALESCE(kpred.w_prob_lat,0) AND kpred.w_id_lat IS NULL THEN NULL
                    ELSE false
                END
            ELSE NULL
        END
    ) END AS kalshi_pred_json,
    -- kalshi prematch prediction (closest forecast before kick-off; no volume stats)
    CASE WHEN kpred.w_prob_pre IS NULL AND kpred.d_prob_pre IS NULL THEN NULL
    ELSE json_build_object(
        'event_ticker', kpred.event_ticker,
        'winner_id', kpred.w_id_pre, 'loser_id', kpred.l_id_pre,
        'is_draw', COALESCE(kpred.d_prob_pre,0) > COALESCE(kpred.w_prob_pre,0),
        'winner_stats', CASE WHEN kpred.w_ticker_pre IS NULL OR kpred.w_prob_pre IS NULL THEN NULL ELSE json_build_object(
            'market_ticker', kpred.w_ticker_pre, 'probability', kpred.w_prob_pre,
            'volume', NULL, 'dollar_volume', NULL, 'open_interest', NULL, 'open_interest_dollar', NULL
        ) END,
        'winner_prematch_stats', NULL, 'loser_prematch_stats', NULL, 'draw_prematch_stats', NULL,
        'loser_stats', CASE WHEN kpred.l_ticker_pre IS NULL OR kpred.l_prob_pre IS NULL THEN NULL ELSE json_build_object(
            'market_ticker', kpred.l_ticker_pre, 'probability', kpred.l_prob_pre,
            'volume', NULL, 'dollar_volume', NULL, 'open_interest', NULL, 'open_interest_dollar', NULL
        ) END,
        'draw_stats', CASE WHEN kpred.d_ticker IS NULL OR kpred.d_prob_pre IS NULL THEN NULL ELSE json_build_object(
            'market_ticker', kpred.d_ticker, 'probability', kpred.d_prob_pre,
            'volume', NULL, 'dollar_volume', NULL, 'open_interest', NULL, 'open_interest_dollar', NULL
        ) END,
        'time_saved_utc', kpred.time_saved,
        'total_volume', NULLIF(kpred.total_dvol_pre, 0),
        'is_correct', CASE
            WHEN m.isplayed != true THEN NULL
            WHEN COALESCE(kpred.d_prob_pre,0) > COALESCE(kpred.w_prob_pre,0) AND m.isdraw = true THEN true
            WHEN COALESCE(kpred.d_prob_pre,0) <= COALESCE(kpred.w_prob_pre,0) AND kpred.w_id_pre IS NOT NULL
                 AND kpred.w_id_pre = m.win_team THEN true
            WHEN COALESCE(kpred.d_prob_pre,0) <= COALESCE(kpred.w_prob_pre,0) AND kpred.w_id_pre IS NULL THEN NULL
            ELSE false
        END
    ) END AS kalshi_prematch_pred_json,
    -- polymarket latest prediction (null if match hasn't started yet)
    CASE WHEN m.match_time_utc IS NULL OR m.match_time_utc > NOW() AT TIME ZONE 'UTC'
              OR (ppred.w_prob_lat IS NULL AND ppred.d_prob_lat IS NULL)
         THEN NULL
    ELSE json_build_object(
        'event_ticker', ppred.event_ticker,
        'winner_id', ppred.w_id_lat, 'loser_id', ppred.l_id_lat,
        'is_draw', COALESCE(ppred.d_prob_lat,0) > COALESCE(ppred.w_prob_lat,0),
        'winner_stats', CASE WHEN ppred.w_ticker_lat IS NULL OR ppred.w_prob_lat IS NULL THEN NULL ELSE json_build_object(
            'market_ticker', ppred.w_ticker_lat, 'probability', ppred.w_prob_lat,
            'volume', ppred.w_vol, 'dollar_volume', NULL,
            'open_interest', ppred.w_oi, 'open_interest_dollar', NULL
        ) END,
        'winner_prematch_stats', NULL, 'loser_prematch_stats', NULL, 'draw_prematch_stats', NULL,
        'loser_stats', CASE WHEN ppred.l_ticker_lat IS NULL OR ppred.l_prob_lat IS NULL THEN NULL ELSE json_build_object(
            'market_ticker', ppred.l_ticker_lat, 'probability', ppred.l_prob_lat,
            'volume', ppred.l_vol, 'dollar_volume', NULL,
            'open_interest', ppred.l_oi, 'open_interest_dollar', NULL
        ) END,
        'draw_stats', CASE WHEN ppred.d_ticker IS NULL OR ppred.d_prob_lat IS NULL THEN NULL ELSE json_build_object(
            'market_ticker', ppred.d_ticker, 'probability', ppred.d_prob_lat,
            'volume', ppred.d_vol, 'dollar_volume', NULL,
            'open_interest', ppred.d_oi, 'open_interest_dollar', NULL
        ) END,
        'time_saved_utc', ppred.time_saved,
        'total_volume', NULLIF(ppred.total_vol_lat, 0),
        'is_correct', CASE
            WHEN m.isplayed = true THEN
                CASE
                    WHEN COALESCE(ppred.d_prob_lat,0) > COALESCE(ppred.w_prob_lat,0) AND m.isdraw = true THEN true
                    WHEN COALESCE(ppred.d_prob_lat,0) <= COALESCE(ppred.w_prob_lat,0) AND ppred.w_id_lat IS NOT NULL
                         AND ppred.w_id_lat = m.win_team THEN true
                    WHEN COALESCE(ppred.d_prob_lat,0) <= COALESCE(ppred.w_prob_lat,0) AND ppred.w_id_lat IS NULL THEN NULL
                    ELSE false
                END
            WHEN m.home_goals IS NOT NULL AND m.away_goals IS NOT NULL THEN
                CASE
                    WHEN COALESCE(ppred.d_prob_lat,0) > COALESCE(ppred.w_prob_lat,0) AND m.home_goals = m.away_goals THEN true
                    WHEN COALESCE(ppred.d_prob_lat,0) <= COALESCE(ppred.w_prob_lat,0) AND ppred.w_id_lat IS NOT NULL
                         AND m.home_goals > m.away_goals AND ppred.w_id_lat = m.home_id THEN true
                    WHEN COALESCE(ppred.d_prob_lat,0) <= COALESCE(ppred.w_prob_lat,0) AND ppred.w_id_lat IS NOT NULL
                         AND m.away_goals > m.home_goals AND ppred.w_id_lat = m.away_id THEN true
                    WHEN COALESCE(ppred.d_prob_lat,0) <= COALESCE(ppred.w_prob_lat,0) AND ppred.w_id_lat IS NULL THEN NULL
                    ELSE false
                END
            ELSE NULL
        END
    ) END AS polym_pred_json,
    -- polymarket prematch prediction (closest forecast before kick-off; no volume stats)
    CASE WHEN ppred.w_prob_pre IS NULL AND ppred.d_prob_pre IS NULL THEN NULL
    ELSE json_build_object(
        'event_ticker', ppred.event_ticker,
        'winner_id', ppred.w_id_pre, 'loser_id', ppred.l_id_pre,
        'is_draw', COALESCE(ppred.d_prob_pre,0) > COALESCE(ppred.w_prob_pre,0),
        'winner_stats', CASE WHEN ppred.w_ticker_pre IS NULL OR ppred.w_prob_pre IS NULL THEN NULL ELSE json_build_object(
            'market_ticker', ppred.w_ticker_pre, 'probability', ppred.w_prob_pre,
            'volume', NULL, 'dollar_volume', NULL, 'open_interest', NULL, 'open_interest_dollar', NULL
        ) END,
        'winner_prematch_stats', NULL, 'loser_prematch_stats', NULL, 'draw_prematch_stats', NULL,
        'loser_stats', CASE WHEN ppred.l_ticker_pre IS NULL OR ppred.l_prob_pre IS NULL THEN NULL ELSE json_build_object(
            'market_ticker', ppred.l_ticker_pre, 'probability', ppred.l_prob_pre,
            'volume', NULL, 'dollar_volume', NULL, 'open_interest', NULL, 'open_interest_dollar', NULL
        ) END,
        'draw_stats', CASE WHEN ppred.d_ticker IS NULL OR ppred.d_prob_pre IS NULL THEN NULL ELSE json_build_object(
            'market_ticker', ppred.d_ticker, 'probability', ppred.d_prob_pre,
            'volume', NULL, 'dollar_volume', NULL, 'open_interest', NULL, 'open_interest_dollar', NULL
        ) END,
        'time_saved_utc', ppred.time_saved,
        'total_volume', NULLIF(ppred.total_vol_pre, 0),
        'is_correct', CASE
            WHEN m.isplayed != true THEN NULL
            WHEN COALESCE(ppred.d_prob_pre,0) > COALESCE(ppred.w_prob_pre,0) AND m.isdraw = true THEN true
            WHEN COALESCE(ppred.d_prob_pre,0) <= COALESCE(ppred.w_prob_pre,0) AND ppred.w_id_pre IS NOT NULL
                 AND ppred.w_id_pre = m.win_team THEN true
            WHEN COALESCE(ppred.d_prob_pre,0) <= COALESCE(ppred.w_prob_pre,0) AND ppred.w_id_pre IS NULL THEN NULL
            ELSE false
        END
    ) END AS polym_prematch_pred_json"""

_BASE_JOINS = f"""
    FROM matches m
    JOIN competitions comp ON comp.competition_id = m.comp_id
    JOIN leagues l ON l.league_id = comp.league_id
    JOIN teams ht ON ht.team_id = m.home_id
    LEFT JOIN countries htc ON htc.country_id = ht.country_id
    JOIN teams awt ON awt.team_id = m.away_id
    LEFT JOIN countries atc ON atc.country_id = awt.country_id
    LEFT JOIN countries lc ON lc.country_id = l.country_id
    LEFT JOIN stadiums s ON s.id = m.stadium_id
    LEFT JOIN LATERAL (
        SELECT
            MAX(ranked_k.event_ticker)                                                                    AS event_ticker,
            MAX(CASE WHEN NOT ranked_k.is_tie AND ranked_k.rn_lat=1 THEN ranked_k.team_id END)            AS w_id_lat,
            MAX(CASE WHEN NOT ranked_k.is_tie AND ranked_k.rn_lat=2 THEN ranked_k.team_id END)            AS l_id_lat,
            MAX(CASE WHEN NOT ranked_k.is_tie AND ranked_k.rn_lat=1 THEN ranked_k.ticker END)             AS w_ticker_lat,
            MAX(CASE WHEN NOT ranked_k.is_tie AND ranked_k.rn_lat=2 THEN ranked_k.ticker END)             AS l_ticker_lat,
            MAX(CASE WHEN NOT ranked_k.is_tie AND ranked_k.rn_lat=1 THEN ranked_k.latest_prob END)        AS w_prob_lat,
            MAX(CASE WHEN NOT ranked_k.is_tie AND ranked_k.rn_lat=2 THEN ranked_k.latest_prob END)        AS l_prob_lat,
            MAX(CASE WHEN NOT ranked_k.is_tie AND ranked_k.rn_lat=1 THEN ranked_k.volume END)             AS w_vol,
            MAX(CASE WHEN NOT ranked_k.is_tie AND ranked_k.rn_lat=1 THEN ranked_k.dollar_volume END)      AS w_dvol,
            MAX(CASE WHEN NOT ranked_k.is_tie AND ranked_k.rn_lat=1 THEN ranked_k.open_interest END)      AS w_oi,
            MAX(CASE WHEN NOT ranked_k.is_tie AND ranked_k.rn_lat=1 THEN ranked_k.dollar_oi END)          AS w_oi_dollar,
            MAX(CASE WHEN NOT ranked_k.is_tie AND ranked_k.rn_lat=2 THEN ranked_k.volume END)             AS l_vol,
            MAX(CASE WHEN NOT ranked_k.is_tie AND ranked_k.rn_lat=2 THEN ranked_k.dollar_volume END)      AS l_dvol,
            MAX(CASE WHEN NOT ranked_k.is_tie AND ranked_k.rn_lat=2 THEN ranked_k.open_interest END)      AS l_oi,
            MAX(CASE WHEN NOT ranked_k.is_tie AND ranked_k.rn_lat=2 THEN ranked_k.dollar_oi END)          AS l_oi_dollar,
            MAX(CASE WHEN ranked_k.is_tie THEN ranked_k.ticker END)                                        AS d_ticker,
            MAX(CASE WHEN ranked_k.is_tie THEN ranked_k.latest_prob END)                                   AS d_prob_lat,
            MAX(CASE WHEN ranked_k.is_tie THEN ranked_k.volume END)                                        AS d_vol,
            MAX(CASE WHEN ranked_k.is_tie THEN ranked_k.dollar_volume END)                                 AS d_dvol,
            MAX(CASE WHEN ranked_k.is_tie THEN ranked_k.open_interest END)                                 AS d_oi,
            MAX(CASE WHEN ranked_k.is_tie THEN ranked_k.dollar_oi END)                                     AS d_oi_dollar,
            MAX(CASE WHEN NOT ranked_k.is_tie AND ranked_k.rn_pre=1 THEN ranked_k.team_id END)            AS w_id_pre,
            MAX(CASE WHEN NOT ranked_k.is_tie AND ranked_k.rn_pre=2 THEN ranked_k.team_id END)            AS l_id_pre,
            MAX(CASE WHEN NOT ranked_k.is_tie AND ranked_k.rn_pre=1 THEN ranked_k.ticker END)             AS w_ticker_pre,
            MAX(CASE WHEN NOT ranked_k.is_tie AND ranked_k.rn_pre=2 THEN ranked_k.ticker END)             AS l_ticker_pre,
            MAX(CASE WHEN NOT ranked_k.is_tie AND ranked_k.rn_pre=1 THEN ranked_k.pre_prob END)           AS w_prob_pre,
            MAX(CASE WHEN NOT ranked_k.is_tie AND ranked_k.rn_pre=2 THEN ranked_k.pre_prob END)           AS l_prob_pre,
            MAX(CASE WHEN ranked_k.is_tie THEN ranked_k.pre_prob END)                                      AS d_prob_pre,
            to_char(to_timestamp(AVG(EXTRACT(EPOCH FROM ranked_k.updated_time))) AT TIME ZONE 'UTC',
                    'YYYY-MM-DD HH24:MI:SS') || '+00'                                                      AS time_saved,
            (SELECT COALESCE(SUM(kv.dvol), 0)
             FROM (
                 SELECT DISTINCT ON (kms.market_ticker) kms.dollar_volume AS dvol
                 FROM kalshi_market_snapshots kms
                 JOIN kalshi_markets km2 ON km2.ticker = kms.market_ticker
                 JOIN kalshi_events ke2 ON ke2.event_ticker = km2.event_ticker
                 WHERE ke2.match_id = m.match_id
                   AND kms.snapshotted_at <= COALESCE(m.match_end_time_utc, m.match_time_utc + INTERVAL '3 hours')
                 ORDER BY kms.market_ticker, kms.snapshotted_at DESC
             ) kv) AS total_dvol_lat,
            (SELECT COALESCE(SUM(kv.dvol), 0)
             FROM (
                 SELECT DISTINCT ON (kms.market_ticker) kms.dollar_volume AS dvol
                 FROM kalshi_market_snapshots kms
                 JOIN kalshi_markets km2 ON km2.ticker = kms.market_ticker
                 JOIN kalshi_events ke2 ON ke2.event_ticker = km2.event_ticker
                 WHERE ke2.match_id = m.match_id
                   AND kms.snapshotted_at <= m.match_time_utc
                 ORDER BY kms.market_ticker, kms.snapshotted_at DESC
             ) kv) AS total_dvol_pre
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY is_tie ORDER BY latest_prob DESC NULLS LAST) AS rn_lat,
                   ROW_NUMBER() OVER (PARTITION BY is_tie ORDER BY pre_prob    DESC NULLS LAST) AS rn_pre
            FROM (
                SELECT ke.event_ticker,
                       km.ticker,
                       (km.ticker ILIKE '%-TIE') AS is_tie,
                       COALESCE(
                           km.team_id,
                           CASE WHEN km.name ILIKE '%' || ht.common_name || '%'
                                 OR km.name ILIKE '%' || ht.name || '%'{_K_HT} THEN ht.team_id END,
                           CASE WHEN km.name ILIKE '%' || awt.common_name || '%'
                                 OR km.name ILIKE '%' || awt.name || '%'{_K_AT} THEN awt.team_id END,
                           CASE WHEN NOT (km.ticker ILIKE '%-TIE') AND EXISTS (
                               SELECT 1 FROM kalshi_markets km2
                               WHERE km2.event_ticker = km.event_ticker AND km2.ticker != km.ticker
                                 AND NOT (km2.ticker ILIKE '%-TIE')
                                 AND (km2.name ILIKE '%' || ht.common_name || '%'
                                   OR km2.name ILIKE '%' || ht.name || '%'{_K2_HT})
                           ) THEN awt.team_id END,
                           CASE WHEN NOT (km.ticker ILIKE '%-TIE') AND EXISTS (
                               SELECT 1 FROM kalshi_markets km2
                               WHERE km2.event_ticker = km.event_ticker AND km2.ticker != km.ticker
                                 AND NOT (km2.ticker ILIKE '%-TIE')
                                 AND (km2.name ILIKE '%' || awt.common_name || '%'
                                   OR km2.name ILIKE '%' || awt.name || '%'{_K2_AT})
                           ) THEN ht.team_id END
                       ) AS team_id,
                       km.volume, km.dollar_volume, km.open_interest, km.dollar_open_interest AS dollar_oi,
                       km.updated_time,
                       COALESCE(
                           NULLIF((km.yes_bid_dollars + km.yes_ask_dollars) / 2.0, 0),
                           (SELECT kfh.raw_numerical_forecast FROM kalshi_forecast_history kfh
                            WHERE kfh.market_ticker = km.ticker ORDER BY kfh.end_period_ts DESC LIMIT 1),
                           (SELECT NULLIF((kms.yes_bid_dollars + kms.yes_ask_dollars) / 2.0, 0)
                            FROM kalshi_market_snapshots kms
                            WHERE kms.market_ticker = km.ticker
                            ORDER BY kms.snapshotted_at DESC LIMIT 1)
                       ) AS latest_prob,
                       COALESCE(
                           (SELECT kfh.raw_numerical_forecast FROM kalshi_forecast_history kfh
                            WHERE kfh.market_ticker = km.ticker
                              AND kfh.end_period_ts <= m.match_time_utc
                            ORDER BY kfh.end_period_ts DESC LIMIT 1),
                           (SELECT NULLIF((kms.yes_bid_dollars + kms.yes_ask_dollars) / 2.0, 0)
                            FROM kalshi_market_snapshots kms
                            WHERE kms.market_ticker = km.ticker
                              AND kms.snapshotted_at <= m.match_time_utc
                            ORDER BY kms.snapshotted_at DESC LIMIT 1)
                       ) AS pre_prob
                FROM kalshi_events ke
                JOIN kalshi_markets km ON km.event_ticker = ke.event_ticker
                WHERE ke.match_id = m.match_id
            ) raw_k
        ) ranked_k
    ) kpred ON true
    LEFT JOIN LATERAL (
        SELECT
            MAX(ranked_p.event_ticker)                                                                       AS event_ticker,
            MAX(CASE WHEN NOT ranked_p.is_draw_mkt AND ranked_p.rn_lat=1 THEN ranked_p.team_id END)         AS w_id_lat,
            MAX(CASE WHEN NOT ranked_p.is_draw_mkt AND ranked_p.rn_lat=2 THEN ranked_p.team_id END)         AS l_id_lat,
            MAX(CASE WHEN NOT ranked_p.is_draw_mkt AND ranked_p.rn_lat=1 THEN ranked_p.ticker END)          AS w_ticker_lat,
            MAX(CASE WHEN NOT ranked_p.is_draw_mkt AND ranked_p.rn_lat=2 THEN ranked_p.ticker END)          AS l_ticker_lat,
            MAX(CASE WHEN NOT ranked_p.is_draw_mkt AND ranked_p.rn_lat=1 THEN ranked_p.latest_prob END)     AS w_prob_lat,
            MAX(CASE WHEN NOT ranked_p.is_draw_mkt AND ranked_p.rn_lat=2 THEN ranked_p.latest_prob END)     AS l_prob_lat,
            MAX(CASE WHEN NOT ranked_p.is_draw_mkt AND ranked_p.rn_lat=1 THEN ranked_p.volume END)          AS w_vol,
            MAX(CASE WHEN NOT ranked_p.is_draw_mkt AND ranked_p.rn_lat=2 THEN ranked_p.volume END)          AS l_vol,
            MAX(CASE WHEN NOT ranked_p.is_draw_mkt AND ranked_p.rn_lat=1 THEN ranked_p.open_interest END)   AS w_oi,
            MAX(CASE WHEN NOT ranked_p.is_draw_mkt AND ranked_p.rn_lat=2 THEN ranked_p.open_interest END)   AS l_oi,
            MAX(CASE WHEN ranked_p.is_draw_mkt THEN ranked_p.ticker END)                                     AS d_ticker,
            MAX(CASE WHEN ranked_p.is_draw_mkt THEN ranked_p.latest_prob END)                                AS d_prob_lat,
            MAX(CASE WHEN ranked_p.is_draw_mkt THEN ranked_p.volume END)                                     AS d_vol,
            MAX(CASE WHEN ranked_p.is_draw_mkt THEN ranked_p.open_interest END)                              AS d_oi,
            MAX(CASE WHEN NOT ranked_p.is_draw_mkt AND ranked_p.rn_pre=1 THEN ranked_p.team_id END)         AS w_id_pre,
            MAX(CASE WHEN NOT ranked_p.is_draw_mkt AND ranked_p.rn_pre=2 THEN ranked_p.team_id END)         AS l_id_pre,
            MAX(CASE WHEN NOT ranked_p.is_draw_mkt AND ranked_p.rn_pre=1 THEN ranked_p.ticker END)          AS w_ticker_pre,
            MAX(CASE WHEN NOT ranked_p.is_draw_mkt AND ranked_p.rn_pre=2 THEN ranked_p.ticker END)          AS l_ticker_pre,
            MAX(CASE WHEN NOT ranked_p.is_draw_mkt AND ranked_p.rn_pre=1 THEN ranked_p.pre_prob END)        AS w_prob_pre,
            MAX(CASE WHEN NOT ranked_p.is_draw_mkt AND ranked_p.rn_pre=2 THEN ranked_p.pre_prob END)        AS l_prob_pre,
            MAX(CASE WHEN ranked_p.is_draw_mkt THEN ranked_p.pre_prob END)                                   AS d_prob_pre,
            to_char(to_timestamp(AVG(EXTRACT(EPOCH FROM ranked_p.updated_at))) AT TIME ZONE 'UTC',
                    'YYYY-MM-DD HH24:MI:SS') || '+00'                                                        AS time_saved,
            (SELECT COALESCE(SUM(pv.vol), 0)
             FROM (
                 SELECT DISTINCT ON (pms.market_id) pms.volume AS vol
                 FROM polymarket_market_snapshots pms
                 JOIN polymarket_markets pm2 ON pm2.id = pms.market_id
                 JOIN polymarket_events pe2 ON pe2.id = pm2.event_id
                 WHERE pe2.match_id = m.match_id
                   AND pms.snapshotted_at <= COALESCE(m.match_end_time_utc, m.match_time_utc + INTERVAL '3 hours')
                 ORDER BY pms.market_id, pms.snapshotted_at DESC
             ) pv) AS total_vol_lat,
            (SELECT COALESCE(SUM(pv.vol), 0)
             FROM (
                 SELECT DISTINCT ON (pms.market_id) pms.volume AS vol
                 FROM polymarket_market_snapshots pms
                 JOIN polymarket_markets pm2 ON pm2.id = pms.market_id
                 JOIN polymarket_events pe2 ON pe2.id = pm2.event_id
                 WHERE pe2.match_id = m.match_id
                   AND pms.snapshotted_at <= m.match_time_utc
                 ORDER BY pms.market_id, pms.snapshotted_at DESC
             ) pv) AS total_vol_pre
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY is_draw_mkt ORDER BY latest_prob DESC NULLS LAST) AS rn_lat,
                   ROW_NUMBER() OVER (PARTITION BY is_draw_mkt ORDER BY pre_prob    DESC NULLS LAST) AS rn_pre
            FROM (
                SELECT pe.ticker AS event_ticker,
                       pm.slug   AS ticker,
                       (pm.slug ILIKE '%draw%' OR pm.question ILIKE '%draw%'
                        OR pm.group_item_title ILIKE '%draw%') AS is_draw_mkt,
                       COALESCE(
                           pm.team_id,
                           CASE WHEN (pm.question ILIKE '%' || ht.common_name || '%'
                                  OR pm.question ILIKE '%' || ht.name || '%'
                                  OR pm.group_item_title ILIKE '%' || ht.common_name || '%'
                                  OR pm.group_item_title ILIKE '%' || ht.name || '%'{_P_HT}{_P_GIT_HT})
                                 AND NOT (pm.question ILIKE '%draw%' OR pm.group_item_title ILIKE '%draw%')
                           THEN ht.team_id END,
                           CASE WHEN (pm.question ILIKE '%' || awt.common_name || '%'
                                  OR pm.question ILIKE '%' || awt.name || '%'
                                  OR pm.group_item_title ILIKE '%' || awt.common_name || '%'
                                  OR pm.group_item_title ILIKE '%' || awt.name || '%'{_P_AT}{_P_GIT_AT})
                                 AND NOT (pm.question ILIKE '%draw%' OR pm.group_item_title ILIKE '%draw%')
                           THEN awt.team_id END,
                           CASE WHEN NOT (pm.question ILIKE '%draw%' OR pm.group_item_title ILIKE '%draw%') AND EXISTS (
                               SELECT 1 FROM polymarket_markets pm2
                               WHERE pm2.event_id = pm.event_id AND pm2.id != pm.id
                                 AND NOT (pm2.question ILIKE '%draw%' OR pm2.group_item_title ILIKE '%draw%')
                                 AND (pm2.question ILIKE '%' || ht.common_name || '%'
                                   OR pm2.question ILIKE '%' || ht.name || '%'{_P2_HT}
                                   OR pm2.group_item_title ILIKE '%' || ht.common_name || '%'
                                   OR pm2.group_item_title ILIKE '%' || ht.name || '%'{_P2_GIT_HT})
                           ) THEN awt.team_id END,
                           CASE WHEN NOT (pm.question ILIKE '%draw%' OR pm.group_item_title ILIKE '%draw%') AND EXISTS (
                               SELECT 1 FROM polymarket_markets pm2
                               WHERE pm2.event_id = pm.event_id AND pm2.id != pm.id
                                 AND NOT (pm2.question ILIKE '%draw%' OR pm2.group_item_title ILIKE '%draw%')
                                 AND (pm2.question ILIKE '%' || awt.common_name || '%'
                                   OR pm2.question ILIKE '%' || awt.name || '%'{_P2_AT}
                                   OR pm2.group_item_title ILIKE '%' || awt.common_name || '%'
                                   OR pm2.group_item_title ILIKE '%' || awt.name || '%'{_P2_GIT_AT})
                           ) THEN ht.team_id END
                       ) AS team_id,
                       pm.volume, pm.open_interest, pm.updated_at,
                       pm.outcomes,
                       COALESCE(
                           NULLIF(pm.outcome_prices[array_position(pm.outcomes, 'Yes')]::float, 0),
                           (SELECT pph.probability FROM polymarket_price_history pph
                            JOIN polymarket_tokens pt ON pt.token_id = pph.token_id
                            WHERE pt.market_id = pm.id::text AND pt.outcome ILIKE 'yes'
                            ORDER BY pph.ts DESC LIMIT 1),
                           (SELECT NULLIF(pms.outcome_prices[1]::float, 0)
                            FROM polymarket_market_snapshots pms
                            WHERE pms.market_id = pm.id
                            ORDER BY pms.snapshotted_at DESC LIMIT 1)
                       ) AS latest_prob,
                       COALESCE(
                           (SELECT pph.probability FROM polymarket_price_history pph
                            JOIN polymarket_tokens pt ON pt.token_id = pph.token_id
                            WHERE pt.market_id = pm.id::text AND pt.outcome ILIKE 'yes'
                              AND to_timestamp(pph.ts) < m.match_time_utc
                            ORDER BY pph.ts DESC LIMIT 1),
                           (SELECT NULLIF(pms.outcome_prices[1]::float, 0)
                            FROM polymarket_market_snapshots pms
                            WHERE pms.market_id = pm.id
                              AND pms.snapshotted_at < m.match_time_utc
                            ORDER BY pms.snapshotted_at DESC LIMIT 1)
                       ) AS pre_prob
                FROM polymarket_events pe
                JOIN polymarket_markets pm ON pm.event_id = pe.id
                WHERE pe.match_id = m.match_id
                  AND pe.ticker NOT ILIKE '%more-markets%'
                  AND pm.slug NOT ILIKE '%more-markets%'
            ) raw_p
        ) ranked_p
    ) ppred ON true"""

# Prediction-free variants for h2h / last-5 base CTEs.
# The kpred + ppred LATERALs are expensive (correlated subqueries into forecast
# history tables per market per match).  Historical matches don't need live
# prediction data, so we strip those two blocks entirely.
_i = _BASE_COLS.index('\n    -- kalshi latest prediction')
_BASE_COLS_NOPRED = _BASE_COLS[:_i] + """
    NULL::json AS kalshi_pred_json,
    NULL::json AS kalshi_prematch_pred_json,
    NULL::json AS polym_pred_json,
    NULL::json AS polym_prematch_pred_json"""

_j = _BASE_JOINS.index('\n    LEFT JOIN LATERAL')
_BASE_JOINS_NOPRED = _BASE_JOINS[:_j]

# Bydate variant: replace per-row color correlated subqueries with pre-materialized CTE joins.
# The CTEs (team_recent_home_colors / team_recent_away_colors) are injected by the bydate route.
_i_color_start = _BASE_COLS.index('\n    COALESCE(\n        m.home_color,')
_i_color_end   = _BASE_COLS.index('\n    ) AS away_color,') + len('\n    ) AS away_color,')
_BASE_COLS_BYDATE = (
    _BASE_COLS[:_i_color_start]
    + '\n    COALESCE(m.home_color, thc.home_color) AS home_color,'
    + '\n    COALESCE(m.away_color, tac.away_color) AS away_color,'
    + _BASE_COLS[_i_color_end:]
).replace(
    '\n    m.stadium_id,',
    '\n    COALESCE(m.stadium_id, fallback_stadia.sid) AS stadium_id,'
)
_BASE_JOINS_BYDATE = (
    _BASE_JOINS.replace(
        '\n    LEFT JOIN stadiums s ON s.id = m.stadium_id',
        """
    LEFT JOIN LATERAL (
        SELECT m2.stadium_id AS sid
        FROM matches m2
        WHERE m2.home_id = m.home_id
          AND m2.stadium_id IS NOT NULL
          AND m2.match_date < m.match_date
        ORDER BY m2.match_date DESC
        LIMIT 1
    ) fallback_stadia ON m.stadium_id IS NULL
    LEFT JOIN stadiums s ON s.id = COALESCE(m.stadium_id, fallback_stadia.sid)"""
    )
    + """
    LEFT JOIN team_recent_home_colors thc ON thc.team_id = m.home_id
    LEFT JOIN team_recent_away_colors tac ON tac.team_id = m.away_id"""
)

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
        'shots',         home_shots,
        'possesion',     home_poss::int,
        'offsides',      home_offsides,
        'corners',       home_corners,
        'xg',            home_xg::float,
        'pass_att',      home_pass_att,
        'pass_succ',     home_pass_succ,
        'league_rank',   home_ranking
    ),
    'home_color',      home_color,
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
        'penalty_goals', pen_away_goals,
        'shots',         away_shots,
        'possesion',     away_poss::int,
        'offsides',      away_offsides,
        'corners',       away_corners,
        'xg',            away_xg::float,
        'pass_att',      away_pass_att,
        'pass_succ',     away_pass_succ,
        'league_rank',   away_ranking
    ),
    'away_color',      away_color,
    'win_team_id',     win_team_id,
    'loss_team_id',    loss_team_id,
    'isdraw',          isdraw,
    'pens',            pens,
    'extra_time',      extra_time,
    'isplayed',        isplayed,
    'round',           round,
    'gameweek_number', gameweek_number,
    'stadium', CASE WHEN stadium_id IS NULL THEN NULL ELSE json_build_object(
        'stadium_id',   stadium_id,
        'stadium_name', stadium_name,
        'attendance',   attendance,
        'capacity',     s_capacity,
        'capacity_pct', CASE WHEN s_capacity > 0 AND attendance IS NOT NULL
                             THEN ROUND((attendance::numeric / s_capacity * 100), 1)::float
                             ELSE NULL END,
        'address', stadium_address
    ) END,
    'kalshi_prediction',              kalshi_pred_json,
    'kalshi_prematch_prediction',     kalshi_prematch_pred_json,
    'polymarket_prediction',          polym_pred_json,
    'polymarket_prematch_prediction', polym_prematch_pred_json
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

#@router.get("", response_model=MatchesByDateResponse)
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
        WITH
        -- Pre-compute the most-recent home/away colors per team over the last
        -- 2 months in a single pass, then JOIN them in base instead of running
        -- one correlated subquery per match row.
        team_recent_home_colors AS MATERIALIZED (
            SELECT DISTINCT ON (home_id) home_id AS team_id, home_color
            FROM matches
            WHERE match_date >= CAST(:match_date AS date) - INTERVAL '2 months'
              AND match_date <= :match_date
              AND home_color IS NOT NULL
            ORDER BY home_id, match_date DESC
        ),
        team_recent_away_colors AS MATERIALIZED (
            SELECT DISTINCT ON (away_id) away_id AS team_id, away_color
            FROM matches
            WHERE match_date >= CAST(:match_date AS date) - INTERVAL '2 months'
              AND match_date <= :match_date
              AND away_color IS NOT NULL
            ORDER BY away_id, match_date DESC
        ),
        base AS (
            SELECT {_BASE_COLS_BYDATE}
            {_BASE_JOINS_BYDATE}
            WHERE m.match_date = :match_date
            AND comp.league_id = ANY(:league_ids)
            AND m.isplayed IS NOT NULL
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
            SELECT m.home_id, m.away_id, m.match_date, m.round, m.comp_id,
                   (ht.type = 'national' AND awt.type = 'national') AS is_intl,
                   CASE WHEN regexp_replace(m.round, '\D', '', 'g') ~ '^\d+$'
                        THEN regexp_replace(m.round, '\D', '', 'g')::int
                        ELSE NULL END AS round_num
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
                    'match_id',      :match_id,
                    'player_id',     pb.player_id,
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
            SELECT match_id FROM (
                SELECT m.match_id, m.match_date
                FROM matches m, match_flags mf
                WHERE m.home_id = mf.home_id AND m.away_id = mf.away_id
                  AND m.isplayed = true AND m.match_id != :match_id
                  AND m.match_date < mf.match_date
                UNION ALL
                SELECT m.match_id, m.match_date
                FROM matches m, match_flags mf
                WHERE m.home_id = mf.away_id AND m.away_id = mf.home_id
                  AND m.isplayed = true AND m.match_id != :match_id
                  AND m.match_date < mf.match_date
            ) t
            ORDER BY match_date DESC LIMIT 6
        ),
        h2h_base AS (
            SELECT {_BASE_COLS_NOPRED}
            {_BASE_JOINS_NOPRED}
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
            SELECT match_id FROM (
                SELECT m.match_id, m.match_date
                FROM matches m, match_flags mf
                WHERE m.home_id = mf.home_id AND m.isplayed = true
                  AND m.match_id != :match_id AND m.match_date < mf.match_date
                UNION ALL
                SELECT m.match_id, m.match_date
                FROM matches m, match_flags mf
                WHERE m.away_id = mf.home_id AND m.isplayed = true
                  AND m.match_id != :match_id AND m.match_date < mf.match_date
            ) t
            ORDER BY match_date DESC LIMIT 12
        ),
        home_last5_base AS (
            SELECT {_BASE_COLS_NOPRED}
            {_BASE_JOINS_NOPRED}
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
            SELECT match_id FROM (
                SELECT m.match_id, m.match_date
                FROM matches m, match_flags mf
                WHERE m.home_id = mf.away_id AND m.isplayed = true
                  AND m.match_id != :match_id AND m.match_date < mf.match_date
                UNION ALL
                SELECT m.match_id, m.match_date
                FROM matches m, match_flags mf
                WHERE m.away_id = mf.away_id AND m.isplayed = true
                  AND m.match_id != :match_id AND m.match_date < mf.match_date
            ) t
            ORDER BY match_date DESC LIMIT 15
        ),
        away_last5_base AS (
            SELECT {_BASE_COLS_NOPRED}
            {_BASE_JOINS_NOPRED}
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

        -- ── league table end-of-round ─────────────────────────────────────────
        is_league_comp AS (
            SELECT 1
            FROM competitions comp, match_flags mf
            WHERE comp.competition_id = mf.comp_id
              AND comp.stage = 'league'
              AND comp.stage_order = 1
        ),
        -- All played matches in this competition up to and including the current round.
        -- mf.round_num IS NOT NULL short-circuits the entire scan for non-league/non-numeric rounds.
        -- The inner subquery computes regexp_replace once per row to avoid calling it 3× per row.
        comp_matches_eod AS (
            SELECT r.home_id, r.away_id, r.home_goals, r.away_goals
            FROM (
                SELECT m.home_id, m.away_id, m.home_goals, m.away_goals,
                       regexp_replace(m.round, '\D', '', 'g') AS rn
                FROM matches m, match_flags mf
                WHERE m.comp_id = mf.comp_id AND m.isplayed = true
                  AND mf.round_num IS NOT NULL
            ) r, match_flags mf
            WHERE r.rn ~ '^\d+$' AND r.rn::int <= mf.round_num
        ),
        -- All played matches strictly before the current round
        comp_matches_prev AS (
            SELECT r.home_id, r.away_id, r.home_goals, r.away_goals
            FROM (
                SELECT m.home_id, m.away_id, m.home_goals, m.away_goals,
                       regexp_replace(m.round, '\D', '', 'g') AS rn
                FROM matches m, match_flags mf
                WHERE m.comp_id = mf.comp_id AND m.isplayed = true
                  AND mf.round_num IS NOT NULL
            ) r, match_flags mf
            WHERE r.rn ~ '^\d+$' AND r.rn::int < mf.round_num
        ),
        -- Per-team result from matches played in the current round
        current_round_team_results AS (
            SELECT team_id,
                   CASE WHEN home_goals > away_goals THEN 3
                        WHEN home_goals = away_goals THEN 1
                        ELSE 0 END AS points,
                   (home_goals - away_goals) AS gd
            FROM (
                SELECT m.home_id AS team_id, m.home_goals, m.away_goals
                FROM matches m, match_flags mf
                WHERE m.comp_id = mf.comp_id AND m.round = mf.round AND m.isplayed = true
                UNION ALL
                SELECT m.away_id, m.away_goals, m.home_goals
                FROM matches m, match_flags mf
                WHERE m.comp_id = mf.comp_id AND m.round = mf.round AND m.isplayed = true
            ) t
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
            SELECT away_id, 1,
                   CASE WHEN away_goals > home_goals THEN 1 ELSE 0 END,
                   CASE WHEN away_goals = home_goals THEN 1 ELSE 0 END,
                   CASE WHEN away_goals < home_goals THEN 1 ELSE 0 END,
                   COALESCE(away_goals, 0), COALESCE(home_goals, 0)
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
                ROW_NUMBER() OVER (ORDER BY tt.points DESC, tt.gd DESC, tt.goals_f DESC)::int AS current_rank,
                tt.team_id, tt.gp, tt.wins, tt.draws, tt.losses,
                tt.goals_f, tt.goals_a, tt.gd, tt.points,
                t.name AS team_name, t.common_name, t.short_name, t.logo_url, t.level, t.type,
                tc.country_id AS tc_id, tc.name AS tc_name, tc.flag_url AS tc_flag,
                tc.continent AS tc_cont, tc.iso_code_3 AS tc_iso
            FROM team_totals_eod tt
            JOIN teams t ON t.team_id = tt.team_id
            LEFT JOIN countries tc ON tc.country_id = t.country_id
        ),
        team_match_stats_prev AS (
            SELECT home_id AS team_id, 1 AS played,
                   CASE WHEN home_goals > away_goals THEN 1 ELSE 0 END AS wins,
                   CASE WHEN home_goals = away_goals THEN 1 ELSE 0 END AS draws,
                   CASE WHEN home_goals < away_goals THEN 1 ELSE 0 END AS losses,
                   COALESCE(home_goals, 0) AS goals_for,
                   COALESCE(away_goals, 0) AS goals_against
            FROM comp_matches_prev
            UNION ALL
            SELECT away_id, 1,
                   CASE WHEN away_goals > home_goals THEN 1 ELSE 0 END,
                   CASE WHEN away_goals = home_goals THEN 1 ELSE 0 END,
                   CASE WHEN away_goals < home_goals THEN 1 ELSE 0 END,
                   COALESCE(away_goals, 0), COALESCE(home_goals, 0)
            FROM comp_matches_prev
        ),
        team_totals_prev AS (
            SELECT
                team_id,
                SUM(goals_for)::int                          AS goals_f,
                (SUM(goals_for) - SUM(goals_against))::int   AS gd,
                SUM(wins)::int                               AS wins,
                SUM(draws)::int                              AS draws,
                (SUM(wins) * 3 + SUM(draws))::int            AS points
            FROM team_match_stats_prev
            GROUP BY team_id
        ),
        ranked_teams_prev AS (
            SELECT
                ROW_NUMBER() OVER (ORDER BY tp.points DESC, tp.gd DESC, tp.goals_f DESC)::int AS prev_rank,
                tp.team_id, tp.gd AS prev_gd, tp.points AS prev_points
            FROM team_totals_prev tp
        ),
        league_ranks_eod_agg AS (
            SELECT
                CASE WHEN EXISTS (SELECT 1 FROM is_league_comp)
                    THEN (
                        SELECT COALESCE(json_agg(json_build_object(
                            'rank', json_build_object(
                                'rank',    rt.current_rank::text,
                                'info',    NULL,
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
                            ),
                            'rank_difference',   CASE WHEN crr.team_id IS NOT NULL THEN COALESCE(rtp.prev_rank - rt.current_rank, 0) ELSE 0 END,
                            'points_difference', COALESCE(crr.points, 0),
                            'gd_difference',     COALESCE(crr.gd, 0)
                        ) ORDER BY rt.current_rank), '[]'::json)
                        FROM ranked_teams_eod rt
                        LEFT JOIN ranked_teams_prev rtp ON rtp.team_id = rt.team_id
                        LEFT JOIN current_round_team_results crr ON crr.team_id = rt.team_id
                    )
                    ELSE NULL
                END AS rankings
        ),

        -- ── possession sequence ──────────────────────────────────────────────
        possesion_seq_agg AS (
            SELECT COALESCE(json_agg(json_build_object(
                'minute',     ms.minute,
                'add_minute', COALESCE(ms.add_minute, 0),
                'home_stats', json_build_object(
                    'goals', ms.home_goals, 'penalty_goals', NULL,
                    'shots', NULL, 'possesion', ms.home_poss::int,
                    'offsides', NULL, 'corners', NULL, 'xg', NULL,
                    'pass_att', NULL, 'pass_succ', NULL, 'league_rank', NULL
                ),
                'away_stats', json_build_object(
                    'goals', ms.away_goals, 'penalty_goals', NULL,
                    'shots', NULL, 'possesion', ms.away_poss::int,
                    'offsides', NULL, 'corners', NULL, 'xg', NULL,
                    'pass_att', NULL, 'pass_succ', NULL, 'league_rank', NULL
                )
            ) ORDER BY ms.minute, ms.add_minute), NULL) AS seq
            FROM match_snapshots ms
            WHERE ms.match_id = :match_id
        )

        -- ── final assembly ───────────────────────────────────────────────────
        SELECT json_build_object(
            'data', json_build_object(
                'match', json_build_object(
                    'match_id',        m.match_id,
                    'competition_id',  m.comp_id,
                    'match_date',           m.match_date::text,
                    'match_time_utc',       m.match_time_utc::text,
                    'match_half_time_utc',  m.match_half_time_utc::text,
                    'match_end_time_utc',   m.match_end_time_utc::text,
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
                            'xg',            m.home_xg::float,
                            'pass_att',      m.home_pass_att,
                            'pass_succ',     m.home_pass_succ,
                            'league_rank',   m.home_ranking
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
                            'xg',            m.away_xg::float,
                            'pass_att',      m.away_pass_att,
                            'pass_succ',     m.away_pass_succ,
                            'league_rank',   m.away_ranking
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
                'events',              (SELECT events FROM events_agg),
                'h2h',                 (SELECT result FROM h2h_agg),
                'home_last5',          (SELECT result FROM home_last5_agg),
                'away_last5',          (SELECT result FROM away_last5_agg),
                'league_ranks_eod',    (SELECT rankings FROM league_ranks_eod_agg),
                'possesion_sequence',  (SELECT seq FROM possesion_seq_agg)
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


# get polym markets forecast history
@router.get("/{match_id}/polym", response_model=PolymForecastResponse)
async def get_polym_markets(
    match_id: int,
    session: DBSession,
    logger: AppLoggerDep,
):
    logger.info(f"Fetching Polymarket forecast for match_id={match_id}")

    query = text(f"""
        WITH
        match_info AS (
            SELECT m.match_time_utc AS match_start_ts,
                   COALESCE(m.match_end_time_utc, m.match_time_utc + INTERVAL '3 hours 10 minutes') AS match_end_ts,
                   m.win_team, m.loss_team, m.isdraw,
                   m.home_id, m.away_id,
                   ht.name AS home_name, ht.common_name AS home_common,
                   awt.name AS away_name, awt.common_name AS away_common
            FROM matches m
            JOIN teams ht  ON ht.team_id  = m.home_id
            JOIN teams awt ON awt.team_id = m.away_id
            WHERE m.match_id = :match_id
        ),
        polym_event AS (
            SELECT pe.ticker
            FROM polymarket_events pe
            WHERE pe.match_id = :match_id
              AND pe.ticker NOT ILIKE '%more-markets%'
            LIMIT 1
        ),
        all_markets AS (
            SELECT pm.id, pm.condition_id, pm.question, pm.slug, pm.outcomes, pm.group_item_title,
                   pm.volume, pm.open_interest, pm.updated_at,
                   COALESCE(
                       pm.team_id,
                       CASE WHEN (pm.question ILIKE '%' || mi.home_common || '%'
                              OR  pm.question ILIKE '%' || mi.home_name   || '%'
                              OR  pm.group_item_title ILIKE '%' || mi.home_common || '%'
                              OR  pm.group_item_title ILIKE '%' || mi.home_name   || '%'{_P_MI_HT}{_P_GIT_MI_HT})
                             AND NOT (pm.question ILIKE '%draw%' OR pm.group_item_title ILIKE '%draw%')
                       THEN mi.home_id END,
                       CASE WHEN (pm.question ILIKE '%' || mi.away_common || '%'
                              OR  pm.question ILIKE '%' || mi.away_name   || '%'
                              OR  pm.group_item_title ILIKE '%' || mi.away_common || '%'
                              OR  pm.group_item_title ILIKE '%' || mi.away_name   || '%'{_P_MI_AT}{_P_GIT_MI_AT})
                             AND NOT (pm.question ILIKE '%draw%' OR pm.group_item_title ILIKE '%draw%')
                       THEN mi.away_id END,
                       CASE WHEN NOT (pm.question ILIKE '%draw%' OR pm.group_item_title ILIKE '%draw%') AND EXISTS (
                           SELECT 1 FROM polymarket_markets pm2
                           WHERE pm2.event_id = pm.event_id AND pm2.id != pm.id
                             AND NOT (pm2.question ILIKE '%draw%' OR pm2.group_item_title ILIKE '%draw%')
                             AND (pm2.question ILIKE '%' || mi.home_common || '%'
                               OR pm2.question ILIKE '%' || mi.home_name   || '%'{_P2_MI_HT}
                               OR pm2.group_item_title ILIKE '%' || mi.home_common || '%'
                               OR pm2.group_item_title ILIKE '%' || mi.home_name   || '%'{_P2_GIT_MI_HT})
                       ) THEN mi.away_id END,
                       CASE WHEN NOT (pm.question ILIKE '%draw%' OR pm.group_item_title ILIKE '%draw%') AND EXISTS (
                           SELECT 1 FROM polymarket_markets pm2
                           WHERE pm2.event_id = pm.event_id AND pm2.id != pm.id
                             AND NOT (pm2.question ILIKE '%draw%' OR pm2.group_item_title ILIKE '%draw%')
                             AND (pm2.question ILIKE '%' || mi.away_common || '%'
                               OR pm2.question ILIKE '%' || mi.away_name   || '%'{_P2_MI_AT}
                               OR pm2.group_item_title ILIKE '%' || mi.away_common || '%'
                               OR pm2.group_item_title ILIKE '%' || mi.away_name   || '%'{_P2_GIT_MI_AT})
                       ) THEN mi.home_id END
                   ) AS resolved_team_id,
                   (pm.slug ILIKE '%draw%' OR pm.question ILIKE '%draw%' OR pm.group_item_title ILIKE '%draw%') AS is_draw_mkt
            FROM polymarket_markets pm
            JOIN polymarket_events pe ON pe.id = pm.event_id
            JOIN polym_event pev ON pev.ticker = pe.ticker
            CROSS JOIN match_info mi
            WHERE pm.slug NOT ILIKE '%more-markets%'
        ),
        -- Only YES tokens
        yes_tokens AS (
            SELECT pt.market_id, pt.token_id, pt.outcome
            FROM polymarket_tokens pt
            JOIN all_markets am ON am.id::text = pt.market_id
            WHERE pt.outcome ILIKE 'yes'
        ),
        -- Price history within window for YES tokens
        filtered_history AS (
            SELECT
                pph.token_id,
                to_char(to_timestamp(pph.ts) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') || '+00' AS end_period_ts,
                pph.probability
            FROM polymarket_price_history pph
            JOIN yes_tokens yt ON yt.token_id = pph.token_id
            CROSS JOIN match_info mi
            WHERE to_timestamp(pph.ts) >= mi.match_start_ts - INTERVAL '4 days'
              AND to_timestamp(pph.ts) <= mi.match_end_ts
        ),
        history_agg AS (
            SELECT
                token_id,
                COALESCE(json_agg(json_build_object(
                    'token_id',      token_id,
                    'end_period_ts', end_period_ts,
                    'probability',   probability
                ) ORDER BY end_period_ts), '[]'::json) AS history_json
            FROM filtered_history
            GROUP BY token_id
        ),
        -- YES tokens per market with history
        tokens_per_market AS (
            SELECT
                yt.market_id,
                COALESCE(json_agg(json_build_object(
                    'market_id',        yt.market_id,
                    'token_id',         yt.token_id,
                    'outcome',          yt.outcome,
                    'forecast_history', COALESCE(ha.history_json, '[]'::json)
                )), '[]'::json) AS tokens_json
            FROM yes_tokens yt
            LEFT JOIN history_agg ha ON ha.token_id = yt.token_id
            GROUP BY yt.market_id
        ),
        markets_agg AS (
            SELECT COALESCE(json_agg(json_build_object(
                'id',           am.id,
                'condition_id', am.condition_id,
                'question',     am.question,
                'slug',         am.slug,
                'outcomes',     am.outcomes,
                'team_id',      am.resolved_team_id,
                'tokens',       COALESCE(tpm.tokens_json, '[]'::json)
            )), '[]'::json) AS markets
            FROM all_markets am
            LEFT JOIN tokens_per_market tpm ON tpm.market_id = am.id::text
        ),
        -- Latest probability per market (outcome_prices or fallback to latest price history)
        latest_prob_per_market AS (
            SELECT am.id, am.slug, am.resolved_team_id, am.is_draw_mkt,
                   am.volume, am.open_interest, am.updated_at,
                   COALESCE(
                       NULLIF(pm2.outcome_prices[array_position(am.outcomes, 'Yes')]::float, 0),
                       (SELECT pph.probability FROM polymarket_price_history pph
                        JOIN yes_tokens yt ON yt.token_id = pph.token_id
                        WHERE yt.market_id = am.id::text
                        ORDER BY pph.ts DESC LIMIT 1)
                   ) AS latest_prob,
                   (SELECT pph.probability FROM polymarket_price_history pph
                    JOIN yes_tokens yt ON yt.token_id = pph.token_id
                    WHERE yt.market_id = am.id::text
                      AND to_timestamp(pph.ts) < (SELECT match_start_ts FROM match_info)
                    ORDER BY pph.ts DESC LIMIT 1) AS pre_prob
            FROM all_markets am
            JOIN polymarket_markets pm2 ON pm2.id = am.id
        ),
        ranked_latest AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY is_draw_mkt ORDER BY latest_prob DESC NULLS LAST) AS rn
            FROM latest_prob_per_market
        ),
        pred AS (
            SELECT
                MAX(CASE WHEN NOT is_draw_mkt AND rn=1 THEN resolved_team_id END) AS winner_id,
                MAX(CASE WHEN NOT is_draw_mkt AND rn=2 THEN resolved_team_id END) AS loser_id,
                MAX(CASE WHEN NOT is_draw_mkt AND rn=1 THEN slug END)             AS winner_ticker,
                MAX(CASE WHEN NOT is_draw_mkt AND rn=2 THEN slug END)             AS loser_ticker,
                MAX(CASE WHEN NOT is_draw_mkt AND rn=1 THEN latest_prob END)      AS winner_prob,
                MAX(CASE WHEN NOT is_draw_mkt AND rn=2 THEN latest_prob END)      AS loser_prob,
                MAX(CASE WHEN NOT is_draw_mkt AND rn=1 THEN pre_prob END)         AS winner_pre_prob,
                MAX(CASE WHEN NOT is_draw_mkt AND rn=2 THEN pre_prob END)         AS loser_pre_prob,
                MAX(CASE WHEN NOT is_draw_mkt AND rn=1 THEN volume END)           AS winner_volume,
                MAX(CASE WHEN NOT is_draw_mkt AND rn=2 THEN volume END)           AS loser_volume,
                MAX(CASE WHEN NOT is_draw_mkt AND rn=1 THEN open_interest END)    AS winner_oi,
                MAX(CASE WHEN NOT is_draw_mkt AND rn=2 THEN open_interest END)    AS loser_oi,
                MAX(CASE WHEN is_draw_mkt THEN slug END)                          AS draw_ticker,
                MAX(CASE WHEN is_draw_mkt THEN latest_prob END)                   AS draw_prob,
                MAX(CASE WHEN is_draw_mkt THEN pre_prob END)                      AS draw_pre_prob,
                MAX(CASE WHEN is_draw_mkt THEN volume END)                        AS draw_volume,
                MAX(CASE WHEN is_draw_mkt THEN open_interest END)                 AS draw_oi,
                to_char(to_timestamp(AVG(EXTRACT(EPOCH FROM updated_at))) AT TIME ZONE 'UTC',
                        'YYYY-MM-DD HH24:MI:SS') || '+00'                         AS time_saved_utc
            FROM ranked_latest
        ),
        -- Snapshot-based total volume across all event markets
        p_total_vol_end AS (
            SELECT NULLIF(COALESCE(SUM(pv.vol), 0), 0) AS val
            FROM (
                SELECT DISTINCT ON (pms.market_id) pms.volume AS vol
                FROM polymarket_market_snapshots pms
                JOIN all_markets am ON am.id = pms.market_id
                WHERE pms.snapshotted_at <= (SELECT COALESCE(match_end_time_utc, match_time_utc + INTERVAL '3 hours')
                                              FROM match_info)
                ORDER BY pms.market_id, pms.snapshotted_at DESC
            ) pv
        ),
        p_total_vol_start AS (
            SELECT NULLIF(COALESCE(SUM(pv.vol), 0), 0) AS val
            FROM (
                SELECT DISTINCT ON (pms.market_id) pms.volume AS vol
                FROM polymarket_market_snapshots pms
                JOIN all_markets am ON am.id = pms.market_id
                WHERE pms.snapshotted_at <= (SELECT match_start_ts FROM match_info)
                ORDER BY pms.market_id, pms.snapshotted_at DESC
            ) pv
        )

        SELECT json_build_object(
            'data', json_build_object(
                'match_id',           :match_id,
                'polym_event_ticker', (SELECT ticker FROM polym_event),
                'markets',            (SELECT markets FROM markets_agg),
                'prediction', (
                    SELECT CASE WHEN winner_ticker IS NULL AND draw_ticker IS NULL THEN NULL ELSE json_build_object(
                        'event_ticker', (SELECT ticker FROM polym_event),
                        'winner_id',    winner_id,
                        'loser_id',     loser_id,
                        'is_draw',      COALESCE(draw_prob, 0) > COALESCE(winner_prob, 0),
                        'winner_stats', CASE WHEN winner_ticker IS NULL THEN NULL ELSE json_build_object(
                            'market_ticker', winner_ticker, 'probability', winner_prob,
                            'volume', winner_volume, 'dollar_volume', NULL,
                            'open_interest', winner_oi, 'open_interest_dollar', NULL
                        ) END,
                        'winner_prematch_stats', CASE WHEN winner_pre_prob IS NULL THEN NULL ELSE json_build_object(
                            'market_ticker', winner_ticker, 'probability', winner_pre_prob,
                            'volume', winner_volume, 'dollar_volume', NULL,
                            'open_interest', winner_oi, 'open_interest_dollar', NULL
                        ) END,
                        'loser_stats', CASE WHEN loser_ticker IS NULL THEN NULL ELSE json_build_object(
                            'market_ticker', loser_ticker, 'probability', loser_prob,
                            'volume', loser_volume, 'dollar_volume', NULL,
                            'open_interest', loser_oi, 'open_interest_dollar', NULL
                        ) END,
                        'loser_prematch_stats', CASE WHEN loser_pre_prob IS NULL THEN NULL ELSE json_build_object(
                            'market_ticker', loser_ticker, 'probability', loser_pre_prob,
                            'volume', loser_volume, 'dollar_volume', NULL,
                            'open_interest', loser_oi, 'open_interest_dollar', NULL
                        ) END,
                        'draw_stats', CASE WHEN draw_ticker IS NULL THEN NULL ELSE json_build_object(
                            'market_ticker', draw_ticker, 'probability', draw_prob,
                            'volume', draw_volume, 'dollar_volume', NULL,
                            'open_interest', draw_oi, 'open_interest_dollar', NULL
                        ) END,
                        'draw_prematch_stats', CASE WHEN draw_pre_prob IS NULL THEN NULL ELSE json_build_object(
                            'market_ticker', draw_ticker, 'probability', draw_pre_prob,
                            'volume', draw_volume, 'dollar_volume', NULL,
                            'open_interest', draw_oi, 'open_interest_dollar', NULL
                        ) END,
                        'time_saved_utc', time_saved_utc,
                        'total_volume', (SELECT val FROM p_total_vol_end),
                        'is_correct', CASE
                            WHEN (SELECT win_team FROM match_info) IS NULL AND (SELECT isdraw FROM match_info) IS NULL THEN NULL
                            WHEN COALESCE(draw_prob, 0) > COALESCE(winner_prob, 0) AND (SELECT isdraw FROM match_info) = true THEN true
                            WHEN COALESCE(draw_prob, 0) <= COALESCE(winner_prob, 0) AND winner_id IS NOT NULL
                                 AND winner_id = (SELECT win_team FROM match_info) THEN true
                            ELSE false
                        END
                    ) END FROM pred
                )
            )
        )
    """)

    result = session.exec(query, params={"match_id": match_id}).first()
    if not result or not result[0] or result[0].get("data", {}).get("polym_event_ticker") is None:
        raise HTTPException(status_code=404, detail="No Polymarket event found for this match")
    return result[0]


# get kalshi markets forecast history
@router.get("/{match_id}/kalshi", response_model=KalshiForecastResponse)
async def get_kalshi_markets(
    match_id: int,
    session: DBSession,
    logger: AppLoggerDep,
):
    logger.info(f"Fetching Kalshi forecast for match_id={match_id}")

    query = text(f"""
        WITH
        match_info AS (
            SELECT m.match_date, m.match_time_utc,
                   m.win_team, m.loss_team, m.isdraw, m.isplayed,
                   m.home_id, m.away_id,
                   ht.name AS home_name, ht.common_name AS home_common,
                   awt.name AS away_name, awt.common_name AS away_common,
                   m.match_time_utc AS match_start_ts,
                   COALESCE(m.match_end_time_utc, m.match_time_utc + INTERVAL '3 hours 10 minutes') AS match_end_ts
            FROM matches m
            JOIN teams ht  ON ht.team_id  = m.home_id
            JOIN teams awt ON awt.team_id = m.away_id
            WHERE m.match_id = :match_id
        ),
        kalshi_event AS (
            SELECT ke.event_ticker
            FROM kalshi_events ke
            WHERE ke.match_id = :match_id
            LIMIT 1
        ),
        all_markets AS (
            SELECT km.market_id, km.ticker, km.title, km.name, km.team_id,
                   km.volume, km.dollar_volume, km.open_interest, km.dollar_open_interest,
                   km.updated_time, km.yes_bid_dollars, km.yes_ask_dollars,
                   COALESCE(
                       km.team_id,
                       CASE WHEN km.name ILIKE '%' || mi.home_common || '%'
                             OR km.name ILIKE '%' || mi.home_name   || '%'{_K_MI_HT} THEN mi.home_id END,
                       CASE WHEN km.name ILIKE '%' || mi.away_common || '%'
                             OR km.name ILIKE '%' || mi.away_name   || '%'{_K_MI_AT} THEN mi.away_id END,
                       CASE WHEN NOT (km.ticker ILIKE '%-TIE') AND EXISTS (
                           SELECT 1 FROM kalshi_markets km2
                           WHERE km2.event_ticker = km.event_ticker AND km2.ticker != km.ticker
                             AND NOT (km2.ticker ILIKE '%-TIE')
                             AND (km2.name ILIKE '%' || mi.home_common || '%'
                               OR km2.name ILIKE '%' || mi.home_name   || '%'{_K2_MI_HT})
                       ) THEN mi.away_id END,
                       CASE WHEN NOT (km.ticker ILIKE '%-TIE') AND EXISTS (
                           SELECT 1 FROM kalshi_markets km2
                           WHERE km2.event_ticker = km.event_ticker AND km2.ticker != km.ticker
                             AND NOT (km2.ticker ILIKE '%-TIE')
                             AND (km2.name ILIKE '%' || mi.away_common || '%'
                               OR km2.name ILIKE '%' || mi.away_name   || '%'{_K2_MI_AT})
                       ) THEN mi.home_id END
                   ) AS resolved_team_id,
                   (km.ticker ILIKE '%-TIE') AS is_tie
            FROM kalshi_markets km
            JOIN kalshi_event ke ON km.event_ticker = ke.event_ticker
            CROSS JOIN match_info mi
        ),
        -- Forecast history within [match_start - 4 days, match_end_ts]
        filtered_history AS (
            SELECT
                kfh.market_ticker,
                to_char(kfh.end_period_ts AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') || '+00' AS end_period_ts,
                kfh.raw_numerical_forecast AS probability
            FROM kalshi_forecast_history kfh
            JOIN all_markets am ON am.ticker = kfh.market_ticker
            CROSS JOIN match_info mi
            WHERE kfh.end_period_ts >= mi.match_start_ts - INTERVAL '4 days'
              AND kfh.end_period_ts <= mi.match_end_ts
        ),
        history_agg AS (
            SELECT
                market_ticker,
                COALESCE(json_agg(json_build_object(
                    'market_ticker', market_ticker,
                    'end_period_ts', end_period_ts,
                    'probability',   probability
                ) ORDER BY end_period_ts), '[]'::json) AS history_json
            FROM filtered_history
            GROUP BY market_ticker
        ),
        markets_agg AS (
            SELECT COALESCE(json_agg(json_build_object(
                'market_id',        am.market_id,
                'ticker',           am.ticker,
                'title',            am.title,
                'name',             am.name,
                'team_id',          am.resolved_team_id,
                'forecast_history', COALESCE(ha.history_json, '[]'::json)
            )), '[]'::json) AS markets
            FROM all_markets am
            LEFT JOIN history_agg ha ON ha.market_ticker = am.ticker
        ),
        -- Latest probability per market (bid/ask midpoint, fallback to forecast history)
        latest_prob_per_market AS (
            SELECT am.market_id, am.ticker, am.resolved_team_id, am.is_tie,
                   am.volume, am.dollar_volume, am.open_interest, am.dollar_open_interest,
                   am.updated_time,
                   COALESCE(
                       NULLIF((am.yes_bid_dollars + am.yes_ask_dollars) / 2.0, 0),
                       (SELECT raw_numerical_forecast FROM kalshi_forecast_history
                        WHERE market_ticker = am.ticker ORDER BY end_period_ts DESC LIMIT 1)
                   ) AS latest_prob,
                   (SELECT raw_numerical_forecast FROM kalshi_forecast_history
                    WHERE market_ticker = am.ticker
                      AND end_period_ts < (SELECT match_start_ts FROM match_info)
                    ORDER BY end_period_ts DESC LIMIT 1) AS pre_prob
            FROM all_markets am
        ),
        -- Ranked by latest prob to find winner/loser/draw
        ranked_latest AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY is_tie ORDER BY latest_prob DESC NULLS LAST) AS rn
            FROM latest_prob_per_market
        ),
        latest_pred AS (
            SELECT
                MAX(CASE WHEN NOT is_tie AND rn=1 THEN resolved_team_id END) AS winner_id,
                MAX(CASE WHEN NOT is_tie AND rn=2 THEN resolved_team_id END) AS loser_id,
                MAX(CASE WHEN NOT is_tie AND rn=1 THEN ticker END)           AS winner_ticker,
                MAX(CASE WHEN NOT is_tie AND rn=2 THEN ticker END)           AS loser_ticker,
                MAX(CASE WHEN NOT is_tie AND rn=1 THEN latest_prob END)      AS winner_prob,
                MAX(CASE WHEN NOT is_tie AND rn=2 THEN latest_prob END)      AS loser_prob,
                MAX(CASE WHEN NOT is_tie AND rn=1 THEN pre_prob END)         AS winner_pre_prob,
                MAX(CASE WHEN NOT is_tie AND rn=2 THEN pre_prob END)         AS loser_pre_prob,
                MAX(CASE WHEN NOT is_tie AND rn=1 THEN volume END)           AS winner_volume,
                MAX(CASE WHEN NOT is_tie AND rn=2 THEN volume END)           AS loser_volume,
                MAX(CASE WHEN NOT is_tie AND rn=1 THEN dollar_volume END)    AS winner_dvolume,
                MAX(CASE WHEN NOT is_tie AND rn=2 THEN dollar_volume END)    AS loser_dvolume,
                MAX(CASE WHEN NOT is_tie AND rn=1 THEN open_interest END)    AS winner_oi,
                MAX(CASE WHEN NOT is_tie AND rn=2 THEN open_interest END)    AS loser_oi,
                MAX(CASE WHEN NOT is_tie AND rn=1 THEN dollar_open_interest END) AS winner_oi_dollar,
                MAX(CASE WHEN NOT is_tie AND rn=2 THEN dollar_open_interest END) AS loser_oi_dollar,
                MAX(CASE WHEN is_tie THEN ticker END)                        AS draw_ticker,
                MAX(CASE WHEN is_tie THEN latest_prob END)                   AS draw_prob,
                MAX(CASE WHEN is_tie THEN pre_prob END)                      AS draw_pre_prob,
                MAX(CASE WHEN is_tie THEN volume END)                        AS draw_volume,
                MAX(CASE WHEN is_tie THEN dollar_volume END)                 AS draw_dvolume,
                MAX(CASE WHEN is_tie THEN open_interest END)                 AS draw_oi,
                MAX(CASE WHEN is_tie THEN dollar_open_interest END)          AS draw_oi_dollar,
                to_char(to_timestamp(AVG(EXTRACT(EPOCH FROM updated_time))) AT TIME ZONE 'UTC',
                        'YYYY-MM-DD HH24:MI:SS') || '+00'                    AS time_saved_utc
            FROM ranked_latest
        ),
        -- Pre-match ranked (closest to start)
        prematch_probs AS (
            SELECT am.market_id, am.ticker, am.resolved_team_id, am.is_tie,
                   am.volume, am.dollar_volume, am.open_interest, am.dollar_open_interest,
                   am.updated_time,
                   (SELECT kfh.raw_numerical_forecast
                    FROM kalshi_forecast_history kfh
                    WHERE kfh.market_ticker = am.ticker
                      AND kfh.end_period_ts <= (SELECT match_start_ts FROM match_info)
                    ORDER BY kfh.end_period_ts DESC LIMIT 1) AS pre_prob
            FROM all_markets am
        ),
        ranked_prematch AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY is_tie ORDER BY pre_prob DESC NULLS LAST) AS rn
            FROM prematch_probs
        ),
        prematch_pred AS (
            SELECT
                MAX(CASE WHEN NOT is_tie AND rn=1 THEN resolved_team_id END) AS winner_id,
                MAX(CASE WHEN NOT is_tie AND rn=2 THEN resolved_team_id END) AS loser_id,
                MAX(CASE WHEN NOT is_tie AND rn=1 THEN ticker END)           AS winner_ticker,
                MAX(CASE WHEN NOT is_tie AND rn=2 THEN ticker END)           AS loser_ticker,
                MAX(CASE WHEN NOT is_tie AND rn=1 THEN pre_prob END)         AS winner_prob,
                MAX(CASE WHEN NOT is_tie AND rn=2 THEN pre_prob END)         AS loser_prob,
                MAX(CASE WHEN NOT is_tie AND rn=1 THEN volume END)           AS winner_volume,
                MAX(CASE WHEN NOT is_tie AND rn=2 THEN volume END)           AS loser_volume,
                MAX(CASE WHEN NOT is_tie AND rn=1 THEN dollar_volume END)    AS winner_dvolume,
                MAX(CASE WHEN NOT is_tie AND rn=2 THEN dollar_volume END)    AS loser_dvolume,
                MAX(CASE WHEN NOT is_tie AND rn=1 THEN open_interest END)    AS winner_oi,
                MAX(CASE WHEN NOT is_tie AND rn=2 THEN open_interest END)    AS loser_oi,
                MAX(CASE WHEN NOT is_tie AND rn=1 THEN dollar_open_interest END) AS winner_oi_dollar,
                MAX(CASE WHEN NOT is_tie AND rn=2 THEN dollar_open_interest END) AS loser_oi_dollar,
                MAX(CASE WHEN is_tie THEN ticker END)                        AS draw_ticker,
                MAX(CASE WHEN is_tie THEN pre_prob END)                      AS draw_prob,
                MAX(CASE WHEN is_tie THEN volume END)                        AS draw_volume,
                MAX(CASE WHEN is_tie THEN dollar_volume END)                 AS draw_dvolume,
                MAX(CASE WHEN is_tie THEN open_interest END)                 AS draw_oi,
                MAX(CASE WHEN is_tie THEN dollar_open_interest END)          AS draw_oi_dollar,
                to_char(to_timestamp(AVG(EXTRACT(EPOCH FROM updated_time))) AT TIME ZONE 'UTC',
                        'YYYY-MM-DD HH24:MI:SS') || '+00'                    AS time_saved_utc
            FROM ranked_prematch
        ),
        -- Snapshot-based total dollar volume across all event markets
        k_total_vol_end AS (
            SELECT NULLIF(COALESCE(SUM(kv.dvol), 0), 0) AS val
            FROM (
                SELECT DISTINCT ON (kms.market_ticker) kms.dollar_volume AS dvol
                FROM kalshi_market_snapshots kms
                JOIN all_markets am ON am.ticker = kms.market_ticker
                WHERE kms.snapshotted_at <= (SELECT COALESCE(match_end_time_utc, match_time_utc + INTERVAL '3 hours')
                                              FROM matches WHERE match_id = :match_id)
                ORDER BY kms.market_ticker, kms.snapshotted_at DESC
            ) kv
        ),
        k_total_vol_start AS (
            SELECT NULLIF(COALESCE(SUM(kv.dvol), 0), 0) AS val
            FROM (
                SELECT DISTINCT ON (kms.market_ticker) kms.dollar_volume AS dvol
                FROM kalshi_market_snapshots kms
                JOIN all_markets am ON am.ticker = kms.market_ticker
                WHERE kms.snapshotted_at <= (SELECT match_time_utc FROM matches WHERE match_id = :match_id)
                ORDER BY kms.market_ticker, kms.snapshotted_at DESC
            ) kv
        )

        SELECT json_build_object(
            'data', json_build_object(
                'match_id',            :match_id,
                'kalshi_event_ticker', (SELECT event_ticker FROM kalshi_event),
                'markets',             (SELECT markets FROM markets_agg),
                'pre_match_prediction', (
                    SELECT CASE WHEN winner_ticker IS NULL AND draw_ticker IS NULL THEN NULL ELSE json_build_object(
                        'event_ticker', (SELECT event_ticker FROM kalshi_event),
                        'winner_id',    winner_id,
                        'loser_id',     loser_id,
                        'is_draw',      COALESCE(draw_prob, 0) > COALESCE(winner_prob, 0),
                        'winner_stats', CASE WHEN winner_ticker IS NULL THEN NULL ELSE json_build_object(
                            'market_ticker', winner_ticker, 'probability', winner_prob,
                            'volume', winner_volume, 'dollar_volume', winner_dvolume,
                            'open_interest', winner_oi, 'open_interest_dollar', winner_oi_dollar
                        ) END,
                        'winner_prematch_stats', NULL,
                        'loser_stats', CASE WHEN loser_ticker IS NULL THEN NULL ELSE json_build_object(
                            'market_ticker', loser_ticker, 'probability', loser_prob,
                            'volume', loser_volume, 'dollar_volume', loser_dvolume,
                            'open_interest', loser_oi, 'open_interest_dollar', loser_oi_dollar
                        ) END,
                        'loser_prematch_stats', NULL,
                        'draw_stats', CASE WHEN draw_ticker IS NULL THEN NULL ELSE json_build_object(
                            'market_ticker', draw_ticker, 'probability', draw_prob,
                            'volume', draw_volume, 'dollar_volume', draw_dvolume,
                            'open_interest', draw_oi, 'open_interest_dollar', draw_oi_dollar
                        ) END,
                        'draw_prematch_stats', NULL,
                        'time_saved_utc', time_saved_utc,
                        'total_volume', (SELECT val FROM k_total_vol_start),
                        'is_correct', CASE
                            WHEN (SELECT win_team FROM match_info) IS NULL AND (SELECT isdraw FROM match_info) IS NULL THEN NULL
                            WHEN COALESCE(draw_prob, 0) > COALESCE(winner_prob, 0) AND (SELECT isdraw FROM match_info) = true THEN true
                            WHEN COALESCE(draw_prob, 0) <= COALESCE(winner_prob, 0) AND winner_id IS NOT NULL
                                 AND winner_id = (SELECT win_team FROM match_info) THEN true
                            ELSE false
                        END
                    ) END FROM prematch_pred
                ),
                'latest_prediction', (
                    SELECT CASE WHEN winner_ticker IS NULL AND draw_ticker IS NULL THEN NULL ELSE json_build_object(
                        'event_ticker', (SELECT event_ticker FROM kalshi_event),
                        'winner_id',    winner_id,
                        'loser_id',     loser_id,
                        'is_draw',      COALESCE(draw_prob, 0) > COALESCE(winner_prob, 0),
                        'winner_stats', CASE WHEN winner_ticker IS NULL THEN NULL ELSE json_build_object(
                            'market_ticker', winner_ticker, 'probability', winner_prob,
                            'volume', winner_volume, 'dollar_volume', winner_dvolume,
                            'open_interest', winner_oi, 'open_interest_dollar', winner_oi_dollar
                        ) END,
                        'winner_prematch_stats', CASE WHEN winner_pre_prob IS NULL THEN NULL ELSE json_build_object(
                            'market_ticker', winner_ticker, 'probability', winner_pre_prob,
                            'volume', winner_volume, 'dollar_volume', winner_dvolume,
                            'open_interest', winner_oi, 'open_interest_dollar', winner_oi_dollar
                        ) END,
                        'loser_stats', CASE WHEN loser_ticker IS NULL THEN NULL ELSE json_build_object(
                            'market_ticker', loser_ticker, 'probability', loser_prob,
                            'volume', loser_volume, 'dollar_volume', loser_dvolume,
                            'open_interest', loser_oi, 'open_interest_dollar', loser_oi_dollar
                        ) END,
                        'loser_prematch_stats', CASE WHEN loser_pre_prob IS NULL THEN NULL ELSE json_build_object(
                            'market_ticker', loser_ticker, 'probability', loser_pre_prob,
                            'volume', loser_volume, 'dollar_volume', loser_dvolume,
                            'open_interest', loser_oi, 'open_interest_dollar', loser_oi_dollar
                        ) END,
                        'draw_stats', CASE WHEN draw_ticker IS NULL THEN NULL ELSE json_build_object(
                            'market_ticker', draw_ticker, 'probability', draw_prob,
                            'volume', draw_volume, 'dollar_volume', draw_dvolume,
                            'open_interest', draw_oi, 'open_interest_dollar', draw_oi_dollar
                        ) END,
                        'draw_prematch_stats', CASE WHEN draw_pre_prob IS NULL THEN NULL ELSE json_build_object(
                            'market_ticker', draw_ticker, 'probability', draw_pre_prob,
                            'volume', draw_volume, 'dollar_volume', draw_dvolume,
                            'open_interest', draw_oi, 'open_interest_dollar', draw_oi_dollar
                        ) END,
                        'time_saved_utc', time_saved_utc,
                        'total_volume', (SELECT val FROM k_total_vol_end),
                        'is_correct', CASE
                            WHEN (SELECT win_team FROM match_info) IS NULL AND (SELECT isdraw FROM match_info) IS NULL THEN NULL
                            WHEN COALESCE(draw_prob, 0) > COALESCE(winner_prob, 0) AND (SELECT isdraw FROM match_info) = true THEN true
                            WHEN COALESCE(draw_prob, 0) <= COALESCE(winner_prob, 0) AND winner_id IS NOT NULL
                                 AND winner_id = (SELECT win_team FROM match_info) THEN true
                            ELSE false
                        END
                    ) END FROM latest_pred
                )
            )
        )
    """)

    result = session.exec(query, params={"match_id": match_id}).first()
    if not result or not result[0] or result[0].get("data", {}).get("kalshi_event_ticker") is None:
        raise HTTPException(status_code=404, detail="No Kalshi event found for this match")
    return result[0]



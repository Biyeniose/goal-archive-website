from datetime import date, timedelta
from sqlalchemy import text
from typing import List, Optional
from fastapi import APIRouter, Query, HTTPException

from ..dependencies import DBSession, AppLoggerDep
from ..models.league import LeagueListResponse, LeagueStandingsResponse, LeagueStatsResponse, LeagueStatsbyDateResponse, LeagueStatsByDateData
from ..constants import STAT_COLUMNS, BYDATE_STAT_COLUMNS, DEFAULT_LEAGUE_IDS, POSITION_GROUPS

router = APIRouter(
    prefix="/v1/leagues",
    tags=["leagues"],
)


@router.get("/", response_model=LeagueListResponse)
async def list_leagues(
    session: DBSession,
    logger: AppLoggerDep,
):
    logger.info("Fetching all leagues")

    query = text("""
        SELECT json_build_object(
            'data', coalesce(json_agg(d ORDER BY sort_name), '[]'::json)
        )
        FROM (
            SELECT
                json_build_object(
                    'league_id',        l.league_id,
                    'league_name',      l.name,
                    'tier_level',       l.tier_level,
                    'format',           l.format,
                    'competiton_level', l.competition_level,
                    'country', CASE
                        WHEN c.country_id IS NULL THEN NULL
                        ELSE json_build_object(
                            'country_id', c.country_id,
                            'name',       c.name,
                            'flag_url',   c.flag_url,
                            'continent',  c.continent,
                            'iso_code_3', c.iso_code_3
                        )
                    END
                ) AS d,
                l.name AS sort_name
            FROM leagues l
            LEFT JOIN countries c ON c.country_id = l.country_id
        ) sub
    """)

    result = session.exec(query).first()
    return result[0] if result else {"data": []}


# get player stats by league_id (s)
@router.get("/stats/{season_year}", response_model=LeagueStatsResponse)
async def get_league_stats(
    season_year: int,
    session: DBSession,
    logger: AppLoggerDep,
    league_ids: List[int] = Query(default=DEFAULT_LEAGUE_IDS, description="One or more league IDs"),
    stat: str = Query("goals", description=f"Stat to sort by: {', '.join(sorted(STAT_COLUMNS))}"),
    limit: int = Query(15, description="Maximum number of players to return"),
    country_id: Optional[int] = Query(None, description="Filter by player primary country"),
    country2_id: Optional[int] = Query(None, description="Filter by player secondary country"),
    age: Optional[int] = Query(None, description="Max age filter (from player_comp_stats.age)"),
    position: Optional[str] = Query(None, description="Filter by position (checks position and other_positions)"),
):
    if stat not in STAT_COLUMNS:
        raise HTTPException(status_code=400, detail=f"Invalid stat '{stat}'. Valid options: {', '.join(sorted(STAT_COLUMNS))}")

    stat_col = stat
    logger.info(f"Fetching league stats for season_year={season_year}, league_ids={league_ids}, stat={stat}, limit={limit}")

    country_filter = ""
    if country_id is not None:
        country_filter += " AND p.country_id = :country_id"
    if country2_id is not None:
        country_filter += " AND p.country2_id = :country2_id"

    age_filter = ""
    if age is not None:
        age_filter = " AND tot.age <= :age"

    position_group = POSITION_GROUPS.get(position.lower()) if position else None
    position_filter = ""
    if position is not None:
        position_filter = " AND p.position = ANY(:positions)" if position_group else " AND p.position ILIKE :position"

    params = {
        "league_ids": league_ids,
        "season_year": season_year,
        "limit": limit,
    }
    if country_id is not None:
        params["country_id"] = country_id
    if country2_id is not None:
        params["country2_id"] = country2_id
    if age is not None:
        params["age"] = age
    if position is not None:
        params["positions" if position_group else "position"] = position_group or position

    query = text(f"""
        WITH
        selected_comps AS (
            SELECT
                comp.competition_id,
                comp.league_id,
                comp.season_year,
                comp.stage,
                comp.name     AS comp_name,
                comp.logo_url AS comp_logo_url
            FROM competitions comp
            WHERE comp.league_id = ANY(:league_ids)
              AND comp.season_year = :season_year
              AND LOWER(COALESCE(comp.stage, '')) != 'qualification'
        ),
        -- sum player stats across all selected competitions
        --         (player may have 2 rows per comp if they played for 2 teams)
        player_totals AS (
            SELECT
                pcs.player_id,
                MAX(pcs.age)           AS age,
                SUM(pcs.games_played)  AS gp,
                SUM(pcs.minutes)       AS minutes,
                ROUND(SUM(pcs.minutes)::numeric / NULLIF(SUM(pcs.games_played), 0), 1) AS mpg,
                SUM(pcs.goals)         AS goals,
                ROUND(SUM(pcs.goals)::numeric    * 90 / NULLIF(SUM(pcs.minutes), 0), 2) AS goals_p90,
                SUM(pcs.assists)       AS assists,
                ROUND(SUM(pcs.assists)::numeric  * 90 / NULLIF(SUM(pcs.minutes), 0), 2) AS assists_p90,
                SUM(pcs.goals_assists) AS goals_assists,
                ROUND(SUM(pcs.goals_assists)::numeric * 90 / NULLIF(SUM(pcs.minutes), 0), 2) AS goals_assists_p90,
                SUM(pcs.shots)         AS shots,
                SUM(pcs.clean_sheets)  AS clean_sheets,
                SUM(pcs.goals_conceded) AS goals_conceded,
                ROUND(SUM(pcs.goals_conceded)::numeric * 90 / NULLIF(SUM(pcs.minutes), 0), 2) AS goals_conceded_p90,
                SUM(pcs.penalty_goals) AS penalty_goals,
                SUM(pcs.pens_att)      AS pens_att,
                SUM(pcs.cards_yellow)  AS cards_yellow,
                SUM(pcs.cards_red)     AS cards_red
            FROM player_comp_stats pcs
            JOIN selected_comps sc ON sc.competition_id = pcs.competition_id
            GROUP BY pcs.player_id
        ),
        -- Step 5: distinct teams per player
        player_teams AS (
            SELECT
                pt.player_id,
                json_agg(json_build_object(
                    'team_id',     t.team_id,
                    'team_name',   t.name,
                    'common_name', t.common_name,
                    'short_name',  t.short_name,
                    'logo_url',    t.logo_url,
                    'level',       t.level,
                    'type',        t.type,
                    'country', CASE
                        WHEN tc.country_id IS NULL THEN NULL
                        ELSE json_build_object(
                            'country_id', tc.country_id,
                            'name',       tc.name,
                            'flag_url',   tc.flag_url,
                            'continent',  tc.continent,
                            'iso_code_3', tc.iso_code_3
                        )
                    END
                )) AS teams
            FROM (
                SELECT DISTINCT pcs.player_id, pcs.team_id
                FROM player_comp_stats pcs
                JOIN selected_comps sc ON sc.competition_id = pcs.competition_id
            ) pt
            JOIN teams t ON t.team_id = pt.team_id
            LEFT JOIN countries tc ON tc.country_id = t.country_id
            GROUP BY pt.player_id
        ),
        -- Step 6: distinct competitions per player
        player_competitions AS (
            SELECT
                pc.player_id,
                json_agg(json_build_object(
                    'competition_id', sc.competition_id,
                    'season_year',    sc.season_year,
                    'stage',          sc.stage,
                    'logo_url',       sc.comp_logo_url,
                    'league', json_build_object(
                        'league_id',        l.league_id,
                        'league_name',      l.name,
                        'tier_level',       l.tier_level,
                        'format',           l.format,
                        'competiton_level', l.competition_level,
                        'country', CASE
                            WHEN lc.country_id IS NULL THEN NULL
                            ELSE json_build_object(
                                'country_id', lc.country_id,
                                'name',       lc.name,
                                'flag_url',   lc.flag_url,
                                'continent',  lc.continent,
                                'iso_code_3', lc.iso_code_3
                            )
                        END
                    )
                )) AS competitions
            FROM (
                SELECT DISTINCT pcs.player_id, pcs.competition_id
                FROM player_comp_stats pcs
                JOIN selected_comps sc ON sc.competition_id = pcs.competition_id
            ) pc
            JOIN selected_comps sc ON sc.competition_id = pc.competition_id
            JOIN leagues l         ON l.league_id = sc.league_id
            LEFT JOIN countries lc ON lc.country_id = l.country_id
            GROUP BY pc.player_id
        ),
        -- Step 7: all competitions used in this search (for LeagueStatsData.competitions)
        all_competitions AS (
            SELECT json_agg(json_build_object(
                'competition_id', sc.competition_id,
                'season_year',    sc.season_year,
                'stage',          sc.stage,
                'logo_url',       sc.comp_logo_url,
                'league', json_build_object(
                    'league_id',        l.league_id,
                    'league_name',      l.name,
                    'tier_level',       l.tier_level,
                    'format',           l.format,
                    'competiton_level', l.competition_level,
                    'country', CASE
                        WHEN lc.country_id IS NULL THEN NULL
                        ELSE json_build_object(
                            'country_id', lc.country_id,
                            'name',       lc.name,
                            'flag_url',   lc.flag_url,
                            'continent',  lc.continent,
                            'iso_code_3', lc.iso_code_3
                        )
                    END
                )
            )) AS comps_json
            FROM selected_comps sc
            JOIN leagues l         ON l.league_id = sc.league_id
            LEFT JOIN countries lc ON lc.country_id = l.country_id
        ),
        -- Step 8: build the full player list sorted by requested stat, limited
        all_players AS (
            SELECT COALESCE(json_agg(
                json_build_object(
                    'player', json_build_object(
                        'player_name',    ranked.player_name,
                        'player_id',      ranked.player_id,
                        'age',            ranked.age,
                        'tfm_pic_url',    ranked.tfm_pic_url,
                        'pic_url',  ranked.pic_url,
                        'position',       ranked.position,
                        'other_positions', ranked.other_positions,
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
                    ),
                    'teams',        pt.teams,
                    'competitions', pc.competitions,
                    'stats', json_build_object(
                        'gp',                ranked.gp,
                        'minutes',           ranked.minutes,
                        'mpg',               ranked.mpg,
                        'goals',             ranked.goals,
                        'goals_p90',         ranked.goals_p90,
                        'assists',           ranked.assists,
                        'assists_p90',       ranked.assists_p90,
                        'goals_assists',     ranked.goals_assists,
                        'goals_assists_p90', ranked.goals_assists_p90,
                        'shots',             ranked.shots,
                        'clean_sheets',      ranked.clean_sheets,
                        'goals_conceded',    ranked.goals_conceded,
                        'goals_conceded_p90', ranked.goals_conceded_p90,
                        'penalty_goals',     ranked.penalty_goals,
                        'pens_att',          ranked.pens_att,
                        'cards_yellow',      ranked.cards_yellow,
                        'cards_red',         ranked.cards_red
                    )
                )
                ORDER BY ranked.{stat_col} DESC NULLS LAST
            ), '[]'::json) AS players_json
            FROM (
                SELECT tot.*, p.player_name, p.position,
                       COALESCE(p.other_positions, ARRAY[]::text[]) AS other_positions,
                       p.tfm_pic_url, p.pic_url,
                       p.country_id AS p_country_id, p.country2_id AS p_country2_id
                FROM player_totals tot
                JOIN players p ON p.player_id = tot.player_id
                WHERE 1=1{country_filter}{age_filter}{position_filter}
                ORDER BY tot.{stat_col} DESC NULLS LAST
                LIMIT :limit
            ) ranked
            LEFT JOIN countries c1      ON c1.country_id = ranked.p_country_id
            LEFT JOIN countries c2      ON c2.country_id = ranked.p_country2_id
            JOIN player_teams pt        ON pt.player_id  = ranked.player_id
            JOIN player_competitions pc ON pc.player_id  = ranked.player_id
        )
        SELECT json_build_object(
            'data', json_build_object(
                'season_year',  :season_year,
                'competitions', ac.comps_json,
                'players',      ap.players_json
            )
        )
        FROM all_competitions ac, all_players ap
    """)

    result = session.exec(query, params=params).first()

    return result[0] if result else {"data": {}}

# get player stats by date
@router.get("/stats-bydate", response_model=LeagueStatsbyDateResponse)
async def get_league_stats_bydate(
    session: DBSession,
    logger: AppLoggerDep,
    league_ids: List[int] = Query(default=DEFAULT_LEAGUE_IDS, description="One or more league IDs"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD), default 3 weeks ago"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD), default today"),
    stat: str = Query("goals", description=f"Stat to sort by: {', '.join(sorted(BYDATE_STAT_COLUMNS))}"),
    limit: int = Query(15, description="Maximum number of players to return"),
    age: Optional[int] = Query(None, description="Max age filter (from players.age)"),
    position: Optional[str] = Query(None, description="Filter by position"),
):
    if stat not in BYDATE_STAT_COLUMNS:
        raise HTTPException(status_code=400, detail=f"Invalid stat '{stat}'. Valid options: {', '.join(sorted(BYDATE_STAT_COLUMNS))}")

    today = date.today()
    resolved_end = end_date or today.isoformat()
    resolved_start = start_date or (today - timedelta(weeks=3)).isoformat()

    stat_col = stat
    logger.info(f"Fetching bydate stats for league_ids={league_ids}, {resolved_start} to {resolved_end}, stat={stat}")

    age_filter = ""
    if age is not None:
        age_filter = " AND p.age <= :age"

    position_group = POSITION_GROUPS.get(position.lower()) if position else None
    position_filter = ""
    if position is not None:
        position_filter = " AND p.position = ANY(:positions)" if position_group else " AND p.position ILIKE :position"

    params = {
        "league_ids": league_ids,
        "start_date": resolved_start,
        "end_date": resolved_end,
        "limit": limit,
    }
    if age is not None:
        params["age"] = age
    if position is not None:
        params["positions" if position_group else "position"] = position_group or position

    query = text(f"""
        WITH
        selected_comps AS (
            SELECT comp.competition_id
            FROM competitions comp
            WHERE comp.league_id = ANY(:league_ids)
        ),
        selected_matches AS (
            SELECT m.match_id
            FROM matches m
            JOIN selected_comps sc ON sc.competition_id = m.comp_id
            WHERE m.match_date BETWEEN :start_date AND :end_date
        ),
        player_totals AS (
            SELECT
                pms.player_id,
                COUNT(DISTINCT pms.match_id)   AS gp,
                SUM(pms.minutes)               AS minutes,
                SUM(pms.goals)                 AS goals,
                ROUND(SUM(pms.goals)::numeric        * 90 / NULLIF(SUM(pms.minutes), 0), 2) AS goals_p90,
                SUM(pms.assists)               AS assists,
                ROUND(SUM(pms.assists)::numeric      * 90 / NULLIF(SUM(pms.minutes), 0), 2) AS assists_p90,
                SUM(pms.goals_assists)         AS goals_assists,
                ROUND(SUM(pms.goals_assists)::numeric * 90 / NULLIF(SUM(pms.minutes), 0), 2) AS goals_assists_p90,
                SUM(pms.shots)                 AS shots,
                SUM(pms.pens_made)             AS pens_made,
                SUM(pms.pens_att)              AS pens_att
            FROM player_match_stats pms
            JOIN selected_matches sm ON sm.match_id = pms.match_id
            GROUP BY pms.player_id
        ),
        player_teams AS (
            SELECT
                pt.player_id,
                json_agg(json_build_object(
                    'team_id',     t.team_id,
                    'team_name',   t.name,
                    'common_name', t.common_name,
                    'short_name',  t.short_name,
                    'logo_url',    t.logo_url,
                    'level',       t.level,
                    'type',        t.type,
                    'country', CASE
                        WHEN tc.country_id IS NULL THEN NULL
                        ELSE json_build_object(
                            'country_id', tc.country_id,
                            'name',       tc.name,
                            'flag_url',   tc.flag_url,
                            'continent',  tc.continent,
                            'iso_code_3', tc.iso_code_3
                        )
                    END
                )) AS teams
            FROM (
                SELECT DISTINCT pms.player_id, pms.team_id
                FROM player_match_stats pms
                JOIN selected_matches sm ON sm.match_id = pms.match_id
            ) pt
            JOIN teams t ON t.team_id = pt.team_id
            LEFT JOIN countries tc ON tc.country_id = t.country_id
            GROUP BY pt.player_id
        ),
        all_players AS (
            SELECT COALESCE(json_agg(
                json_build_object(
                    'player', json_build_object(
                        'player_name',    ranked.player_name,
                        'player_id',      ranked.player_id,
                        'age',            ranked.age,
                        'tfm_pic_url',    ranked.tfm_pic_url,
                        'pic_url',  ranked.pic_url,
                        'position',       ranked.position,
                        'other_positions', ranked.other_positions,
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
                    ),
                    'teams', pt.teams,
                    'stats', json_build_object(
                        'gp',                ranked.gp,
                        'minutes',           ranked.minutes,
                        'mpg',               ROUND(ranked.minutes::numeric / NULLIF(ranked.gp, 0), 1),
                        'goals',             ranked.goals,
                        'goals_p90',         ranked.goals_p90,
                        'assists',           ranked.assists,
                        'assists_p90',       ranked.assists_p90,
                        'goals_assists',     ranked.goals_assists,
                        'goals_assists_p90', ranked.goals_assists_p90,
                        'shots',             ranked.shots,
                        'clean_sheets',      NULL,
                        'goals_conceded',    NULL,
                        'goals_conceded_p90', NULL,
                        'penalty_goals',     ranked.pens_made,
                        'pens_att',          ranked.pens_att,
                        'cards_yellow',      NULL,
                        'cards_red',         NULL
                    )
                )
                ORDER BY ranked.{stat_col} DESC NULLS LAST
            ), '[]'::json) AS players_json
            FROM (
                SELECT tot.*, p.player_name, p.age, p.position,
                       COALESCE(p.other_positions, ARRAY[]::text[]) AS other_positions,
                       p.tfm_pic_url, p.pic_url,
                       p.country_id AS p_country_id, p.country2_id AS p_country2_id
                FROM player_totals tot
                JOIN players p ON p.player_id = tot.player_id
                WHERE 1=1{age_filter}{position_filter}
                ORDER BY tot.{stat_col} DESC NULLS LAST
                LIMIT :limit
            ) ranked
            LEFT JOIN countries c1      ON c1.country_id = ranked.p_country_id
            LEFT JOIN countries c2      ON c2.country_id = ranked.p_country2_id
            JOIN player_teams pt        ON pt.player_id  = ranked.player_id
        )
        SELECT json_build_object(
            'data', json_build_object(
                'start_date', :start_date,
                'end_date',   :end_date,
                'players',    ap.players_json
            )
        )
        FROM all_players ap
    """)

    result = session.exec(query, params=params).first()
    return result[0] if result else {"data": {"start_date": resolved_start, "end_date": resolved_end, "players": []}}

# get league standings
@router.get("/{league_id}/standings", response_model=LeagueStandingsResponse)
async def get_league_standings(
    league_id: int,
    session: DBSession,
    logger: AppLoggerDep,
    season_year: int = Query(..., description="Season year (e.g. 2024)"),
):
    logger.info(f"Fetching standings for league_id: {league_id}, season_year: {season_year}")

    query = text("""
        SELECT json_build_object(
            'data', coalesce(json_agg(d ORDER BY sort_rank), '[]'::json)
        )
        FROM (
            SELECT json_build_object(
                'team', json_build_object(
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
                ),
                'rank',    r.rank::text,
                'info',    r.info,
                'points',  r.points,
                'gp',      r.gp,
                'gd',      r.gd,
                'wins',    r.wins,
                'losses',  r.losses,
                'draws',   r.draws,
                'goals_f', r.goals_f,
                'goals_a', r.goals_a
            ) AS d,
            r.rank AS sort_rank
            FROM ranks r
            JOIN teams t             ON t.team_id = r.team_id
            JOIN competitions comp   ON comp.competition_id = r.competition_id
            LEFT JOIN countries c    ON c.country_id = t.country_id
            WHERE comp.league_id  = :league_id
              AND comp.season_year = :season_year
        ) sub
    """)

    result = session.exec(query, params={"league_id": league_id, "season_year": season_year}).first()
    return result[0] if result else {"data": []}




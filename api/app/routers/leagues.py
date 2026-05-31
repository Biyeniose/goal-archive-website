from datetime import date, timedelta
from sqlalchemy import text
from typing import List, Optional
from fastapi import APIRouter, Query, HTTPException

from ..dependencies import DBSession, AppLoggerDep
from ..models.league import LeagueListResponse, LeagueStandingsResponse, LeagueStatsResponse, LeagueStatsbyDateResponse, LeagueStatsByDateData, BestLoaneesResponse, LongestDroughtsReponse
from ..models.match import CompetitionSquadsResponse
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
                    'scope',            l.scope,
                    'logo_url',         (SELECT c2.logo_url FROM competitions c2 WHERE c2.league_id = l.league_id ORDER BY c2.season_year DESC LIMIT 1),
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
    league_ids: List[int] = Query( description="One or more league IDs"),
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
        -- Step 6: build the full player list sorted by requested stat, limited
        all_players AS (
            SELECT COALESCE(json_agg(
                json_build_object(
                    'player', json_build_object(
                        'player_name',    ranked.player_name,
                        'player_id',      ranked.player_id,
                        'age',            ranked.age,
                        'tfm_pic_url',    ranked.tfm_pic_url,
                        'pic_url',  ranked.pic_url,
                        'pixel_pic_url', ranked.pixel_pic_url,
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
                        'cards_red',         ranked.cards_red,
                        'competition',       NULL
                    )
                )
                ORDER BY ranked.{stat_col} DESC NULLS LAST
            ), '[]'::json) AS players_json
            FROM (
                SELECT tot.*, p.player_name, p.position,
                       COALESCE(p.other_positions, ARRAY[]::text[]) AS other_positions,
                       p.tfm_pic_url, p.pic_url, p.pixel_pic_url,
                       p.country_id AS p_country_id, p.country2_id AS p_country2_id
                FROM player_totals tot
                JOIN players p ON p.player_id = tot.player_id
                WHERE 1=1{country_filter}{age_filter}{position_filter}
                ORDER BY tot.{stat_col} DESC NULLS LAST
                LIMIT :limit
            ) ranked
            LEFT JOIN countries c1 ON c1.country_id = ranked.p_country_id
            LEFT JOIN countries c2 ON c2.country_id = ranked.p_country2_id
            JOIN player_teams pt   ON pt.player_id  = ranked.player_id
        )
        SELECT json_build_object(
            'data', json_build_object(
                'season_year', :season_year,
                'players',     ap.players_json
            )
        )
        FROM all_players ap
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
                        'pixel_pic_url', ranked.pixel_pic_url,
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
                        'cards_red',         NULL,
                        'competition',       NULL
                    )
                )
                ORDER BY ranked.{stat_col} DESC NULLS LAST
            ), '[]'::json) AS players_json
            FROM (
                SELECT tot.*, p.player_name, p.age, p.position,
                       COALESCE(p.other_positions, ARRAY[]::text[]) AS other_positions,
                       p.tfm_pic_url, p.pic_url, p.pixel_pic_url,
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


# get best loan players currently
@router.get("/best-loanees", response_model=BestLoaneesResponse)
async def get_best_loanees(
    session: DBSession,
    logger: AppLoggerDep,
    start_date: str = Query("2025-07-01", description="Loan window start (YYYY-MM-DD)"),
    end_date: str = Query("2026-06-30", description="Loan window end (YYYY-MM-DD)"),
    stat: str = Query("goals", description=f"Stat to sort by: {', '.join(sorted(STAT_COLUMNS))}"),
    limit: int = Query(20, description="Maximum number of players to return"),
):
    if stat not in STAT_COLUMNS:
        raise HTTPException(status_code=400, detail=f"Invalid stat '{stat}'. Valid options: {', '.join(sorted(STAT_COLUMNS))}")

    from datetime import datetime as dt
    start_year = dt.strptime(start_date, "%Y-%m-%d").year
    end_year = dt.strptime(end_date, "%Y-%m-%d").year
    stat_col = stat

    logger.info(f"Fetching best loanees: {start_date} to {end_date}, stat={stat}, limit={limit}")

    params = {
        "start_date": start_date,
        "end_date": end_date,
        "start_year": start_year,
        "end_year": end_year,
        "limit": limit,
    }

    query = text(f"""
        WITH
        lt AS (
            SELECT
                t.id AS transfer_id,
                t.player_id,
                t.to_team_id   AS loan_team_id,
                t.from_team_id AS parent_team_id,
                t.transfer_date AS loan_start_date,
                t.fee,
                t.player_value,
                t.season,
                t.season_str
            FROM transfers t
            WHERE t.isloan = true
              AND t.transfer_date BETWEEN :start_date AND :end_date
              AND NOT EXISTS (
                  SELECT 1 FROM transfers lo
                  WHERE lo.player_id    = t.player_id
                    AND lo.from_team_id = t.to_team_id
                    AND lo.to_team_id   = t.from_team_id
                    AND lo.isloan       = true
                    AND lo.transfer_date < t.transfer_date
              )
        ),
        rt AS (
            SELECT DISTINCT ON (lt.transfer_id)
                lt.transfer_id,
                t.to_team_id   AS return_to_team_id,
                t.from_team_id AS return_from_team_id,
                t.isloan       AS return_isloan,
                t.fee          AS return_fee,
                t.player_value AS return_player_value,
                t.transfer_date AS return_date,
                t.season       AS return_season,
                t.season_str   AS return_season_str
            FROM lt
            JOIN transfers t ON t.player_id    = lt.player_id
                             AND t.from_team_id = lt.loan_team_id
                             AND t.transfer_date > lt.loan_start_date
            ORDER BY lt.transfer_id, t.transfer_date ASC
        ),
        lc AS (
            SELECT DISTINCT
                lt.transfer_id,
                lt.player_id,
                lt.loan_team_id,
                pcs.competition_id
            FROM lt
            JOIN player_comp_stats pcs ON pcs.player_id = lt.player_id
                                      AND pcs.team_id   = lt.loan_team_id
            JOIN competitions comp ON comp.competition_id = pcs.competition_id
            JOIN leagues lf        ON lf.league_id        = comp.league_id
            WHERE (lf.competition_level IS NULL OR lf.competition_level != 'national')
              AND comp.season_year BETWEEN :start_year AND :end_year
        ),
        pcs_data AS (
            SELECT
                lc.transfer_id,
                lc.player_id,
                lc.competition_id,
                SUM(pcs.games_played)   AS gp,
                SUM(pcs.minutes)        AS minutes,
                ROUND(SUM(pcs.minutes)::numeric / NULLIF(SUM(pcs.games_played), 0), 1) AS mpg,
                SUM(pcs.goals)          AS goals,
                ROUND(SUM(pcs.goals)::numeric          * 90 / NULLIF(SUM(pcs.minutes), 0), 2) AS goals_p90,
                SUM(pcs.assists)        AS assists,
                ROUND(SUM(pcs.assists)::numeric        * 90 / NULLIF(SUM(pcs.minutes), 0), 2) AS assists_p90,
                SUM(pcs.goals_assists)  AS goals_assists,
                ROUND(SUM(pcs.goals_assists)::numeric  * 90 / NULLIF(SUM(pcs.minutes), 0), 2) AS goals_assists_p90,
                SUM(pcs.shots)          AS shots,
                SUM(pcs.clean_sheets)   AS clean_sheets,
                SUM(pcs.goals_conceded) AS goals_conceded,
                ROUND(SUM(pcs.goals_conceded)::numeric * 90 / NULLIF(SUM(pcs.minutes), 0), 2) AS goals_conceded_p90,
                SUM(pcs.penalty_goals)  AS penalty_goals,
                SUM(pcs.pens_att)       AS pens_att,
                SUM(pcs.cards_yellow)   AS cards_yellow,
                SUM(pcs.cards_red)      AS cards_red
            FROM lc
            JOIN player_comp_stats pcs ON pcs.player_id    = lc.player_id
                                      AND pcs.competition_id = lc.competition_id
                                      AND pcs.team_id       = lc.loan_team_id
            GROUP BY lc.transfer_id, lc.player_id, lc.competition_id
        ),
        ptot AS (
            SELECT
                transfer_id,
                player_id,
                SUM(gp)             AS gp,
                SUM(minutes)        AS minutes,
                SUM(goals)          AS goals,
                SUM(assists)        AS assists,
                SUM(goals_assists)  AS goals_assists,
                SUM(shots)          AS shots,
                SUM(clean_sheets)   AS clean_sheets,
                SUM(goals_conceded) AS goals_conceded,
                SUM(penalty_goals)  AS penalty_goals,
                SUM(pens_att)       AS pens_att,
                SUM(cards_yellow)   AS cards_yellow,
                SUM(cards_red)      AS cards_red
            FROM pcs_data
            GROUP BY transfer_id, player_id
        ),
        top_loanees AS (
            SELECT transfer_id, player_id, {stat_col} AS sort_val
            FROM ptot
            ORDER BY {stat_col} DESC NULLS LAST
            LIMIT :limit
        ),
        stats_agg AS (
            SELECT
                pd.transfer_id,
                pd.player_id,
                json_agg(json_build_object(
                    'gp',                pd.gp,
                    'minutes',           pd.minutes,
                    'mpg',               pd.mpg,
                    'goals',             pd.goals,
                    'goals_p90',         pd.goals_p90,
                    'assists',           pd.assists,
                    'assists_p90',       pd.assists_p90,
                    'goals_assists',     pd.goals_assists,
                    'goals_assists_p90', pd.goals_assists_p90,
                    'shots',             pd.shots,
                    'clean_sheets',      pd.clean_sheets,
                    'goals_conceded',    pd.goals_conceded,
                    'goals_conceded_p90', pd.goals_conceded_p90,
                    'penalty_goals',     pd.penalty_goals,
                    'pens_att',          pd.pens_att,
                    'cards_yellow',      pd.cards_yellow,
                    'cards_red',         pd.cards_red,
                    'competition', json_build_object(
                        'competition_id', comp.competition_id,
                        'name',           comp.name,
                        'season_year',    comp.season_year,
                        'stage',          comp.stage,
                        'logo_url',       comp.logo_url,
                        'league', json_build_object(
                            'league_id',        lf.league_id,
                            'league_name',      lf.name,
                            'tier_level',       lf.tier_level,
                            'format',           lf.format,
                            'competiton_level', lf.competition_level,
                            'scope',            lf.scope,
                            'logo_url',         (SELECT c2.logo_url FROM competitions c2 WHERE c2.league_id = lf.league_id ORDER BY c2.season_year DESC LIMIT 1),
                            'country', CASE
                                WHEN lc_cty.country_id IS NULL THEN NULL
                                ELSE json_build_object(
                                    'country_id', lc_cty.country_id,
                                    'name',       lc_cty.name,
                                    'flag_url',   lc_cty.flag_url,
                                    'continent',  lc_cty.continent,
                                    'iso_code_3', lc_cty.iso_code_3
                                )
                            END
                        )
                    )
                )) AS stats_json
            FROM pcs_data pd
            JOIN top_loanees tl    ON tl.transfer_id    = pd.transfer_id
            JOIN competitions comp ON comp.competition_id = pd.competition_id
            JOIN leagues lf        ON lf.league_id        = comp.league_id
            LEFT JOIN countries lc_cty ON lc_cty.country_id = lf.country_id
            GROUP BY pd.transfer_id, pd.player_id
        )
        SELECT json_build_object(
            'data', COALESCE(json_agg(
                json_build_object(
                    'player', json_build_object(
                        'player_name',     p.player_name,
                        'player_id',       p.player_id,
                        'age',             p.age,
                        'tfm_pic_url',     p.tfm_pic_url,
                        'pic_url',         p.pic_url,
                        'pixel_pic_url',   p.pixel_pic_url,
                        'position',        p.position,
                        'other_positions', COALESCE(p.other_positions, ARRAY[]::text[]),
                        'countries', json_build_object(
                            'country1', CASE WHEN c1.country_id IS NULL THEN NULL
                                ELSE json_build_object('country_id', c1.country_id, 'name', c1.name, 'flag_url', c1.flag_url, 'continent', c1.continent, 'iso_code_3', c1.iso_code_3) END,
                            'country2', CASE WHEN c2.country_id IS NULL THEN NULL
                                ELSE json_build_object('country_id', c2.country_id, 'name', c2.name, 'flag_url', c2.flag_url, 'continent', c2.continent, 'iso_code_3', c2.iso_code_3) END
                        )
                    ),
                    'loan_transfer', json_build_object(
                        'buying_team', json_build_object(
                            'team_id',     lt_team.team_id,
                            'team_name',   lt_team.name,
                            'common_name', lt_team.common_name,
                            'short_name',  lt_team.short_name,
                            'logo_url',    lt_team.logo_url,
                            'level',       lt_team.level,
                            'type',        lt_team.type,
                            'country', CASE WHEN ltc.country_id IS NULL THEN NULL
                                ELSE json_build_object('country_id', ltc.country_id, 'name', ltc.name, 'flag_url', ltc.flag_url, 'continent', ltc.continent, 'iso_code_3', ltc.iso_code_3) END
                        ),
                        'selling_team', CASE WHEN lt.parent_team_id IS NULL THEN NULL ELSE json_build_object(
                            'team_id',     st.team_id,
                            'team_name',   st.name,
                            'common_name', st.common_name,
                            'short_name',  st.short_name,
                            'logo_url',    st.logo_url,
                            'level',       st.level,
                            'type',        st.type,
                            'country', CASE WHEN stc.country_id IS NULL THEN NULL
                                ELSE json_build_object('country_id', stc.country_id, 'name', stc.name, 'flag_url', stc.flag_url, 'continent', stc.continent, 'iso_code_3', stc.iso_code_3) END
                        ) END,
                        'isloan',        true,
                        'fee',           lt.fee,
                        'player_value',  lt.player_value,
                        'transfer_date', lt.loan_start_date::text,
                        'season',        lt.season,
                        'season_str',    lt.season_str
                    ),
                    'return_transfer', CASE WHEN rt.transfer_id IS NULL THEN NULL ELSE json_build_object(
                        'buying_team', json_build_object(
                            'team_id',     rt_to_team.team_id,
                            'team_name',   rt_to_team.name,
                            'common_name', rt_to_team.common_name,
                            'short_name',  rt_to_team.short_name,
                            'logo_url',    rt_to_team.logo_url,
                            'level',       rt_to_team.level,
                            'type',        rt_to_team.type,
                            'country', CASE WHEN rt_to_cty.country_id IS NULL THEN NULL
                                ELSE json_build_object('country_id', rt_to_cty.country_id, 'name', rt_to_cty.name, 'flag_url', rt_to_cty.flag_url, 'continent', rt_to_cty.continent, 'iso_code_3', rt_to_cty.iso_code_3) END
                        ),
                        'selling_team', json_build_object(
                            'team_id',     rt_from_team.team_id,
                            'team_name',   rt_from_team.name,
                            'common_name', rt_from_team.common_name,
                            'short_name',  rt_from_team.short_name,
                            'logo_url',    rt_from_team.logo_url,
                            'level',       rt_from_team.level,
                            'type',        rt_from_team.type,
                            'country', CASE WHEN rt_from_cty.country_id IS NULL THEN NULL
                                ELSE json_build_object('country_id', rt_from_cty.country_id, 'name', rt_from_cty.name, 'flag_url', rt_from_cty.flag_url, 'continent', rt_from_cty.continent, 'iso_code_3', rt_from_cty.iso_code_3) END
                        ),
                        'isloan',        rt.return_isloan,
                        'fee',           rt.return_fee,
                        'player_value',  rt.return_player_value,
                        'transfer_date', rt.return_date::text,
                        'season',        rt.return_season,
                        'season_str',    rt.return_season_str
                    ) END,
                    'stats', sa.stats_json
                )
                ORDER BY tl.sort_val DESC NULLS LAST
            ), '[]'::json)
        )
        FROM top_loanees tl
        JOIN lt             ON lt.transfer_id     = tl.transfer_id
        JOIN players p      ON p.player_id        = lt.player_id
        LEFT JOIN countries c1          ON c1.country_id  = p.country_id
        LEFT JOIN countries c2          ON c2.country_id  = p.country2_id
        JOIN teams lt_team              ON lt_team.team_id = lt.loan_team_id
        LEFT JOIN countries ltc         ON ltc.country_id  = lt_team.country_id
        LEFT JOIN teams st              ON st.team_id      = lt.parent_team_id
        LEFT JOIN countries stc         ON stc.country_id  = st.country_id
        LEFT JOIN rt                    ON rt.transfer_id  = tl.transfer_id
        LEFT JOIN teams rt_to_team      ON rt_to_team.team_id  = rt.return_to_team_id
        LEFT JOIN countries rt_to_cty   ON rt_to_cty.country_id = rt_to_team.country_id
        LEFT JOIN teams rt_from_team    ON rt_from_team.team_id  = rt.return_from_team_id
        LEFT JOIN countries rt_from_cty ON rt_from_cty.country_id = rt_from_team.country_id
        JOIN stats_agg sa   ON sa.transfer_id     = tl.transfer_id
    """)

    result = session.exec(query, params=params).first()
    return result[0] if result else {"data": []}

DROUGHT_STATS = {"goals", "assists", "goals_or_assists"}

# get longest goal/assist droughts
@router.get("/droughts/{season_year}", response_model=LongestDroughtsReponse)
async def get_longest_droughts(
    season_year: int,
    session: DBSession,
    logger: AppLoggerDep,
    league_ids: List[int] = Query(default=DEFAULT_LEAGUE_IDS, description="One or more league IDs"),
    stat: str = Query("goals", description="Drought type: goals, assists, goals_or_assists"),
    limit: int = Query(15, description="Maximum number of players to return"),
):
    if stat not in DROUGHT_STATS:
        raise HTTPException(status_code=400, detail=f"Invalid stat '{stat}'. Valid options: {', '.join(sorted(DROUGHT_STATS))}")

    logger.info(f"Fetching droughts: league_ids={league_ids}, season_year={season_year}, stat={stat}")

    if stat == "goals":
        last_events_sql = """
        last_events AS (
            SELECT me.active_player_id AS player_id, MAX(m.match_date) AS last_event_date
            FROM match_events me
            JOIN matches m ON m.match_id = me.match_id
            WHERE me.event_type IN ('goal', 'penalty goal')
            GROUP BY me.active_player_id
        ),"""
        last_ga_filter = "me.event_type IN ('goal', 'penalty goal') AND me.active_player_id = td.player_id"
    elif stat == "assists":
        last_events_sql = """
        last_events AS (
            SELECT me.passive_player_id AS player_id, MAX(m.match_date) AS last_event_date
            FROM match_events me
            JOIN matches m ON m.match_id = me.match_id
            WHERE me.passive_player_id IS NOT NULL
              AND me.event_type NOT IN ('penalty goal', 'own goal')
            GROUP BY me.passive_player_id
        ),"""
        last_ga_filter = "me.passive_player_id = td.player_id AND me.event_type NOT IN ('penalty goal', 'own goal')"
    else:  # goals_or_assists
        last_events_sql = """
        last_events AS (
            SELECT player_id, MAX(event_date) AS last_event_date
            FROM (
                SELECT me.active_player_id AS player_id, m.match_date AS event_date
                FROM match_events me
                JOIN matches m ON m.match_id = me.match_id
                WHERE me.event_type IN ('goal', 'penalty goal')
                UNION ALL
                SELECT me.passive_player_id AS player_id, m.match_date AS event_date
                FROM match_events me
                JOIN matches m ON m.match_id = me.match_id
                WHERE me.passive_player_id IS NOT NULL
                  AND me.event_type NOT IN ('penalty goal', 'own goal')
            ) evt
            GROUP BY player_id
        ),"""
        last_ga_filter = "(me.event_type IN ('goal', 'penalty goal') AND me.active_player_id = td.player_id) OR (me.passive_player_id = td.player_id AND me.event_type NOT IN ('penalty goal', 'own goal'))"

    params = {
        "league_ids": league_ids,
        "season_year": season_year,
        "limit": limit,
    }

    query = text(f"""
        WITH
        selected_comps AS (
            SELECT comp.competition_id
            FROM competitions comp
            WHERE comp.league_id = ANY(:league_ids)
              AND comp.season_year = :season_year
        ),
        selected_matches AS (
            SELECT m.match_id, m.match_date, m.comp_id,
                   m.home_id AS home_team_id, m.away_id AS away_team_id,
                   m.home_goals, m.away_goals, m.isdraw
            FROM matches m
            JOIN selected_comps sc ON sc.competition_id = m.comp_id
        ),
        active_players AS (
            SELECT pms.player_id
            FROM player_match_stats pms
            JOIN selected_matches sm ON sm.match_id = pms.match_id
            JOIN players p2 ON p2.player_id = pms.player_id
            WHERE pms.minutes > 0
              AND p2.position = ANY(ARRAY[
                  'Centre-Forward', 'Second Striker',
                  'Right Winger', 'Left Winger', 'Left Midfield', 'Right Midfield'
              ])
            GROUP BY pms.player_id
            HAVING COUNT(DISTINCT pms.match_id) > 3
        ),
        {last_events_sql}
        player_drought_stats AS (
            SELECT
                ap.player_id,
                le.last_event_date,
                COUNT(DISTINCT pms.match_id)   AS gp,
                SUM(pms.minutes)               AS total_minutes,
                ROUND(SUM(pms.minutes)::numeric / NULLIF(COUNT(DISTINCT pms.match_id), 0), 1) AS mpg,
                MAX(sm.match_date)             AS last_match_date
            FROM active_players ap
            JOIN last_events le ON le.player_id = ap.player_id
            JOIN player_match_stats pms ON pms.player_id = ap.player_id AND pms.minutes > 0
            JOIN selected_matches sm    ON sm.match_id   = pms.match_id
            WHERE sm.match_date > le.last_event_date
            GROUP BY ap.player_id, le.last_event_date
            HAVING COUNT(DISTINCT pms.match_id) > 3
        ),
        top_droughts AS (
            SELECT *,
                   (last_match_date - last_event_date)::int AS days
            FROM player_drought_stats
            ORDER BY (last_match_date - last_event_date) DESC NULLS LAST
            LIMIT :limit
        ),
        drought_team_matches AS (
            SELECT DISTINCT td.player_id, pms.team_id
            FROM top_droughts td
            JOIN player_match_stats pms ON pms.player_id = td.player_id AND pms.minutes > 0
            JOIN selected_matches sm    ON sm.match_id   = pms.match_id
            WHERE sm.match_date > td.last_event_date
        ),
        drought_teams_agg AS (
            SELECT
                dtm.player_id,
                json_agg(json_build_object(
                    'team_id',     t.team_id,
                    'team_name',   t.name,
                    'common_name', t.common_name,
                    'short_name',  t.short_name,
                    'logo_url',    t.logo_url,
                    'level',       t.level,
                    'type',        t.type,
                    'country', CASE WHEN tc.country_id IS NULL THEN NULL
                        ELSE json_build_object('country_id', tc.country_id, 'name', tc.name, 'flag_url', tc.flag_url, 'continent', tc.continent, 'iso_code_3', tc.iso_code_3) END
                )) AS teams_json
            FROM drought_team_matches dtm
            JOIN teams t         ON t.team_id    = dtm.team_id
            LEFT JOIN countries tc ON tc.country_id = t.country_id
            GROUP BY dtm.player_id
        ),
        drought_comp_matches AS (
            SELECT DISTINCT td.player_id, sm.comp_id
            FROM top_droughts td
            JOIN player_match_stats pms ON pms.player_id = td.player_id AND pms.minutes > 0
            JOIN selected_matches sm    ON sm.match_id   = pms.match_id
            WHERE sm.match_date > td.last_event_date
        ),
        drought_comps_agg AS (
            SELECT
                dcm.player_id,
                json_agg(json_build_object(
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
                        'country', CASE WHEN lc.country_id IS NULL THEN NULL
                            ELSE json_build_object('country_id', lc.country_id, 'name', lc.name, 'flag_url', lc.flag_url, 'continent', lc.continent, 'iso_code_3', lc.iso_code_3) END
                    )
                )) AS comps_json
            FROM drought_comp_matches dcm
            JOIN competitions comp ON comp.competition_id = dcm.comp_id
            JOIN leagues l         ON l.league_id         = comp.league_id
            LEFT JOIN countries lc ON lc.country_id       = l.country_id
            GROUP BY dcm.player_id
        ),
        last_match_data AS (
            SELECT DISTINCT ON (td.player_id)
                td.player_id,
                sm.match_id,
                sm.match_date,
                sm.home_team_id,
                sm.away_team_id,
                sm.home_goals,
                sm.away_goals,
                sm.isdraw,
                sm.comp_id
            FROM top_droughts td
            JOIN player_match_stats pms ON pms.player_id = td.player_id AND pms.minutes > 0
            JOIN selected_matches sm    ON sm.match_id   = pms.match_id
            WHERE sm.match_date > td.last_event_date
            ORDER BY td.player_id, sm.match_date DESC, sm.match_id DESC
        ),
        last_ga_match_data AS (
            SELECT DISTINCT ON (td.player_id)
                td.player_id,
                m.match_id,
                m.match_date,
                m.home_id   AS home_team_id,
                m.away_id   AS away_team_id,
                m.home_goals,
                m.away_goals,
                m.isdraw,
                m.comp_id
            FROM top_droughts td
            JOIN match_events me ON {last_ga_filter}
            JOIN matches m       ON m.match_id   = me.match_id
                                AND m.match_date = td.last_event_date
            ORDER BY td.player_id, m.match_id DESC
        )
        SELECT json_build_object(
            'data', COALESCE(json_agg(
                json_build_object(
                    'player', json_build_object(
                        'player_name',     p.player_name,
                        'player_id',       p.player_id,
                        'age',             p.age,
                        'tfm_pic_url',     p.tfm_pic_url,
                        'pic_url',         p.pic_url,
                        'pixel_pic_url',   p.pixel_pic_url,
                        'position',        p.position,
                        'other_positions', COALESCE(p.other_positions, ARRAY[]::text[]),
                        'countries', json_build_object(
                            'country1', CASE WHEN c1.country_id IS NULL THEN NULL ELSE json_build_object('country_id', c1.country_id, 'name', c1.name, 'flag_url', c1.flag_url, 'continent', c1.continent, 'iso_code_3', c1.iso_code_3) END,
                            'country2', CASE WHEN c2.country_id IS NULL THEN NULL ELSE json_build_object('country_id', c2.country_id, 'name', c2.name, 'flag_url', c2.flag_url, 'continent', c2.continent, 'iso_code_3', c2.iso_code_3) END
                        )
                    ),
                    'teams',        dta.teams_json,
                    'competitions', dca.comps_json,
                    'days',         td.days,
                    'gp',           td.gp,
                    'mpg',          td.mpg,
                    'last_match', json_build_object(
                        'match_id',   lmd.match_id,
                        'match_date', lmd.match_date::text,
                        'home_team', json_build_object(
                            'team_id',     ht.team_id,
                            'team_name',   ht.name,
                            'common_name', ht.common_name,
                            'short_name',  ht.short_name,
                            'logo_url',    ht.logo_url,
                            'level',       ht.level,
                            'type',        ht.type,
                            'country', CASE WHEN htc.country_id IS NULL THEN NULL ELSE json_build_object('country_id', htc.country_id, 'name', htc.name, 'flag_url', htc.flag_url, 'continent', htc.continent, 'iso_code_3', htc.iso_code_3) END
                        ),
                        'home_goals', lmd.home_goals,
                        'away_team', json_build_object(
                            'team_id',     at2.team_id,
                            'team_name',   at2.name,
                            'common_name', at2.common_name,
                            'short_name',  at2.short_name,
                            'logo_url',    at2.logo_url,
                            'level',       at2.level,
                            'type',        at2.type,
                            'country', CASE WHEN atc.country_id IS NULL THEN NULL ELSE json_build_object('country_id', atc.country_id, 'name', atc.name, 'flag_url', atc.flag_url, 'continent', atc.continent, 'iso_code_3', atc.iso_code_3) END
                        ),
                        'away_goals', lmd.away_goals,
                        'isdraw',     lmd.isdraw,
                        'competition', json_build_object(
                            'competition_id', mcomp.competition_id,
                            'name',           mcomp.name,
                            'season_year',    mcomp.season_year,
                            'stage',          mcomp.stage,
                            'logo_url',       mcomp.logo_url,
                            'league', json_build_object(
                                'league_id',        ml.league_id,
                                'league_name',      ml.name,
                                'tier_level',       ml.tier_level,
                                'format',           ml.format,
                                'competiton_level', ml.competition_level,
                                'scope',            ml.scope,
                                'logo_url',         (SELECT c2.logo_url FROM competitions c2 WHERE c2.league_id = ml.league_id ORDER BY c2.season_year DESC LIMIT 1),
                                'country', CASE WHEN mlc.country_id IS NULL THEN NULL ELSE json_build_object('country_id', mlc.country_id, 'name', mlc.name, 'flag_url', mlc.flag_url, 'continent', mlc.continent, 'iso_code_3', mlc.iso_code_3) END
                            )
                        )
                    ),
                    'last_match_ga', CASE WHEN lgamd.match_id IS NULL THEN NULL ELSE json_build_object(
                        'match_id',   lgamd.match_id,
                        'match_date', lgamd.match_date::text,
                        'home_team', json_build_object(
                            'team_id',     gaht.team_id,
                            'team_name',   gaht.name,
                            'common_name', gaht.common_name,
                            'short_name',  gaht.short_name,
                            'logo_url',    gaht.logo_url,
                            'level',       gaht.level,
                            'type',        gaht.type,
                            'country', CASE WHEN gahtc.country_id IS NULL THEN NULL ELSE json_build_object('country_id', gahtc.country_id, 'name', gahtc.name, 'flag_url', gahtc.flag_url, 'continent', gahtc.continent, 'iso_code_3', gahtc.iso_code_3) END
                        ),
                        'home_goals', lgamd.home_goals,
                        'away_team', json_build_object(
                            'team_id',     gaat.team_id,
                            'team_name',   gaat.name,
                            'common_name', gaat.common_name,
                            'short_name',  gaat.short_name,
                            'logo_url',    gaat.logo_url,
                            'level',       gaat.level,
                            'type',        gaat.type,
                            'country', CASE WHEN gaatc.country_id IS NULL THEN NULL ELSE json_build_object('country_id', gaatc.country_id, 'name', gaatc.name, 'flag_url', gaatc.flag_url, 'continent', gaatc.continent, 'iso_code_3', gaatc.iso_code_3) END
                        ),
                        'away_goals', lgamd.away_goals,
                        'isdraw',     lgamd.isdraw,
                        'competition', json_build_object(
                            'competition_id', gacomp.competition_id,
                            'name',           gacomp.name,
                            'season_year',    gacomp.season_year,
                            'stage',          gacomp.stage,
                            'logo_url',       gacomp.logo_url,
                            'league', json_build_object(
                                'league_id',        gaml.league_id,
                                'league_name',      gaml.name,
                                'tier_level',       gaml.tier_level,
                                'format',           gaml.format,
                                'competiton_level', gaml.competition_level,
                                'scope',            gaml.scope,
                                'logo_url',         (SELECT c2.logo_url FROM competitions c2 WHERE c2.league_id = gaml.league_id ORDER BY c2.season_year DESC LIMIT 1),
                                'country', CASE WHEN gamlc.country_id IS NULL THEN NULL ELSE json_build_object('country_id', gamlc.country_id, 'name', gamlc.name, 'flag_url', gamlc.flag_url, 'continent', gamlc.continent, 'iso_code_3', gamlc.iso_code_3) END
                            )
                        )
                    ) END
                )
                ORDER BY td.days DESC NULLS LAST
            ), '[]'::json)
        )
        FROM top_droughts td
        JOIN players p         ON p.player_id    = td.player_id
        LEFT JOIN countries c1 ON c1.country_id  = p.country_id
        LEFT JOIN countries c2 ON c2.country_id  = p.country2_id
        JOIN drought_teams_agg dta ON dta.player_id = td.player_id
        JOIN drought_comps_agg dca ON dca.player_id = td.player_id
        JOIN last_match_data lmd        ON lmd.player_id   = td.player_id
        JOIN teams ht                   ON ht.team_id      = lmd.home_team_id
        LEFT JOIN countries htc         ON htc.country_id  = ht.country_id
        JOIN teams at2                  ON at2.team_id     = lmd.away_team_id
        LEFT JOIN countries atc         ON atc.country_id  = at2.country_id
        JOIN competitions mcomp         ON mcomp.competition_id = lmd.comp_id
        JOIN leagues ml                 ON ml.league_id    = mcomp.league_id
        LEFT JOIN countries mlc         ON mlc.country_id  = ml.country_id
        LEFT JOIN last_ga_match_data lgamd ON lgamd.player_id = td.player_id
        LEFT JOIN teams gaht               ON gaht.team_id    = lgamd.home_team_id
        LEFT JOIN countries gahtc          ON gahtc.country_id = gaht.country_id
        LEFT JOIN teams gaat               ON gaat.team_id    = lgamd.away_team_id
        LEFT JOIN countries gaatc          ON gaatc.country_id = gaat.country_id
        LEFT JOIN competitions gacomp      ON gacomp.competition_id = lgamd.comp_id
        LEFT JOIN leagues gaml             ON gaml.league_id  = gacomp.league_id
        LEFT JOIN countries gamlc          ON gamlc.country_id = gaml.country_id
    """)

    result = session.exec(query, params=params).first()
    return result[0] if result else {"data": []}


# get international competition squads
@router.get("/{league_id}/squads/{season_year}/{country_id}", response_model=CompetitionSquadsResponse)
async def get_competition_squads(
    league_id: int,
    season_year: int,
    country_id: int,
    session: DBSession,
    logger: AppLoggerDep,
):
    logger.info(f"Fetching squads league_id={league_id}, season_year={season_year}, country_id={country_id}")

    query = text("""
        WITH
        -- Competition for this league/season (exclude qualification)
        comp AS (
            SELECT c.competition_id, c.name, c.season_year, c.stage, c.logo_url,
                   l.league_id, l.name AS league_name, l.tier_level, l.format,
                   l.competition_level, l.scope,
                   (SELECT c2.logo_url FROM competitions c2
                    WHERE c2.league_id = l.league_id
                    ORDER BY c2.season_year DESC LIMIT 1) AS league_logo,
                   lc.country_id AS lc_id, lc.name AS lc_name,
                   lc.flag_url AS lc_flag, lc.continent AS lc_cont, lc.iso_code_3 AS lc_iso
            FROM competitions c
            JOIN leagues l ON l.league_id = c.league_id
            LEFT JOIN countries lc ON lc.country_id = l.country_id
            WHERE l.league_id = :league_id
              AND c.season_year = :season_year
              AND LOWER(COALESCE(c.stage, '')) NOT IN ('qualification', 'qualifiers')
            LIMIT 1
        ),

        -- National team for this country
        nat_team AS (
            SELECT t.team_id, t.name, t.common_name, t.short_name,
                   t.logo_url, t.level, t.type,
                   tc.country_id AS tc_id, tc.name AS tc_name,
                   tc.flag_url AS tc_flag, tc.continent AS tc_cont, tc.iso_code_3 AS tc_iso
            FROM teams t
            LEFT JOIN countries tc ON tc.country_id = t.country_id
            WHERE t.type = 'national'
              AND t.country_id = :country_id
            LIMIT 1
        ),

        -- Prefer 'final' squad type; fall back to 'preliminary'
        squad_type AS (
            SELECT CASE
                WHEN EXISTS (
                    SELECT 1 FROM squads sq
                    WHERE sq.team_id = (SELECT team_id FROM nat_team)
                      AND sq.season_year = :season_year
                      AND sq.type = 'final'
                ) THEN 'final'
                ELSE 'preliminary'
            END AS type_to_use
        ),

        -- Squad rows for this team/season/type
        squad_rows AS (
            SELECT sq.player_id, sq.number, sq.club_team_id,
                   sq.date_announced, sq.type, sq.manager_id
            FROM squads sq
            WHERE sq.team_id = (SELECT team_id FROM nat_team)
              AND sq.season_year = :season_year
              AND sq.type = (SELECT type_to_use FROM squad_type)
        ),

        -- Most common manager_id in the squad
        manager_pick AS (
            SELECT manager_id
            FROM squad_rows
            WHERE manager_id IS NOT NULL
            GROUP BY manager_id
            ORDER BY COUNT(*) DESC
            LIMIT 1
        ),

        -- Players JSON
        players_agg AS (
            SELECT COALESCE(json_agg(json_build_object(
                'player', json_build_object(
                    'player_id',       p.player_id,
                    'player_name',     p.player_name,
                    'age',             CASE WHEN p.dob IS NOT NULL
                                           THEN :season_year - EXTRACT(YEAR FROM p.dob)::int
                                           ELSE NULL END,
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
                'number', sr.number,
                'club_team', CASE WHEN ct.team_id IS NULL THEN NULL ELSE json_build_object(
                    'team_id',     ct.team_id,
                    'team_name',   ct.name,
                    'common_name', ct.common_name,
                    'short_name',  ct.short_name,
                    'logo_url',    ct.logo_url,
                    'level',       ct.level,
                    'type',        ct.type,
                    'country', CASE WHEN ctc.country_id IS NULL THEN NULL ELSE json_build_object(
                        'country_id', ctc.country_id, 'name', ctc.name,
                        'flag_url', ctc.flag_url, 'continent', ctc.continent,
                        'iso_code_3', ctc.iso_code_3) END
                ) END
            ) ORDER BY sr.number NULLS LAST), '[]'::json) AS players_json
            FROM squad_rows sr
            JOIN players p ON p.player_id = sr.player_id
            LEFT JOIN countries c1  ON c1.country_id  = p.country_id
            LEFT JOIN countries c2  ON c2.country_id  = p.country2_id
            LEFT JOIN teams ct      ON ct.team_id      = sr.club_team_id
            LEFT JOIN countries ctc ON ctc.country_id  = ct.country_id
        ),

        -- Manager JSON
        manager_info AS (
            SELECT CASE WHEN mgr.id IS NULL THEN NULL ELSE json_build_object(
                'manager_id', mgr.id,
                'name',       mgr.name,
                'country', CASE WHEN mc.country_id IS NULL THEN NULL ELSE json_build_object(
                    'country_id', mc.country_id, 'name', mc.name,
                    'flag_url', mc.flag_url, 'continent', mc.continent,
                    'iso_code_3', mc.iso_code_3) END
            ) END AS manager_json
            FROM manager_pick mp
            LEFT JOIN managers mgr ON mgr.id = mp.manager_id
            LEFT JOIN countries mc ON mc.country_id = mgr.country_id
        ),

        -- Matches for this team in the competition
        match_list AS (
            SELECT COALESCE(json_agg(json_build_object(
                'match_id',        mm.match_id,
                'match_date',      mm.match_date::text,
                'match_time_utc',  mm.match_time_utc::text,
                'home_team', json_build_object(
                    'team_id',     ht.team_id,   'team_name',   ht.name,
                    'common_name', ht.common_name, 'short_name', ht.short_name,
                    'logo_url',    ht.logo_url,  'level',       ht.level,
                    'type',        ht.type,
                    'country', CASE WHEN htc.country_id IS NULL THEN NULL ELSE json_build_object(
                        'country_id', htc.country_id, 'name', htc.name,
                        'flag_url', htc.flag_url, 'continent', htc.continent,
                        'iso_code_3', htc.iso_code_3) END
                ),
                'home_stats', json_build_object(
                    'goals', mm.home_goals, 'penalty_goals', mm.pen_home_goals,
                    'shots', NULL, 'possesion', NULL, 'offsides', NULL,
                    'corners', NULL, 'xg', NULL, 'pass_att', NULL,
                    'pass_succ', mm.home_pass_succ, 'league_rank', NULL
                ),
                'away_team', json_build_object(
                    'team_id',     awt.team_id,   'team_name',   awt.name,
                    'common_name', awt.common_name, 'short_name', awt.short_name,
                    'logo_url',    awt.logo_url,  'level',       awt.level,
                    'type',        awt.type,
                    'country', CASE WHEN atc.country_id IS NULL THEN NULL ELSE json_build_object(
                        'country_id', atc.country_id, 'name', atc.name,
                        'flag_url', atc.flag_url, 'continent', atc.continent,
                        'iso_code_3', atc.iso_code_3) END
                ),
                'away_stats', json_build_object(
                    'goals', mm.away_goals, 'penalty_goals', NULL,
                    'shots', NULL, 'possesion', NULL, 'offsides', NULL,
                    'corners', NULL, 'xg', NULL, 'pass_att', NULL,
                    'pass_succ', mm.away_pass_succ, 'league_rank', NULL
                ),
                'win_team_id',     mm.win_team,
                'loss_team_id',    mm.loss_team,
                'isdraw',          mm.isdraw,
                'pens',            mm.pens,
                'extra_time',      mm.extra_time,
                'isplayed',        mm.isplayed,
                'round',           mm.round,
                'gameweek_number', mm.gameweek_number,
                'stadium',         NULL
            ) ORDER BY mm.match_date), '[]'::json) AS matches_json
            FROM matches mm
            JOIN comp c ON c.competition_id = mm.comp_id
            JOIN nat_team nt ON (mm.home_id = nt.team_id OR mm.away_id = nt.team_id)
            JOIN teams ht           ON ht.team_id  = mm.home_id
            LEFT JOIN countries htc ON htc.country_id = ht.country_id
            JOIN teams awt          ON awt.team_id = mm.away_id
            LEFT JOIN countries atc ON atc.country_id = awt.country_id
        )

        SELECT json_build_object(
            'data', json_build_object(
                'team', json_build_object(
                    'team_id',     nt.team_id,
                    'team_name',   nt.name,
                    'common_name', nt.common_name,
                    'short_name',  nt.short_name,
                    'logo_url',    nt.logo_url,
                    'level',       nt.level,
                    'type',        nt.type,
                    'country', CASE WHEN nt.tc_id IS NULL THEN NULL ELSE json_build_object(
                        'country_id', nt.tc_id, 'name', nt.tc_name,
                        'flag_url', nt.tc_flag, 'continent', nt.tc_cont,
                        'iso_code_3', nt.tc_iso) END
                ),
                'competition', json_build_object(
                    'competition_id', c.competition_id,
                    'name',           c.name,
                    'season_year',    c.season_year,
                    'stage',          c.stage,
                    'logo_url',       c.logo_url,
                    'league', json_build_object(
                        'league_id',        c.league_id,
                        'league_name',      c.league_name,
                        'tier_level',       c.tier_level,
                        'format',           c.format,
                        'competiton_level', c.competition_level,
                        'scope',            c.scope,
                        'logo_url',         c.league_logo,
                        'country', CASE WHEN c.lc_id IS NULL THEN NULL ELSE json_build_object(
                            'country_id', c.lc_id, 'name', c.lc_name,
                            'flag_url', c.lc_flag, 'continent', c.lc_cont,
                            'iso_code_3', c.lc_iso) END
                    )
                ),
                'matches',        (SELECT matches_json FROM match_list),
                'players',        (SELECT players_json FROM players_agg),
                'manager',        (SELECT manager_json FROM manager_info),
                'date_announced', (SELECT MAX(date_announced::text) FROM squad_rows),
                'type',           (SELECT type_to_use FROM squad_type)
            )
        )
        FROM nat_team nt, comp c
    """)

    result = session.exec(query, params={
        "league_id": league_id,
        "season_year": season_year,
        "country_id": country_id,
    }).first()
    if not result or result[0].get("data") is None:
        raise HTTPException(status_code=404, detail="Squad not found")
    return result[0]

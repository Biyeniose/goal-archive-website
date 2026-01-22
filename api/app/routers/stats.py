import logging
from sqlalchemy import text
from typing import Annotated, List, Optional, Union
#import requests, randomz
from fastapi import APIRouter, Depends, Query, HTTPException
from datetime import date, timedelta, datetime, timezone

from ..dependencies import DBSession, get_logger

from ..models.stats import BestGamesResponse, InstaFollowersHistoryWithGamesResponse, LoanWatchResponse, PlayerSearchResponse, PlayerStatsDetailedResponse, PlayerStatsDetailedWithOppResponse, PlayerStatsTableResponse, SeasonStatsLeadersEnhancedResponse, SeasonStatsLeadersResponse, TeamSearchResponse, InstaFollowersResponse, InstaFollowersDecreaseResponse, InstaFollowersHistoryResponse

from ..models.response import NationDistResponse, TeamH2HResponse, PlayerWeeklyStats, PlayerRecordResponse, LeagueFormResponse, PlayerPerformance, PlayerPerformanceData, PlayerPerformanceResponse

# set up router
router = APIRouter(
    prefix="/v1/stats",
    tags=["stats"],
    #dependencies=[Depends(get_supabase_client)],
    #responses={404: {"description": "Not found"}},
)

LoggerDep = Annotated[logging.Logger, Depends(get_logger)]

# weekly performances leaders by league id + year
@router.get("/weekly-leaders/{league_id}", response_model=PlayerPerformanceResponse)
async def get_gameweek_stats_leaders(
    league_id: int,
    session: DBSession,
    league_ids: List[int] = Query([], description="List of league IDs"),
    start_date: str = Query("2025-11-07", description="Start date in YYYY-MM-DD format"),
    end_date: str = Query("2025-11-09", description="End date in YYYY-MM-DD format"),
    stat: str = Query("goals"),
    age: int = Query(80),
    limit: int = Query(15)
):
    # Combine the path parameter league_id with query parameter league_ids
    all_league_ids = [league_id] + league_ids
    
    query = text(f"""
        WITH player_data AS (
            SELECT 
                json_build_object(
                    'player', json_build_object(
                        'player_id', p.player_id,
                        'player_name', p.player_name,
                        'pixel_pic_url', p.pixel_pic_url,
                        'age', pms.age,
                        'position', pms.position,
                        'nations', json_build_object(
                            'country_id', p.country_id,
                            'country2_id', p.country2_id,
                            'country', c1.name,
                            'country2', c2.name,
                            'country_logo', c1.flag_url,
                            'country2_logo', c2.flag_url
                        ),
                        'team', json_build_object(
                            'team_id', pt.team_id,
                            'team_name', pt.name,
                            'logo_url', pt.logo_url
                        ),
                        'stats', to_json(pms.*)
                    ),
                    'match_info', json_build_object(
                        'match_id', m.match_id,
                        'comp_id', m.comp_id,
                        'match_date', m.match_date::text,
                        'round', m.round,
                        'season_year', comp.season_year,
                        'result_string', m.result_string,
                        'comp_name', comp.name,
                        'comp_logo', comp.logo_url,
                        'home_team', json_build_object(
                            'team_id', ht.team_id,
                            'team_name', ht.name,
                            'logo_url', ht.logo_url
                        ),
                        'home_goals', m.home_goals,
                        'pen_home_goals', m.pen_home_goals,
                        'away_team',
                        json_build_object(
                            'team_id', at.team_id,
                            'team_name', at.name,
                            'logo_url', at.logo_url
                        ),
                        'away_goals', m.away_goals,
                        'pen_away_goals', m.pen_away_goals
                    )
                ) as performance_data
            FROM player_match_stats pms
            JOIN players p ON pms.player_id = p.player_id
            JOIN countries c1 ON p.country_id = c1.country_id
            LEFT JOIN countries c2 ON p.country2_id = c2.country_id
            JOIN matches m ON pms.match_id = m.match_id
            JOIN teams pt ON pms.team_id = pt.team_id
            JOIN teams ht ON m.home_id = ht.team_id
            JOIN teams at ON m.away_id = at.team_id
            JOIN competitions comp ON m.comp_id = comp.competition_id
            WHERE m.match_date BETWEEN :start_date AND :end_date
                AND comp.league_id = ANY(:league_ids)
                AND comp.stage IN ('league', 'knockout phase')
                AND pms.{stat} IS NOT NULL
                AND pms.age <= :age 
            ORDER BY pms.{stat} DESC, pms.minutes DESC
            LIMIT :limit
        )
        SELECT json_build_object(
            'data', coalesce(json_agg(performance_data), '[]'::json)
        ) as result
        FROM player_data
    """)
    
    result = session.exec(query, params={
        "league_ids": all_league_ids,
        "start_date": start_date,
        "end_date": end_date,
        "age": age,
        "limit": limit
    }).first()
    
    return result[0] if result else {"data": []}


# get dom league player stats leaders by league id + year
@router.get("/stats-leaders/{league_id}", response_model=SeasonStatsLeadersResponse)
async def get_season_stats_leaders(
    league_id: int,
    session: DBSession,
    league_ids: List[int] = Query([], description="List of additional league IDs"),
    season_year: int = Query(2025, description="Year"),
    stat: str = Query("goals_assists_p90", description="Stat to order by (use _p90 suffix for per-90 stats)"),
    min_minutes: int = Query(450, description="Minimum minutes played"),
    max_age: int = Query(80, description="Maximum age"),
    country_id: Optional[int] = Query(None, description="Filter by country ID"),
    min_gp: Optional[int] = Query(None, description="Minimum games played"),
    limit: int = Query(20)
):
    # Combine the path parameter league_id with query parameter league_ids
    all_league_ids = [league_id] + league_ids
    
    # Determine if it's a per-90 stat to use weighted average or regular sum
    is_per90 = stat.endswith('_p90')
    
    if is_per90:
        stat_value = f"ROUND((SUM(pcs.{stat} * pcs.minutes) / NULLIF(SUM(pcs.minutes), 0))::numeric, 2)"
    else:
        stat_value = f"SUM(pcs.{stat})"
    
    # Build country filter conditionally
    country_filter = "AND p.country_id = :country_id" if country_id else ""
    
    # Build min_gp filter conditionally
    min_gp_having = "AND SUM(pcs.games_played) >= :min_gp" if min_gp else ""
    
    query = text(f"""
        WITH player_data AS (
            SELECT 
                json_build_object(
                    'player_name', p.player_name,
                    'player_id', p.player_id,
                    'position', p.position,
                    'height', p.height,
                    'age', MAX(pcs.age),
                    'mpg', ROUND(COALESCE(AVG(pcs.minutes_per_game), SUM(pcs.minutes)::numeric / NULLIF(SUM(pcs.games_played), 0))::numeric, 2),
                    'mins', SUM(pcs.minutes),
                    'games', SUM(pcs.games_played),
                    '{stat}', {stat_value},
                    'goals', SUM(pcs.goals),
                    'goals_p90', ROUND((SUM(pcs.goals_p90 * pcs.minutes) / NULLIF(SUM(pcs.minutes), 0))::numeric, 2),
                    'assists', SUM(pcs.assists),
                    'assists_p90', ROUND((SUM(pcs.assists_p90 * pcs.minutes) / NULLIF(SUM(pcs.minutes), 0))::numeric, 2),
                    'goals_assists', SUM(pcs.goals_assists),
                    'goals_assists_p90', ROUND((SUM(pcs.goals_assists_p90 * pcs.minutes) / NULLIF(SUM(pcs.minutes), 0))::numeric, 2),
                    'passes_completed', SUM(pcs.passes_completed),
                    'passes_completed_p90', ROUND((SUM(pcs.passes_completed_p90 * pcs.minutes) / NULLIF(SUM(pcs.minutes), 0))::numeric, 2),
                    'progressive_carries', SUM(pcs.progressive_carries),
                    'progressive_carries_p90', ROUND((SUM(pcs.progressive_carries_p90 * pcs.minutes) / NULLIF(SUM(pcs.minutes), 0))::numeric, 2),
                    'shots', SUM(pcs.shots),
                    'shots_p90', ROUND((SUM(pcs.shots_p90 * pcs.minutes) / NULLIF(SUM(pcs.minutes), 0))::numeric, 2),
                    'tackles', SUM(pcs.tackles),
                    'tackles_p90', ROUND((SUM(pcs.tackles_p90 * pcs.minutes) / NULLIF(SUM(pcs.minutes), 0))::numeric, 2),
                    'blocks', SUM(pcs.blocks),
                    'blocks_p90', ROUND((SUM(pcs.blocks_p90 * pcs.minutes) / NULLIF(SUM(pcs.minutes), 0))::numeric, 2),
                    'take_ons_won', SUM(pcs.take_ons_won),
                    'take_ons_won_p90', ROUND((SUM(pcs.take_ons_won_p90 * pcs.minutes) / NULLIF(SUM(pcs.minutes), 0))::numeric, 2),
                    'team', (ARRAY_AGG(t.name ORDER BY pcs.minutes DESC))[1],
                    'team_id', (ARRAY_AGG(t.team_id ORDER BY pcs.minutes DESC))[1],
                    'team_logo', (ARRAY_AGG(t.logo_url ORDER BY pcs.minutes DESC))[1],
                    'team2', (ARRAY_AGG(t.name ORDER BY pcs.minutes DESC))[2],
                    'team2_id', (ARRAY_AGG(t.team_id ORDER BY pcs.minutes DESC))[2],
                    'team2_logo', (ARRAY_AGG(t.logo_url ORDER BY pcs.minutes DESC))[2],
                    'country', c1.name,
                    'country_flag', c1.flag_url,
                    'country2', c2.name,
                    'country2_flag', c2.flag_url
                ) as player_data,
                {stat_value} as stat_value_for_order,
                SUM(pcs.goals_assists) as goals_assists_for_order
            FROM player_comp_stats pcs
            JOIN players p ON pcs.player_id = p.player_id
            LEFT JOIN teams t ON pcs.team_id = t.team_id
            LEFT JOIN countries c1 ON p.country_id = c1.country_id
            LEFT JOIN countries c2 ON p.country2_id = c2.country_id
            JOIN competitions comp ON pcs.competition_id = comp.competition_id
            WHERE comp.league_id = ANY(:league_ids)
                AND comp.season_year = :season_year
                AND pcs.{stat} IS NOT NULL
                AND pcs.{stat} > 0
                AND pcs.minutes >= :min_minutes
                AND pcs.age <= :max_age
                {country_filter}
            GROUP BY p.player_id, p.player_name, p.position, p.height, c1.name, c1.flag_url, c2.name, c2.flag_url
            HAVING SUM(pcs.minutes) >= :min_minutes
                {min_gp_having}
            ORDER BY stat_value_for_order DESC, goals_assists_for_order DESC
            LIMIT :limit
        )
        SELECT json_build_object(
            'data', coalesce(json_agg(player_data), '[]'::json)
        ) as result
        FROM player_data
    """)
    
    # Build params dict conditionally
    params = {
        "league_ids": all_league_ids,
        "season_year": season_year,
        "min_minutes": min_minutes,
        "max_age": max_age,
        "limit": limit
    }
    
    if country_id:
        params["country_id"] = country_id
    
    if min_gp:
        params["min_gp"] = min_gp
    
    result = session.exec(query, params=params).first()
    
    return result[0] if result else {"data": []}

# version that uses date range instead of league
@router.get("/stats-leaders-bydate/{league_id}", response_model=SeasonStatsLeadersEnhancedResponse)
async def get_season_stats_leaders_by_dates(
    league_id: int,
    session: DBSession,
    league_ids: List[int] = Query([], description="List of additional league IDs"),
    start_date: date = Query(date(2025, 1, 1), description="Start date in YYYY-MM-DD format"),
    end_date: date = Query(date(2025, 11, 26), description="End date in YYYY-MM-DD format"),
    stat: str = Query("goals", description="Stat to order by (use _p90 suffix for per-90 stats)"),
    min_minutes: int = Query(0, description="Minimum minutes played"),
    country_id: Optional[int] = Query(None, description="Filter by country ID"),
    min_gp: Optional[int] = Query(None, description="Minimum games played"),
    limit: int = Query(20)
):
    # Combine the path parameter league_id with query parameter league_ids
    all_league_ids = [league_id] + league_ids
    
    # Determine if it's a per-90 stat to use weighted average or regular sum
    is_per90 = stat.endswith('_p90')
    
    # Map the stat name (remove _p90 suffix if present for the actual column)
    base_stat = stat.replace('_p90', '') if is_per90 else stat
    
    if is_per90:
        stat_value = f"ROUND((SUM(pms.{base_stat}) / NULLIF((SUM(pms.minutes)::numeric / 90.0), 0))::numeric, 2)"
    else:
        stat_value = f"SUM(pms.{base_stat})"
    
    # Build country filter conditionally
    country_filter = "AND p.country_id = :country_id" if country_id else ""
    
    # Build min_gp filter conditionally
    min_gp_having = "AND COUNT(DISTINCT pms.match_id) >= :min_gp" if min_gp else ""
    
    query = text(f"""
        WITH player_teams AS (
            SELECT DISTINCT
                pms.player_id,
                COALESCE(pms.team_id, 0) as team_id,
                COALESCE(t.name, 'Unknown Team') as team_name,
                t.logo_url,
                SUM(pms.minutes) as total_minutes
            FROM player_match_stats pms
            JOIN matches m ON pms.match_id = m.match_id
            JOIN competitions comp ON m.comp_id = comp.competition_id
            LEFT JOIN teams t ON pms.team_id = t.team_id
            WHERE comp.league_id = ANY(:league_ids)
                AND m.match_date BETWEEN :start_date AND :end_date
            GROUP BY pms.player_id, pms.team_id, t.name, t.logo_url
        ),
        player_teams_agg AS (
            SELECT 
                player_id,
                json_agg(
                    json_build_object(
                        'team_id', team_id,
                        'team_name', team_name,
                        'logo_url', logo_url
                    )
                    ORDER BY total_minutes DESC
                ) as teams
            FROM player_teams
            GROUP BY player_id
        ),
        player_ga_against AS (
            SELECT 
                pms.player_id,
                CASE 
                    WHEN pms.team_id = m.home_id THEN m.away_id
                    ELSE m.home_id
                END as opponent_team_id,
                SUM(pms.goals) as goals,
                SUM(pms.assists) as assists
            FROM player_match_stats pms
            JOIN matches m ON pms.match_id = m.match_id
            JOIN competitions comp ON m.comp_id = comp.competition_id
            WHERE comp.league_id = ANY(:league_ids)
                AND m.match_date BETWEEN :start_date AND :end_date
                -- AND pms.minutes >= 1
            GROUP BY pms.player_id, opponent_team_id
            HAVING SUM(pms.goals) > 0 OR SUM(pms.assists) > 0
        ),
        player_ga_against_agg AS (
            SELECT
                pga.player_id,
                json_agg(
                    json_build_object(
                        'team', json_build_object(
                            'team_id', COALESCE(t.team_id, 0),
                            'team_name', COALESCE(t.name, 'Unknown'),
                            'logo_url', t.logo_url
                        ),
                        'stats', json_build_object(
                            'goals', pga.goals,
                            'assists', pga.assists,
                            'goals_assists', pga.goals + pga.assists
                        )
                    )
                    ORDER BY (pga.goals + pga.assists) DESC, pga.goals DESC
                ) as ga_against
            FROM player_ga_against pga
            LEFT JOIN teams t ON pga.opponent_team_id = t.team_id
            GROUP BY pga.player_id
        ),
        player_matches AS (
            SELECT
                pms.player_id,
                pms.match_id,
                m.comp_id,
                m.match_date,
                m.round,
                comp.season_year,
                m.result_string,
                comp.name as comp_name,
                comp.logo_url as comp_logo,
                json_build_object(
                    'team_id', COALESCE(ht.team_id, 0),
                    'team_name', COALESCE(ht.name, 'Unknown'),
                    'logo_url', ht.logo_url
                ) as home_team,
                json_build_object(
                    'team_id', COALESCE(at.team_id, 0),
                    'team_name', COALESCE(at.name, 'Unknown'),
                    'logo_url', at.logo_url
                ) as away_team,
                json_build_object(
                    'team_id', COALESCE(pt.team_id, 0),
                    'team_name', COALESCE(pt.name, 'Unknown'),
                    'logo_url', pt.logo_url
                ) as player_team,
                pms.position,
                pms.goals,
                pms.assists,
                pms.shots,
                pms.sca,
                pms.xg,
                pms.xg_assist
            FROM player_match_stats pms
            JOIN matches m ON pms.match_id = m.match_id
            JOIN competitions comp ON m.comp_id = comp.competition_id
            LEFT JOIN teams ht ON m.home_id = ht.team_id
            LEFT JOIN teams at ON m.away_id = at.team_id
            LEFT JOIN teams pt ON pms.team_id = pt.team_id
            WHERE comp.league_id = ANY(:league_ids)
                AND m.match_date BETWEEN :start_date AND :end_date
                --AND pms.minutes >= 1
        ),
        player_matches_agg AS (
            SELECT 
                player_id,
                json_agg(
                    json_build_object(
                        'match_info', json_build_object(
                            'match_id', match_id,
                            'comp_id', comp_id,
                            'match_date', match_date,
                            'round', round,
                            'season_year', season_year,
                            'result_string', result_string,
                            'comp_name', comp_name,
                            'comp_logo', comp_logo,
                            'home_team', home_team,
                            'away_team', away_team
                        ),
                        'stats', json_build_object(
                            'team', player_team,
                            'position', position,
                            'goals', goals,
                            'assists', assists,
                            'shots', shots,
                            'sca', sca,
                            'xg', xg,
                            'xg_assist', xg_assist
                        )
                    )
                    ORDER BY match_date DESC
                ) as matches
            FROM player_matches
            GROUP BY player_id
        ),
        player_data_base AS (
            SELECT 
                p.player_id,
                p.player_name,
                p.position,
                p.height,
                MAX(pms.age) as age,
                ROUND((SUM(pms.minutes)::numeric / NULLIF(COUNT(DISTINCT pms.match_id), 0))::numeric, 2) as mpg,
                SUM(pms.minutes) as mins,
                COUNT(DISTINCT pms.match_id) as games,
                {stat_value} as stat_value,
                SUM(pms.goals) as goals,
                ROUND((SUM(pms.goals) / NULLIF((SUM(pms.minutes)::numeric / 90.0), 0))::numeric, 2) as goals_p90,
                SUM(pms.assists) as assists,
                ROUND((SUM(pms.assists) / NULLIF((SUM(pms.minutes)::numeric / 90.0), 0))::numeric, 2) as assists_p90,
                SUM(pms.goals_assists) as goals_assists,
                ROUND((SUM(pms.goals_assists) / NULLIF((SUM(pms.minutes)::numeric / 90.0), 0))::numeric, 2) as goals_assists_p90,
                SUM(pms.passes_completed) as passes_completed,
                ROUND((SUM(pms.passes_completed) / NULLIF((SUM(pms.minutes)::numeric / 90.0), 0))::numeric, 2) as passes_completed_p90,
                SUM(pms.progressive_carries) as progressive_carries,
                ROUND((SUM(pms.progressive_carries) / NULLIF((SUM(pms.minutes)::numeric / 90.0), 0))::numeric, 2) as progressive_carries_p90,
                SUM(pms.shots) as shots,
                ROUND((SUM(pms.shots) / NULLIF((SUM(pms.minutes)::numeric / 90.0), 0))::numeric, 2) as shots_p90,
                SUM(pms.tackles) as tackles,
                ROUND((SUM(pms.tackles) / NULLIF((SUM(pms.minutes)::numeric / 90.0), 0))::numeric, 2) as tackles_p90,
                SUM(pms.blocks) as blocks,
                ROUND((SUM(pms.blocks) / NULLIF((SUM(pms.minutes)::numeric / 90.0), 0))::numeric, 2) as blocks_p90,
                SUM(pms.take_ons_won) as take_ons_won,
                ROUND((SUM(pms.take_ons_won) / NULLIF((SUM(pms.minutes)::numeric / 90.0), 0))::numeric, 2) as take_ons_won_p90,
                c1.name as country,
                c1.flag_url as country_flag,
                c2.name as country2,
                c2.flag_url as country2_flag,
                {stat_value} as stat_value_for_order,
                SUM(pms.goals_assists) as goals_assists_for_order
            FROM player_match_stats pms
            JOIN players p ON pms.player_id = p.player_id
            JOIN matches m ON pms.match_id = m.match_id
            JOIN competitions comp ON m.comp_id = comp.competition_id
            LEFT JOIN countries c1 ON p.country_id = c1.country_id
            LEFT JOIN countries c2 ON p.country2_id = c2.country_id
            WHERE comp.league_id = ANY(:league_ids)
                AND m.match_date BETWEEN :start_date AND :end_date
                --AND pms.minutes >= 1
                {country_filter}
            GROUP BY p.player_id, p.player_name, p.position, p.height, c1.name, c1.flag_url, c2.name, c2.flag_url
            HAVING COALESCE(SUM(pms.minutes), 0) >= :min_minutes
                AND {stat_value} > 0

        ),
        player_data AS (
            SELECT 
                json_build_object(
                    'player_name', pdb.player_name,
                    'player_id', pdb.player_id,
                    'position', pdb.position,
                    'height', pdb.height,
                    'age', pdb.age,
                    'mpg', pdb.mpg,
                    'mins', pdb.mins,
                    'games', pdb.games,
                    '{stat}', pdb.stat_value,
                    'goals', pdb.goals,
                    'goals_p90', pdb.goals_p90,
                    'assists', pdb.assists,
                    'assists_p90', pdb.assists_p90,
                    'goals_assists', pdb.goals_assists,
                    'goals_assists_p90', pdb.goals_assists_p90,
                    'passes_completed', pdb.passes_completed,
                    'passes_completed_p90', pdb.passes_completed_p90,
                    'progressive_carries', pdb.progressive_carries,
                    'progressive_carries_p90', pdb.progressive_carries_p90,
                    'shots', pdb.shots,
                    'shots_p90', pdb.shots_p90,
                    'tackles', pdb.tackles,
                    'tackles_p90', pdb.tackles_p90,
                    'blocks', pdb.blocks,
                    'blocks_p90', pdb.blocks_p90,
                    'take_ons_won', pdb.take_ons_won,
                    'take_ons_won_p90', pdb.take_ons_won_p90,
                    'teams', COALESCE(pta.teams, '[]'::json),
                    'ga_against', COALESCE(pgaa.ga_against, '[]'::json),
                    'matches', COALESCE(pma.matches, '[]'::json),
                    'country', pdb.country,
                    'country_flag', pdb.country_flag,
                    'country2', pdb.country2,
                    'country2_flag', pdb.country2_flag
                ) as player_data,
                pdb.stat_value_for_order,
                pdb.goals_assists_for_order
            FROM player_data_base pdb
            LEFT JOIN player_teams_agg pta ON pdb.player_id = pta.player_id
            LEFT JOIN player_ga_against_agg pgaa ON pdb.player_id = pgaa.player_id
            LEFT JOIN player_matches_agg pma ON pdb.player_id = pma.player_id
            ORDER BY pdb.stat_value_for_order DESC, pdb.goals_assists_for_order DESC
            LIMIT :limit
        )
        SELECT json_build_object(
            'data', coalesce(json_agg(player_data), '[]'::json)
        ) as result
        FROM player_data
    """)
    
    # Build params dict conditionally (removed max_age)
    params = {
        "league_ids": all_league_ids,
        "start_date": start_date,
        "end_date": end_date,
        "min_minutes": min_minutes,
        "limit": limit
    }
    
    if country_id:
        params["country_id"] = country_id
    
    if min_gp:
        params["min_gp"] = min_gp
    
    result = session.exec(query, params=params).first()
    
    return result[0] if result else {"data": []}

# version that uses date range instead of league
@router.get("/stats-leaders-bydate-2/{league_id}", response_model=SeasonStatsLeadersEnhancedResponse)
async def get_season_stats_leaders_by_dates_ga(
    league_id: int,
    session: DBSession,
    league_ids: List[int] = Query([], description="List of additional league IDs"),
    start_date: date = Query(date(2025, 1, 1), description="Start date in YYYY-MM-DD format"),
    end_date: date = Query(date(2025, 11, 26), description="End date in YYYY-MM-DD format"),
    stat: str = Query("goals", description="Stat to order by (use _p90 suffix for per-90 stats)"),
    min_minutes: int = Query(1, description="Minimum minutes played"),
    max_age: int = Query(80, description="Maximum age"),
    country_id: Optional[int] = Query(None, description="Filter by country ID"),
    min_gp: Optional[int] = Query(None, description="Minimum games played"),
    limit: int = Query(20)
):
    # Combine the path parameter league_id with query parameter league_ids
    all_league_ids = [league_id] + league_ids
    
    # Determine if it's a per-90 stat to use weighted average or regular sum
    is_per90 = stat.endswith('_p90')
    
    # Map the stat name (remove _p90 suffix if present for the actual column)
    base_stat = stat.replace('_p90', '') if is_per90 else stat
    
    if is_per90:
        stat_value = f"ROUND((SUM(pms.{base_stat}) / NULLIF((SUM(pms.minutes)::numeric / 90.0), 0))::numeric, 2)"
    else:
        stat_value = f"SUM(pms.{base_stat})"
    
    # Build country filter conditionally
    country_filter = "AND p.country_id = :country_id" if country_id else ""
    
    # Build min_gp filter conditionally
    min_gp_having = "AND COUNT(DISTINCT pms.match_id) >= :min_gp" if min_gp else ""
    
    query = text(f"""
        WITH player_teams AS (
            SELECT DISTINCT
                pms.player_id,
                COALESCE(pms.team_id, 0) as team_id,
                COALESCE(t.name, 'Unknown Team') as team_name,
                t.logo_url,
                SUM(pms.minutes) as total_minutes
            FROM player_match_stats pms
            JOIN matches m ON pms.match_id = m.match_id
            JOIN competitions comp ON m.comp_id = comp.competition_id
            LEFT JOIN teams t ON pms.team_id = t.team_id
            WHERE comp.league_id = ANY(:league_ids)
                AND m.match_date BETWEEN :start_date AND :end_date
            GROUP BY pms.player_id, pms.team_id, t.name, t.logo_url
        ),
        player_teams_agg AS (
            SELECT 
                player_id,
                json_agg(
                    json_build_object(
                        'team_id', team_id,
                        'team_name', team_name,
                        'logo_url', logo_url
                    )
                    ORDER BY total_minutes DESC
                ) as teams
            FROM player_teams
            GROUP BY player_id
        ),
        player_ga_against AS (
            SELECT 
                pms.player_id,
                CASE 
                    WHEN pms.team_id = m.home_id THEN m.away_id
                    ELSE m.home_id
                END as opponent_team_id,
                SUM(pms.goals) as goals,
                SUM(pms.assists) as assists
            FROM player_match_stats pms
            JOIN matches m ON pms.match_id = m.match_id
            JOIN competitions comp ON m.comp_id = comp.competition_id
            WHERE comp.league_id = ANY(:league_ids)
                AND m.match_date BETWEEN :start_date AND :end_date
                AND pms.minutes >= 1
            GROUP BY pms.player_id, opponent_team_id
            HAVING SUM(pms.goals) > 0 OR SUM(pms.assists) > 0
        ),
        player_ga_against_agg AS (
            SELECT
                pga.player_id,
                json_agg(
                    json_build_object(
                        'team', json_build_object(
                            'team_id', COALESCE(t.team_id, 0),
                            'team_name', COALESCE(t.name, 'Unknown'),
                            'logo_url', t.logo_url
                        ),
                        'stats', json_build_object(
                            'goals', pga.goals,
                            'assists', pga.assists,
                            'goals_assists', pga.goals + pga.assists
                        )
                    )
                    ORDER BY (pga.goals + pga.assists) DESC, pga.goals DESC
                ) as ga_against
            FROM player_ga_against pga
            LEFT JOIN teams t ON pga.opponent_team_id = t.team_id
            GROUP BY pga.player_id
        ),
        player_data_base AS (
            SELECT 
                p.player_id,
                p.player_name,
                p.position,
                p.height,
                MAX(pms.age) as age,
                ROUND((SUM(pms.minutes)::numeric / NULLIF(COUNT(DISTINCT pms.match_id), 0))::numeric, 2) as mpg,
                SUM(pms.minutes) as mins,
                COUNT(DISTINCT pms.match_id) as games,
                {stat_value} as stat_value,
                SUM(pms.goals) as goals,
                ROUND((SUM(pms.goals) / NULLIF((SUM(pms.minutes)::numeric / 90.0), 0))::numeric, 2) as goals_p90,
                SUM(pms.assists) as assists,
                ROUND((SUM(pms.assists) / NULLIF((SUM(pms.minutes)::numeric / 90.0), 0))::numeric, 2) as assists_p90,
                SUM(pms.goals_assists) as goals_assists,
                ROUND((SUM(pms.goals_assists) / NULLIF((SUM(pms.minutes)::numeric / 90.0), 0))::numeric, 2) as goals_assists_p90,
                SUM(pms.passes_completed) as passes_completed,
                ROUND((SUM(pms.passes_completed) / NULLIF((SUM(pms.minutes)::numeric / 90.0), 0))::numeric, 2) as passes_completed_p90,
                SUM(pms.progressive_carries) as progressive_carries,
                ROUND((SUM(pms.progressive_carries) / NULLIF((SUM(pms.minutes)::numeric / 90.0), 0))::numeric, 2) as progressive_carries_p90,
                SUM(pms.shots) as shots,
                ROUND((SUM(pms.shots) / NULLIF((SUM(pms.minutes)::numeric / 90.0), 0))::numeric, 2) as shots_p90,
                SUM(pms.tackles) as tackles,
                ROUND((SUM(pms.tackles) / NULLIF((SUM(pms.minutes)::numeric / 90.0), 0))::numeric, 2) as tackles_p90,
                SUM(pms.blocks) as blocks,
                ROUND((SUM(pms.blocks) / NULLIF((SUM(pms.minutes)::numeric / 90.0), 0))::numeric, 2) as blocks_p90,
                SUM(pms.take_ons_won) as take_ons_won,
                ROUND((SUM(pms.take_ons_won) / NULLIF((SUM(pms.minutes)::numeric / 90.0), 0))::numeric, 2) as take_ons_won_p90,
                c1.name as country,
                c1.flag_url as country_flag,
                c2.name as country2,
                c2.flag_url as country2_flag,
                {stat_value} as stat_value_for_order,
                SUM(pms.goals_assists) as goals_assists_for_order
            FROM player_match_stats pms
            JOIN players p ON pms.player_id = p.player_id
            JOIN matches m ON pms.match_id = m.match_id
            JOIN competitions comp ON m.comp_id = comp.competition_id
            LEFT JOIN countries c1 ON p.country_id = c1.country_id
            LEFT JOIN countries c2 ON p.country2_id = c2.country_id
            WHERE comp.league_id = ANY(:league_ids)
                AND m.match_date BETWEEN :start_date AND :end_date
                AND pms.minutes >= 1
                AND pms.age <= :max_age
                {country_filter}
            GROUP BY p.player_id, p.player_name, p.position, p.height, c1.name, c1.flag_url, c2.name, c2.flag_url
            HAVING SUM(pms.minutes) >= :min_minutes
                AND {stat_value} > 0
                {min_gp_having}
        ),
        player_data AS (
            SELECT 
                json_build_object(
                    'player_name', pdb.player_name,
                    'player_id', pdb.player_id,
                    'position', pdb.position,
                    'height', pdb.height,
                    'age', pdb.age,
                    'mpg', pdb.mpg,
                    'mins', pdb.mins,
                    'games', pdb.games,
                    '{stat}', pdb.stat_value,
                    'goals', pdb.goals,
                    'goals_p90', pdb.goals_p90,
                    'assists', pdb.assists,
                    'assists_p90', pdb.assists_p90,
                    'goals_assists', pdb.goals_assists,
                    'goals_assists_p90', pdb.goals_assists_p90,
                    'passes_completed', pdb.passes_completed,
                    'passes_completed_p90', pdb.passes_completed_p90,
                    'progressive_carries', pdb.progressive_carries,
                    'progressive_carries_p90', pdb.progressive_carries_p90,
                    'shots', pdb.shots,
                    'shots_p90', pdb.shots_p90,
                    'tackles', pdb.tackles,
                    'tackles_p90', pdb.tackles_p90,
                    'blocks', pdb.blocks,
                    'blocks_p90', pdb.blocks_p90,
                    'take_ons_won', pdb.take_ons_won,
                    'take_ons_won_p90', pdb.take_ons_won_p90,
                    'teams', COALESCE(pta.teams, '[]'::json),
                    'ga_against', COALESCE(pgaa.ga_against, '[]'::json),
                    'country', pdb.country,
                    'country_flag', pdb.country_flag,
                    'country2', pdb.country2,
                    'country2_flag', pdb.country2_flag
                ) as player_data,
                pdb.stat_value_for_order,
                pdb.goals_assists_for_order
            FROM player_data_base pdb
            LEFT JOIN player_teams_agg pta ON pdb.player_id = pta.player_id
            LEFT JOIN player_ga_against_agg pgaa ON pdb.player_id = pgaa.player_id
            ORDER BY pdb.stat_value_for_order DESC, pdb.goals_assists_for_order DESC
            LIMIT :limit
        )
        SELECT json_build_object(
            'data', coalesce(json_agg(player_data), '[]'::json)
        ) as result
        FROM player_data
    """)
    
    # Build params dict conditionally
    params = {
        "league_ids": all_league_ids,
        "start_date": start_date,
        "end_date": end_date,
        "min_minutes": min_minutes,
        "max_age": max_age,
        "limit": limit
    }
    
    if country_id:
        params["country_id"] = country_id
    
    if min_gp:
        params["min_gp"] = min_gp
    
    result = session.exec(query, params=params).first()
    
    return result[0] if result else {"data": []}

# get stats ALL comps
@router.get("/players-allcomps/{season_year}", response_model=SeasonStatsLeadersResponse)
async def get_season_stats_leaders_all_comps(
    season_year: int,
    session: DBSession,
    stat: str = Query("goals", description="Stat to order by (use _p90 suffix for per-90 stats)"),
    min_minutes: int = Query(450, description="Minimum minutes played"),
    max_age: int = Query(80, description="Maximum age"),
    country_id: Optional[int] = Query(None, description="Filter by country ID"),
    limit: int = Query(10)
):
    # Determine if it's a per-90 stat to use weighted average or regular sum
    is_per90 = stat.endswith('_p90')
    
    if is_per90:
        stat_value = f"ROUND((SUM(pcs.{stat} * pcs.minutes) / NULLIF(SUM(pcs.minutes), 0))::numeric, 2)"
    else:
        stat_value = f"SUM(pcs.{stat})"
    
    # Build country filter conditionally
    country_filter = "AND p.country_id = :country_id" if country_id else ""
    
    query = text(f"""
        WITH player_data AS (
            SELECT 
                json_build_object(
                    'player_name', p.player_name,
                    'player_id', p.player_id,
                    'position', p.position,
                    'height', p.height,
                    'age', MAX(pcs.age),
                    'mpg', ROUND(COALESCE(AVG(pcs.minutes_per_game), SUM(pcs.minutes)::numeric / NULLIF(SUM(pcs.games_played), 0))::numeric, 2),
                    'mins', SUM(pcs.minutes),
                    'games', SUM(pcs.games_played),
                    '{stat}', {stat_value},
                    'goals', SUM(pcs.goals),
                    'goals_p90', ROUND((SUM(pcs.goals_p90 * pcs.minutes) / NULLIF(SUM(pcs.minutes), 0))::numeric, 2),
                    'assists', SUM(pcs.assists),
                    'assists_p90', ROUND((SUM(pcs.assists_p90 * pcs.minutes) / NULLIF(SUM(pcs.minutes), 0))::numeric, 2),
                    'goals_assists', SUM(pcs.goals_assists),
                    'goals_assists_p90', ROUND((SUM(pcs.goals_assists_p90 * pcs.minutes) / NULLIF(SUM(pcs.minutes), 0))::numeric, 2),
                    'passes_completed', SUM(pcs.passes_completed),
                    'passes_completed_p90', ROUND((SUM(pcs.passes_completed_p90 * pcs.minutes) / NULLIF(SUM(pcs.minutes), 0))::numeric, 2),
                    'progressive_carries', SUM(pcs.progressive_carries),
                    'progressive_carries_p90', ROUND((SUM(pcs.progressive_carries_p90 * pcs.minutes) / NULLIF(SUM(pcs.minutes), 0))::numeric, 2),
                    'shots', SUM(pcs.shots),
                    'shots_p90', ROUND((SUM(pcs.shots_p90 * pcs.minutes) / NULLIF(SUM(pcs.minutes), 0))::numeric, 2),
                    'tackles', SUM(pcs.tackles),
                    'tackles_p90', ROUND((SUM(pcs.tackles_p90 * pcs.minutes) / NULLIF(SUM(pcs.minutes), 0))::numeric, 2),
                    'blocks', SUM(pcs.blocks),
                    'blocks_p90', ROUND((SUM(pcs.blocks_p90 * pcs.minutes) / NULLIF(SUM(pcs.minutes), 0))::numeric, 2),
                    'take_ons_won', SUM(pcs.take_ons_won),
                    'take_ons_won_p90', ROUND((SUM(pcs.take_ons_won_p90 * pcs.minutes) / NULLIF(SUM(pcs.minutes), 0))::numeric, 2),
                    'team', (ARRAY_AGG(t.name ORDER BY pcs.minutes DESC))[1],
                    'team_id', (ARRAY_AGG(t.team_id ORDER BY pcs.minutes DESC))[1],
                    'team_logo', (ARRAY_AGG(t.logo_url ORDER BY pcs.minutes DESC))[1],
                    'team2', (ARRAY_AGG(t.name ORDER BY pcs.minutes DESC))[2],
                    'team2_id', (ARRAY_AGG(t.team_id ORDER BY pcs.minutes DESC))[2],
                    'team2_logo', (ARRAY_AGG(t.logo_url ORDER BY pcs.minutes DESC))[2],
                    'country', c1.name,
                    'country_flag', c1.flag_url,
                    'country2', c2.name,
                    'country2_flag', c2.flag_url
                ) as player_data
            FROM player_comp_stats pcs
            JOIN players p ON pcs.player_id = p.player_id
            LEFT JOIN teams t ON pcs.team_id = t.team_id
            LEFT JOIN countries c1 ON p.country_id = c1.country_id
            LEFT JOIN countries c2 ON p.country2_id = c2.country_id
            JOIN competitions comp ON pcs.competition_id = comp.competition_id
            WHERE comp.season_year = :season_year
                AND pcs.{stat} IS NOT NULL
                AND pcs.{stat} > 0
                AND pcs.minutes >= :min_minutes
                AND pcs.age <= :max_age
                {country_filter}
            GROUP BY p.player_id, p.player_name, p.position, p.height, c1.name, c1.flag_url, c2.name, c2.flag_url
            HAVING SUM(pcs.minutes) >= :min_minutes
            ORDER BY {stat_value} DESC
            LIMIT :limit
        )
        SELECT json_build_object(
            'data', coalesce(json_agg(player_data), '[]'::json)
        ) as result
        FROM player_data
    """)
    
    # Build params dict conditionally
    params = {
        "season_year": season_year,
        "min_minutes": min_minutes,
        "max_age": max_age,
        "limit": limit
    }
    
    if country_id:
        params["country_id"] = country_id
    
    result = session.exec(query, params=params).first()
    
    return result[0] if result else {"data": []}

# get player stats against a certain team
@router.get("/players-records/{player_id}", response_model=PlayerRecordResponse)
async def get_player_record_against_team(
    player_id: int,
    session: DBSession,
    team_id: int = Query(10, description="ID of the opponent team"),
    end_year: int = Query(2019, description="Minimum season year to include (e.g., 2019 includes 2019 and later)")
):
    """
    Get a player's performance statistics against a specific team.
    """
    
    query = text("""
    WITH player_matches AS (
        SELECT 
            pms.player_id,
            p.player_name,
            p.tfm_pic_url,
            pms.match_id,
            m.match_time_utc,
            comp.name AS comp_name,
            comp.season_year,
            m.result_string,
            home_team.team_id AS home_id,
            home_team.name AS home_name,
            home_team.logo_url AS home_logo,
            away_team.team_id AS away_id,
            away_team.name AS away_name,
            away_team.logo_url AS away_logo,
            pms.started,
            pms.subbed_on,
            pms.subbed_off,
            pms.minutes,
            pms.goals,
            pms.assists,
            pms.goals_assists,
            pms.xg,
            pms.xg_assist,
            pms.xga,
            pms.npxg,
            pms.pens_made,
            pms.pens_att,
            pms.shots,
            pms.shots_on_target,
            pms.touches,
            pms.touches_def_pen_area,
            pms.touches_def_3rd,
            pms.touches_mid_3rd,
            pms.touches_att_3rd,
            pms.touches_att_pen_area,
            pms.tackles,
            pms.tackles_won,
            pms.tackles_def_3rd,
            pms.tackles_mid_3rd,
            pms.tackles_att_3rd,
            pms.challenges,
            pms.challenges_lost,
            pms.blocks,
            pms.blocked_shots,
            pms.blocked_passes,
            pms.interceptions,
            pms.clearances,
            pms.errors,
            pms.sca,
            pms.gca,
            pms.passes_completed,
            pms.passes,
            pms.passes_pct,
            pms.progressive_passes,
            pms.carries,
            pms.progressive_carries,
            pms.carries_distance,
            pms.carries_progressive_distance,
            pms.carries_into_final_third,
            pms.carries_into_penalty_area,
            pms.miscontrols,
            pms.dispossessed,
            pms.passes_received,
            pms.progressive_passes_received,
            pms.take_ons,
            pms.take_ons_won,
            pms.take_ons_won_pct,
            pms.take_ons_tackled,
            pms.take_ons_tackled_pct,
            pms.passes_total_distance,
            pms.passes_progressive_distance,
            pms.passes_long,
            pms.passes_completed_long,
            pms.passes_medium,
            pms.passes_completed_medium,
            pms.passes_short,
            pms.passes_completed_short,
            pms.assisted_shots,
            pms.passes_into_final_third,
            pms.passes_into_penalty_area,
            pms.crosses_into_penalty_area,
            pms.passes_live,
            pms.passes_dead,
            pms.through_balls,
            pms.passes_switches,
            pms.passes_offsides,
            pms.passes_blocked,
            pms.crosses,
            pms.throw_ins,
            pms.corner_kicks,
            pms.cards_yellow,
            pms.cards_red,
            pms.cards_yellow_red,
            pms.fouls,
            pms.fouled,
            pms.offsides,
            pms.pens_won,
            pms.pens_conceded,
            pms.own_goals,
            pms.ball_recoveries,
            pms.aerials_won,
            pms.aerials_lost,
            pms.aerials_won_pct,
            pms.position,
            pms.age,
            pms.value,
            pms.gk_shots_on_target_against,
            pms.gk_goals_against,
            pms.gk_saves,
            pms.gk_save_pct,
            pms.gk_psxg,
            pms.gk_passes_completed_launched,
            pms.gk_passes_launched,
            pms.gk_passes_pct_launched,
            pms.gk_passes,
            pms.gk_passes_throws,
            pms.gk_pct_passes_launched,
            pms.gk_passes_length_avg,
            pms.gk_goal_kicks,
            pms.gk_pct_goal_kicks_launched,
            pms.gk_goal_kick_length_avg,
            pms.gk_crosses,
            pms.gk_crosses_stopped,
            pms.gk_crosses_stopped_pct,
            pms.gk_def_actions_outside_pen_area
        FROM player_match_stats pms
        JOIN players p ON pms.player_id = p.player_id
        JOIN matches m ON pms.match_id = m.match_id
        JOIN competitions comp ON m.comp_id = comp.competition_id
        JOIN teams home_team ON m.home_id = home_team.team_id
        JOIN teams away_team ON m.away_id = away_team.team_id
        WHERE pms.player_id = :player_id
            AND (m.home_id = :team_id OR m.away_id = :team_id)
            AND pms.team_id != :team_id
            AND comp.season_year >= :end_year
    )
    SELECT jsonb_build_object(
        'data', jsonb_build_object(
            'player_name', MAX(player_name),
            'player_id', MAX(player_id),
            'tfm_pic_url', MAX(tfm_pic_url),
            
            'matches_stats', (
                SELECT jsonb_agg(
                    jsonb_build_object(
                        'match_id', match_id,
                        'match_time_utc', match_time_utc,
                        'comp_name', comp_name,
                        'season_year', season_year,
                        'result_string', result_string,
                        'home_id', home_id,
                        'home_name', home_name,
                        'home_logo', home_logo,
                        'away_id', away_id,
                        'away_name', away_name,
                        'away_logo', away_logo,
                        'stats', jsonb_build_object(
                            'started', started,
                            'subbed_on', subbed_on,
                            'subbed_off', subbed_off,
                            'minutes', minutes,
                            'position', position,
                            'age', age
                        ) || jsonb_build_object(
                            'goals', goals,
                            'assists', assists,
                            'goals_assists', goals_assists,
                            'xg', xg,
                            'xg_assist', xg_assist,
                            'xga', xga,
                            'npxg', npxg,
                            'pens_made', pens_made,
                            'pens_att', pens_att
                        ) || jsonb_build_object(
                            'shots', shots,
                            'shots_on_target', shots_on_target,
                            'touches', touches,
                            'touches_def_pen_area', touches_def_pen_area,
                            'touches_def_3rd', touches_def_3rd,
                            'touches_mid_3rd', touches_mid_3rd,
                            'touches_att_3rd', touches_att_3rd,
                            'touches_att_pen_area', touches_att_pen_area
                        ) || jsonb_build_object(
                            'tackles', tackles,
                            'tackles_won', tackles_won,
                            'tackles_def_3rd', tackles_def_3rd,
                            'tackles_mid_3rd', tackles_mid_3rd,
                            'tackles_att_3rd', tackles_att_3rd,
                            'challenges', challenges,
                            'challenges_lost', challenges_lost,
                            'blocks', blocks,
                            'blocked_shots', blocked_shots,
                            'blocked_passes', blocked_passes
                        ) || jsonb_build_object(
                            'interceptions', interceptions,
                            'clearances', clearances,
                            'errors', errors,
                            'sca', sca,
                            'gca', gca,
                            'passes_completed', passes_completed,
                            'passes', passes,
                            'passes_pct', passes_pct,
                            'progressive_passes', progressive_passes
                        ) || jsonb_build_object(
                            'carries', carries,
                            'progressive_carries', progressive_carries,
                            'carries_distance', carries_distance,
                            'carries_progressive_distance', carries_progressive_distance,
                            'carries_into_final_third', carries_into_final_third,
                            'carries_into_penalty_area', carries_into_penalty_area,
                            'miscontrols', miscontrols,
                            'dispossessed', dispossessed
                        ) || jsonb_build_object(
                            'passes_received', passes_received,
                            'progressive_passes_received', progressive_passes_received,
                            'take_ons', take_ons,
                            'take_ons_won', take_ons_won,
                            'take_ons_won_pct', take_ons_won_pct,
                            'take_ons_tackled', take_ons_tackled,
                            'take_ons_tackled_pct', take_ons_tackled_pct,
                            'passes_total_distance', passes_total_distance,
                            'passes_progressive_distance', passes_progressive_distance
                        ) || jsonb_build_object(
                            'passes_long', passes_long,
                            'passes_completed_long', passes_completed_long,
                            'passes_medium', passes_medium,
                            'passes_completed_medium', passes_completed_medium,
                            'passes_short', passes_short,
                            'passes_completed_short', passes_completed_short,
                            'assisted_shots', assisted_shots,
                            'passes_into_final_third', passes_into_final_third,
                            'passes_into_penalty_area', passes_into_penalty_area
                        ) || jsonb_build_object(
                            'crosses_into_penalty_area', crosses_into_penalty_area,
                            'passes_live', passes_live,
                            'passes_dead', passes_dead,
                            'through_balls', through_balls,
                            'passes_switches', passes_switches,
                            'passes_offsides', passes_offsides,
                            'passes_blocked', passes_blocked,
                            'crosses', crosses,
                            'throw_ins', throw_ins
                        ) || jsonb_build_object(
                            'corner_kicks', corner_kicks,
                            'cards_yellow', cards_yellow,
                            'cards_red', cards_red,
                            'cards_yellow_red', cards_yellow_red,
                            'fouls', fouls,
                            'fouled', fouled,
                            'offsides', offsides,
                            'pens_won', pens_won,
                            'pens_conceded', pens_conceded
                        ) || jsonb_build_object(
                            'own_goals', own_goals,
                            'ball_recoveries', ball_recoveries,
                            'aerials_won', aerials_won,
                            'aerials_lost', aerials_lost,
                            'aerials_won_pct', aerials_won_pct,
                            'value', value
                        ) || jsonb_build_object(
                            'gk_shots_on_target_against', gk_shots_on_target_against,
                            'gk_goals_against', gk_goals_against,
                            'gk_saves', gk_saves,
                            'gk_save_pct', gk_save_pct,
                            'gk_psxg', gk_psxg,
                            'gk_passes_completed_launched', gk_passes_completed_launched,
                            'gk_passes_launched', gk_passes_launched,
                            'gk_passes_pct_launched', gk_passes_pct_launched
                        ) || jsonb_build_object(
                            'gk_passes', gk_passes,
                            'gk_passes_throws', gk_passes_throws,
                            'gk_pct_passes_launched', gk_pct_passes_launched,
                            'gk_passes_length_avg', gk_passes_length_avg,
                            'gk_goal_kicks', gk_goal_kicks,
                            'gk_pct_goal_kicks_launched', gk_pct_goal_kicks_launched,
                            'gk_goal_kick_length_avg', gk_goal_kick_length_avg,
                            'gk_crosses', gk_crosses,
                            'gk_crosses_stopped', gk_crosses_stopped,
                            'gk_crosses_stopped_pct', gk_crosses_stopped_pct,
                            'gk_def_actions_outside_pen_area', gk_def_actions_outside_pen_area
                        )
                    )
                    ORDER BY match_time_utc DESC
                )
                FROM player_matches
            ),
            
            'total', jsonb_build_object(
                'matches_played', COUNT(DISTINCT match_id),
                'total_minutes', SUM(minutes),
                'goals', SUM(goals),
                'assists', SUM(assists),
                'goals_assists', SUM(goals_assists)
            ) || jsonb_build_object(
                'xg', ROUND(SUM(xg)::numeric, 2),
                'xg_assist', ROUND(SUM(xg_assist)::numeric, 2),
                'xga', ROUND(SUM(xga)::numeric, 2),
                'npxg', ROUND(SUM(npxg)::numeric, 2),
                'pens_made', SUM(pens_made),
                'pens_att', SUM(pens_att)
            ) || jsonb_build_object(
                'shots', SUM(shots),
                'shots_on_target', SUM(shots_on_target),
                'touches', SUM(touches),
                'touches_def_pen_area', SUM(touches_def_pen_area),
                'touches_def_3rd', SUM(touches_def_3rd),
                'touches_mid_3rd', SUM(touches_mid_3rd)
            ) || jsonb_build_object(
                'touches_att_3rd', SUM(touches_att_3rd),
                'touches_att_pen_area', SUM(touches_att_pen_area),
                'tackles', SUM(tackles),
                'tackles_won', SUM(tackles_won),
                'tackles_def_3rd', SUM(tackles_def_3rd),
                'tackles_mid_3rd', SUM(tackles_mid_3rd)
            ) || jsonb_build_object(
                'tackles_att_3rd', SUM(tackles_att_3rd),
                'challenges', SUM(challenges),
                'challenges_lost', SUM(challenges_lost),
                'blocks', SUM(blocks),
                'blocked_shots', SUM(blocked_shots),
                'blocked_passes', SUM(blocked_passes)
            ) || jsonb_build_object(
                'interceptions', SUM(interceptions),
                'clearances', SUM(clearances),
                'errors', SUM(errors),
                'sca', SUM(sca),
                'gca', SUM(gca),
                'passes_completed', SUM(passes_completed)
            ) || jsonb_build_object(
                'passes', SUM(passes),
                'passes_pct', ROUND((SUM(passes_completed)::numeric / NULLIF(SUM(passes), 0)) * 100, 2),
                'progressive_passes', SUM(progressive_passes),
                'carries', SUM(carries),
                'progressive_carries', SUM(progressive_carries),
                'carries_distance', SUM(carries_distance)
            ) || jsonb_build_object(
                'carries_progressive_distance', SUM(carries_progressive_distance),
                'carries_into_final_third', SUM(carries_into_final_third),
                'carries_into_penalty_area', SUM(carries_into_penalty_area),
                'miscontrols', SUM(miscontrols),
                'dispossessed', SUM(dispossessed),
                'passes_received', SUM(passes_received)
            ) || jsonb_build_object(
                'progressive_passes_received', SUM(progressive_passes_received),
                'take_ons', SUM(take_ons),
                'take_ons_won', SUM(take_ons_won),
                'take_ons_won_pct', ROUND((SUM(take_ons_won)::numeric / NULLIF(SUM(take_ons), 0)) * 100, 2),
                'take_ons_tackled', SUM(take_ons_tackled),
                'take_ons_tackled_pct', ROUND((SUM(take_ons_tackled)::numeric / NULLIF(SUM(take_ons), 0)) * 100, 2)
            ) || jsonb_build_object(
                'passes_total_distance', SUM(passes_total_distance),
                'passes_progressive_distance', SUM(passes_progressive_distance),
                'passes_long', SUM(passes_long),
                'passes_completed_long', SUM(passes_completed_long),
                'passes_medium', SUM(passes_medium),
                'passes_completed_medium', SUM(passes_completed_medium)
            ) || jsonb_build_object(
                'passes_short', SUM(passes_short),
                'passes_completed_short', SUM(passes_completed_short),
                'assisted_shots', SUM(assisted_shots),
                'passes_into_final_third', SUM(passes_into_final_third),
                'passes_into_penalty_area', SUM(passes_into_penalty_area),
                'crosses_into_penalty_area', SUM(crosses_into_penalty_area)
            ) || jsonb_build_object(
                'passes_live', SUM(passes_live),
                'passes_dead', SUM(passes_dead),
                'through_balls', SUM(through_balls),
                'passes_switches', SUM(passes_switches),
                'passes_offsides', SUM(passes_offsides),
                'passes_blocked', SUM(passes_blocked)
            ) || jsonb_build_object(
                'crosses', SUM(crosses),
                'throw_ins', SUM(throw_ins),
                'corner_kicks', SUM(corner_kicks),
                'cards_yellow', SUM(cards_yellow),
                'cards_red', SUM(cards_red),
                'cards_yellow_red', SUM(cards_yellow_red)
            ) || jsonb_build_object(
                'fouls', SUM(fouls),
                'fouled', SUM(fouled),
                'offsides', SUM(offsides),
                'pens_won', SUM(pens_won),
                'pens_conceded', SUM(pens_conceded),
                'own_goals', SUM(own_goals)
            ) || jsonb_build_object(
                'ball_recoveries', SUM(ball_recoveries),
                'aerials_won', SUM(aerials_won),
                'aerials_lost', SUM(aerials_lost),
                'aerials_won_pct', ROUND((SUM(aerials_won)::numeric / NULLIF(SUM(aerials_won) + SUM(aerials_lost), 0)) * 100, 2),
                'gk_shots_on_target_against', SUM(gk_shots_on_target_against),
                'gk_goals_against', SUM(gk_goals_against)
            ) || jsonb_build_object(
                'gk_saves', SUM(gk_saves),
                'gk_save_pct', ROUND((SUM(gk_saves)::numeric / NULLIF(SUM(gk_shots_on_target_against), 0)) * 100, 2),
                'gk_psxg', ROUND(SUM(gk_psxg)::numeric, 2)
            ),
            
            'p90_stats', jsonb_build_object(
                'goals_p90', ROUND((SUM(goals)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'assists_p90', ROUND((SUM(assists)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'goals_assists_p90', ROUND((SUM(goals_assists)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'xg_p90', ROUND((SUM(xg)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'xg_assist_p90', ROUND((SUM(xg_assist)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2)
            ) || jsonb_build_object(
                'xga_p90', ROUND((SUM(xga)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'npxg_p90', ROUND((SUM(npxg)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'pens_made_p90', ROUND((SUM(pens_made)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'pens_att_p90', ROUND((SUM(pens_att)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'shots_p90', ROUND((SUM(shots)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2)
            ) || jsonb_build_object(
                'shots_on_target_p90', ROUND((SUM(shots_on_target)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'touches_p90', ROUND((SUM(touches)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'touches_def_pen_area_p90', ROUND((SUM(touches_def_pen_area)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'touches_def_3rd_p90', ROUND((SUM(touches_def_3rd)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'touches_mid_3rd_p90', ROUND((SUM(touches_mid_3rd)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2)
            ) || jsonb_build_object(
                'touches_att_3rd_p90', ROUND((SUM(touches_att_3rd)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'touches_att_pen_area_p90', ROUND((SUM(touches_att_pen_area)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'tackles_p90', ROUND((SUM(tackles)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'tackles_won_p90', ROUND((SUM(tackles_won)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'tackles_def_3rd_p90', ROUND((SUM(tackles_def_3rd)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2)
            ) || jsonb_build_object(
                'tackles_mid_3rd_p90', ROUND((SUM(tackles_mid_3rd)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'tackles_att_3rd_p90', ROUND((SUM(tackles_att_3rd)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'challenges_p90', ROUND((SUM(challenges)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'challenges_lost_p90', ROUND((SUM(challenges_lost)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'blocks_p90', ROUND((SUM(blocks)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2)
            ) || jsonb_build_object(
                'blocked_shots_p90', ROUND((SUM(blocked_shots)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'blocked_passes_p90', ROUND((SUM(blocked_passes)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'interceptions_p90', ROUND((SUM(interceptions)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'clearances_p90', ROUND((SUM(clearances)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'errors_p90', ROUND((SUM(errors)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2)
            ) || jsonb_build_object(
                'sca_p90', ROUND((SUM(sca)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'gca_p90', ROUND((SUM(gca)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'passes_completed_p90', ROUND((SUM(passes_completed)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'passes_p90', ROUND((SUM(passes)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'progressive_passes_p90', ROUND((SUM(progressive_passes)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2)
            ) || jsonb_build_object(
                'carries_p90', ROUND((SUM(carries)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'progressive_carries_p90', ROUND((SUM(progressive_carries)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'carries_distance_p90', ROUND((SUM(carries_distance)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'carries_progressive_distance_p90', ROUND((SUM(carries_progressive_distance)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'carries_into_final_third_p90', ROUND((SUM(carries_into_final_third)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2)
            ) || jsonb_build_object(
                'carries_into_penalty_area_p90', ROUND((SUM(carries_into_penalty_area)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'miscontrols_p90', ROUND((SUM(miscontrols)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'dispossessed_p90', ROUND((SUM(dispossessed)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'passes_received_p90', ROUND((SUM(passes_received)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'progressive_passes_received_p90', ROUND((SUM(progressive_passes_received)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2)
            ) || jsonb_build_object(
                'take_ons_p90', ROUND((SUM(take_ons)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'take_ons_won_p90', ROUND((SUM(take_ons_won)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'take_ons_tackled_p90', ROUND((SUM(take_ons_tackled)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'passes_total_distance_p90', ROUND((SUM(passes_total_distance)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'passes_progressive_distance_p90', ROUND((SUM(passes_progressive_distance)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2)
            ) || jsonb_build_object(
                'passes_long_p90', ROUND((SUM(passes_long)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'passes_completed_long_p90', ROUND((SUM(passes_completed_long)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'passes_medium_p90', ROUND((SUM(passes_medium)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'passes_completed_medium_p90', ROUND((SUM(passes_completed_medium)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'passes_short_p90', ROUND((SUM(passes_short)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2)
            ) || jsonb_build_object(
                'passes_completed_short_p90', ROUND((SUM(passes_completed_short)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'assisted_shots_p90', ROUND((SUM(assisted_shots)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'passes_into_final_third_p90', ROUND((SUM(passes_into_final_third)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'passes_into_penalty_area_p90', ROUND((SUM(passes_into_penalty_area)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'crosses_into_penalty_area_p90', ROUND((SUM(crosses_into_penalty_area)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2)
            ) || jsonb_build_object(
                'passes_live_p90', ROUND((SUM(passes_live)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'passes_dead_p90', ROUND((SUM(passes_dead)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'through_balls_p90', ROUND((SUM(through_balls)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'passes_switches_p90', ROUND((SUM(passes_switches)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'passes_offsides_p90', ROUND((SUM(passes_offsides)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2)
            ) || jsonb_build_object(
                'passes_blocked_p90', ROUND((SUM(passes_blocked)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'crosses_p90', ROUND((SUM(crosses)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'throw_ins_p90', ROUND((SUM(throw_ins)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'corner_kicks_p90', ROUND((SUM(corner_kicks)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'cards_yellow_p90', ROUND((SUM(cards_yellow)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2)
            ) || jsonb_build_object(
                'cards_red_p90', ROUND((SUM(cards_red)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'cards_yellow_red_p90', ROUND((SUM(cards_yellow_red)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'fouls_p90', ROUND((SUM(fouls)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'fouled_p90', ROUND((SUM(fouled)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'offsides_p90', ROUND((SUM(offsides)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2)
            ) || jsonb_build_object(
                'pens_won_p90', ROUND((SUM(pens_won)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'pens_conceded_p90', ROUND((SUM(pens_conceded)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'own_goals_p90', ROUND((SUM(own_goals)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'ball_recoveries_p90', ROUND((SUM(ball_recoveries)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'aerials_won_p90', ROUND((SUM(aerials_won)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2)
            ) || jsonb_build_object(
                'aerials_lost_p90', ROUND((SUM(aerials_lost)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'gk_shots_on_target_against_p90', ROUND((SUM(gk_shots_on_target_against)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'gk_goals_against_p90', ROUND((SUM(gk_goals_against)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'gk_saves_p90', ROUND((SUM(gk_saves)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2),
                'gk_psxg_p90', ROUND((SUM(gk_psxg)::numeric / NULLIF(SUM(minutes), 0)) * 90, 2)
            )
        )
    ) AS result
    FROM player_matches;
    """)
    
    result = session.exec(query, params={"player_id": player_id, "team_id": team_id, "end_year": end_year}).first()
    
    return result[0] if result else {"data": {}}

# get team Dom league ranks between a time period
@router.get("/ranks/{stat}/{league_id}", response_model=LeagueFormResponse)
async def get_form_by_dates2(
    stat: str,
    league_id: int,
    session: DBSession,
    league_ids: List[int] = Query([], description="List of additional league IDs"),
    start_date: date = Query("2025-10-01", description="Start date in YYYY-MM-DD format"),
    end_date: date = Query("2025-12-31", description="End date in YYYY-MM-DD format"),
    max_gp: Optional[int] = Query(None, description="Maximum games per team (most recent matches)"),
    order: str = Query("desc", description="Order direction: 'asc' or 'desc'"),
):
    # Combine the path parameter league_id with query parameter league_ids
    all_league_ids = [league_id] + league_ids
    
    # Build the max_gp filter conditionally
    max_gp_filter = ""
    if max_gp is not None:
        max_gp_filter = "AND match_rank <= :max_gp"
    
    # Determine order direction
    order_direction = "DESC" if order.lower() == "desc" else "ASC"
    
    query = text(f"""
        WITH team_matches_raw AS (
            -- Get ALL matches within the date range for each team
            SELECT 
                t.team_id,
                t.name as team_name,
                t.logo_url as logo,
                m.match_id,
                m.match_date,
                CASE 
                    WHEN t.team_id = m.home_id THEN m.home_goals
                    ELSE m.away_goals
                END as goals_for,
                CASE 
                    WHEN t.team_id = m.home_id THEN m.away_goals
                    ELSE m.home_goals
                END as goals_against,
                CASE 
                    WHEN (t.team_id = m.home_id AND m.home_goals > m.away_goals) OR 
                        (t.team_id = m.away_id AND m.away_goals > m.home_goals) THEN 3
                    WHEN m.home_goals = m.away_goals THEN 1
                    ELSE 0
                END as points,
                CASE 
                    WHEN (t.team_id = m.home_id AND m.home_goals > m.away_goals) OR 
                        (t.team_id = m.away_id AND m.away_goals > m.home_goals) THEN 1
                    ELSE 0
                END as wins,
                CASE 
                    WHEN m.home_goals = m.away_goals THEN 1
                    ELSE 0
                END as draws,
                CASE 
                    WHEN (t.team_id = m.home_id AND m.home_goals < m.away_goals) OR 
                        (t.team_id = m.away_id AND m.away_goals < m.home_goals) THEN 1
                    ELSE 0
                END as losses,
                -- Rank matches by date (most recent first) for each team
                ROW_NUMBER() OVER (
                    PARTITION BY t.team_id 
                    ORDER BY m.match_date DESC, m.match_id DESC
                ) as match_rank
            FROM matches m
            JOIN competitions c ON m.comp_id = c.competition_id
            JOIN teams t ON (t.team_id = m.home_id OR t.team_id = m.away_id)
            WHERE c.league_id = ANY(:league_ids)
            AND m.isplayed = TRUE
            AND m.match_date BETWEEN :start_date AND :end_date
        ),
        
        team_matches AS (
            -- Filter to max_gp most recent matches if specified
            SELECT *
            FROM team_matches_raw
            WHERE 1=1
            {max_gp_filter}
        ),

        form_stats AS (
            -- Calculate stats for the filtered matches
            SELECT 
                team_id,
                team_name,
                logo,
                COUNT(DISTINCT match_id) as gp,
                SUM(points) as points,
                SUM(wins) as wins,
                SUM(draws) as draws,
                SUM(losses) as losses,
                SUM(goals_for) as goals_f,
                SUM(goals_against) as goals_a,
                SUM(goals_for) - SUM(goals_against) as gd
            FROM team_matches
            GROUP BY team_id, team_name, logo
            HAVING COUNT(DISTINCT match_id) > 0
        ),

        ranked_form AS (
            SELECT 
                team_id,
                team_name,
                logo,
                points,
                gp,
                wins,
                draws,
                losses,
                goals_f,
                goals_a,
                gd,
                ROW_NUMBER() OVER (
                    ORDER BY 
                        CASE WHEN :stat = 'points' THEN points END {order_direction},
                        CASE WHEN :stat = 'goals_f' THEN goals_f END {order_direction},
                        CASE WHEN :stat = 'goals_a' THEN goals_a END {order_direction},
                        CASE WHEN :stat = 'gd' THEN gd END {order_direction},
                        CASE WHEN :stat = 'wins' THEN wins END {order_direction},
                        CASE WHEN :stat = 'losses' THEN losses END {order_direction},
                        CASE WHEN :stat = 'draws' THEN draws END {order_direction},
                        gd DESC, 
                        goals_f DESC, 
                        team_name ASC
                ) as rank
            FROM form_stats
        )

        SELECT 
            json_build_object(
                'data', json_build_object(
                    'form', COALESCE(
                        json_agg(
                            json_build_object(
                                'team', json_build_object(
                                    'team_id', team_id,
                                    'team_name', team_name,
                                    'logo_url', logo
                                ),
                                'rank', rank::text,
                                'info', NULL,
                                'points', points,
                                'gp', gp,
                                'gd', gd,
                                'wins', wins,
                                'losses', losses,
                                'draws', draws,
                                'goals_f', goals_f,
                                'goals_a', goals_a
                            )
                            ORDER BY rank
                        ),
                        '[]'::json
                    )
                )
            ) as result
        FROM ranked_form
    """)
    
    # Build params dict
    params = {
        "league_ids": all_league_ids,
        "start_date": start_date,
        "end_date": end_date,
        "stat": stat
    }
    
    if max_gp is not None:
        params["max_gp"] = max_gp
    
    result = session.exec(query, params=params).first()
    
    return result[0] if result else {"data": {"form": []}}

# h2h
@router.get("/h2h/{team1_id}/{team2_id}", response_model=TeamH2HResponse)
async def get_team_h2h(
    team1_id: int,
    team2_id: int,
    session: DBSession,
    match_limit: int = Query(5, description="Number of recent matches to retrieve")
):
    query = text("""
    WITH match_data AS (
        SELECT 
            m.match_id,
            m.match_time_utc,
            m.result_string,
            comp.name AS competition_name,
            comp.season_year,
            home_team.name AS home_team_name,
            home_team.team_id AS home_team_id,
            home_team.logo_url AS home_team_logo,
            away_team.name AS away_team_name,
            away_team.team_id AS away_team_id,
            away_team.logo_url AS away_team_logo,
            m.home_goals,
            m.away_goals,
            m.home_poss,
            m.away_poss,
            m.home_fouls,
            m.away_fouls,
            m.home_corners,
            m.away_corners,
            m.home_offsides,
            m.away_offsides,
            m.home_xg,
            m.away_xg,
            m.home_touches,
            m.away_touches,
            m.home_clearances,
            m.away_clearances,
            m.home_interceptions,
            m.away_interceptions,
            m.home_tackles,
            m.away_tackles,
            m.home_saves_succ,
            m.home_saves_att,
            m.away_saves_succ,
            m.away_saves_att,
            m.home_shots_att,
            m.home_shots_succ,
            m.away_shots_succ,
            m.away_shots_att,
            m.home_pass_att,
            m.away_pass_att,
            m.home_pass_succ,
            m.away_pass_succ
        FROM matches m
        JOIN teams home_team ON m.home_id = home_team.team_id
        JOIN teams away_team ON m.away_id = away_team.team_id
        JOIN competitions comp ON m.comp_id = comp.competition_id
        WHERE ((m.home_id = :team1_id AND m.away_id = :team2_id)
           OR (m.home_id = :team2_id AND m.away_id = :team1_id))
           AND m.isplayed is TRUE
        ORDER BY m.match_time_utc DESC
        LIMIT :match_limit
    ),
    team_totals AS (
        SELECT
            :team1_id AS team_id,
            MAX(CASE WHEN home_team_id = :team1_id THEN home_team_name ELSE away_team_name END) AS team_name,
            MAX(CASE WHEN home_team_id = :team1_id THEN home_team_logo ELSE away_team_logo END) AS logo_url,
            SUM(CASE WHEN home_team_id = :team1_id THEN home_goals ELSE away_goals END) AS goals,
            SUM(CASE WHEN home_team_id = :team1_id THEN away_goals ELSE home_goals END) AS goals_conceded,
            ROUND(AVG(CASE WHEN home_team_id = :team1_id THEN home_poss ELSE away_poss END)::numeric, 2) AS poss,
            SUM(CASE WHEN home_team_id = :team1_id THEN home_fouls ELSE away_fouls END) AS fouls,
            SUM(CASE WHEN home_team_id = :team1_id THEN home_corners ELSE away_corners END) AS corners,
            SUM(CASE WHEN home_team_id = :team1_id THEN home_offsides ELSE away_offsides END) AS offsides,
            ROUND(SUM(CASE WHEN home_team_id = :team1_id THEN home_xg ELSE away_xg END)::numeric, 2) AS xg,
            ROUND(SUM(CASE WHEN home_team_id = :team1_id THEN away_xg ELSE home_xg END)::numeric, 2) AS xg_conceded,
            SUM(CASE WHEN home_team_id = :team1_id THEN home_touches ELSE away_touches END) AS touches,
            SUM(CASE WHEN home_team_id = :team1_id THEN home_clearances ELSE away_clearances END) AS clearances,
            SUM(CASE WHEN home_team_id = :team1_id THEN home_interceptions ELSE away_interceptions END) AS interceptions,
            SUM(CASE WHEN home_team_id = :team1_id THEN home_tackles ELSE away_tackles END) AS tackles,
            SUM(CASE WHEN home_team_id = :team1_id THEN home_saves_succ ELSE away_saves_succ END) AS saves_succ,
            SUM(CASE WHEN home_team_id = :team1_id THEN home_saves_att ELSE away_saves_att END) AS saves_att,
            SUM(CASE WHEN home_team_id = :team1_id THEN home_shots_att ELSE away_shots_att END) AS shots_att,
            SUM(CASE WHEN home_team_id = :team1_id THEN home_shots_succ ELSE away_shots_succ END) AS shots_succ,
            SUM(CASE WHEN home_team_id = :team1_id THEN home_pass_att ELSE away_pass_att END) AS pass_att,
            SUM(CASE WHEN home_team_id = :team1_id THEN home_pass_succ ELSE away_pass_succ END) AS pass_succ,
            ROUND((SUM(CASE WHEN home_team_id = :team1_id THEN home_pass_succ ELSE away_pass_succ END)::numeric / 
                   NULLIF(SUM(CASE WHEN home_team_id = :team1_id THEN home_pass_att ELSE away_pass_att END), 0)) * 100, 2) AS pass_pct,
            COUNT(*) AS matches_played
        FROM match_data
        
        UNION ALL
        
        SELECT
            :team2_id AS team_id,
            MAX(CASE WHEN home_team_id = :team2_id THEN home_team_name ELSE away_team_name END) AS team_name,
            MAX(CASE WHEN home_team_id = :team2_id THEN home_team_logo ELSE away_team_logo END) AS logo_url,
            SUM(CASE WHEN home_team_id = :team2_id THEN home_goals ELSE away_goals END) AS goals,
            SUM(CASE WHEN home_team_id = :team2_id THEN away_goals ELSE home_goals END) AS goals_conceded,
            ROUND(AVG(CASE WHEN home_team_id = :team2_id THEN home_poss ELSE away_poss END)::numeric, 2) AS poss,
            SUM(CASE WHEN home_team_id = :team2_id THEN home_fouls ELSE away_fouls END) AS fouls,
            SUM(CASE WHEN home_team_id = :team2_id THEN home_corners ELSE away_corners END) AS corners,
            SUM(CASE WHEN home_team_id = :team2_id THEN home_offsides ELSE away_offsides END) AS offsides,
            ROUND(SUM(CASE WHEN home_team_id = :team2_id THEN home_xg ELSE away_xg END)::numeric, 2) AS xg,
            ROUND(SUM(CASE WHEN home_team_id = :team2_id THEN away_xg ELSE home_xg END)::numeric, 2) AS xg_conceded,
            SUM(CASE WHEN home_team_id = :team2_id THEN home_touches ELSE away_touches END) AS touches,
            SUM(CASE WHEN home_team_id = :team2_id THEN home_clearances ELSE away_clearances END) AS clearances,
            SUM(CASE WHEN home_team_id = :team2_id THEN home_interceptions ELSE away_interceptions END) AS interceptions,
            SUM(CASE WHEN home_team_id = :team2_id THEN home_tackles ELSE away_tackles END) AS tackles,
            SUM(CASE WHEN home_team_id = :team2_id THEN home_saves_succ ELSE away_saves_succ END) AS saves_succ,
            SUM(CASE WHEN home_team_id = :team2_id THEN home_saves_att ELSE away_saves_att END) AS saves_att,
            SUM(CASE WHEN home_team_id = :team2_id THEN home_shots_att ELSE away_shots_att END) AS shots_att,
            SUM(CASE WHEN home_team_id = :team2_id THEN home_shots_succ ELSE away_shots_succ END) AS shots_succ,
            SUM(CASE WHEN home_team_id = :team2_id THEN home_pass_att ELSE away_pass_att END) AS pass_att,
            SUM(CASE WHEN home_team_id = :team2_id THEN home_pass_succ ELSE away_pass_succ END) AS pass_succ,
            ROUND((SUM(CASE WHEN home_team_id = :team2_id THEN home_pass_succ ELSE away_pass_succ END)::numeric / 
                   NULLIF(SUM(CASE WHEN home_team_id = :team2_id THEN home_pass_att ELSE away_pass_att END), 0)) * 100, 2) AS pass_pct,
            COUNT(*) AS matches_played
        FROM match_data
    ),
    per_match_averages AS (
        SELECT
            team_id,
            team_name,
            logo_url,
            matches_played,
            ROUND((goals::numeric / NULLIF(matches_played, 0)), 2) AS goals_per_match,
            ROUND((goals_conceded::numeric / NULLIF(matches_played, 0)), 2) AS goals_conceded_per_match,
            poss AS avg_poss,
            ROUND((fouls::numeric / NULLIF(matches_played, 0)), 2) AS fouls_per_match,
            ROUND((corners::numeric / NULLIF(matches_played, 0)), 2) AS corners_per_match,
            ROUND((offsides::numeric / NULLIF(matches_played, 0)), 2) AS offsides_per_match,
            ROUND((xg / NULLIF(matches_played, 0)), 2) AS xg_per_match,
            ROUND((xg_conceded / NULLIF(matches_played, 0)), 2) AS xg_conceded_per_match,
            ROUND((touches::numeric / NULLIF(matches_played, 0)), 2) AS touches_per_match,
            ROUND((clearances::numeric / NULLIF(matches_played, 0)), 2) AS clearances_per_match,
            ROUND((interceptions::numeric / NULLIF(matches_played, 0)), 2) AS interceptions_per_match,
            ROUND((tackles::numeric / NULLIF(matches_played, 0)), 2) AS tackles_per_match,
            ROUND((saves_succ::numeric / NULLIF(matches_played, 0)), 2) AS saves_succ_per_match,
            ROUND((saves_att::numeric / NULLIF(matches_played, 0)), 2) AS saves_att_per_match,
            ROUND((shots_att::numeric / NULLIF(matches_played, 0)), 2) AS shots_att_per_match,
            ROUND((shots_succ::numeric / NULLIF(matches_played, 0)), 2) AS shots_succ_per_match,
            ROUND((pass_att::numeric / NULLIF(matches_played, 0)), 2) AS pass_att_per_match,
            ROUND((pass_succ::numeric / NULLIF(matches_played, 0)), 2) AS pass_succ_per_match,
            pass_pct
        FROM team_totals
    ),
    combined_totals AS (
        SELECT
            COUNT(*) AS matches_played,
            SUM(home_goals + away_goals) AS total_goals,
            SUM(home_corners + away_corners) AS total_corners,
            SUM(home_fouls + away_fouls) AS total_fouls,
            SUM(home_offsides + away_offsides) AS total_offsides,
            ROUND(SUM(home_xg + away_xg)::numeric, 2) AS total_xg,
            SUM(home_shots_att + away_shots_att) AS total_shots_att,
            SUM(home_shots_succ + away_shots_succ) AS total_shots_succ
        FROM match_data
    )
    SELECT jsonb_build_object(
        'data', jsonb_build_object(
            'matches', (
                SELECT jsonb_agg(
                    jsonb_build_object(
                        'match_info', jsonb_build_object(
                            'match_id', match_id,
                            'match_time_utc', match_time_utc,
                            'result_string', result_string,
                            'competition_name', competition_name,
                            'season_year', season_year,
                            'home_team_name', home_team_name,
                            'home_team_id', home_team_id,
                            'home_team_logo', home_team_logo,
                            'away_team_name', away_team_name,
                            'away_team_id', away_team_id,
                            'away_team_logo', away_team_logo
                        ),
                        'stats', jsonb_build_object(
                            'home_goals', home_goals,
                            'away_goals', away_goals,
                            'home_poss', home_poss,
                            'away_poss', away_poss,
                            'home_fouls', home_fouls,
                            'away_fouls', away_fouls,
                            'home_corners', home_corners,
                            'away_corners', away_corners,
                            'home_offsides', home_offsides,
                            'away_offsides', away_offsides,
                            'home_xg', home_xg,
                            'away_xg', away_xg,
                            'home_touches', home_touches,
                            'away_touches', away_touches,
                            'home_clearances', home_clearances,
                            'away_clearances', away_clearances,
                            'home_interceptions', home_interceptions,
                            'away_interceptions', away_interceptions,
                            'home_tackles', home_tackles,
                            'away_tackles', away_tackles,
                            'home_saves_succ', home_saves_succ,
                            'home_saves_att', home_saves_att,
                            'away_saves_succ', away_saves_succ,
                            'away_saves_att', away_saves_att,
                            'home_shots_att', home_shots_att,
                            'home_shots_succ', home_shots_succ,
                            'away_shots_succ', away_shots_succ,
                            'away_shots_att', away_shots_att,
                            'home_pass_att', home_pass_att,
                            'away_pass_att', away_pass_att,
                            'home_pass_succ', home_pass_succ,
                            'away_pass_succ', away_pass_succ
                        )
                    )
                    ORDER BY match_time_utc DESC
                )
                FROM match_data
            ),
            'total_stats', (
                SELECT jsonb_object_agg(
                    team_id::text,
                    jsonb_build_object(
                        'team_id', team_id,
                        'team_name', team_name,
                        'logo_url', logo_url,
                        'matches_played', matches_played,
                        'goals', goals,
                        'goals_conceded', goals_conceded,
                        'poss', poss,
                        'fouls', fouls,
                        'corners', corners,
                        'offsides', offsides,
                        'xg', xg,
                        'xg_conceded', xg_conceded,
                        'touches', touches,
                        'clearances', clearances,
                        'interceptions', interceptions,
                        'tackles', tackles,
                        'saves_succ', saves_succ,
                        'saves_att', saves_att,
                        'shots_att', shots_att,
                        'shots_succ', shots_succ,
                        'pass_att', pass_att,
                        'pass_succ', pass_succ,
                        'pass_pct', pass_pct
                    )
                ) FROM team_totals
            ),
            'per_match_averages', (
                SELECT jsonb_object_agg(
                    team_id::text,
                    jsonb_build_object(
                        'team_id', team_id,
                        'team_name', team_name,
                        'logo_url', logo_url,
                        'matches_played', matches_played,
                        'goals_per_match', goals_per_match,
                        'goals_conceded_per_match', goals_conceded_per_match,
                        'avg_poss', avg_poss,
                        'fouls_per_match', fouls_per_match,
                        'corners_per_match', corners_per_match,
                        'offsides_per_match', offsides_per_match,
                        'xg_per_match', xg_per_match,
                        'xg_conceded_per_match', xg_conceded_per_match,
                        'touches_per_match', touches_per_match,
                        'clearances_per_match', clearances_per_match,
                        'interceptions_per_match', interceptions_per_match,
                        'tackles_per_match', tackles_per_match,
                        'saves_succ_per_match', saves_succ_per_match,
                        'saves_att_per_match', saves_att_per_match,
                        'shots_att_per_match', shots_att_per_match,
                        'shots_succ_per_match', shots_succ_per_match,
                        'pass_att_per_match', pass_att_per_match,
                        'pass_succ_per_match', pass_succ_per_match,
                        'pass_pct', pass_pct
                    )
                ) FROM per_match_averages
            ),
            'combined_averages', (
                SELECT jsonb_build_object(
                    'matches_played', matches_played,
                    'goals_per_match', ROUND((total_goals::numeric / NULLIF(matches_played, 0)), 2),
                    'corners_per_match', ROUND((total_corners::numeric / NULLIF(matches_played, 0)), 2),
                    'fouls_per_match', ROUND((total_fouls::numeric / NULLIF(matches_played, 0)), 2),
                    'offsides_per_match', ROUND((total_offsides::numeric / NULLIF(matches_played, 0)), 2),
                    'xg_per_match', ROUND((total_xg / NULLIF(matches_played, 0)), 2),
                    'shots_att_per_match', ROUND((total_shots_att::numeric / NULLIF(matches_played, 0)), 2),
                    'shots_succ_per_match', ROUND((total_shots_succ::numeric / NULLIF(matches_played, 0)), 2)
                )
                FROM combined_totals
            )
        )
    ) AS result;
    """)
    
    result = session.exec(
        query, 
        params={
            "team1_id": team1_id, 
            "team2_id": team2_id, 
            "match_limit": match_limit
        }
    ).first()
    
    return result[0] if result else {
        "data": {
            "matches": [], 
            "total_stats": {}, 
            "per_match_averages": {},
            "combined_averages": {"matches_played": 0}
        }
    }

# h2h home or away
@router.get("/h2h-ha/{home_id}/{away_id}", response_model=TeamH2HResponse)
async def get_team_h2h_ha(
    home_id: int,
    away_id: int,
    session: DBSession,
    match_limit: int = Query(5, description="Number of recent matches to retrieve")
):
    """
    Get head-to-head match history where a specific team is always home and another is always away.
    
    Args:
        home_id: ID of the home team
        away_id: ID of the away team
        match_limit: Number of recent matches to retrieve (default: 5)
    
    Returns:
        TeamH2HResponse containing match history, team stats, and combined averages
    """
    
    query = text("""
    WITH match_data AS (
        SELECT 
            m.match_id,
            m.match_time_utc,
            m.result_string,
            comp.name AS competition_name,
            comp.season_year,
            home_team.name AS home_team_name,
            home_team.team_id AS home_team_id,
            home_team.logo_url AS home_team_logo,
            away_team.name AS away_team_name,
            away_team.team_id AS away_team_id,
            away_team.logo_url AS away_team_logo,
            m.home_goals,
            m.away_goals,
            m.home_poss,
            m.away_poss,
            m.home_fouls,
            m.away_fouls,
            m.home_corners,
            m.away_corners,
            m.home_offsides,
            m.away_offsides,
            m.home_xg,
            m.away_xg,
            m.home_touches,
            m.away_touches,
            m.home_clearances,
            m.away_clearances,
            m.home_interceptions,
            m.away_interceptions,
            m.home_tackles,
            m.away_tackles,
            m.home_saves_succ,
            m.home_saves_att,
            m.away_saves_succ,
            m.away_saves_att,
            m.home_shots_att,
            m.home_shots_succ,
            m.away_shots_succ,
            m.away_shots_att,
            m.home_pass_att,
            m.away_pass_att,
            m.home_pass_succ,
            m.away_pass_succ
        FROM matches m
        JOIN teams home_team ON m.home_id = home_team.team_id
        JOIN teams away_team ON m.away_id = away_team.team_id
        JOIN competitions comp ON m.comp_id = comp.competition_id
        WHERE m.home_id = :home_id AND m.away_id = :away_id
            AND m.isplayed = TRUE
        ORDER BY m.match_time_utc DESC
        LIMIT :match_limit
    ),
    team_totals AS (
        SELECT
            :home_id AS team_id,
            MAX(home_team_name) AS team_name,
            MAX(home_team_logo) AS logo_url,
            SUM(home_goals) AS goals,
            SUM(away_goals) AS goals_conceded,
            ROUND(AVG(home_poss)::numeric, 2) AS poss,
            SUM(home_fouls) AS fouls,
            SUM(home_corners) AS corners,
            SUM(home_offsides) AS offsides,
            ROUND(SUM(home_xg)::numeric, 2) AS xg,
            ROUND(SUM(away_xg)::numeric, 2) AS xg_conceded,
            SUM(home_touches) AS touches,
            SUM(home_clearances) AS clearances,
            SUM(home_interceptions) AS interceptions,
            SUM(home_tackles) AS tackles,
            SUM(home_saves_succ) AS saves_succ,
            SUM(home_saves_att) AS saves_att,
            SUM(home_shots_att) AS shots_att,
            SUM(home_shots_succ) AS shots_succ,
            SUM(home_pass_att) AS pass_att,
            SUM(home_pass_succ) AS pass_succ,
            ROUND((SUM(home_pass_succ)::numeric / NULLIF(SUM(home_pass_att), 0)) * 100, 2) AS pass_pct,
            COUNT(*) AS matches_played
        FROM match_data
        
        UNION ALL
        
        SELECT
            :away_id AS team_id,
            MAX(away_team_name) AS team_name,
            MAX(away_team_logo) AS logo_url,
            SUM(away_goals) AS goals,
            SUM(home_goals) AS goals_conceded,
            ROUND(AVG(away_poss)::numeric, 2) AS poss,
            SUM(away_fouls) AS fouls,
            SUM(away_corners) AS corners,
            SUM(away_offsides) AS offsides,
            ROUND(SUM(away_xg)::numeric, 2) AS xg,
            ROUND(SUM(home_xg)::numeric, 2) AS xg_conceded,
            SUM(away_touches) AS touches,
            SUM(away_clearances) AS clearances,
            SUM(away_interceptions) AS interceptions,
            SUM(away_tackles) AS tackles,
            SUM(away_saves_succ) AS saves_succ,
            SUM(away_saves_att) AS saves_att,
            SUM(away_shots_att) AS shots_att,
            SUM(away_shots_succ) AS shots_succ,
            SUM(away_pass_att) AS pass_att,
            SUM(away_pass_succ) AS pass_succ,
            ROUND((SUM(away_pass_succ)::numeric / NULLIF(SUM(away_pass_att), 0)) * 100, 2) AS pass_pct,
            COUNT(*) AS matches_played
        FROM match_data
    ),
    per_match_averages AS (
        SELECT
            team_id,
            team_name,
            logo_url,
            matches_played,
            ROUND((goals::numeric / NULLIF(matches_played, 0)), 2) AS goals_per_match,
            ROUND((goals_conceded::numeric / NULLIF(matches_played, 0)), 2) AS goals_conceded_per_match,
            poss AS avg_poss,
            ROUND((fouls::numeric / NULLIF(matches_played, 0)), 2) AS fouls_per_match,
            ROUND((corners::numeric / NULLIF(matches_played, 0)), 2) AS corners_per_match,
            ROUND((offsides::numeric / NULLIF(matches_played, 0)), 2) AS offsides_per_match,
            ROUND((xg / NULLIF(matches_played, 0)), 2) AS xg_per_match,
            ROUND((xg_conceded / NULLIF(matches_played, 0)), 2) AS xg_conceded_per_match,
            ROUND((touches::numeric / NULLIF(matches_played, 0)), 2) AS touches_per_match,
            ROUND((clearances::numeric / NULLIF(matches_played, 0)), 2) AS clearances_per_match,
            ROUND((interceptions::numeric / NULLIF(matches_played, 0)), 2) AS interceptions_per_match,
            ROUND((tackles::numeric / NULLIF(matches_played, 0)), 2) AS tackles_per_match,
            ROUND((saves_succ::numeric / NULLIF(matches_played, 0)), 2) AS saves_succ_per_match,
            ROUND((saves_att::numeric / NULLIF(matches_played, 0)), 2) AS saves_att_per_match,
            ROUND((shots_att::numeric / NULLIF(matches_played, 0)), 2) AS shots_att_per_match,
            ROUND((shots_succ::numeric / NULLIF(matches_played, 0)), 2) AS shots_succ_per_match,
            ROUND((pass_att::numeric / NULLIF(matches_played, 0)), 2) AS pass_att_per_match,
            ROUND((pass_succ::numeric / NULLIF(matches_played, 0)), 2) AS pass_succ_per_match,
            pass_pct
        FROM team_totals
    ),
    combined_totals AS (
        SELECT
            COUNT(*) AS matches_played,
            SUM(home_goals + away_goals) AS total_goals,
            SUM(home_corners + away_corners) AS total_corners,
            SUM(home_fouls + away_fouls) AS total_fouls,
            SUM(home_offsides + away_offsides) AS total_offsides,
            ROUND(SUM(home_xg + away_xg)::numeric, 2) AS total_xg,
            SUM(home_shots_att + away_shots_att) AS total_shots_att,
            SUM(home_shots_succ + away_shots_succ) AS total_shots_succ
        FROM match_data
    )
    SELECT jsonb_build_object(
        'data', jsonb_build_object(
            'matches', (
                SELECT jsonb_agg(
                    jsonb_build_object(
                        'match_info', jsonb_build_object(
                            'match_id', match_id,
                            'match_time_utc', match_time_utc,
                            'result_string', result_string,
                            'competition_name', competition_name,
                            'season_year', season_year,
                            'home_team_name', home_team_name,
                            'home_team_id', home_team_id,
                            'home_team_logo', home_team_logo,
                            'away_team_name', away_team_name,
                            'away_team_id', away_team_id,
                            'away_team_logo', away_team_logo
                        ),
                        'stats', jsonb_build_object(
                            'home_goals', home_goals,
                            'away_goals', away_goals,
                            'home_poss', home_poss,
                            'away_poss', away_poss,
                            'home_fouls', home_fouls,
                            'away_fouls', away_fouls,
                            'home_corners', home_corners,
                            'away_corners', away_corners,
                            'home_offsides', home_offsides,
                            'away_offsides', away_offsides,
                            'home_xg', home_xg,
                            'away_xg', away_xg,
                            'home_touches', home_touches,
                            'away_touches', away_touches,
                            'home_clearances', home_clearances,
                            'away_clearances', away_clearances,
                            'home_interceptions', home_interceptions,
                            'away_interceptions', away_interceptions,
                            'home_tackles', home_tackles,
                            'away_tackles', away_tackles,
                            'home_saves_succ', home_saves_succ,
                            'home_saves_att', home_saves_att,
                            'away_saves_succ', away_saves_succ,
                            'away_saves_att', away_saves_att,
                            'home_shots_att', home_shots_att,
                            'home_shots_succ', home_shots_succ,
                            'away_shots_succ', away_shots_succ,
                            'away_shots_att', away_shots_att,
                            'home_pass_att', home_pass_att,
                            'away_pass_att', away_pass_att,
                            'home_pass_succ', home_pass_succ,
                            'away_pass_succ', away_pass_succ
                        )
                    )
                    ORDER BY match_time_utc DESC
                )
                FROM match_data
            ),
            'total_stats', (
                SELECT jsonb_object_agg(
                    team_id::text,
                    jsonb_build_object(
                        'team_id', team_id,
                        'team_name', team_name,
                        'logo_url', logo_url,
                        'matches_played', matches_played,
                        'goals', goals,
                        'goals_conceded', goals_conceded,
                        'poss', poss,
                        'fouls', fouls,
                        'corners', corners,
                        'offsides', offsides,
                        'xg', xg,
                        'xg_conceded', xg_conceded,
                        'touches', touches,
                        'clearances', clearances,
                        'interceptions', interceptions,
                        'tackles', tackles,
                        'saves_succ', saves_succ,
                        'saves_att', saves_att,
                        'shots_att', shots_att,
                        'shots_succ', shots_succ,
                        'pass_att', pass_att,
                        'pass_succ', pass_succ,
                        'pass_pct', pass_pct
                    )
                ) FROM team_totals
            ),
            'per_match_averages', (
                SELECT jsonb_object_agg(
                    team_id::text,
                    jsonb_build_object(
                        'team_id', team_id,
                        'team_name', team_name,
                        'logo_url', logo_url,
                        'matches_played', matches_played,
                        'goals_per_match', goals_per_match,
                        'goals_conceded_per_match', goals_conceded_per_match,
                        'avg_poss', avg_poss,
                        'fouls_per_match', fouls_per_match,
                        'corners_per_match', corners_per_match,
                        'offsides_per_match', offsides_per_match,
                        'xg_per_match', xg_per_match,
                        'xg_conceded_per_match', xg_conceded_per_match,
                        'touches_per_match', touches_per_match,
                        'clearances_per_match', clearances_per_match,
                        'interceptions_per_match', interceptions_per_match,
                        'tackles_per_match', tackles_per_match,
                        'saves_succ_per_match', saves_succ_per_match,
                        'saves_att_per_match', saves_att_per_match,
                        'shots_att_per_match', shots_att_per_match,
                        'shots_succ_per_match', shots_succ_per_match,
                        'pass_att_per_match', pass_att_per_match,
                        'pass_succ_per_match', pass_succ_per_match,
                        'pass_pct', pass_pct
                    )
                ) FROM per_match_averages
            ),
            'combined_averages', (
                SELECT jsonb_build_object(
                    'matches_played', matches_played,
                    'goals_per_match', ROUND((total_goals::numeric / NULLIF(matches_played, 0)), 2),
                    'corners_per_match', ROUND((total_corners::numeric / NULLIF(matches_played, 0)), 2),
                    'fouls_per_match', ROUND((total_fouls::numeric / NULLIF(matches_played, 0)), 2),
                    'offsides_per_match', ROUND((total_offsides::numeric / NULLIF(matches_played, 0)), 2),
                    'xg_per_match', ROUND((total_xg / NULLIF(matches_played, 0)), 2),
                    'shots_att_per_match', ROUND((total_shots_att::numeric / NULLIF(matches_played, 0)), 2),
                    'shots_succ_per_match', ROUND((total_shots_succ::numeric / NULLIF(matches_played, 0)), 2)
                )
                FROM combined_totals
            )
        )
    ) AS result;
    """)
    
    result = session.exec(
        query, 
        params={
            "home_id": home_id, 
            "away_id": away_id, 
            "match_limit": match_limit
        }
    ).first()
    
    return result[0] if result else {
        "data": {
            "matches": [], 
            "total_stats": {}, 
            "per_match_averages": {},
            "combined_averages": {"matches_played": 0}
        }
    }

# player search
@router.get("/players/search", response_model=PlayerSearchResponse)
async def search_players_by_name(
    session: DBSession,
    name_search: str = Query(..., description="Player name or slug", min_length=2)
):
    """
    Args:
        name_search: Player name or slug to search for (minimum 2 characters)
    """
    
    query = text("""
    SELECT 
        p.player_name,
        p.player_id,
        p.age,
        p.tfm_pic_url,
        c.name AS country,
        c.country_id,
        c.flag_url AS country_flag
    FROM players p
    LEFT JOIN countries c ON p.country_id = c.country_id
    WHERE unaccent(lower(p.player_name)) LIKE unaccent(lower(:search_pattern))
       OR unaccent(lower(p.player_slug)) LIKE unaccent(lower(:search_pattern))
    ORDER BY p.player_name
    LIMIT 50;
    """)
    
    search_pattern = f"%{name_search}%"
    
    result = session.exec(query, params={"search_pattern": search_pattern}).all()
    
    # Convert result rows to list of dicts
    players = [
        {
            "player_name": row[0],
            "player_id": row[1],
            "age": row[2],
            "tfm_pic_url": row[3],
            "country": row[4],
            "country_id": row[5],
            "country_flag": row[6]
        }
        for row in result
    ]
    
    return {"data": {"players": players}}

# team search
@router.get("/teams/search", response_model=TeamSearchResponse)
async def search_teams_by_name(
    session: DBSession,
    name_search: str = Query(..., description="Team name to search for", min_length=2)
):
    """
    Search for teams by name (accent-insensitive).
    
    Args:
        name_search: Team name to search for (minimum 2 characters)
    
    Returns:
        TeamSearchResponse containing matching teams
    """
    
    query = text("""
    SELECT 
        t.name AS team_name,
        t.common_name,
        t.team_id,
        t.logo_url
    FROM teams t
    WHERE unaccent(lower(t.name)) LIKE unaccent(lower(:search_pattern))
       OR unaccent(lower(t.common_name)) LIKE unaccent(lower(:search_pattern))
    ORDER BY t.name
    LIMIT 50;
    """)
    
    search_pattern = f"%{name_search}%"
    
    result = session.exec(query, params={"search_pattern": search_pattern}).all()
    
    # Convert result rows to list of dicts
    teams = [
        {
            "team_name": row[0],
            "common_name": row[1],
            "team_id": row[2],
            "logo_url": row[3]
        }
        for row in result
    ]
    
    return {"data": {"teams": teams}}

# /stats/nation-dist
@router.get("/nation-dist/{season_year}", response_model=NationDistResponse)
async def get_nation_league_distribution(
    season_year: int,
    session: DBSession,
    country_id: Optional[int] = Query(None, description="Primary country ID"),
    country2_id: Optional[int] = Query(None, description="Secondary country ID")
):
    """
    Get league distribution for players from a specific nation.
    
    Args:
        season_year: Season year to analyze
        country_id: Primary country ID (players.country_id)
        country2_id: Secondary country ID (players.country2_id)
    
    Returns:
        NationDistResponse containing league distribution and player details
    """
    
    # Treat 0 as None (not provided)
    if country_id == 0:
        country_id = None
    if country2_id == 0:
        country2_id = None
    
    # Build WHERE clause based on provided parameters
    country_filter = ""
    params = {"season_year": season_year}
    
    # Check if at least one country is provided
    if country_id is None and country2_id is None:
        raise HTTPException(
            status_code=400,
            detail="At least one of country_id or country2_id must be provided"
        )
    
    if country_id is not None and country2_id is not None:
        # Both provided: match both
        country_filter = "AND p.country_id = :country_id AND p.country2_id = :country2_id"
        params["country_id"] = country_id
        params["country2_id"] = country2_id
    elif country_id is not None:
        # Only country_id provided
        country_filter = "AND p.country_id = :country_id"
        params["country_id"] = country_id
    elif country2_id is not None:
        # Only country2_id provided
        country_filter = "AND p.country2_id = :country2_id"
        params["country2_id"] = country2_id
    
    query = text(f"""
    WITH player_league_data AS (
        SELECT 
            l.league_id,
            comp.name AS comp_name,
            l.tier_level,
            lc.name AS country,
            lc.flag_url AS flag_url,
            p.player_id,
            p.player_name,
            p.tfm_pic_url,
            pc.name AS player_country,
            pc.country_id AS player_country_id,
            pc.flag_url AS player_flag_url,
            pc2.name AS player_country2,
            pc2.country_id AS player_country2_id,
            pc2.flag_url AS player_flag2_url,
            t.name AS team_name,
            t.team_id,
            t.logo_url,
            pcs.age,
            pcs.games_played,
            pcs.minutes,
            pcs.minutes_per_game,
            pcs.goals,
            pcs.assists,
            pcs.goals_assists
        FROM player_comp_stats pcs
        JOIN players p ON pcs.player_id = p.player_id
        JOIN competitions comp ON pcs.competition_id = comp.competition_id
        JOIN leagues l ON comp.league_id = l.league_id
        JOIN countries lc ON l.country_id = lc.country_id
        JOIN teams t ON pcs.team_id = t.team_id
        LEFT JOIN countries pc ON p.country_id = pc.country_id
        LEFT JOIN countries pc2 ON p.country2_id = pc2.country_id
        WHERE comp.season_year = :season_year
            AND l.format = 'league'
            AND pcs.games_played > 0
            AND pcs.minutes IS NOT NULL
            {country_filter}
    )
    SELECT jsonb_build_object(
        'data', jsonb_build_object(
            'league_dist', COALESCE(
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'league_id', league_id,
                            'comp_name', comp_name,
                            'tier_level', tier_level,
                            'country', country,
                            'flag_url', flag_url,
                            'player_count', player_count,
                            'players', players
                        )
                        ORDER BY player_count DESC
                    )
                    FROM (
                        SELECT 
                            league_id,
                            MAX(comp_name) AS comp_name,
                            MAX(tier_level) AS tier_level,
                            MAX(country) AS country,
                            MAX(flag_url) AS flag_url,
                            COUNT(DISTINCT player_id) AS player_count,
                            jsonb_agg(
                                jsonb_build_object(
                                    'player_name', player_name,
                                    'player_id', player_id,
                                    'tfm_pic_url', tfm_pic_url,
                                    'country', player_country,
                                    'country_id', player_country_id,
                                    'flag_url', player_flag_url,
                                    'country2', player_country2,
                                    'country2_id', player_country2_id,
                                    'flag2_url', player_flag2_url,
                                    'team_name', team_name,
                                    'team_id', team_id,
                                    'logo_url', logo_url,
                                    'age', age,
                                    'games_played', games_played,
                                    'minutes', minutes,
                                    'minutes_per_game', minutes_per_game,
                                    'goals', goals,
                                    'assists', assists,
                                    'goals_assists', goals_assists
                                )
                                ORDER BY minutes DESC, goals_assists DESC
                            ) AS players
                        FROM player_league_data
                        GROUP BY league_id
                    ) league_summary
                ),
                '[]'::jsonb
            )
        )
    ) AS result;
    """)
    
    result = session.exec(query, params=params).first()
    
    return result[0] if result else {"data": {"league_dist": []}}

# best highscoring games by dates
@router.get("/best-games/{league_id}", response_model=BestGamesResponse)
async def get_best_games(
    league_id: int,
    session: DBSession,
    league_ids: List[int] = Query([], description="List of additional league IDs"),
    start_date: Optional[date] = Query(None, description="Start date in YYYY-MM-DD format"),
    end_date: Optional[date] = Query(None, description="End date in YYYY-MM-DD format"),
    min_goals: int = Query(3, description="Minimum total goals in match"),
    limit: int = Query(20, description="Number of matches to return")
):
    """
    Get high-scoring matches from specified leagues.
    Returns matches where total goals (home + away) exceeds min_goals threshold.
    Defaults to November 1-7, 2025 if no dates provided.
    """
    # Combine the path parameter league_id with query parameter league_ids
    all_league_ids = [league_id] + league_ids
    
    # Set default dates if not provided
    if start_date is None:
        start_date = date(2025, 11, 7)
    if end_date is None:
        end_date = date(2025, 11, 9)
    
    query = text("""
        WITH match_data AS (
            SELECT 
                json_build_object(
                    'match_id', m.match_id,
                    'comp_id', m.comp_id,
                    'match_date', m.match_date,
                    'round', m.round,
                    'season_year', comp.season_year,
                    'result_string', CONCAT(m.home_goals, ':', m.away_goals),
                    'total_goals', m.home_goals + m.away_goals,
                    'comp_name', l.name,
                    'home_team', json_build_object(
                        'team_id', ht.team_id,
                        'team_name', ht.name,
                        'logo_url', ht.logo_url
                    ),
                    'away_team', json_build_object(
                        'team_id', at.team_id,
                        'team_name', at.name,
                        'logo_url', at.logo_url
                    )
                ) as match_info
            FROM matches m
            JOIN competitions comp ON m.comp_id = comp.competition_id
            JOIN leagues l ON comp.league_id = l.league_id
            JOIN teams ht ON m.home_id = ht.team_id
            JOIN teams at ON m.away_id = at.team_id
            WHERE comp.league_id = ANY(:league_ids)
                AND (m.home_goals + m.away_goals) > :min_goals
                AND m.match_date >= :start_date
                AND m.match_date <= :end_date
            ORDER BY (m.home_goals + m.away_goals) DESC, m.match_date DESC
            LIMIT :limit
        )
        SELECT json_build_object(
            'data', json_build_object(
                'matches', coalesce(json_agg(match_info), '[]'::json)
            )
        ) as result
        FROM match_data
    """)
    
    # Build params dict - ALWAYS include dates
    params = {
        "league_ids": all_league_ids,
        "min_goals": min_goals,
        "start_date": start_date,
        "end_date": end_date,
        "limit": limit
    }
    
    result = session.exec(query, params=params).first()
    
    return result[0] if result else {"data": {"matches": []}}

# player stats in all comps in a year + ga against
@router.get("/player-stats-detailed/{player_id}/{season_year}", response_model=PlayerStatsDetailedResponse)
async def get_player_stats_detailed(
    player_id: int,
    season_year: int,
    session: DBSession
):
    query = text("""
        WITH player_matches AS (
            SELECT 
                pms.player_id,
                pms.match_id,
                pms.team_id,
                pms.minutes,
                pms.goals,
                pms.assists,
                pms.goals_assists,
                m.home_id,
                m.away_id,
                m.comp_id,
                comp.competition_id,
                comp.name as comp_name,
                comp.season_year,
                CASE 
                    WHEN pms.team_id = m.home_id THEN m.away_id
                    ELSE m.home_id
                END as opponent_id
            FROM player_match_stats pms
            JOIN matches m ON pms.match_id = m.match_id
            JOIN competitions comp ON m.comp_id = comp.competition_id
            WHERE pms.player_id = :player_id
                AND comp.season_year = :season_year
        ),
        player_teams AS (
            SELECT DISTINCT
                t.team_id,
                t.name as team_name,
                t.logo_url
            FROM player_matches pm
            JOIN teams t ON pm.team_id = t.team_id
        ),
        comp_stats AS (
            SELECT 
                pm.competition_id,
                pm.comp_name,
                pm.season_year,
                SUM(pm.minutes) as total_minutes,
                COUNT(*) as games_played,
                ROUND(SUM(pm.minutes)::numeric / NULLIF(COUNT(*), 0), 2) as minutes_per_game,
                SUM(pm.goals) as total_goals,
                SUM(pm.assists) as total_assists,
                SUM(pm.goals_assists) as total_goals_assists,
                pm.opponent_id,
                SUM(pm.goals) as opp_goals,
                SUM(pm.assists) as opp_assists,
                SUM(pm.goals_assists) as opp_goals_assists
            FROM player_matches pm
            GROUP BY pm.competition_id, pm.comp_name, pm.season_year, pm.opponent_id
        ),
        comp_totals AS (
            SELECT 
                competition_id,
                comp_name,
                season_year,
                SUM(total_minutes) as total_minutes,
                SUM(games_played) as games_played,
                ROUND(SUM(total_minutes)::numeric / NULLIF(SUM(games_played), 0), 2) as minutes_per_game,
                SUM(total_goals) as total_goals,
                SUM(total_assists) as total_assists,
                SUM(total_goals_assists) as total_goals_assists
            FROM comp_stats
            GROUP BY competition_id, comp_name, season_year
        ),
        ga_against AS (
            SELECT 
                cs.competition_id,
                cs.comp_name,
                cs.season_year,
                json_agg(
                    json_build_object(
                        'team', json_build_object(
                            'team_name', t.name,
                            'team_id', t.team_id,
                            'logo_url', t.logo_url
                        ),
                        'stats', json_build_object(
                            'goals', cs.opp_goals,
                            'assists', cs.opp_assists,
                            'goals_assists', cs.opp_goals_assists
                        )
                    )
                    ORDER BY cs.opp_goals_assists DESC
                ) FILTER (WHERE cs.opp_goals > 0 OR cs.opp_assists > 0) as ga_against_teams
            FROM comp_stats cs
            LEFT JOIN teams t ON cs.opponent_id = t.team_id
            GROUP BY cs.competition_id, cs.comp_name, cs.season_year
        )
        SELECT json_build_object(
            'data', json_build_object(
                'player', (
                    SELECT json_build_object(
                        'player_name', p.player_name,
                        'player_id', p.player_id,
                        'tfm_pic_url', p.tfm_pic_url
                    )
                    FROM players p
                    WHERE p.player_id = :player_id
                ),
                'teams', (
                    SELECT COALESCE(json_agg(
                        json_build_object(
                            'team_name', pt.team_name,
                            'team_id', pt.team_id,
                            'logo_url', pt.logo_url
                        )
                    ), '[]'::json)
                    FROM player_teams pt
                ),
                'stats', COALESCE((
                    SELECT json_agg(
                        json_build_object(
                            'comp', json_build_object(
                                'comp_name', ct.comp_name,
                                'season_year', ct.season_year
                            ),
                            'total_stats', json_build_object(
                                'minutes', ct.total_minutes,
                                'minutes_per_game', ct.minutes_per_game,
                                'games', ct.games_played,
                                'goals', ct.total_goals,
                                'assists', ct.total_assists,
                                'goals_assists', ct.total_goals_assists
                            ),
                            'ga_against', COALESCE(ga.ga_against_teams, '[]'::json)
                        )
                    )
                    FROM comp_totals ct
                    LEFT JOIN ga_against ga ON ct.competition_id = ga.competition_id
                ), '[]'::json)
            )
        ) as result
    """)
    
    params = {
        "player_id": player_id,
        "season_year": season_year
    }
    
    result = session.exec(query, params=params).first()
    
    return result[0] if result else {"data": {"player": None, "teams": [], "stats": []}}

# version for loan watch
@router.get("/loan-watch/{team_id}", response_model=LoanWatchResponse)
async def get_loan_watch(
    team_id: int,
    session: DBSession,
    season_year: int = Query(2025, description="Season year")
):
    """
    Get stats for all players on loan from a specific parent team.
    """
    
    query = text("""
        WITH loaned_players AS (
            SELECT player_id
            FROM players
            WHERE parent_team_id = :team_id
                AND "onLoan" = true
        ),
        player_matches AS (
            SELECT 
                pms.player_id,
                pms.match_id,
                pms.team_id,
                pms.minutes,
                pms.goals,
                pms.assists,
                pms.goals_assists,
                comp.competition_id,
                comp.name as comp_name,
                comp.season_year
            FROM player_match_stats pms
            JOIN matches m ON pms.match_id = m.match_id
            JOIN competitions comp ON m.comp_id = comp.competition_id
            WHERE pms.player_id IN (SELECT player_id FROM loaned_players)
                AND comp.season_year = :season_year
        ),
        player_matches_details AS (
            SELECT 
                pms.player_id,
                pms.match_id,
                pms.team_id,
                pms.position,
                pms.minutes,
                pms.goals,
                pms.assists,
                pms.goals_assists,
                pms.shots,
                pms.sca,
                pms.xg,
                pms.xg_assist,
                m.comp_id,
                m.match_date,
                m.round,
                m.result_string,
                m.home_formation,
                m.away_formation,
                comp.competition_id,
                comp.name as comp_name,
                comp.season_year,
                ht.team_id as home_team_id,
                ht.name as home_team_name,
                ht.logo_url as home_team_logo,
                at.team_id as away_team_id,
                at.name as away_team_name,
                at.logo_url as away_team_logo,
                pt.team_id as player_team_id,
                pt.name as player_team_name,
                pt.logo_url as player_team_logo
            FROM player_match_stats pms
            JOIN matches m ON pms.match_id = m.match_id
            JOIN competitions comp ON m.comp_id = comp.competition_id
            JOIN teams ht ON m.home_id = ht.team_id
            JOIN teams at ON m.away_id = at.team_id
            JOIN teams pt ON pms.team_id = pt.team_id
            WHERE pms.player_id IN (SELECT player_id FROM loaned_players)
                AND comp.season_year = :season_year
        ),
        player_matches_agg AS (
            SELECT 
                pmd.player_id,
                pmd.competition_id,
                json_agg(
                    json_build_object(
                        'match_info', json_build_object(
                            'match_id', pmd.match_id,
                            'comp_id', pmd.comp_id,
                            'match_date', pmd.match_date,
                            'round', pmd.round,
                            'season_year', pmd.season_year,
                            'result_string', NULLIF(pmd.result_string, ''),
                            'comp_name', pmd.comp_name,
                            'home_team', json_build_object(
                                'team_id', pmd.home_team_id,
                                'team_name', pmd.home_team_name,
                                'logo_url', pmd.home_team_logo,
                                'formation', pmd.home_formation
                            ),
                            'away_team', json_build_object(
                                'team_id', pmd.away_team_id,
                                'team_name', pmd.away_team_name,
                                'logo_url', pmd.away_team_logo,
                                'formation', pmd.away_formation
                            )
                        ),
                        'stats', json_build_object(
                            'team', json_build_object(
                                'team_id', pmd.player_team_id,
                                'team_name', pmd.player_team_name,
                                'logo_url', pmd.player_team_logo
                            ),
                            'position', pmd.position,
                            'goals', pmd.goals,
                            'assists', pmd.assists,
                            'shots', pmd.shots,
                            'sca', pmd.sca,
                            'xg', pmd.xg,
                            'xg_assist', pmd.xg_assist
                        ),
                        'opp_defenders', '[]'::json
                    )
                    ORDER BY pmd.match_date DESC
                ) as all_matches
            FROM player_matches_details pmd
            GROUP BY pmd.player_id, pmd.competition_id
        ),
        player_teams AS (
            SELECT 
                pmd.player_id,
                t.team_id,
                t.name as team_name,
                t.logo_url
            FROM player_matches_details pmd
            JOIN teams t ON pmd.team_id = t.team_id
            GROUP BY pmd.player_id, t.team_id, t.name, t.logo_url
        ),
        player_teams_agg AS (
            SELECT 
                player_id,
                json_agg(
                    json_build_object(
                        'team_name', team_name,
                        'team_id', team_id,
                        'logo_url', logo_url
                    )
                ) as teams
            FROM player_teams
            GROUP BY player_id
        ),
        comp_stats AS (
            SELECT 
                pmd.player_id,
                pmd.competition_id,
                pmd.comp_name,
                pmd.season_year,
                COALESCE(SUM(pmd.minutes), 0) as total_minutes,
                COALESCE(COUNT(*), 0) as games_played,
                COALESCE(ROUND(SUM(pmd.minutes)::numeric / NULLIF(COUNT(*), 0), 2), 0) as minutes_per_game,
                COALESCE(SUM(pmd.goals), 0) as total_goals,
                COALESCE(SUM(pmd.assists), 0) as total_assists,
                COALESCE(SUM(pmd.goals_assists), 0) as total_goals_assists
            FROM player_matches_details pmd
            GROUP BY pmd.player_id, pmd.competition_id, pmd.comp_name, pmd.season_year
        ),
        player_comp_stats_agg AS (
            SELECT 
                cs.player_id,
                json_agg(
                    json_build_object(
                        'comp', json_build_object(
                            'comp_name', cs.comp_name,
                            'season_year', cs.season_year
                        ),
                        'total_stats', json_build_object(
                            'minutes', cs.total_minutes,
                            'minutes_per_game', cs.minutes_per_game,
                            'games', cs.games_played,
                            'goals', cs.total_goals,
                            'assists', cs.total_assists,
                            'goals_assists', cs.total_goals_assists
                        ),
                        'all_matches', COALESCE(pma.all_matches, '[]'::json)
                    )
                ) as stats
            FROM comp_stats cs
            LEFT JOIN player_matches_agg pma ON cs.player_id = pma.player_id AND cs.competition_id = pma.competition_id
            GROUP BY cs.player_id
        ),
        all_players_data AS (
            SELECT 
                json_build_object(
                    'player', json_build_object(
                        'player_name', p.player_name,
                        'player_id', p.player_id,
                        'tfm_pic_url', p.tfm_pic_url
                    ),
                    'teams', COALESCE(pta.teams, '[]'::json),
                    'stats', COALESCE(pcsa.stats, '[]'::json)
                ) as player_data
            FROM loaned_players lp
            JOIN players p ON lp.player_id = p.player_id
            LEFT JOIN player_teams_agg pta ON p.player_id = pta.player_id
            LEFT JOIN player_comp_stats_agg pcsa ON p.player_id = pcsa.player_id
        )
        SELECT json_build_object(
            'data', COALESCE(json_agg(player_data), '[]'::json)
        ) as result
        FROM all_players_data
    """)
    
    params = {
        "team_id": team_id,
        "season_year": season_year
    }
    
    result = session.exec(query, params=params).first()
    
    return result[0] if result else {"data": []}


# player matches in a time window with Opp defenders
@router.get("/player-stats-detailed-with-opp/{player_id}", response_model=PlayerStatsDetailedWithOppResponse)
async def get_player_stats_detailed_with_opp(
    player_id: int,
    session: DBSession,
    start_date: datetime = Query(
        datetime(2025, 8, 1, tzinfo=timezone.utc), 
        description="Start date to retrieve matches from (UTC)"
    ),
    end_date: datetime = Query(
        datetime(2025, 11, 25, tzinfo=timezone.utc), 
        description="End date to retrieve matches until (UTC)"
    )
):
    """
    Get detailed player stats with opponent defenders information for matches within a date range.
    """
    
    # Ensure dates are in UTC
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)
    if end_date.tzinfo is None:
        end_date = end_date.replace(tzinfo=timezone.utc)
    
    query = text("""
        WITH player_matches AS (
            SELECT 
                pms.player_id,
                pms.match_id,
                pms.team_id,
                pms.position,
                pms.minutes,
                pms.goals,
                pms.assists,
                pms.goals_assists,
                pms.shots,
                pms.sca,
                pms.xg,
                pms.xg_assist,
                m.home_id,
                m.away_id,
                m.comp_id,
                m.match_date,
                m.match_time_utc,
                m.round,
                m.result_string,
                m.isdraw,
                m.win_team,
                m.loss_team,
                m.home_formation,
                m.away_formation,
                comp.competition_id,
                comp.name as comp_name,
                comp.season_year,
                CASE 
                    WHEN pms.team_id = m.home_id THEN m.away_id
                    ELSE m.home_id
                END as opponent_id
            FROM player_match_stats pms
            JOIN matches m ON pms.match_id = m.match_id
            JOIN competitions comp ON m.comp_id = comp.competition_id
            WHERE pms.player_id = :player_id
                AND m.match_date BETWEEN :start_date AND :end_date
        ),
        player_teams AS (
            SELECT DISTINCT
                t.team_id,
                t.name as team_name,
                t.logo_url
            FROM player_matches pm
            JOIN teams t ON pm.team_id = t.team_id
        ),
        comp_stats AS (
            SELECT 
                pm.competition_id,
                pm.comp_name,
                pm.season_year,
                SUM(pm.minutes) as total_minutes,
                COUNT(*) as games_played,
                ROUND(SUM(pm.minutes)::numeric / NULLIF(COUNT(*), 0), 2) as minutes_per_game,
                SUM(pm.goals) as total_goals,
                SUM(pm.assists) as total_assists,
                SUM(pm.goals_assists) as total_goals_assists
            FROM player_matches pm
            GROUP BY pm.competition_id, pm.comp_name, pm.season_year
        ),
        match_details AS (
            SELECT 
                pm.competition_id,
                pm.match_id,
                pm.comp_id,
                pm.match_date,
                pm.match_time_utc,
                pm.round,
                pm.season_year,
                pm.result_string,
                pm.comp_name,
                pm.team_id,
                pm.position,
                pm.goals,
                pm.assists,
                pm.shots,
                pm.sca,
                pm.xg,
                pm.xg_assist,
                pm.opponent_id,
                pm.home_formation,
                pm.away_formation,
                CASE
                    WHEN pm.isdraw = TRUE THEN 'draw'
                    WHEN pm.team_id = pm.win_team THEN 'win'
                    WHEN pm.team_id = pm.loss_team THEN 'loss'
                    ELSE NULL
                END as outcome,
                ht.team_id as home_team_id,
                ht.name as home_team_name,
                ht.logo_url as home_team_logo,
                at.team_id as away_team_id,
                at.name as away_team_name,
                at.logo_url as away_team_logo,
                pt.team_id as player_team_id,
                pt.name as player_team_name,
                pt.logo_url as player_team_logo
            FROM player_matches pm
            JOIN teams ht ON pm.home_id = ht.team_id
            JOIN teams at ON pm.away_id = at.team_id
            JOIN teams pt ON pm.team_id = pt.team_id
        ),
        opponent_defenders AS (
            SELECT 
                md.match_id,
                json_agg(
                    json_build_object(
                        'player', json_build_object(
                            'player_name', p.player_name,
                            'player_id', p.player_id,
                            'tfm_pic_url', p.tfm_pic_url,
                            'countries', json_build_object(
                                'country_id', c1.country_id,
                                'country', c1.name,
                                'country_flag', c1.flag_url,
                                'country2_id', c2.country_id,
                                'country2', c2.name,
                                'country2_flag', c2.flag_url
                            )
                        ),
                        'position', opms.position,
                        'age', opms.age,
                        'minutes', opms.minutes
                    )
                ) as defenders
            FROM match_details md
            JOIN player_match_stats opms ON md.match_id = opms.match_id
            JOIN players p ON opms.player_id = p.player_id
            LEFT JOIN countries c1 ON p.country_id = c1.country_id
            LEFT JOIN countries c2 ON p.country2_id = c2.country_id
            WHERE opms.team_id = md.opponent_id
                AND opms.position IN ('Centre-Back', 'Left-Back', 'Right-Back')
            GROUP BY md.match_id
        ),
        all_matches_agg AS (
            SELECT 
                md.competition_id,
                json_agg(
                    json_build_object(
                        'match_info', json_build_object(
                            'match_id', md.match_id,
                            'comp_id', md.comp_id,
                            'match_date', md.match_date,
                            'match_time_utc', md.match_time_utc AT TIME ZONE 'UTC',
                            'round', md.round,
                            'season_year', md.season_year,
                            'result_string', NULLIF(md.result_string, ''),
                            'outcome', md.outcome,
                            'comp_name', md.comp_name,
                            'home_team', json_build_object(
                                'team_id', md.home_team_id,
                                'team_name', md.home_team_name,
                                'logo_url', md.home_team_logo,
                                'formation', md.home_formation
                            ),
                            'away_team', json_build_object(
                                'team_id', md.away_team_id,
                                'team_name', md.away_team_name,
                                'logo_url', md.away_team_logo,
                                'formation', md.away_formation
                            )
                        ),
                        'stats', json_build_object(
                            'team', json_build_object(
                                'team_id', md.player_team_id,
                                'team_name', md.player_team_name,
                                'logo_url', md.player_team_logo
                            ),
                            'position', md.position,
                            'goals', md.goals,
                            'assists', md.assists,
                            'shots', md.shots,
                            'sca', md.sca,
                            'xg', md.xg,
                            'xg_assist', md.xg_assist
                        ),
                        'opp_defenders', COALESCE(od.defenders, '[]'::json)
                    )
                    ORDER BY md.match_date ASC
                ) as all_matches
            FROM match_details md
            LEFT JOIN opponent_defenders od ON md.match_id = od.match_id
            GROUP BY md.competition_id
        )
        SELECT json_build_object(
            'data', json_build_object(
                'player', (
                    SELECT json_build_object(
                        'player_name', p.player_name,
                        'player_id', p.player_id,
                        'tfm_pic_url', p.tfm_pic_url
                    )
                    FROM players p
                    WHERE p.player_id = :player_id
                ),
                'teams', (
                    SELECT COALESCE(json_agg(
                        json_build_object(
                            'team_name', pt.team_name,
                            'team_id', pt.team_id,
                            'logo_url', pt.logo_url
                        )
                    ), '[]'::json)
                    FROM player_teams pt
                ),
                'stats', COALESCE((
                    SELECT json_agg(
                        json_build_object(
                            'comp', json_build_object(
                                'comp_name', cs.comp_name,
                                'season_year', cs.season_year
                            ),
                            'total_stats', json_build_object(
                                'minutes', cs.total_minutes,
                                'minutes_per_game', cs.minutes_per_game,
                                'games', cs.games_played,
                                'goals', cs.total_goals,
                                'assists', cs.total_assists,
                                'goals_assists', cs.total_goals_assists
                            ),
                            'all_matches', COALESCE(ama.all_matches, '[]'::json)
                        )
                    )
                    FROM comp_stats cs
                    LEFT JOIN all_matches_agg ama ON cs.competition_id = ama.competition_id
                ), '[]'::json)
            )
        ) as result
    """)
    
    params = {
        "player_id": player_id,
        "start_date": start_date,
        "end_date": end_date
    }
    
    result = session.exec(query, params=params).first()
    
    return result[0] if result else {"data": {"player": None, "teams": [], "stats": []}}


# show league table with player's ga
@router.get("/player-stats-table/{player_id}/{season_year}", response_model=PlayerStatsTableResponse)
async def get_player_stats_table(
    player_id: int,
    season_year: int,
    session: DBSession
):
    query = text("""
        WITH detected_league AS (
            SELECT t.league_id
            FROM player_match_stats pms
            JOIN matches m ON pms.match_id = m.match_id
            JOIN teams t ON pms.team_id = t.team_id
            WHERE pms.player_id = :player_id
                AND m.match_date >= MAKE_DATE(:season_year, 8, 1)
                AND m.match_date <= MAKE_DATE(:season_year + 1, 7, 20)
            GROUP BY pms.team_id, t.league_id
            ORDER BY COUNT(*) DESC
            LIMIT 1
        ),
        league_comps AS (
            SELECT competition_id
            FROM competitions
            WHERE league_id = (SELECT league_id FROM detected_league)
                AND season_year = :season_year
        ),
        player_teams AS (
            SELECT DISTINCT
                t.team_id,
                t.name as team_name,
                t.logo_url
            FROM player_match_stats pms
            JOIN matches m ON pms.match_id = m.match_id
            JOIN competitions comp ON m.comp_id = comp.competition_id
            JOIN teams t ON pms.team_id = t.team_id
            WHERE pms.player_id = :player_id
                AND comp.league_id = (SELECT league_id FROM detected_league)
                AND comp.season_year = :season_year
        ),
        player_stats_vs_teams AS (
            SELECT
                CASE
                    WHEN pms.team_id = m.home_id THEN m.away_id
                    ELSE m.home_id
                END as opponent_id,
                SUM(pms.goals) as goals_against,
                SUM(pms.assists) as assists_against,
                SUM(pms.minutes) as minutes_against,
                ROUND(AVG(pms.minutes)::numeric, 2) as minutes_per_game_against,
                SUM(pms.cards_yellow) as cards_yellow_against,
                SUM(pms.cards_red) as cards_red_against,
                SUM(pms.cards_yellow_red) as cards_yellow_red_against,
                BOOL_OR(
                    COALESCE(pms.started, false) OR
                    COALESCE(pms.subbed_on, false) OR
                    COALESCE(pms.goals, 0) > 0 OR
                    COALESCE(pms.assists, 0) > 0 OR
                    COALESCE(pms.minutes, 0) > 0 OR
                    COALESCE(pms.cards_yellow, 0) > 0 OR
                    COALESCE(pms.cards_red, 0) > 0 OR
                    COALESCE(pms.cards_yellow_red, 0) > 0
                ) as played
            FROM player_match_stats pms
            JOIN matches m ON pms.match_id = m.match_id
            JOIN competitions comp ON m.comp_id = comp.competition_id
            WHERE pms.player_id = :player_id
                AND comp.league_id = (SELECT league_id FROM detected_league)
                AND comp.season_year = :season_year
            GROUP BY opponent_id
        ),
        league_ranks AS (
            SELECT
                r.team_id,
                r.rank,
                r.points,
                r.wins,
                r.draws,
                r.losses,
                r.gd,
                r.gp,
                t.name as team_name,
                t.logo_url,
                t.league_id,
                l.tier_level,
                t.country_id,
                c.name as country_name,
                c.flag_url as country_flag
            FROM ranks r
            JOIN teams t ON r.team_id = t.team_id
            LEFT JOIN leagues l ON t.league_id = l.league_id
            LEFT JOIN countries c ON t.country_id = c.country_id
            WHERE r.competition_id IN (SELECT competition_id FROM league_comps)
        ),
        opponent_teams AS (
            SELECT DISTINCT
                CASE
                    WHEN pms.team_id = m.home_id THEN m.away_id
                    ELSE m.home_id
                END as opponent_team_id
            FROM player_match_stats pms
            JOIN matches m ON pms.match_id = m.match_id
            WHERE pms.player_id = :player_id
                AND m.match_date >= MAKE_DATE(:season_year, 8, 1)
                AND m.match_date <= MAKE_DATE(:season_year + 1, 7, 20)
        ),
        opponent_leagues AS (
            SELECT DISTINCT t.league_id
            FROM opponent_teams ot
            JOIN teams t ON ot.opponent_team_id = t.team_id
            WHERE t.league_id != (SELECT league_id FROM detected_league)
        ),
        other_league_comps AS (
            SELECT DISTINCT comp.competition_id, comp.league_id
            FROM competitions comp
            WHERE comp.league_id IN (SELECT league_id FROM opponent_leagues)
                AND comp.season_year = :season_year
        ),
        other_player_stats_vs_teams AS (
            SELECT
                opp_team.league_id,
                CASE
                    WHEN pms.team_id = m.home_id THEN m.away_id
                    ELSE m.home_id
                END as opponent_id,
                SUM(pms.goals) as goals_against,
                SUM(pms.assists) as assists_against,
                SUM(pms.minutes) as minutes_against,
                ROUND(AVG(pms.minutes)::numeric, 2) as minutes_per_game_against,
                SUM(pms.cards_yellow) as cards_yellow_against,
                SUM(pms.cards_red) as cards_red_against,
                SUM(pms.cards_yellow_red) as cards_yellow_red_against,
                BOOL_OR(
                    COALESCE(pms.started, false) OR
                    COALESCE(pms.subbed_on, false) OR
                    COALESCE(pms.goals, 0) > 0 OR
                    COALESCE(pms.assists, 0) > 0 OR
                    COALESCE(pms.minutes, 0) > 0 OR
                    COALESCE(pms.cards_yellow, 0) > 0 OR
                    COALESCE(pms.cards_red, 0) > 0 OR
                    COALESCE(pms.cards_yellow_red, 0) > 0
                ) as played
            FROM player_match_stats pms
            JOIN matches m ON pms.match_id = m.match_id
            JOIN teams opp_team ON (CASE WHEN pms.team_id = m.home_id THEN m.away_id ELSE m.home_id END) = opp_team.team_id
            WHERE pms.player_id = :player_id
                AND m.match_date >= MAKE_DATE(:season_year, 8, 1)
                AND m.match_date <= MAKE_DATE(:season_year + 1, 7, 20)
                AND opp_team.league_id IN (SELECT league_id FROM opponent_leagues)
            GROUP BY opp_team.league_id, opponent_id
        ),
        other_league_ranks AS (
            SELECT
                r.team_id,
                r.rank,
                r.points,
                r.wins,
                r.draws,
                r.losses,
                r.gd,
                r.gp,
                t.name as team_name,
                t.logo_url,
                t.league_id,
                l.tier_level,
                t.country_id,
                c.name as country_name,
                c.flag_url as country_flag
            FROM ranks r
            JOIN teams t ON r.team_id = t.team_id
            LEFT JOIN leagues l ON t.league_id = l.league_id
            LEFT JOIN countries c ON t.country_id = c.country_id
            WHERE r.competition_id IN (SELECT competition_id FROM other_league_comps)
        )
        SELECT json_build_object(
            'data', json_build_object(
                'player', (
                    SELECT json_build_object(
                        'player_id', p.player_id,
                        'player_name', p.player_name,
                        'tfm_pic_url', p.tfm_pic_url
                    )
                    FROM players p
                    WHERE p.player_id = :player_id
                ),
                'teams', (
                    SELECT COALESCE(json_agg(
                        json_build_object(
                            'team_id', pt.team_id,
                            'team_name', pt.team_name,
                            'logo_url', pt.logo_url
                        )
                    ), '[]'::json)
                    FROM player_teams pt
                ),
                'ranks', COALESCE((
                    SELECT json_agg(
                        json_build_object(
                            'team', json_build_object(
                                'team_id', lr.team_id,
                                'team_name', lr.team_name,
                                'logo_url', lr.logo_url,
                                'league_id', lr.league_id,
                                'tier_level', lr.tier_level,
                                'country_name', lr.country_name,
                                'country_flag', lr.country_flag,
                                'country_id', lr.country_id
                            ),
                            'rank', lr.rank,
                            'points', lr.points,
                            'wins', lr.wins,
                            'draws', lr.draws,
                            'losses', lr.losses,
                            'gd', lr.gd,
                            'gp', lr.gp,
                            'player_goals', COALESCE(pvt.goals_against, 0),
                            'player_assists', COALESCE(pvt.assists_against, 0),
                            'minutes', COALESCE(pvt.minutes_against, 0),
                            'minutes_per_game', COALESCE(pvt.minutes_per_game_against, 0),
                            'cards_yellow', COALESCE(pvt.cards_yellow_against, 0),
                            'cards_red', COALESCE(pvt.cards_red_against, 0),
                            'cards_yellow_red', COALESCE(pvt.cards_yellow_red_against, 0),
                            'played', COALESCE(pvt.played, false)
                        ) ORDER BY lr.rank ASC
                    )
                    FROM league_ranks lr
                    LEFT JOIN player_stats_vs_teams pvt ON lr.team_id = pvt.opponent_id
                ), '[]'::json),
                'other_ranks', COALESCE((
                    SELECT json_agg(
                        json_build_object(
                            'team', json_build_object(
                                'team_id', olr.team_id,
                                'team_name', olr.team_name,
                                'logo_url', olr.logo_url,
                                'league_id', olr.league_id,
                                'tier_level', olr.tier_level,
                                'country_name', olr.country_name,
                                'country_flag', olr.country_flag,
                                'country_id', olr.country_id
                            ),
                            'rank', olr.rank,
                            'points', olr.points,
                            'wins', olr.wins,
                            'draws', olr.draws,
                            'losses', olr.losses,
                            'gd', olr.gd,
                            'gp', olr.gp,
                            'player_goals', COALESCE(opvt.goals_against, 0),
                            'player_assists', COALESCE(opvt.assists_against, 0),
                            'minutes', COALESCE(opvt.minutes_against, 0),
                            'minutes_per_game', COALESCE(opvt.minutes_per_game_against, 0),
                            'cards_yellow', COALESCE(opvt.cards_yellow_against, 0),
                            'cards_red', COALESCE(opvt.cards_red_against, 0),
                            'cards_yellow_red', COALESCE(opvt.cards_yellow_red_against, 0),
                            'played', COALESCE(opvt.played, false)
                        ) ORDER BY olr.league_id, olr.rank ASC
                    )
                    FROM other_league_ranks olr
                    LEFT JOIN other_player_stats_vs_teams opvt ON olr.team_id = opvt.opponent_id AND olr.league_id = opvt.league_id
                ), '[]'::json)
            )
        ) as result
    """)

    params = {
        "player_id": player_id,
        "season_year": season_year
    }

    result = session.exec(query, params=params).first()

    return result[0] if result else {"data": {"player": None, "teams": [], "ranks": []}}


# get ig followers + difference by each entry
@router.get("/insta-followers/history", response_model=InstaFollowersHistoryResponse)
async def get_insta_followers_history_by_ids(
    session: DBSession,
    player_ids: List[int] = Query(..., description="List of player IDs to retrieve follower data for"),
    start_date: datetime = Query(
        datetime(2025, 11, 11, tzinfo=timezone.utc), 
        description="Start date to retrieve follower entries from (UTC)"
    ),
    end_date: datetime = Query(
        datetime(2025, 11, 13, tzinfo=timezone.utc), 
        description="End date to retrieve follower entries until (UTC)"
    )
):
    """
    Get all Instagram follower count entries for specific player IDs within a date range.
    
    Returns all follower entries between start_date and end_date for each player,
    including the difference from the previous entry. All dates are in UTC.
    """
    
    # Ensure dates are in UTC
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)
    if end_date.tzinfo is None:
        end_date = end_date.replace(tzinfo=timezone.utc)
    
    query = text("""
        WITH player_follower_data AS (
            SELECT 
                p.player_id,
                p.player_name,
                p.tfm_pic_url,
                ig.num_followers,
                ig.updated_at AT TIME ZONE 'UTC' as updated_at,
                LAG(ig.num_followers) OVER (
                    PARTITION BY p.player_id 
                    ORDER BY ig.updated_at ASC
                ) as previous_followers
            FROM players p
            JOIN ig_followers ig ON p.player_id = ig.player_id
            WHERE p.player_id = ANY(:player_ids)
                AND ig.updated_at BETWEEN :start_date AND :end_date
        ),
        player_follower_history AS (
            SELECT 
                player_id,
                player_name,
                tfm_pic_url,
                json_agg(
                    json_build_object(
                        'num_followers', num_followers,
                        'updated_at', updated_at,
                        'difference', num_followers - previous_followers
                    )
                    ORDER BY updated_at ASC
                ) as follower_entries
            FROM player_follower_data
            GROUP BY player_id, player_name, tfm_pic_url
        )
        SELECT 
            json_build_object(
                'start_date', :start_date,
                'end_date', :end_date,
                'players', COALESCE(json_agg(
                    json_build_object(
                        'player_name', player_name,
                        'player_id', player_id,
                        'tfm_pic_url', tfm_pic_url,
                        'follower_entries', follower_entries
                    )
                ), '[]'::json)
            ) as data
        FROM player_follower_history
    """)
    
    params = {
        "player_ids": player_ids,
        "start_date": start_date,
        "end_date": end_date
    }
    
    result = session.exec(query, params=params).first()
    
    return {"data": result[0]} if result else {
        "data": {
            "start_date": start_date,
            "end_date": end_date,
            "players": []
        }
    }

# version with games + stats
@router.get("/insta-followers/history-games", response_model=InstaFollowersHistoryWithGamesResponse)
async def get_insta_followers_history_with_games(
    session: DBSession,
    player_ids: List[int] = Query(..., description="List of player IDs to retrieve follower data for"),
    start_date: datetime = Query(
        datetime(2025, 11, 24, tzinfo=timezone.utc), 
        description="Start date to retrieve follower entries from (UTC)"
    ),
    end_date: datetime = Query(
        datetime(2025, 11, 28, tzinfo=timezone.utc), 
        description="End date to retrieve follower entries until (UTC)"
    )
):
    """
    Get all Instagram follower count entries for specific player IDs within a date range.
    """
    
    # Ensure dates are in UTC
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)
    if end_date.tzinfo is None:
        end_date = end_date.replace(tzinfo=timezone.utc)
    
    query = text("""
        WITH player_follower_data AS (
            SELECT 
                p.player_id,
                p.player_name,
                p.tfm_pic_url,
                ig.num_followers,
                ig.updated_at AT TIME ZONE 'UTC' as updated_at,
                LAG(ig.num_followers) OVER (
                    PARTITION BY p.player_id 
                    ORDER BY ig.updated_at ASC
                ) as previous_followers
            FROM players p
            JOIN ig_followers ig ON p.player_id = ig.player_id
            WHERE p.player_id = ANY(:player_ids)
                AND ig.updated_at BETWEEN :start_date AND :end_date
        ),
        player_follower_history AS (
            SELECT 
                player_id,
                player_name,
                tfm_pic_url,
                json_agg(
                    json_build_object(
                        'num_followers', num_followers,
                        'updated_at', updated_at,
                        'difference', num_followers - previous_followers
                    )
                    ORDER BY updated_at ASC
                ) as follower_entries
            FROM player_follower_data
            GROUP BY player_id, player_name, tfm_pic_url
        ),
        player_matches AS (
            SELECT 
                pms.player_id,
                json_agg(
                    json_build_object(
                        'match', json_build_object(
                            'match_id', m.match_id,
                            'comp_id', m.comp_id,
                            'comp_logo', comp.logo_url,
                            'match_date', m.match_date,
                            'match_time_utc', m.match_time_utc AT TIME ZONE 'UTC',
                            'round', m.round,
                            'season_year', comp.season_year,
                            'result_string', m.result_string,
                            'home_goals', m.home_goals,
                            'away_goals', m.away_goals,
                            'outcome', CASE
                                WHEN m.isdraw = TRUE THEN 'draw'
                                WHEN pms.team_id = m.win_team THEN 'win'
                                WHEN pms.team_id = m.loss_team THEN 'loss'
                                ELSE NULL
                            END,
                            'comp_name', comp.name,
                            'home_team', json_build_object(
                                'team_id', ht.team_id,
                                'team_name', ht.name,
                                'logo_url', ht.logo_url
                            ),
                            'away_team', json_build_object(
                                'team_id', at.team_id,
                                'team_name', at.name,
                                'logo_url', at.logo_url
                            )
                        ),
                        'stats', json_build_object(
                            'team', json_build_object(
                                'team_id', pt.team_id,
                                'team_name', pt.name,
                                'logo_url', pt.logo_url
                            ),
                            'minutes', pms.minutes,
                            'xi', pms.started,
                            'goals', pms.goals,
                            'assists', pms.assists,
                            'yellows', pms.cards_yellow,
                            'reds', pms.cards_red
                        )
                    )
                    ORDER BY m.match_date ASC
                ) as matches_played
            FROM player_match_stats pms
            JOIN matches m ON pms.match_id = m.match_id
            JOIN competitions comp ON m.comp_id = comp.competition_id
            JOIN teams ht ON m.home_id = ht.team_id
            JOIN teams at ON m.away_id = at.team_id
            JOIN teams pt ON pms.team_id = pt.team_id
            WHERE pms.player_id = ANY(:player_ids)
                AND m.match_date BETWEEN :start_date AND :end_date
            GROUP BY pms.player_id
        ),
        all_players AS (
            SELECT DISTINCT
                p.player_id,
                p.player_name,
                p.tfm_pic_url
            FROM players p
            WHERE p.player_id = ANY(:player_ids)
        )
        SELECT 
            json_build_object(
                'start_date', :start_date,
                'end_date', :end_date,
                'players', COALESCE(json_agg(
                    json_build_object(
                        'player_name', ap.player_name,
                        'player_id', ap.player_id,
                        'tfm_pic_url', ap.tfm_pic_url,
                        'follower_entries', COALESCE(pfh.follower_entries, '[]'::json),
                        'matches_played', COALESCE(pm.matches_played, '[]'::json)
                    )
                ), '[]'::json)
            ) as data
        FROM all_players ap
        LEFT JOIN player_follower_history pfh ON ap.player_id = pfh.player_id
        LEFT JOIN player_matches pm ON ap.player_id = pm.player_id
    """)
    
    params = {
        "player_ids": player_ids,
        "start_date": start_date,
        "end_date": end_date
    }
    
    result = session.exec(query, params=params).first()
    
    return {"data": result[0]} if result else {
        "data": {
            "start_date": start_date,
            "end_date": end_date,
            "players": []
        }
    }
    
# get followers increase
@router.get("/insta-followers", response_model=InstaFollowersResponse)
async def get_insta_followers_increase(
    session: DBSession,
    end_date: Optional[datetime] = Query(
        None, 
        description="End date to calculate follower increase from (defaults to 3 days ago)"
    ),
    limit: int = Query(10, description="Number of top players to return")
):
    """
    Get players with the highest Instagram follower increase since end_date.
    
    Returns players ordered by follower increase (most to least).
    """
    # Default to 3 days ago if end_date not provided
    if end_date is None:
        end_date = datetime.now() - timedelta(days=2)
    
    # Current datetime for start_date
    start_date = datetime.now()
    
    query = text("""
        WITH follower_changes AS (
            SELECT 
                p.player_id,
                p.player_name,
                p.tfm_pic_url,
                -- Get the most recent follower count
                (
                    SELECT num_followers 
                    FROM ig_followers 
                    WHERE ig_followers.player_id = p.player_id 
                    ORDER BY updated_at DESC 
                    LIMIT 1
                ) as followers_now,
                -- Get the follower count closest to the end_date (before or at that time)
                (
                    SELECT num_followers 
                    FROM ig_followers 
                    WHERE ig_followers.player_id = p.player_id 
                        AND updated_at <= :end_date
                    ORDER BY updated_at DESC 
                    LIMIT 1
                ) as followers_before
            FROM players p
            WHERE EXISTS (
                SELECT 1 FROM ig_followers 
                WHERE ig_followers.player_id = p.player_id
            )
        ),
        top_players AS (
            SELECT 
                player_name,
                player_id,
                tfm_pic_url,
                followers_now - followers_before as followers_increase,
                followers_before,
                followers_now
            FROM follower_changes
            WHERE followers_before IS NOT NULL 
                AND followers_now IS NOT NULL
                AND followers_now > followers_before
            ORDER BY (followers_now - followers_before) DESC
            LIMIT :limit
        )
        SELECT 
            json_build_object(
                'start_date', :start_date,
                'end_date', :end_date,
                'players', COALESCE(json_agg(
                    json_build_object(
                        'player_name', player_name,
                        'player_id', player_id,
                        'tfm_pic_url', tfm_pic_url,
                        'followers_increase', followers_increase,
                        'followers_before', followers_before,
                        'followers_now', followers_now
                    )
                ), '[]'::json)
            ) as data
        FROM top_players
    """)
    
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "limit": limit
    }
    
    result = session.exec(query, params=params).first()
    
    return {"data": result[0]} if result else {
        "data": {
            "start_date": start_date,
            "end_date": end_date,
            "players": []
        }
    }

# get followers increase - by player ids
@router.get("/insta-followers/by-ids", response_model=InstaFollowersResponse)
async def get_insta_followers_increase_by_ids(
    session: DBSession,
    player_ids: List[int] = Query(..., description="List of player IDs to retrieve follower data for"),
    end_date: Optional[datetime] = Query(
        None, 
        description="End date to calculate follower increase from (defaults to 3 days ago)"
    )
):
    """
    Get Instagram follower increase for specific player IDs since end_date.
    
    Returns players ordered by follower increase (most to least).
    """
    # Default to 3 days ago if end_date not provided
    if end_date is None:
        end_date = datetime.now() - timedelta(days=3)
    
    # Current datetime for start_date
    start_date = datetime.now()
    
    query = text("""
        WITH follower_changes AS (
            SELECT 
                p.player_id,
                p.player_name,
                p.tfm_pic_url,
                -- Get the most recent follower count
                (
                    SELECT num_followers 
                    FROM ig_followers 
                    WHERE ig_followers.player_id = p.player_id 
                    ORDER BY updated_at DESC 
                    LIMIT 1
                ) as followers_now,
                -- Get the follower count closest to the end_date (before or at that time)
                (
                    SELECT num_followers 
                    FROM ig_followers 
                    WHERE ig_followers.player_id = p.player_id 
                        AND updated_at <= :end_date
                    ORDER BY updated_at DESC 
                    LIMIT 1
                ) as followers_before
            FROM players p
            WHERE p.player_id = ANY(:player_ids)
                AND EXISTS (
                    SELECT 1 FROM ig_followers 
                    WHERE ig_followers.player_id = p.player_id
                )
        ),
        top_players AS (
            SELECT 
                player_name,
                player_id,
                tfm_pic_url,
                followers_now - followers_before as followers_increase,
                followers_before,
                followers_now
            FROM follower_changes
            WHERE followers_before IS NOT NULL 
                AND followers_now IS NOT NULL
                AND followers_now > followers_before
            ORDER BY (followers_now - followers_before) DESC
        )
        SELECT 
            json_build_object(
                'start_date', :start_date,
                'end_date', :end_date,
                'players', COALESCE(json_agg(
                    json_build_object(
                        'player_name', player_name,
                        'player_id', player_id,
                        'tfm_pic_url', tfm_pic_url,
                        'followers_increase', followers_increase,
                        'followers_before', followers_before,
                        'followers_now', followers_now
                    )
                ), '[]'::json)
            ) as data
        FROM top_players
    """)
    
    params = {
        "player_ids": player_ids,
        "start_date": start_date,
        "end_date": end_date
    }
    
    result = session.exec(query, params=params).first()
    
    return {"data": result[0]} if result else {
        "data": {
            "start_date": start_date,
            "end_date": end_date,
            "players": []
        }
    }

# get followers decrease
@router.get("/insta-followers-decrease", response_model=InstaFollowersDecreaseResponse)
async def get_insta_followers_decrease(
    session: DBSession,
    logger: LoggerDep,
    end_date: Optional[datetime] = Query(
        None, 
        description="End date to calculate follower decrease from (defaults to 3 days ago)"
    ),
    limit: int = Query(10, description="Number of top players to return")
):
    """
    Get players with the highest Instagram follower decrease since end_date.
    
    Returns players ordered by follower decrease (most to least).
    """
    # Default to 3 days ago if end_date not provided
    if end_date is None:
        end_date = datetime.now() - timedelta(days=3)
    
    # Current datetime for start_date
    start_date = datetime.now()
    
    query = text("""
        WITH follower_changes AS (
            SELECT 
                p.player_id,
                p.player_name,
                p.tfm_pic_url,
                -- Get the most recent follower count
                (
                    SELECT num_followers 
                    FROM ig_followers 
                    WHERE ig_followers.player_id = p.player_id 
                    ORDER BY updated_at DESC 
                    LIMIT 1
                ) as followers_now,
                -- Get the follower count closest to the end_date (before or at that time)
                (
                    SELECT num_followers 
                    FROM ig_followers 
                    WHERE ig_followers.player_id = p.player_id 
                        AND updated_at <= :end_date
                    ORDER BY updated_at DESC 
                    LIMIT 1
                ) as followers_before
            FROM players p
            WHERE EXISTS (
                SELECT 1 FROM ig_followers 
                WHERE ig_followers.player_id = p.player_id
            )
        ),
        top_players AS (
            SELECT 
                player_name,
                player_id,
                tfm_pic_url,
                followers_before - followers_now as followers_decrease,
                followers_before,
                followers_now
            FROM follower_changes
            WHERE followers_before IS NOT NULL 
                AND followers_now IS NOT NULL
                AND followers_now < followers_before
            ORDER BY (followers_before - followers_now) DESC
            LIMIT :limit
        )
        SELECT 
            json_build_object(
                'start_date', :start_date,
                'end_date', :end_date,
                'players', COALESCE(json_agg(
                    json_build_object(
                        'player_name', player_name,
                        'player_id', player_id,
                        'tfm_pic_url', tfm_pic_url,
                        'followers_decrease', followers_decrease,
                        'followers_before', followers_before,
                        'followers_now', followers_now
                    )
                ), '[]'::json)
            ) as data
        FROM top_players
    """)
    
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "limit": limit
    }
    
    result = session.exec(query, params=params).first()

    logger.info('Data returned')
    
    return {"data": result[0]} if result else {
        "data": {
            "start_date": start_date,
            "end_date": end_date,
            "players": []
        }
    }

# get followers decrease - by player ids
@router.get("/insta-followers-decrease/by-ids", response_model=InstaFollowersDecreaseResponse)
async def get_insta_followers_decrease_by_ids(
    session: DBSession,
    player_ids: List[int] = Query(..., description="List of player IDs to retrieve follower data for"),
    end_date: Optional[datetime] = Query(
        None, 
        description="End date to calculate follower decrease from (defaults to 3 days ago)"
    )
):
    """
    Get Instagram follower decrease for specific player IDs since end_date.
    
    Returns players ordered by follower decrease (most to least).
    """
    # Default to 3 days ago if end_date not provided
    if end_date is None:
        end_date = datetime.now() - timedelta(days=3)
    
    # Current datetime for start_date
    start_date = datetime.now()
    
    query = text("""
        WITH follower_changes AS (
            SELECT 
                p.player_id,
                p.player_name,
                p.tfm_pic_url,
                -- Get the most recent follower count
                (
                    SELECT num_followers 
                    FROM ig_followers 
                    WHERE ig_followers.player_id = p.player_id 
                    ORDER BY updated_at DESC 
                    LIMIT 1
                ) as followers_now,
                -- Get the follower count closest to the end_date (before or at that time)
                (
                    SELECT num_followers 
                    FROM ig_followers 
                    WHERE ig_followers.player_id = p.player_id 
                        AND updated_at <= :end_date
                    ORDER BY updated_at DESC 
                    LIMIT 1
                ) as followers_before
            FROM players p
            WHERE p.player_id = ANY(:player_ids)
                AND EXISTS (
                    SELECT 1 FROM ig_followers 
                    WHERE ig_followers.player_id = p.player_id
                )
        ),
        top_players AS (
            SELECT 
                player_name,
                player_id,
                tfm_pic_url,
                followers_before - followers_now as followers_decrease,
                followers_before,
                followers_now
            FROM follower_changes
            WHERE followers_before IS NOT NULL 
                AND followers_now IS NOT NULL
                AND followers_now < followers_before
            ORDER BY (followers_before - followers_now) DESC
        )
        SELECT 
            json_build_object(
                'start_date', :start_date,
                'end_date', :end_date,
                'players', COALESCE(json_agg(
                    json_build_object(
                        'player_name', player_name,
                        'player_id', player_id,
                        'tfm_pic_url', tfm_pic_url,
                        'followers_decrease', followers_decrease,
                        'followers_before', followers_before,
                        'followers_now', followers_now
                    )
                ), '[]'::json)
            ) as data
        FROM top_players
    """)
    
    params = {
        "player_ids": player_ids,
        "start_date": start_date,
        "end_date": end_date
    }
    
    result = session.exec(query, params=params).first()
    
    return {"data": result[0]} if result else {
        "data": {
            "start_date": start_date,
            "end_date": end_date,
            "players": []
        }
    }










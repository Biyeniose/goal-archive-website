from fastapi import APIRouter, Depends, Query, HTTPException
from supabase import Client
from datetime import date

from ..dependencies import DBSession
#from ..classes.stat import StatsRanking, StatsService, LeagueStats, TeamMatches, TeamMatchesResponse
#from app.models.response import H2HResponse, InfoMatch, LeagueFormResponse, PlayerPerformance, PlayerPerformanceData, PlayerPerformanceResponse, PlayerRecordResponse, PlayerWeeklyStats, TeamH2HResponse
from ..models.response import TeamH2HResponse, PlayerWeeklyStats, PlayerRecordResponse, LeagueFormResponse, PlayerPerformance, PlayerPerformanceData, PlayerPerformanceResponse

from typing import List, Optional, Union
#import requests, randomz
from sqlmodel import select
from sqlalchemy import text
from pydantic import BaseModel
from sqlalchemy.orm import aliased


router = APIRouter(
    prefix="/v1/stats",
    tags=["stats"],
    #dependencies=[Depends(get_supabase_client)],
    #responses={404: {"description": "Not found"}},
)

# Response model
class TeamResponse(BaseModel):
    league_id: int
    team_name: str
    team_id: int


@router.get("/{league_id}/teams", response_model=List[TeamResponse])
async def get_teams_by_league(league_id: int, session: DBSession):
    """Get all teams in a specific league"""
    
    query = text("""
        SELECT 
            league_id,
            name AS team_name,
            team_id
        FROM teams
        WHERE league_id = :league_id
        ORDER BY name
    """)
    
    result = session.exec(query, params={"league_id": league_id})
    
    # Convert to list of dicts
    teams = [
        {
            "league_id": row[0],
            "team_name": row[1],
            "team_id": row[2]
        }
        for row in result
    ]
    
    return teams


# weekly performances leaders
@router.get("/leaders/{league_id}", response_model=PlayerPerformanceResponse)
async def get_gameweek_stats_leaders(
    league_id: int,
    session: DBSession,
    league_ids: List[int] = Query([], description="List of league IDs"),
    season_year: int = Query(2025, description="Year"),
    start_date: str = Query("2025-08-21", description="Start date in YYYY-MM-DD format"),
    end_date: str = Query("2025-08-25", description="End date in YYYY-MM-DD format"),
    stat: str = Query("xg"),
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
                            'logo', pt.logo_url
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
                        'comp_logo', NULL,
                        'home_team', json_build_object(
                            'team_id', ht.team_id,
                            'team_name', ht.name,
                            'logo', ht.logo_url
                        ),
                        'away_team', json_build_object(
                            'team_id', at.team_id,
                            'team_name', at.name,
                            'logo', at.logo_url
                        )
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
                AND comp.season_year = :season_year
                AND comp.stage IN ('league', 'knockout phase')
                AND pms.{stat} IS NOT NULL
                AND pms.age <= :age 
            ORDER BY pms.{stat} DESC
            LIMIT :limit
        )
        SELECT json_build_object(
            'data', coalesce(json_agg(performance_data), '[]'::json)
        ) as result
        FROM player_data
    """)
    
    result = session.exec(query, params={
        "league_ids": all_league_ids,
        "season_year": season_year,
        "start_date": start_date,
        "end_date": end_date,
        "age": age,
        "limit": limit
    }).first()
    
    return result[0] if result else {"data": []}

class SeasonStatsLeader(BaseModel):
    player_name: str
    player_id: int
    position: Optional[str] = None
    height: Optional[Union[int, float]] = None
    age: Optional[int] = None
    mpg: Optional[float] = None
    mins: Optional[int] = None
    games: Optional[int] = None
    
    # Goals and assists
    goals: Optional[int] = None
    goals_p90: Optional[float] = None
    assists: Optional[int] = None
    assists_p90: Optional[float] = None
    goals_assists: Optional[int] = None
    goals_assists_p90: Optional[float] = None
    
    # Passing
    passes_completed: Optional[int] = None
    passes_completed_p90: Optional[float] = None
    
    # Carries
    progressive_carries: Optional[int] = None
    progressive_carries_p90: Optional[float] = None
    
    # Shooting
    shots: Optional[int] = None
    shots_p90: Optional[float] = None
    
    # Defending
    tackles: Optional[int] = None
    tackles_p90: Optional[float] = None
    blocks: Optional[int] = None
    blocks_p90: Optional[float] = None
    
    # Dribbling
    take_ons_won: Optional[int] = None
    take_ons_won_p90: Optional[float] = None
    
    # Teams
    team: Optional[str] = None
    team_id: Optional[int] = None
    team_logo: Optional[str] = None
    team2: Optional[str] = None
    team2_id: Optional[int] = None
    team2_logo: Optional[str] = None
    
    # Countries
    country: Optional[str] = None
    country_flag: Optional[str] = None
    country2: Optional[str] = None
    country2_flag: Optional[str] = None

    class Config:
        extra = "allow"

class SeasonStatsLeadersResponse(BaseModel):
    data: List[SeasonStatsLeader]

# get dom league player stats leaders 
@router.get("/players-leaders/{league_id}", response_model=SeasonStatsLeadersResponse)
async def get_season_stats_leaders(
    league_id: int,
    session: DBSession,
    league_ids: List[int] = Query([], description="List of additional league IDs"),
    season_year: int = Query(2025, description="Year"),
    stat: str = Query("goals_assists_p90", description="Stat to order by (use _p90 suffix for per-90 stats)"),
    min_minutes: int = Query(450, description="Minimum minutes played"),
    max_age: int = Query(80, description="Maximum age"),
    country_id: Optional[int] = Query(None, description="Filter by country ID"),
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
            WHERE comp.league_id = ANY(:league_ids)
                AND comp.season_year = :season_year
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
        "league_ids": all_league_ids,
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
    team_id: int = Query(..., description="ID of the opponent team"),
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

# get team Dom league stats between a time period
@router.get("/ranks/{stat}/", response_model=LeagueFormResponse)
async def get_form_by_dates(
    stat: str,
    session: DBSession,
    comp_ids: List[int] = Query([202025], description="List of competition IDs"),
    start_date: date = Query("2025-08-01", description="Start date in YYYY-MM-DD format"),
    end_date: date = Query("2025-12-31", description="End date in YYYY-MM-DD format"),
):
    query = text("""
        WITH team_matches AS (
            -- Get ONLY matches within the date range for each team
            SELECT 
                t.team_id,
                t.name as team_name,
                t.logo_url as logo,
                m.match_id,  -- Added match_id for counting distinct matches
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
                        (t.team_id = m.away_id AND m.away_goals > m.home_goals) THEN 3  -- Win
                    WHEN m.home_goals = m.away_goals THEN 1  -- Draw
                    ELSE 0  -- Loss
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
                END as losses
            FROM matches m
            JOIN teams t ON (t.team_id = m.home_id OR t.team_id = m.away_id)
            WHERE m.comp_id = ANY(:comp_ids)
            AND m.isplayed = TRUE
            AND m.match_date BETWEEN :start_date AND :end_date  -- Strict date range
        ),

        form_stats AS (
            -- Calculate stats ONLY for matches in the date range
            SELECT 
                team_id,
                team_name,
                logo,
                COUNT(DISTINCT match_id) as gp,  -- Count distinct matches, not rows
                SUM(points) as points,
                SUM(wins) as wins,
                SUM(draws) as draws,
                SUM(losses) as losses,
                SUM(goals_for) as goals_f,
                SUM(goals_against) as goals_a,
                SUM(goals_for) - SUM(goals_against) as gd
            FROM team_matches
            GROUP BY team_id, team_name, logo
            HAVING COUNT(DISTINCT match_id) > 0  -- Only include teams with matches in this period
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
                        CASE WHEN :stat = 'points' THEN points END DESC,
                        CASE WHEN :stat = 'goals_f' THEN goals_f END DESC,
                        CASE WHEN :stat = 'goals_a' THEN goals_a END ASC,  -- Fewer goals against is better
                        CASE WHEN :stat = 'gd' THEN gd END DESC,
                        CASE WHEN :stat = 'wins' THEN wins END DESC,
                        CASE WHEN :stat = 'losses' THEN losses END ASC,  -- Fewer losses is better
                        CASE WHEN :stat = 'draws' THEN draws END DESC,
                        -- Default tiebreakers
                        gd DESC, 
                        goals_f DESC, 
                        team_name ASC
                ) as rank
            FROM form_stats
        )

        SELECT 
            json_build_object(
                'data', json_build_object(
                    'form', json_agg(
                        json_build_object(
                            'team', json_build_object(
                                'team_id', team_id,
                                'team_name', team_name,
                                'logo', logo
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
                    )
                )
            ) as result
        FROM ranked_form
    """)
    
    result = session.exec(query, params={
        "comp_ids": comp_ids,
        "start_date": start_date,
        "end_date": end_date,
        "stat": stat
    }).first()
    
    return result[0] if result else {"data": {"form": []}}


# h2h
@router.get("/h2h/{team1_id}/{team2_id}", response_model=TeamH2HResponse)
async def get_team_h2h(
    team1_id: int,
    team2_id: int,
    session: DBSession,
    match_limit: int = Query(10, description="Number of recent matches to retrieve")
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



class PlayerSearchResult(BaseModel):
    """Single player search result"""
    player_name: str
    player_id: int
    tfm_pic_url: Optional[str] = None
    country: Optional[str] = None
    country_id: Optional[int] = None
    country_flag: Optional[str] = None


class PlayerSearchData(BaseModel):
    """Player search results data"""
    players: List[PlayerSearchResult]


class PlayerSearchResponse(BaseModel):
    """Root response model"""
    data: PlayerSearchData


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
            "tfm_pic_url": row[2],
            "country": row[3],
            "country_id": row[4],
            "country_flag": row[5]
        }
        for row in result
    ]
    
    return {"data": {"players": players}}

class TeamSearchResult(BaseModel):
    """Single team search result"""
    team_name: str
    common_name: Optional[str] = None
    team_id: int
    logo_url: Optional[str] = None


class TeamSearchData(BaseModel):
    """Team search results data"""
    teams: List[TeamSearchResult]


class TeamSearchResponse(BaseModel):
    """Root response model"""
    data: TeamSearchData
    
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



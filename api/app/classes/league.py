from fastapi import HTTPException
from datetime import date
#from typing import List
from pydantic import BaseModel
from supabase import Client
import requests, json
from typing import Optional
#from ..models.league import TeamRank, LeagueRanking, TopLeaguesResponse
from app.models.response import LeagueDataResponse, LeagueStatsResponse
from app.models.player import PlayerBasicInfo
from app.models.team import Team

class LeagueService:
    def __init__(self, supabase_client: Client):
        self.supabase = supabase_client
        self.key = supabase_client.supabase_key
        self.url = f"{supabase_client.rest_url}/rpc/execute_sql"
        self.headers = {
            "Authorization": f"Bearer {self.key}",
            "apikey": self.key,
            "Content-Type": "application/json"
        }

    # /leagues/:id/infos
    def get_league_info(self, comp_id: int, season: int):
        query = f"""
            WITH league_check AS (
                SELECT l.type 
                FROM leagues l 
                WHERE l.league_id = {comp_id}
            )
            SELECT 
                CASE WHEN (SELECT type FROM league_check) LIKE '%League%' THEN
                    json_build_object(
                        'data', json_agg(
                            json_build_object(
                                'info', json_build_object(
                                    'comp_id', l.league_id,
                                    'league_name', l.league_name,
                                    'league_logo', l.logo_url,
                                    'country_id', l.country_id,
                                    'type', l.type,
                                    'country_url', t.logo_url
                                ),
                                'ranks', (
                                    SELECT json_agg(subq)
                                    FROM (
                                        SELECT 
                                            json_build_object(
                                                'rank', lr.rank::text,
                                                'team', json_build_object(
                                                    'team_id', lr.team_id,
                                                    'team_name', t2.team_name,
                                                    'logo', t2.logo_url
                                                ),
                                                'info', COALESCE(lr.info, ''),
                                                'points', lr.points,
                                                'gp', lr.gp,
                                                'wins', lr.wins,
                                                'losses', lr.losses,
                                                'draws', lr.draws,
                                                'goals_f', lr.goals_f,
                                                'goals_a', lr.goals_a,
                                                'gd', lr.gd
                                            ) as subq
                                        FROM league_ranks lr
                                        JOIN teams t2 ON lr.team_id = t2.team_id
                                        WHERE lr.comp_id = {comp_id}
                                        AND lr.season_year = {season}
                                        ORDER BY lr.rank::integer ASC
                                        LIMIT 20
                                    ) as subq
                                ),
                                'matches', (
                                    SELECT json_agg(match_subq)
                                    FROM (
                                        SELECT 
                                            json_build_object(
                                                'teams', json_build_object(
                                                    'home', json_build_object(
                                                        'stats', json_build_object(
                                                            'goals', COALESCE(m.home_goals, 0),
                                                            'pen_goals', COALESCE(m.pen_home_goals, 0),
                                                            'ranking', m.home_ranking
                                                        ),
                                                        'team', json_build_object(
                                                            'team_id', m.home_id,
                                                            'team_name', ht.team_name,
                                                            'logo', ht.logo_url
                                                        )
                                                    ),
                                                    'away', json_build_object(
                                                        'stats', json_build_object(
                                                            'goals', COALESCE(m.away_goals, 0),
                                                            'pen_goals', COALESCE(m.pen_away_goals, 0),
                                                            'ranking', m.away_ranking
                                                        ),
                                                        'team', json_build_object(
                                                            'team_id', m.away_id,
                                                            'team_name', at.team_name,
                                                            'logo', at.logo_url
                                                        )
                                                    )
                                                ),
                                                'match_info', json_build_object(
                                                    'match_id', m.match_id,
                                                    'match_date', m.match_date,
                                                    'date_time_utc', m.date_time_utc,
                                                    'round', m.round,
                                                    'season_year', m.season_year,
                                                    'draw', m."isDraw",
                                                    'et', m.extra_time,
                                                    'pens', m.pens,
                                                    'result', m.result,
                                                    'comp_id', m.comp_id,
                                                    'comp', l.league_name,
                                                    'comp_logo', l.logo_url
                                                )
                                            ) as match_subq
                                        FROM matches m
                                        JOIN teams ht ON m.home_id = ht.team_id
                                        JOIN teams at ON m.away_id = at.team_id
                                        JOIN leagues l ON m.comp_id = l.league_id
                                        WHERE m.comp_id = {comp_id}
                                        AND m.season_year = {season}
                                        ORDER BY m.match_date DESC, m.match_time DESC
                                        LIMIT 8
                                    ) as match_subq
                                )
                            )
                        )
                    )
                ELSE
                    json_build_object('error', 'wrong competition type, make sure id is for a non-league competition')
                END AS result
            FROM leagues l
            LEFT JOIN teams t ON l.country_id = t.team_id
            WHERE l.league_id = {comp_id}
            GROUP BY l.league_id, l.league_name, l.country_id, l.type, t.logo_url

        """
        try:    
            response = requests.post(self.url, headers=self.headers, json={"sql_query": query})
            response.raise_for_status()
            result = response.json()
            #print(f"Supabase raw response status: {response.status_code}")
            #print(f"Supabase raw response text: {response.text}")
            if not result or not result.get("data"):
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for league {comp_id}"
                )

            return LeagueDataResponse(data=result["data"])
            
        except requests.exceptions.HTTPError as http_err:
            error_detail = response.text if hasattr(response, 'text') else str(http_err)
            raise HTTPException(status_code=500, detail=f"HTTP error occurred: {error_detail}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
        
    # /leagues/{league_id}/stats get highest stats of a league by year and stat
    def most_stats_league(self, league_id: int, season: int, stat: str, age: int):
        query = f"""
            WITH team_stats AS (
            SELECT
                ps.season_year,
                json_build_object(
                    'team_id', t.team_id,
                    'team_name', t.team_name,
                    'logo', t.logo_url
                ) AS team,
                json_build_object(
                    'id', pl.player_id,
                    'name', pl.player_name,
                    'current_age', pl.age,
                    'pic_url', pl.pic_url,
                    'nations', json_build_object(
                        'nation1_id', pl.nation1_id,
                        'nation1', n1.team_name,
                        'nation1_logo', n1.logo_url,
                        'nation2_id', pl.nation2_id,
                        'nation2', n2.team_name,
                        'nation2_logo', n2.logo_url
                    )
                ) AS player,
                ps.age,
                pl.position,
                ps.ga,
                ps.ga_pg,
                ps.goals,
                ps.goals_pg,
                ps.assists,
                ps.assists_pg,
                ps.penalty_goals,
                ps.gp,
                ps.minutes,
                ps.minutes_pg,
                ps.cs,
                ps.pass_compl_pg,
                ps.passes_pg,
                ps.errors_pg,
                ps.shots_pg,
                ps.shots_on_target_pg,
                ps.sca_pg,
                ps.gca_pg,
                ps.take_ons_pg,
                ps.take_ons_won_pg,
                ps.goals_concede,
                ps.yellows,
                ps.yellows2,
                ps.reds,
                ps.own_goals,
                ps.stats_id
            FROM player_stats ps
            LEFT JOIN players pl ON ps.player_id = pl.player_id
            LEFT JOIN teams t ON ps.team_id = t.team_id
            LEFT JOIN teams n1 ON pl.nation1_id = n1.team_id
            LEFT JOIN teams n2 ON pl.nation2_id = n2.team_id
            WHERE ps.season_year = {season} AND ps.comp_id = {league_id} AND ps.age <= {age}
            ORDER BY ps.{stat} DESC
            LIMIT 15
        )
        SELECT json_build_object(
            'data', json_build_object(
                'stats', (SELECT coalesce(json_agg(row_to_json(team_stats)), '[]'::json) FROM team_stats)
            )
        ) as result;
        """
        try:
            response = requests.post(
                self.url,
                headers=self.headers,
                json={"sql_query": query}
            )
            response.raise_for_status()
            result = response.json()
            #print(f"Supabase raw response text: {result}")
            if not result or not result.get("data"):
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for league {league_id}"
                )
            # Parse the response according to the actual structure
            #return LeagueStatsResponse(data=result["data"])
            return result
        
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error: {str(e)}"
            )

    # /leagues/{league_id}/allstats get highest stats of a league by year and stat
    def most_alltime_stats_league(self, league_id: int, stat: str, age: int):
        query = f"""
            WITH team_stats AS (
            SELECT
                ps.season_year,
                json_build_object(
                    'team_id', t.team_id,
                    'team_name', t.team_name,
                    'logo', t.logo_url
                ) AS team,
                json_build_object(
                    'id', pl.player_id,
                    'name', pl.player_name,
                    'current_age', pl.age,
                    'pic_url', pl.pic_url,
                    'nations', json_build_object(
                        'nation1_id', pl.nation1_id,
                        'nation1', n1.team_name,
                        'nation1_logo', n1.logo_url,
                        'nation2_id', pl.nation2_id,
                        'nation2', n2.team_name,
                        'nation2_logo', n2.logo_url
                    )
                ) AS player,
                ps.age,
                pl.position,
                ps.ga,
                ps.ga_pg,
                ps.goals,
                ps.goals_pg,
                ps.assists,
                ps.assists_pg,
                ps.penalty_goals,
                ps.gp,
                ps.minutes,
                ps.minutes_pg,
                ps.cs,
                ps.pass_compl_pg,
                ps.passes_pg,
                ps.errors_pg,
                ps.shots_pg,
                ps.shots_on_target_pg,
                ps.sca_pg,
                ps.gca_pg,
                ps.take_ons_pg,
                ps.take_ons_won_pg,
                ps.goals_concede,
                ps.yellows,
                ps.yellows2,
                ps.reds,
                ps.own_goals,
                ps.stats_id
            FROM player_stats ps
            LEFT JOIN players pl ON ps.player_id = pl.player_id
            LEFT JOIN teams t ON ps.team_id = t.team_id
            LEFT JOIN teams n1 ON pl.nation1_id = n1.team_id
            LEFT JOIN teams n2 ON pl.nation2_id = n2.team_id
            WHERE ps.comp_id = {league_id} AND ps.age <= {age}
            ORDER BY ps.{stat} DESC
            LIMIT 15
        )
        SELECT json_build_object(
            'data', json_build_object(
                'stats', (SELECT coalesce(json_agg(row_to_json(team_stats)), '[]'::json) FROM team_stats)
            )
        ) as result;
        """
        try:
            response = requests.post(
                self.url,
                headers=self.headers,
                json={"sql_query": query}
            )
            response.raise_for_status()
            result = response.json()
            #print(f"Supabase raw response text: {result}")
            if not result or not result.get("data"):
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for league {league_id}"
                )
            # Parse the response according to the actual structure
            #return LeagueStatsResponse(data=result["data"])
            return result
        
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error: {str(e)}"
            )

    # /leagues/:league_id/:team_id/stats get highest stats of a league by year and stat
    def most_league_stats_by_team(self, team_id: int, league_id: int, season: Optional[int] = None, stat: str = "goals", age: int = 999, all_time: bool = False):
        # Base WHERE clauses
        where_clauses = [
            f"ps.comp_id = {league_id}",
            f"ps.age <= {age}",
            f"ps.team_id = {team_id}"
        ]
        # Conditionally add season_year to WHERE clause
        if not all_time and season is not None:
            where_clauses.append(f"ps.season_year = {season}")
        elif not all_time and season is None:
            # Handle case where all_time is False but no season is provided
            # You might want to raise an error or set a default season here
            raise ValueError("Season must be provided if 'all_time' is False.")

        # Join all WHERE conditions with ' AND '
        where_condition = " AND ".join(where_clauses)

        query = f"""
            WITH team_stats AS (
            SELECT
                ps.season_year,
                json_build_object(
                    'team_id', t.team_id,
                    'team_name', t.team_name,
                    'logo', t.logo_url
                ) AS team,
                json_build_object(
                    'id', pl.player_id,
                    'name', pl.player_name,
                    'current_age', pl.age,
                    'pic_url', pl.pic_url,
                    'nations', json_build_object(
                        'nation1_id', n1.team_id,
                        'nation1', n1.team_name,
                        'nation1_logo', n1.logo_url,
                        'nation2_id', n2.team_id,
                        'nation2', n2.team_name,
                        'nation2_logo', n2.logo_url
                    )
                ) AS player,
                ps.age,
                pl.position,
                ps.ga,
                ps.ga_pg,
                ps.goals,
                ps.goals_pg,
                ps.assists,
                ps.assists_pg,
                ps.penalty_goals,
                ps.gp,
                ps.minutes,
                ps.minutes_pg,
                ps.cs,
                ps.pass_compl_pg,
                ps.passes_pg,
                ps.errors_pg,
                ps.shots_pg,
                ps.shots_on_target_pg,
                ps.sca_pg,
                ps.gca_pg,
                ps.take_ons_pg,
                ps.take_ons_won_pg,
                ps.goals_concede,
                ps.yellows,
                ps.yellows2,
                ps.reds,
                ps.own_goals,
                ps.stats_id
            FROM player_stats ps
            LEFT JOIN players pl ON ps.player_id = pl.player_id
            LEFT JOIN teams t ON ps.team_id = t.team_id
            LEFT JOIN teams n1 ON pl.nation1_id = n1.team_id
            LEFT JOIN teams n2 ON pl.nation2_id = n2.team_id
            WHERE {where_condition}
            ORDER BY ps.{stat} DESC
            LIMIT 40
        )
        SELECT json_build_object(
            'data', json_build_object(
                'stats', (SELECT coalesce(json_agg(row_to_json(team_stats)), '[]'::json) FROM team_stats)
            )
        ) as result;
        """
        try:
            response = requests.post(
                self.url,
                headers=self.headers,
                json={"sql_query": query}
            )
            response.raise_for_status()
            result = response.json()
            
            if not result or not result.get("data"):
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for team {team_id} in league {league_id}"
                )
            
            return result
        
        except requests.exceptions.HTTPError as http_err:
            error_detail = response.text if hasattr(response, 'text') else str(http_err)
            raise HTTPException(
                status_code=500,
                detail=f"Supabase error: {error_detail}"
            )
        except ValueError as ve: # Catch the specific ValueError for missing season
            raise HTTPException(
                status_code=400, # Bad request for missing required parameter
                detail=str(ve)
            )
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error: {str(e)}"
            )

    # GET the rankings of Top Leagues
    def top_leagues_rankings(self, season: int):
        # List of competition IDs we want to include
        comp_ids = [1, 2, 3, 4, 5, 7, 8,9, 25, 10, 11, 12, 13, 20, 85, 75, 291, 5179]
        
        try:
            # Query to get league info and top 5 rankings for each competition
            query = f"""
            SELECT json_agg(league_data ORDER BY league_order)
            FROM (
                SELECT 
                    l.league_id AS comp_id,
                    l.league_name,
                    l.country_id,
                    l.type,
                    t.logo_url AS country_url,
                    (
                        SELECT json_agg(team_ranks)
                        FROM (
                            SELECT 
                                lr.rank::text AS rank,
                                lr.team_id,
                                t2.team_name,
                                t2.logo_url AS team_logo,
                                COALESCE(lr.info, '') AS info,
                                lr.points,
                                lr.gp,
                                lr.wins,
                                lr.losses,
                                lr.draws,
                                lr.goals_f,
                                lr.goals_a,
                                lr.gd
                            FROM league_ranks lr
                            JOIN teams t2 ON lr.team_id = t2.team_id
                            WHERE lr.comp_id = l.league_id
                            AND lr.season_year = {season}
                            ORDER BY lr.rank::integer ASC
                            LIMIT 20
                        ) team_ranks
                    ) AS ranks,
                    CASE l.league_id
                        WHEN 1 THEN 1
                        WHEN 2 THEN 2
                        WHEN 3 THEN 3
                        WHEN 4 THEN 4
                        WHEN 5 THEN 5
                        WHEN 7 THEN 6
                        WHEN 8 THEN 7
                        WHEN 9 THEN 8
                        WHEN 10 THEN 9
                        WHEN 11 THEN 10
                        WHEN 12 THEN 11
                        WHEN 13 THEN 12
                        WHEN 20 THEN 13
                        WHEN 291 THEN 14
                        ELSE 999
                    END AS league_order
                FROM leagues l
                LEFT JOIN teams t ON l.country_id = t.team_id
                WHERE l.league_id IN ({','.join(map(str, comp_ids))})
                ORDER BY league_order
            ) league_data;
            """
            response = requests.post(self.url, headers=self.headers, json={"sql_query": query})
            #response.raise_for_status()
            
            results = response.json()
            
            if not results or not results[0].get("result"):
                raise HTTPException(
                    status_code=404,
                    detail="No league rankings found for the specified season"
                )
            # Parse the response
            league_data = results[0]["result"]
            if isinstance(league_data, dict):
                league_data = [league_data]
            return {"data": league_data}
        except requests.exceptions.HTTPError as http_err:
            error_detail = response.text if hasattr(response, 'text') else str(http_err)
            raise HTTPException(status_code=500, detail=f"HTTP error occurred: {error_detail}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
    
    # /leagues/{league_id}/stats get highest stats of a league by year and stat
    def get_league_matches(self, league_id: int, season: int):
        query = f"""
        SELECT json_build_object(
            'data', json_build_object(
                'matches', json_agg(
                    json_build_object(
                        'teams', json_build_object(
                            'home', json_build_object(
                                'team', json_build_object(
                                    'team_id', match_data.home_id,
                                    'team_name', match_data.home_team_name,
                                    'logo', match_data.home_logo_url
                                ),
                                'stats', json_build_object(
                                    'goals', COALESCE(match_data.home_goals, 0),
                                    'pen_goals', COALESCE(match_data.pen_home_goals, 0),
                                    'ranking', match_data.home_ranking
                                )
                            ),
                            'away', json_build_object(
                                'team', json_build_object(
                                    'team_id', match_data.away_id,
                                    'team_name', match_data.away_team_name,
                                    'logo', match_data.away_logo_url
                                ),
                                'stats', json_build_object(
                                    'goals', COALESCE(match_data.away_goals, 0),
                                    'pen_goals', COALESCE(match_data.pen_away_goals, 0),
                                    'ranking', match_data.away_ranking
                                )
                            )
                        ),
                        'match_info', json_build_object(
                            'match_id', match_data.match_id,
                            'match_date', match_data.match_date,
                            'date_time_utc', match_data.date_time_utc,
                            'round', match_data.round,
                            'season_year', match_data.season_year,
                            'draw', match_data."isDraw",
                            'et', match_data.extra_time,
                            'pens', match_data.pens,
                            'result', match_data.result,
                            'comp_id', match_data.comp_id,
                            'comp', match_data.league_name,
                            'comp_logo', match_data.league_logo_url
                        )
                    )
                )
            )
        ) as result
        FROM (
            SELECT 
                m.match_id,
                m.match_date,
                m.home_ranking,
                m.away_ranking,
                m.date_time_utc,
                m.round,
                m.season_year,
                m."isDraw",
                m.extra_time,
                m.pens,
                m.result,
                m.comp_id,
                m.home_id,
                m.away_id,
                m.home_goals,
                m.away_goals,
                COALESCE(m.pen_home_goals, 0) as pen_home_goals,
                COALESCE(m.pen_away_goals, 0) as pen_away_goals,
                ht.team_name as home_team_name,
                ht.logo_url as home_logo_url,
                at.team_name as away_team_name,
                at.logo_url as away_logo_url,
                l.league_name,
                l.logo_url as league_logo_url
            FROM matches m
            LEFT JOIN teams ht ON m.home_id = ht.team_id
            LEFT JOIN teams at ON m.away_id = at.team_id
            LEFT JOIN leagues l ON m.comp_id = l.league_id
            WHERE m.comp_id = {league_id} AND m.season_year = {season}
            ORDER BY m.match_date DESC
            LIMIT 300
        ) as match_data;
        """
        try:
            response = requests.post(
                self.url,
                headers=self.headers,
                json={"sql_query": query}
            )
            response.raise_for_status()
            result = response.json()
            #print(f"Supabase raw response text: {result}")
            if not result or not result.get("data"):
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for league {league_id}"
                )
            # Parse the response according to the actual structure
            #return LeagueStatsResponse(data=result["data"])
            return result
        
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error: {str(e)}"
            )

    # /leagues/:id/ranks?season
    def get_league_ranks(self, comp_id: int, season: int):
        try:
            # Query to get league info and rankings with type check
            query = f"""
            WITH league_check AS (
                SELECT l.type
                FROM leagues l
                WHERE l.league_id = {comp_id}
            ),
            rank_entries AS (
                SELECT
                    json_build_object(
                        'rank', lr.rank::text,
                        'team', json_build_object(
                            'team_id', lr.team_id,
                            'team_name', t2.team_name,
                            'logo', t2.logo_url
                        ),
                        'info', COALESCE(lr.info, ''),
                        'points', lr.points,
                        'gp', lr.gp,
                        'wins', lr.wins,
                        'losses', lr.losses,
                        'draws', lr.draws,
                        'goals_f', lr.goals_f,
                        'goals_a', lr.goals_a,
                        'gd', lr.gd
                    ) as rank_json
                FROM league_ranks lr
                JOIN teams t2 ON lr.team_id = t2.team_id
                WHERE lr.comp_id = {comp_id}
                AND lr.season_year = {season}
                ORDER BY lr.rank::integer ASC
                LIMIT 40
            )
            SELECT
                CASE WHEN (SELECT type FROM league_check) LIKE '%League%' THEN
                    json_build_object(
                        'data', json_build_object( -- <--- Changed from json_agg to json_build_object
                            'ranks', (SELECT coalesce(json_agg(rank_json), '[]'::json) FROM rank_entries)
                        )
                    )
                ELSE
                    json_build_object('error', 'wrong competition type, make sure id is for a non-league competition')
                END AS result
            FROM leagues l
            WHERE l.league_id = {comp_id};
            """
            response = requests.post(self.url, headers=self.headers, json={"sql_query": query})
            response.raise_for_status()
            result = response.json()
            #print(f"Supabase raw response status: {response.status_code}")
            #print(f"Supabase raw response text: {response.text}")
            
            if not result or not result.get("data"):
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for league {comp_id}"
                )
            return result
            
        except requests.exceptions.HTTPError as http_err:
            error_detail = response.text if hasattr(response, 'text') else str(http_err)
            raise HTTPException(status_code=500, detail=f"HTTP error occurred: {error_detail}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
      
    # /leagues/{league_id}/form-recent 
    def get_league_form_by_year(self, league_id: int, season: int):
        query = f"""
        WITH team_matches AS (
            -- Get all matches for each team (both home and away)
            SELECT 
                lr.team_id,
                t.team_name,
                t.logo_url as logo,
                m.match_date,
                m.home_id,
                m.away_id,
                m.home_goals,
                m.away_goals,
                CASE 
                    WHEN lr.team_id = m.home_id THEN m.home_goals
                    ELSE m.away_goals
                END as goals_for,
                CASE 
                    WHEN lr.team_id = m.home_id THEN m.away_goals
                    ELSE m.home_goals
                END as goals_against,
                CASE 
                    WHEN (lr.team_id = m.home_id AND m.home_goals > m.away_goals) OR 
                        (lr.team_id = m.away_id AND m.away_goals > m.home_goals) THEN 3  -- Win
                    WHEN m.home_goals = m.away_goals THEN 1  -- Draw
                    ELSE 0  -- Loss
                END as points,
                CASE 
                    WHEN (lr.team_id = m.home_id AND m.home_goals > m.away_goals) OR 
                        (lr.team_id = m.away_id AND m.away_goals > m.home_goals) THEN 1
                    ELSE 0
                END as wins,
                CASE 
                    WHEN m.home_goals = m.away_goals THEN 1
                    ELSE 0
                END as draws,
                CASE 
                    WHEN (lr.team_id = m.home_id AND m.home_goals < m.away_goals) OR 
                        (lr.team_id = m.away_id AND m.away_goals < m.home_goals) THEN 1
                    ELSE 0
                END as losses,
                ROW_NUMBER() OVER (PARTITION BY lr.team_id ORDER BY m.match_date DESC) as match_rank
            FROM league_ranks lr
            JOIN teams t ON lr.team_id = t.team_id
            JOIN matches m ON (lr.team_id = m.home_id OR lr.team_id = m.away_id)
            WHERE m.comp_id = {league_id}  -- Replace with your desired competition ID
            AND m.season_year = {season}  -- Replace with your desired season
            AND lr.comp_id = {league_id}  -- Ensure we're getting teams from the same competition
            AND lr.season_year = {season}
        ),

        last_6_matches AS (
            -- Get only the last 6 matches for each team
            SELECT *
            FROM team_matches
            WHERE match_rank <= 6
        ),

        form_stats AS (
            -- Calculate form statistics for each team
            SELECT 
                team_id,
                team_name,
                logo,
                COUNT(*) as gp,
                SUM(points) as points,
                SUM(wins) as wins,
                SUM(draws) as draws,
                SUM(losses) as losses,
                SUM(goals_for) as goals_f,
                SUM(goals_against) as goals_a,
                SUM(goals_for) - SUM(goals_against) as gd
            FROM last_6_matches
            GROUP BY team_id, team_name, logo
        ),

        ranked_form AS (
            -- Rank teams based on form (points first, then goal difference, then goals scored)
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
                    ORDER BY points DESC, gd DESC, goals_f DESC, team_name ASC
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
                            'rank', CAST(rank AS VARCHAR),
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
        FROM ranked_form;
        """
        try:
            response = requests.post(
                self.url,
                headers=self.headers,
                json={"sql_query": query}
            )
            response.raise_for_status()
            result = response.json()
            #print(f"Supabase raw response text: {result}")
            if not result or not result.get("data"):
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for league {league_id}"
                )
            # Parse the response according to the actual structure
            #return LeagueStatsResponse(data=result["data"])
            return result
        
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error: {str(e)}"
            )


    # /leagues/{league_id}/form-dates 
    def get_league_form_by_dates(self, league_id: int, start_date: str, end_date: str):
        query = f"""
        WITH team_matches AS (
            -- Get ONLY matches within the date range for each team
            SELECT 
                t.team_id,
                t.team_name,
                t.logo_url as logo,
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
            WHERE m.comp_id = {league_id}
            AND m.match_date BETWEEN '{start_date}' AND '{end_date}'  -- Strict date range
        ),

        form_stats AS (
            -- Calculate stats ONLY for matches in the date range
            SELECT 
                team_id,
                team_name,
                logo,
                COUNT(*) as gp,
                SUM(points) as points,
                SUM(wins) as wins,
                SUM(draws) as draws,
                SUM(losses) as losses,
                SUM(goals_for) as goals_f,
                SUM(goals_against) as goals_a,
                SUM(goals_for) - SUM(goals_against) as gd
            FROM team_matches
            GROUP BY team_id, team_name, logo
            HAVING COUNT(*) > 0  -- Only include teams with matches in this period
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
                    ORDER BY points DESC, gd DESC, goals_f DESC, team_name ASC
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
        FROM ranked_form;
        """
        try:
            response = requests.post(
                self.url,
                headers=self.headers,
                json={"sql_query": query}
            )
            response.raise_for_status()
            result = response.json()
            #print(f"Supabase raw response text: {result}")
            if not result or not result.get("data"):
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for league {league_id}"
                )
            # Parse the response according to the actual structure
            #return LeagueStatsResponse(data=result["data"])
            return result
        
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error: {str(e)}"
            )


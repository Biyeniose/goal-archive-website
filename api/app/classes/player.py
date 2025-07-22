from fastapi import HTTPException
from typing import List
from pydantic import BaseModel
from app.models.response import PlayerSeasonStatsResponse
from supabase import Client
import requests
from typing import Optional

class PlayerService:
    def __init__(self, supabase_client: Client):
        self.supabase = supabase_client
        self.key = supabase_client.supabase_key
        self.url = f"{supabase_client.rest_url}/rpc/execute_sql"
        self.headers = {
            "Authorization": f"Bearer {self.key}",
            "apikey": self.key,
            "Content-Type": "application/json"
        }

        
    
    def get_player_page_data(self, player_id: int):
        try:
            query = f"""
            WITH player_info AS (
                SELECT
                    p.player_id,
                    p.player_name,
                    p.full_name,
                    p.pic_url,
                    p."isRetired",
                    p.curr_team_id,
                    t.team_name AS curr_team_name,
                    t.logo_url AS curr_team_logo,
                    p.curr_number,
                    p."onLoan",
                    p.instagram,
                    p.parent_team_id,
                    t2.team_name AS parent_team_name,
                    t2.logo_url AS parent_team_logo,
                    p.position,
                    p.dob,
                    p.age,
                    p.pob,
                    json_build_object(
                        'nation1_id', p.nation1_id,
                        'nation2_id', p.nation2_id,
                        'nation1', n1.team_name,
                        'nation2', n2.team_name,
                        'nation1_url', n1.logo_url,
                        'nation2_url', n2.logo_url
                    ) as nations,
                    p.market_value,
                    p.height,
                    p.foot,
                    p.date_joined,
                    p.contract_end,
                    p.last_extension,
                    p.parent_club_exp,
                    p."noClub"
                FROM players p
                LEFT JOIN teams t ON p.curr_team_id = t.team_id
                LEFT JOIN teams t2 ON p.parent_team_id = t2.team_id
                LEFT JOIN teams n1 ON p.nation1_id = n1.team_id
                LEFT JOIN teams n2 ON p.nation2_id = n2.team_id
                

                WHERE p.player_id = {player_id}
            ),
            player_transfers AS (
                SELECT
                    tr.transfer_id,
                    tr.player_id,
                    p.player_name,
                    json_build_object(
                        'team_id', ft.team_id,
                        'team_name', ft.team_name,
                        'team_url', ft.logo_url,
                        'nation', fn.team_name,
                        'nation_url', fn.logo_url
                    ) as from_team,
                    json_build_object(
                        'team_id', tt.team_id,
                        'team_name', tt.team_name,
                        'team_url', tt.logo_url,
                        'nation', tn.team_name,
                        'nation_url', tn.logo_url
                    ) as to_team,
                    tr."isLoan",
                    tr.fee,
                    tr.value,
                    tr.date,
                    tr.season
                FROM transfers tr
                LEFT JOIN players p ON tr.player_id = p.player_id
                LEFT JOIN teams ft ON tr.from_team_id = ft.team_id
                LEFT JOIN teams tt ON tr.to_team_id = tt.team_id
                LEFT JOIN leagues fl ON ft.league_id = fl.league_id
                LEFT JOIN leagues tl ON tt.league_id = tl.league_id
                LEFT JOIN teams fn ON fl.country_id = fn.team_id
                LEFT JOIN teams tn ON tl.country_id = tn.team_id
                WHERE tr.player_id = {player_id}
                ORDER BY tr.date DESC
            ),

            player_stats AS (
                SELECT
                    json_build_object(
                        'player_id', ps.player_id,
                        'player_name', p.player_name,
                        'img', p.pic_url
                    ) as player,
                    json_build_object(
                        'comp_id', ps.comp_id,
                        'comp_name', l.league_name,
                        'comp_url', l.logo_url
                    ) as comp,
                    json_build_object(
                        'team_id', ps.team_id,
                        'team_name', t.team_name,
                        'logo', t.logo_url
                    ) as team,
                    ps.season_year,
                    ps.age,
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
                JOIN players p ON ps.player_id = p.player_id
                JOIN teams t ON ps.team_id = t.team_id
                JOIN leagues l ON ps.comp_id = l.league_id

                WHERE ps.player_id = {player_id}
                and ps.season_year = 2024
                ORDER BY ps.ga DESC
            )
            SELECT json_build_object(
                'data', json_build_object(
                    'info', (SELECT row_to_json(player_info) FROM player_info),
                    'transfers', (SELECT coalesce(json_agg(row_to_json(player_transfers)), '[]'::json) FROM player_transfers),
                    'stats', (SELECT coalesce(json_agg(row_to_json(player_stats)), '[]'::json) FROM player_stats)
                )
            ) as result;
            """
            
            response = requests.post(
                self.url,
                headers=self.headers,
                json={"sql_query": query}
            )
            response.raise_for_status()
            result = response.json() 
            #print(f"Supabase raw response status: {response.status_code}")
            #print(f"Supabase response: {result}")
            if not result or not result.get("data"):
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for"
                )
            return result
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error: {str(e)}"
            )


    def get_player_stats_all_seasons(self, player_id: int, season: int):
        query = f"""
        WITH all_stats AS (
            SELECT
                json_agg(
                    json_build_object(
                        'player', json_build_object(
                            'player_id', p.player_id,
                            'player_name', p.player_name,
                            'img', p.pic_url
                        ),
                        'comp', json_build_object(
                            'comp_id', l.league_id,
                            'comp_name', l.league_name,
                            'comp_url', l.logo_url
                        ),
                        'team', json_build_object(
                            'team_id', t.team_id,
                            'team_name', t.team_name,
                            'logo', t.logo_url
                        ),
                        'season_year', ps.season_year,
                        'age', ps.age,
                        'ga', ps.ga,
                        'ga_pg', ps.ga_pg,
                        'goals', ps.goals,
                        'goals_pg', ps.goals_pg,
                        'assists', ps.assists,
                        'assists_pg', ps.assists_pg,
                        'penalty_goals', ps.penalty_goals,
                        'gp', ps.gp,
                        'minutes', ps.minutes,
                        'minutes_pg', ps.minutes_pg,
                        'cs', ps.cs,
                        'pass_compl_pg', ps.pass_compl_pg,
                        'passes_pg', ps.passes_pg,
                        'errors_pg', ps.errors_pg,
                        'shots_pg', ps.shots_pg,
                        'shots_on_target_pg', ps.shots_on_target_pg,
                        'sca_pg', ps.sca_pg,
                        'gca_pg', ps.gca_pg,
                        'take_ons_pg', ps.take_ons_pg,
                        'take_ons_won_pg', ps.take_ons_won_pg,
                        'goals_concede', ps.goals_concede,
                        'yellows', ps.yellows,
                        'yellows2', ps.yellows2,
                        'reds', ps.reds,
                        'own_goals', ps.own_goals,
                        'stats_id', ps.stats_id
                    )
                ) AS stats_array
            FROM player_stats ps
            JOIN players p ON ps.player_id = p.player_id
            JOIN leagues l ON ps.comp_id = l.league_id
            JOIN teams t ON ps.team_id = t.team_id
            WHERE ps.player_id = {player_id} AND ps.season_year = {season}
        )
        SELECT 
            json_build_object(
                'data', json_build_object(
                    'stats', stats_array
                )
            ) AS result
        FROM all_stats;    
        """

        try:
            # Use "query" instead of "sql_query" for the parameter name
            response = requests.post(
                self.url,
                headers=self.headers,
                json={"sql_query": query}  # Changed from "sql_query" to "query"
            )
            response.raise_for_status()
            result = response.json()
            if not result or not result.get("data"):
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for player {player_id}"
                )
            
            return result
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error: {str(e)}"
            )


from fastapi import HTTPException
from typing import List
from pydantic import BaseModel
from ..models.player import PlayerPageDataResponse
from app.models.response import PlayerSeasonStatsResponse
from supabase import Client
import requests
from typing import Optional


# Define a Pydantic model
class PlayerBioInfo(BaseModel):
    player_id: int
    player_name: str
    full_name: Optional[str] = None
    isRetired: Optional[bool] = None
    onLoan: Optional[bool] = None
    curr_team_id: Optional[int] = None
    curr_team_name: Optional[str] = None
    curr_team_logo: Optional[str] = None
    curr_number: Optional[int] = None
    position: Optional[str] = None
    dob: str 
    age: Optional[int] = None
    pob: Optional[str] = None
    nation1_id: Optional[int] = None
    nation2_id: Optional[int] = None 
    nation1: Optional[str] = None
    nation2: Optional[str] = None
    nation1_url: Optional[str] = None
    nation2_url: Optional[str] = None 
    market_value: Optional[float] = None
    height: Optional[float] = None
    foot: Optional[str] = None
    date_joined: Optional[str] = None
    contract_end: Optional[str] = None

class PlayerStats(BaseModel):
    player_id: int
    age: Optional[int] = None
    isRetired: bool


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

    def get_player_details(self, player_id: int):
        query = f"""
        SELECT row_to_json(p) 
        FROM (
            SELECT 
                player_id,
                age,
                "isRetired"
            FROM players p
            WHERE p.player_id = {player_id}
        ) p;
        """

        try:
            response = requests.post(self.url, headers=self.headers, json={"query": query})
            if response.status_code == 200:
                results = response.json()
                data = [PlayerStats(**entry["result"]) for entry in results]
                return data[0] if data else None
            else:
                raise HTTPException(status_code=response.status_code, detail=response.text)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
        
    # add stats of their 2 most recent season
    def get_player_bio(self, player_id: int):
        query = f"""
        SELECT row_to_json(p) 
        FROM (
            SELECT 
                p.player_id,
                p.player_name,
                p.full_name,
                p."isRetired",
                p.curr_team_id,
                t.team_name AS curr_team_name,
                t.logo_url AS curr_team_logo,
                p.curr_number,
                p."onLoan",
                p.position,
                p.dob,
                p.age,
                p.pob,
                p.nation1_id,
                p.nation2_id,
                n1.team_name AS nation1,
                n2.team_name AS nation2,
                n1.logo_url AS nation1_url,
                n2.logo_url AS nation2_url,
                p.market_value,
                p.height,
                p.foot,
                p.date_joined,
                p.contract_end,
            FROM players p
            LEFT JOIN teams t ON p.curr_team_id = t.team_id
            LEFT JOIN teams n1 ON p.nation1_id = n1.team_id
            LEFT JOIN teams n2 ON p.nation2_id = n2.team_id
            WHERE p.player_id = {player_id}
        ) p;
        """

        try:
            response = requests.post(self.url, headers=self.headers, json={"query": query})
            if response.status_code == 200:
                results = response.json()
                data = [PlayerBioInfo(**entry["result"]) for entry in results]
                return data[0] if data else None
            else:
                raise HTTPException(status_code=response.status_code, detail=response.text)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

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

            goal_data AS (
                SELECT
                    me.id as event_id,
                    me.match_id,
                    m.match_date,
                    CASE
                        WHEN me.active_player_id = {player_id} THEN me.active_player_id
                        ELSE me.passive_player_id
                    END as player_id,
                    p.player_name,
                    me.event_type,
                    me.minute,
                    me.add_minute,
                    CASE
                        WHEN me.active_player_id = {player_id} THEN TRUE
                        ELSE FALSE
                    END as isGoalScorer,
                    json_build_object(
                        'team_name', ht.team_name,
                        'team_id', m.home_id,
                        'team_logo', ht.logo_url,
                        'goals', m.home_goals
                    ) as home_team,
                    json_build_object(
                        'team_name', at.team_name,
                        'team_id', m.away_id,
                        'team_logo', at.logo_url,
                        'goals', m.away_goals
                    ) as away_team
                FROM match_events me
                JOIN matches m ON me.match_id = m.match_id
                JOIN players p ON
                    (me.active_player_id = {player_id} AND p.player_id = me.active_player_id) OR
                    (me.passive_player_id = {player_id} AND p.player_id = me.passive_player_id)
                JOIN teams ht ON m.home_id = ht.team_id
                JOIN teams at ON m.away_id = at.team_id
                WHERE (me.active_player_id = {player_id} OR me.passive_player_id = {player_id})
                AND m.season_year = 2024
                AND me.event_type = 'Goal'
            ),
            latest_goal_data AS (
            SELECT
                gd.*,
                COUNT(CASE WHEN gd.isGoalScorer THEN 1 END) OVER (PARTITION BY gd.match_id) as total_match_goals,
                COUNT(CASE WHEN NOT gd.isGoalScorer THEN 1 END) OVER (PARTITION BY gd.match_id) as total_match_assists
            FROM goal_data gd
            ORDER BY gd.match_date DESC
            LIMIT 50
            ),
            player_stats AS (
                SELECT
                    ps.comp_id,
                    l.league_name as comp_name,
                    l.logo_url as comp_url,
                    ps.player_id,
                    ps.season_year,
                    p.player_name,
                    ps.age,
                    ps.team_id,
                    t.team_name,
                    t.logo_url as team_logo,
                    ps.ga,
                    ps.goals,
                    ps.assists,
                    ps.penalty_goals,
                    ps.gp,
                    ps.minutes,
                    ps.subbed_on,
                    ps.subbed_off,
                    ps.yellows,
                    ps.yellows2,
                    ps.reds,
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
                    'goal_data', (SELECT coalesce(json_agg(row_to_json(latest_goal_data)), '[]'::json) FROM latest_goal_data),
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
            # Debug prints (keep these for troubleshooting)
            #print(f"Supabase raw response status: {response.status_code}")
            #print(f"Supabase raw response text: {response.text}")
            #print(f"Supabase response: {result}")
            if not result or not result.get("data"):
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for"
                )
            return PlayerPageDataResponse(data=result["data"])
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error: {str(e)}"
            )


    def get_player_stats_all_seasons(self, player_id: int, season: int):
        query = f"""
        WITH all_stats AS (
            SELECT
                -- Player info (same for all rows)
                p.player_id,
                p.player_name,
                p.pic_url AS img,
                
                -- Array of all competitions/stats
                json_agg(
                    json_build_object(
                        'comp', json_build_object(
                            'league_id', l.league_id,
                            'league_name', l.league_name,
                            'logo', l.logo_url
                        ),
                        'team', json_build_object(
                            'team_id', t.team_id,
                            'team_name', t.team_name,
                            'logo', t.logo_url
                        ),
                        'id', ps.stats_id,
                        'season_year', ps.season_year,
                        'age', ps.age,
                        'gp', ps.gp,
                        'goals', ps.goals,
                        'assists', ps.assists,
                        'ga', ps.ga,
                        'penalty_goals', ps.penalty_goals,
                        'minutes', ps.minutes,
                        'minutes_pg', ps.minutes_pg,
                        'goals_pg', ps.goals_pg,
                        'shots_pg', ps.shots_pg,
                        'shots_on_target_pg', ps.shots_on_target_pg,
                        'passes_pg', ps.passes_pg,
                        'pass_compl_pg', ps.pass_compl_pg,
                        'take_ons_pg', ps.take_ons_pg,
                        'take_ons_won_pg', ps.take_ons_won_pg,
                        'ga_pg', ps.ga_pg,
                        'sca_pg', ps.sca_pg
                        
                    )
                ) AS stats_array
            FROM player_stats ps
            JOIN players p ON ps.player_id = p.player_id
            JOIN leagues l ON ps.comp_id = l.league_id
            JOIN teams t ON ps.team_id = t.team_id
            WHERE ps.player_id = {player_id} AND ps.season_year = {season}
            GROUP BY p.player_id, p.player_name, p.pic_url
        )
        SELECT 
            json_build_object(
                'data', json_build_object(
                    'player', json_build_object(
                        'player_id', player_id,
                        'player_name', player_name,
                        'img', img
                    ),
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
                    detail=f"No data found for match "
                )
                
            # Parse the response according to the actual structure
            return PlayerSeasonStatsResponse(data=result["data"])
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error: {str(e)}"
            )


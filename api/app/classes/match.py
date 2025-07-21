from fastapi import HTTPException
from typing import List
from pydantic import BaseModel
from app.models.response import MatchInfoResponse
from supabase import Client
import requests
from typing import Optional


class MatchService:
    def __init__(self, supabase_client: Client):
        self.supabase = supabase_client
        self.key = supabase_client.supabase_key
        self.url = f"{supabase_client.rest_url}/rpc/execute_sql"
        self.headers = {
            "Authorization": f"Bearer {self.key}",
            "apikey": self.key,
            "Content-Type": "application/json"
        }
    
    def get_match_data(self, match_id: int):
        query = f"""
        WITH events AS (
            SELECT
                id,
                match_id,
                event_type,
                minute,
                add_minute,
                home_goals,
                away_goals,
                match_events.team_id,
                json_build_object(
                    'id', active_p.player_id,
                    'name', active_p.player_name,
                    'current_age', active_p.age,
                    'pic_url', active_p.pic_url,
                    'nations', json_build_object(
                        'nation1_id', active_p.nation1_id,
                        'nation1', a_n1.team_name,
                        'nation1_logo', a_n1.logo_url,
                        'nation2_id', active_p.nation2_id,
                        'nation2', a_n2.team_name,
                        'nation2_logo', a_n2.logo_url
                    )
                ) as active_player,
                json_build_object(
                    'id', passive_p.player_id,
                    'name', passive_p.player_name,
                    'current_age', passive_p.age,
                    'pic_url', passive_p.pic_url,
                    'nations', json_build_object(
                        'nation1_id', passive_p.nation1_id,
                        'nation1', p_n1.team_name,
                        'nation1_logo', p_n1.logo_url,
                        'nation2_id', passive_p.nation2_id,
                        'nation2', p_n2.team_name,
                        'nation2_logo', p_n2.logo_url
                    )
                ) as passive_player
            FROM match_events
            LEFT JOIN players active_p ON match_events.active_player_id = active_p.player_id
            LEFT JOIN players passive_p ON match_events.passive_player_id = passive_p.player_id
            LEFT JOIN teams a_n1 ON active_p.nation1_id = a_n1.team_id
            LEFT JOIN teams a_n2 ON active_p.nation2_id = a_n2.team_id
            LEFT JOIN teams p_n1 ON passive_p.nation1_id = p_n1.team_id
            LEFT JOIN teams p_n2 ON passive_p.nation2_id = p_n2.team_id
            WHERE match_events.match_id = {match_id}
            ORDER BY (minute + add_minute) ASC
        ),
        teams AS (
            SELECT
                json_build_object(
                    'home', json_build_object(
                        'info', json_build_object(
                            'team', json_build_object(
                                'team_id', m.home_id,
                                'team_name', ht.team_name,
                                'team_logo', ht.logo_url,
                                'league_id', ht.league_id
                            ),
                            'manager', json_build_object(
                                'name', h_manager.name,
                                'manager_id', h_manager.id
                            )
                        ),
                        'lineups', (SELECT json_agg(
                            json_build_object(
                                'id', lh.id,
                                'player', json_build_object(
                                    'id', ph.player_id,
                                    'name', ph.player_name,
                                    'current_age', ph.age,
                                    'pic_url', ph.pic_url,
                                    'nations', json_build_object(
                                        'nation1_id', ph.nation1_id,
                                        'nation1', n1_h.team_name,
                                        'nation1_logo', n1_h.logo_url,
                                        'nation2_id', ph.nation2_id,
                                        'nation2', n2_h.team_name,
                                        'nation2_logo', n2_h.logo_url
                                    )
                                ),
                                'age', lh.age,
                                'number', lh.number,
                                'position', lh.position,
                                'xi', lh.xi,
                                'team_id', lh.team_id,
                                'stats', (SELECT row_to_json(pms_h) FROM player_match_stats pms_h 
                                         WHERE pms_h.player_id = lh.player_id AND pms_h.match_id = m.match_id)
                            )
                        ) FROM lineups lh
                        LEFT JOIN players ph ON lh.player_id = ph.player_id
                        LEFT JOIN teams n1_h ON ph.nation1_id = n1_h.team_id
                        LEFT JOIN teams n2_h ON ph.nation2_id = n2_h.team_id
                        WHERE lh.match_id = m.match_id AND lh.team_id = m.home_id),
                        'team_stats', json_build_object(
                            'ranking', m.home_ranking,
                            'goals', m.home_goals,
                            'pen_goals', m.pen_home_goals,
                            'xg', m.home_xg,
                            'formation', m.home_formation,
                            'possesion', m.home_poss,
                            'offsides', m.home_offsides,
                            'fouls', m.home_fouls,
                            'freekicks', m.home_freekicks,
                            'corners', m.home_corners,
                            'saves_succ', m.home_saves_succ,
                            'saves_att', m.home_saves_att,
                            'saves_acc', m.home_saves_acc,
                            'shots_att', m.home_shots_att,
                            'shots_succ', m.home_shots_succ,
                            'shot_acc', m.home_shot_acc,
                            'pass_att', m.home_pass_att,
                            'pass_succ', m.home_pass_succ,
                            'pass_acc', m.home_pass_acc
                        )
                    ),
                    'away', json_build_object(
                        'info', json_build_object(
                            'team', json_build_object(
                                'team_id', m.away_id,
                                'team_name', at.team_name,
                                'team_logo', at.logo_url,
                                'league_id', at.league_id
                            ),
                            'manager', json_build_object(
                                'name', a_manager.name,
                                'manager_id', a_manager.id
                            )
                        ),
                        'lineups', (SELECT json_agg(
                            json_build_object(
                                'id', la.id,
                                'player', json_build_object(
                                    'id', pa.player_id,
                                    'current_age', pa.age,
                                    'name', pa.player_name,
                                    'pic_url', pa.pic_url,
                                    'nations', json_build_object(
                                        'nation1_id', pa.nation1_id,
                                        'nation1', n1_a.team_name,
                                        'nation1_logo', n1_a.logo_url,
                                        'nation2_id', pa.nation2_id,
                                        'nation2', n2_a.team_name,
                                        'nation2_logo', n2_a.logo_url
                                    )
                                ),
                                'age', la.age,
                                'number', la.number,
                                'position', la.position,
                                'xi', la.xi,
                                'team_id', la.team_id,
                                'stats', (SELECT row_to_json(pms_a) FROM player_match_stats pms_a 
                                         WHERE pms_a.player_id = la.player_id AND pms_a.match_id = m.match_id)
                            )
                        ) FROM lineups la
                        LEFT JOIN players pa ON la.player_id = pa.player_id
                        LEFT JOIN teams n1_a ON pa.nation1_id = n1_a.team_id
                        LEFT JOIN teams n2_a ON pa.nation2_id = n2_a.team_id
                        WHERE la.match_id = m.match_id AND la.team_id = m.away_id),
                        'team_stats', json_build_object(
                            'ranking', m.away_ranking,
                            'goals', m.away_goals,
                            'pen_goals', m.pen_away_goals,
                            'xg', m.away_xg,
                            'formation', m.away_formation,
                            'poss', m.away_poss,
                            'offsides', m.away_offsides,
                            'possesion', m.away_poss,
                            'fouls', m.away_fouls,
                            'freekicks', m.away_freekicks,
                            'corners', m.away_corners,
                            'saves_succ', m.away_saves_succ,
                            'saves_att', m.away_saves_att,
                            'saves_acc', m.away_saves_acc,
                            'shots_att', m.away_shots_att,
                            'shots_succ', m.away_shots_succ,
                            'shot_acc', m.away_shot_acc,
                            'pass_att', m.away_pass_att,
                            'pass_succ', m.away_pass_succ,
                            'pass_acc', m.away_pass_acc
                        )
                    )
                ) as teams_data
            FROM matches m
            LEFT JOIN teams ht ON m.home_id = ht.team_id
            LEFT JOIN teams at ON m.away_id = at.team_id
            LEFT JOIN people h_manager ON m.home_manager_id = h_manager.id
            LEFT JOIN people a_manager ON m.away_manager_id = a_manager.id
            WHERE m.match_id = {match_id}
        )
        SELECT json_build_object(
            'data', json_build_object(
                'events', (SELECT coalesce(json_agg(row_to_json(events)), '[]'::json) FROM events),
                'teams', (SELECT teams_data FROM teams),
                'match_info', (SELECT row_to_json(m) FROM (
                    SELECT 
                        match_id, 
                        match_date,
                        date_time_utc,
                        round,
                        season_year,
                        "isDraw" as draw,
                        extra_time as et,
                        pens,
                        result,
                        comp_id, 
                        l.league_name as comp, 
                        l.logo_url as comp_logo
                    FROM matches
                    LEFT JOIN leagues l ON matches.comp_id = l.league_id
                    WHERE matches.match_id = {match_id}
                ) m)
            )
        ) as result;
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
            
            # Debug prints (keep these for troubleshooting)
            #print(f"Supabase raw response status: {response.status_code}")
            #print(f"Supabase raw response text: {response.text}")
            
            # The response structure is {"data": {...}} not a list with result[0]
            if not result or not result.get("data"):
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for match {match_id}"
                )
                
            # Parse the response according to the actual structure
            return MatchInfoResponse(data=result["data"])
            
        except requests.exceptions.HTTPError as http_err:
            error_detail = response.text if hasattr(response, 'text') else str(http_err)
            raise HTTPException(
                status_code=500, 
                detail=f"Supabase error: {error_detail}"
            )
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error: {str(e)}"
            )
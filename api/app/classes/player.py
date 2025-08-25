from fastapi import HTTPException
from typing import List
from pydantic import BaseModel
from app.models.response import StatsDist, TeamDist, Pens, PlayerGADistResponse, PlayerGADistData, TotalGA, GoalDist, Comp2
from app.models.league import Comp
from app.models.team import Team

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

    def player_search(self, player_name: str):
        try:
            # Search players with related team data
            result = self.supabase.table('players') \
                .select('''
                    player_id,
                    player_name,
                    full_name,
                    pic_url,
                    isRetired,
                    curr_team_id,
                    curr_number,
                    onLoan,
                    instagram,
                    parent_team_id,
                    position,
                    dob,
                    age,
                    pob,
                    nation1,
                    nation2,
                    nation1_id,
                    nation2_id,
                    market_value,
                    height,
                    foot,
                    date_joined,
                    contract_end,
                    last_extension,
                    parent_club_exp,
                    noClub,
                    curr_team:teams!curr_team_id(team_name, logo_url),
                    parent_team:teams!parent_team_id(team_name, logo_url),
                    nation1_team:teams!nation1_id(team_name, logo_url),
                    nation2_team:teams!nation2_id(team_name, logo_url)
                ''') \
                .or_(f'player_name.ilike.%{player_name}%,player_slug.ilike.%{player_name}%') \
                .order('isRetired', desc=False) \
                .order('market_value', desc=True) \
                .order('player_name') \
                .limit(15) \
                .execute()

            if not result.data:
                # Return empty search results in the correct format
                return {
                    "data": {
                        "search": []
                    }
                }

            # Transform the result to match PlayerSearchResponse format
            players_data = []
            for row in result.data:
                # Extract team information from nested objects
                curr_team = row.get("curr_team")
                parent_team = row.get("parent_team")
                nation1_team = row.get("nation1_team")
                nation2_team = row.get("nation2_team")
                
                curr_team_name = curr_team.get("team_name") if curr_team else None
                curr_team_logo = curr_team.get("logo_url") if curr_team else None
                parent_team_name = parent_team.get("team_name") if parent_team else None
                parent_team_logo = parent_team.get("logo_url") if parent_team else None
                
                # Build nations object if nations exist
                nations = None
                if row.get("nation1") or row.get("nation2"):
                    nations = {
                        "nation1_id": row.get("nation1_id"),
                        "nation1": row.get("nation1"),
                        "nation1_url": nation1_team.get("logo_url") if nation1_team else None,
                        "nation2_id": row.get("nation2_id"),
                        "nation2": row.get("nation2"),
                        "nation2_url": nation2_team.get("logo_url") if nation2_team else None
                    }

                player_info = {
                    "player_id": row["player_id"],
                    "player_name": row["player_name"],
                    "full_name": row.get("full_name"),
                    "pic_url": row.get("pic_url"),
                    "isRetired": row["isRetired"],
                    "curr_team_id": row.get("curr_team_id"),
                    "curr_team_name": curr_team_name,
                    "curr_team_logo": curr_team_logo,
                    "curr_number": row.get("curr_number"),
                    "onLoan": row.get("onLoan"),
                    "instagram": row.get("instagram"),
                    "parent_team_id": row.get("parent_team_id"),
                    "parent_team_name": parent_team_name,
                    "parent_team_logo": parent_team_logo,
                    "position": row.get("position"),
                    "dob": row["dob"],
                    "age": row["age"],
                    "pob": row.get("pob"),
                    "nations": nations,
                    "market_value": row.get("market_value"),
                    "height": row.get("height"),
                    "foot": row.get("foot"),
                    "date_joined": row.get("date_joined"),
                    "contract_end": row.get("contract_end"),
                    "last_extension": row.get("last_extension"),
                    "parent_club_exp": row.get("parent_club_exp"),
                    "noClub": row.get("noClub")
                }
                players_data.append(player_info)

            # Format response to match PlayerSearchResponse model
            formatted_response = {
                "data": {
                    "search": players_data
                }
            }

            return formatted_response

        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error: {str(e)}"
            )

    # basic player data for player page
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

    # random player transfer
    def get_random_transfer(self, start_date: str, end_date: str):
        try:
            query = f"""
            WITH transfer_data AS (
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
                WHERE tr.date >= '{start_date}'
                    AND tr.date <= '{end_date}'
                    AND tr.fee is NOT null
                    AND tr.fee >= 20000000
                    AND tr."isLoan" is false
                ORDER BY RANDOM()
                LIMIT 1
                )
            SELECT json_build_object(
            'data', json_build_object(
                'transfer', (SELECT row_to_json(transfer_data) FROM transfer_data)
            )
            ) as result
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


    # player teams in career
    def get_player_career_teams(self, player_id: int):
        try:
            query = f"""
            WITH player_teams AS (
                -- Get all unique teams the player has played for
                SELECT DISTINCT 
                    t.player_id,
                    t.from_team_id as team_id
                FROM transfers t
                WHERE t.player_id = {player_id}
                    AND t.from_team_id IS NOT NULL
                
                UNION
                
                SELECT DISTINCT 
                    t.player_id,
                    t.to_team_id as team_id
                FROM transfers t
                WHERE t.player_id = {player_id}
                    AND t.to_team_id IS NOT NULL
            ),
            
            player_info AS (
                SELECT 
                    p.player_id,
                    p.player_name,
                    p.age,
                    p.pic_url,
                    p.nation1_id,
                    n1.team_name as nation1,
                    n1.logo_url as nation1_logo,
                    p.nation2_id,
                    n2.team_name as nation2,
                    n2.logo_url as nation2_logo
                FROM players p
                LEFT JOIN teams n1 ON p.nation1_id = n1.team_id
                LEFT JOIN teams n2 ON p.nation2_id = n2.team_id
                WHERE p.player_id = {player_id}
            ),
            
            team_info AS (
                SELECT 
                    t.team_id,
                    t.team_name,
                    t.logo_url as team_url,
                    nt.team_name as nation,
                    nt.logo_url as nation_url
                FROM player_teams pt
                LEFT JOIN teams t ON pt.team_id = t.team_id
                LEFT JOIN teams nt ON t.nation_id = nt.team_id
                ORDER BY t.team_name
            )
            
            SELECT json_build_object(
                'data', json_build_object(
                    'player', (SELECT row_to_json(player_info) FROM player_info LIMIT 1),
                    'teams', (SELECT coalesce(json_agg(row_to_json(team_info)), '[]'::json) FROM team_info)
                )
            ) as result
            """
            response = requests.post(
                self.url,
                headers=self.headers,
                json={"sql_query": query},
                timeout=20,
            )
            response.raise_for_status()
            result = response.json() 
            #print(f"Supabase raw response status: {response.status_code}")
            #print(f"Supabase response: {result}")
            if not result or not result.get("data"):
                raise HTTPException(
                    status_code=404,
                    detail="No data found for"
                )
            return result
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error: {str(e)}"
            )

    # player teams in career2
    def get_player_career_teams2(self, player_id: int):
        try:
            query = f"""
            WITH all_player_teams AS (
                -- Get all team associations with dates
                SELECT 
                    t.player_id,
                    t.from_team_id as team_id,
                    t.date
                FROM transfers t
                WHERE t.player_id = {player_id}
                    AND t.from_team_id IS NOT NULL
                
                UNION ALL
                
                SELECT 
                    t.player_id,
                    t.to_team_id as team_id,
                    t.date
                FROM transfers t
                WHERE t.player_id = {player_id}
                    AND t.to_team_id IS NOT NULL
            ),
            
            player_teams AS (
                -- Get unique teams with their earliest date
                SELECT 
                    player_id,
                    team_id,
                    MIN(date) as earliest_date
                FROM all_player_teams
                GROUP BY player_id, team_id
            ),
            
            player_info AS (
                SELECT 
                    p.player_id,
                    p.player_name,
                    p.age,
                    p.pic_url,
                    p.nation1_id,
                    n1.team_name as nation1,
                    n1.logo_url as nation1_logo,
                    p.nation2_id,
                    n2.team_name as nation2,
                    n2.logo_url as nation2_logo
                FROM players p
                LEFT JOIN teams n1 ON p.nation1_id = n1.team_id
                LEFT JOIN teams n2 ON p.nation2_id = n2.team_id
                WHERE p.player_id = {player_id}
            ),
            
            team_info AS (
                SELECT 
                    t.team_id,
                    t.team_name,
                    t.logo_url as team_url,
                    nt.team_name as nation,
                    nt.logo_url as nation_url,
                    pt.earliest_date
                FROM player_teams pt
                LEFT JOIN teams t ON pt.team_id = t.team_id
                LEFT JOIN teams nt ON t.nation_id = nt.team_id
                ORDER BY pt.earliest_date ASC, t.team_name
            )
            
            SELECT json_build_object(
                'data', json_build_object(
                    'player', (SELECT row_to_json(player_info) FROM player_info LIMIT 1),
                    'teams', (SELECT coalesce(json_agg(row_to_json(team_info)), '[]'::json) FROM team_info)
                )
            ) as result
            """
            response = requests.post(
                self.url,
                headers=self.headers,
                json={"sql_query": query},
                timeout=20,
            )
            response.raise_for_status()
            result = response.json() 
            #print(f"Supabase raw response status: {response.status_code}")
            #print(f"Supabase response: {result}")
            if not result or not result.get("data"):
                raise HTTPException(
                    status_code=404,
                    detail="No data found for"
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

    def get_career_stats(self, player_id: int):
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
            WHERE ps.player_id = {player_id}
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


    def get_matches_by_season(self, player_id: int, season: int):
        query = f"""
        WITH match_data AS (
            SELECT 
                l.xi,
                l.id AS lineup_id,
                l.team_id,
                m.match_id,
                m.match_date,
                m.date_time_utc,
                m.round,
                m.season_year,
                m."isDraw" AS draw,
                m.extra_time AS et,
                m.pens,
                m.result,
                m.comp_id,
                lg.league_name AS comp,
                lg.logo_url AS comp_logo,
                json_build_object(
                    'team_id', ht.team_id,
                    'team_name', ht.team_name,
                    'logo', ht.logo_url
                ) AS home_team,
                json_build_object(
                    'goals', m.home_goals,
                    'pen_goals', COALESCE(m.pen_home_goals, 0),
                    'ranking', m.home_ranking
                ) AS home_stats,
                json_build_object(
                    'team_id', at.team_id,
                    'team_name', at.team_name,
                    'logo', at.logo_url
                ) AS away_team,
                json_build_object(
                    'goals', m.away_goals,
                    'pen_goals', COALESCE(m.pen_away_goals, 0),
                    'ranking', m.away_ranking
                ) AS away_stats,
                (
                    SELECT json_build_object(
                        'basic', json_build_object(
                            'id', pms.id,
                            'player_id', pms.player_id,
                            'match_id', pms.match_id,
                            'team_id', pms.team_id,
                            'minutes', pms.minutes,
                            'goals', pms.goals,
                            'assists', pms.assists,
                            'goals_assists', pms.goals_assists,
                            'pens_made', pms.pens_made,
                            'pens_att', pms.pens_att,
                            'age', pms.age,
                            'shots', pms.shots,
                            'shots_on_target', pms.shots_on_target,
                            'cards_yellow', pms.cards_yellow,
                            'cards_red', pms.cards_red,
                            'touches', pms.touches
                        ),
                        'defensive', json_build_object(
                            'tackles', pms.tackles,
                            'interceptions', pms.interceptions,
                            'blocks', pms.blocks,
                            'tackles_won', pms.tackles_won,
                            'tackles_def_3rd', pms.tackles_def_3rd,
                            'tackles_mid_3rd', pms.tackles_mid_3rd,
                            'tackles_att_3rd', pms.tackles_att_3rd,
                            'challenge_tackles', pms.challenge_tackles,
                            'challenges', pms.challenges,
                            'challenge_tackles_pct', pms.challenge_tackles_pct,
                            'challenges_lost', pms.challenges_lost,
                            'blocked_shots', pms.blocked_shots,
                            'blocked_passes', pms.blocked_passes,
                            'tackles_interceptions', pms.tackles_interceptions,
                            'clearances', pms.clearances,
                            'errors', pms.errors,
                            'ball_recoveries', pms.ball_recoveries
                        ),
                        'attacking', json_build_object(
                            'xg', pms.xg,
                            'npxg', pms.npxg,
                            'xg_assist', pms.xg_assist,
                            'sca', pms.sca,
                            'gca', pms.gca,
                            'take_ons', pms.take_ons,
                            'take_ons_won', pms.take_ons_won,
                            'take_ons_won_pct', pms.take_ons_won_pct,
                            'take_ons_tackled', pms.take_ons_tackled,
                            'take_ons_tackled_pct', pms.take_ons_tackled_pct,
                            'crosses', pms.crosses,
                            'own_goals', pms.own_goals,
                            'pens_won', pms.pens_won,
                            'pens_conceded', pms.pens_conceded
                        ),
                        'passing', json_build_object(
                            'passes_completed', pms.passes_completed,
                            'passes', pms.passes,
                            'passes_pct', pms.passes_pct,
                            'progressive_passes', pms.progressive_passes,
                            'passes_received', pms.passes_received,
                            'progressive_passes_received', pms.progressive_passes_received
                        ),
                        'possession', json_build_object(
                            'carries', pms.carries,
                            'progressive_carries', pms.progressive_carries,
                            'carries_distance', pms.carries_distance,
                            'carries_progressive_distance', pms.carries_progressive_distance,
                            'carries_into_final_third', pms.carries_into_final_third,
                            'carries_into_penalty_area', pms.carries_into_penalty_area,
                            'miscontrols', pms.miscontrols,
                            'dispossessed', pms.dispossessed,
                            'touches_def_pen_area', pms.touches_def_pen_area,
                            'touches_def_3rd', pms.touches_def_3rd,
                            'touches_mid_3rd', pms.touches_mid_3rd,
                            'touches_att_3rd', pms.touches_att_3rd,
                            'touches_att_pen_area', pms.touches_att_pen_area,
                            'touches_live_ball', pms.touches_live_ball
                        ),
                        'discipline', json_build_object(
                            'fouls', pms.fouls,
                            'fouled', pms.fouled,
                            'offsides', pms.offsides,
                            'cards_yellow_red', pms.cards_yellow_red,
                            'aerials_lost', pms.aerials_lost,
                            'aerials_won', pms.aerials_won,
                            'aerials_won_pct', pms.aerials_won_pct
                        )
                    )
                    FROM player_match_stats pms
                    WHERE pms.player_id = l.player_id
                    AND pms.match_id = l.match_id
                    AND pms.team_id = l.team_id
                ) AS player_stats
            FROM lineups l
            JOIN matches m ON l.match_id = m.match_id
            JOIN teams ht ON m.home_id = ht.team_id
            JOIN teams at ON m.away_id = at.team_id
            JOIN leagues lg ON m.comp_id = lg.league_id
            WHERE l.player_id = {player_id}
            AND m.season_year = {season}
            ORDER BY m.match_date DESC
        )
        SELECT 
            json_build_object(
                'data', json_build_object(
                    'matches', COALESCE(json_agg(
                        json_build_object(
                            'xi', md.xi,
                            'lineup_id', md.lineup_id,
                            'team_id', md.team_id,
                            'match_info', json_build_object(
                                'match_id', md.match_id,
                                'match_date', md.match_date,
                                'date_time_utc', md.date_time_utc,
                                'round', md.round,
                                'season_year', md.season_year,
                                'draw', md.draw,
                                'et', md.et,
                                'pens', md.pens,
                                'result', md.result,
                                'comp_id', md.comp_id,
                                'comp', md.comp,
                                'comp_logo', md.comp_logo
                            ),
                            'teams', json_build_object(
                                'home', json_build_object(
                                    'team', md.home_team,
                                    'stats', md.home_stats
                                ),
                                'away', json_build_object(
                                    'team', md.away_team,
                                    'stats', md.away_stats
                                )
                            ),
                            'player_stats', COALESCE(md.player_stats, 'null'::json)
                        )
                    ), '[]'::json)
                )
            ) AS result
        FROM match_data md;
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
                    detail=f"No data found for player {player_id}"
                )
            
            return result
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error: {str(e)}"
            )

    def get_matches_by_dates(self, player_id: int, start_date: str, end_date: str):
        query = f"""
        WITH match_data AS (
            SELECT 
                l.xi,
                l.id AS lineup_id,
                l.team_id,
                m.match_id,
                m.match_date,
                m.date_time_utc,
                m.round,
                m.season_year,
                m."isDraw" AS draw,
                m.extra_time AS et,
                m.pens,
                m.result,
                m.comp_id,
                lg.league_name AS comp,
                lg.logo_url AS comp_logo,
                json_build_object(
                    'team_id', ht.team_id,
                    'team_name', ht.team_name,
                    'logo', ht.logo_url
                ) AS home_team,
                json_build_object(
                    'goals', m.home_goals,
                    'pen_goals', COALESCE(m.pen_home_goals, 0),
                    'ranking', m.home_ranking
                ) AS home_stats,
                json_build_object(
                    'team_id', at.team_id,
                    'team_name', at.team_name,
                    'logo', at.logo_url
                ) AS away_team,
                json_build_object(
                    'goals', m.away_goals,
                    'pen_goals', COALESCE(m.pen_away_goals, 0),
                    'ranking', m.away_ranking
                ) AS away_stats,
                (
                    SELECT json_build_object(
                        'basic', json_build_object(
                            'id', pms.id,
                            'player_id', pms.player_id,
                            'match_id', pms.match_id,
                            'team_id', pms.team_id,
                            'minutes', pms.minutes,
                            'goals', pms.goals,
                            'assists', pms.assists,
                            'goals_assists', pms.goals_assists,
                            'pens_made', pms.pens_made,
                            'pens_att', pms.pens_att,
                            'age', pms.age,
                            'shots', pms.shots,
                            'shots_on_target', pms.shots_on_target,
                            'cards_yellow', pms.cards_yellow,
                            'cards_red', pms.cards_red,
                            'touches', pms.touches
                        ),
                        'defensive', json_build_object(
                            'tackles', pms.tackles,
                            'interceptions', pms.interceptions,
                            'blocks', pms.blocks,
                            'tackles_won', pms.tackles_won,
                            'tackles_def_3rd', pms.tackles_def_3rd,
                            'tackles_mid_3rd', pms.tackles_mid_3rd,
                            'tackles_att_3rd', pms.tackles_att_3rd,
                            'challenge_tackles', pms.challenge_tackles,
                            'challenges', pms.challenges,
                            'challenge_tackles_pct', pms.challenge_tackles_pct,
                            'challenges_lost', pms.challenges_lost,
                            'blocked_shots', pms.blocked_shots,
                            'blocked_passes', pms.blocked_passes,
                            'tackles_interceptions', pms.tackles_interceptions,
                            'clearances', pms.clearances,
                            'errors', pms.errors,
                            'ball_recoveries', pms.ball_recoveries
                        ),
                        'attacking', json_build_object(
                            'xg', pms.xg,
                            'npxg', pms.npxg,
                            'xg_assist', pms.xg_assist,
                            'sca', pms.sca,
                            'gca', pms.gca,
                            'take_ons', pms.take_ons,
                            'take_ons_won', pms.take_ons_won,
                            'take_ons_won_pct', pms.take_ons_won_pct,
                            'take_ons_tackled', pms.take_ons_tackled,
                            'take_ons_tackled_pct', pms.take_ons_tackled_pct,
                            'crosses', pms.crosses,
                            'own_goals', pms.own_goals,
                            'pens_won', pms.pens_won,
                            'pens_conceded', pms.pens_conceded
                        ),
                        'passing', json_build_object(
                            'passes_completed', pms.passes_completed,
                            'passes', pms.passes,
                            'passes_pct', pms.passes_pct,
                            'progressive_passes', pms.progressive_passes,
                            'passes_received', pms.passes_received,
                            'progressive_passes_received', pms.progressive_passes_received
                        ),
                        'possession', json_build_object(
                            'carries', pms.carries,
                            'progressive_carries', pms.progressive_carries,
                            'carries_distance', pms.carries_distance,
                            'carries_progressive_distance', pms.carries_progressive_distance,
                            'carries_into_final_third', pms.carries_into_final_third,
                            'carries_into_penalty_area', pms.carries_into_penalty_area,
                            'miscontrols', pms.miscontrols,
                            'dispossessed', pms.dispossessed,
                            'touches_def_pen_area', pms.touches_def_pen_area,
                            'touches_def_3rd', pms.touches_def_3rd,
                            'touches_mid_3rd', pms.touches_mid_3rd,
                            'touches_att_3rd', pms.touches_att_3rd,
                            'touches_att_pen_area', pms.touches_att_pen_area,
                            'touches_live_ball', pms.touches_live_ball
                        ),
                        'discipline', json_build_object(
                            'fouls', pms.fouls,
                            'fouled', pms.fouled,
                            'offsides', pms.offsides,
                            'cards_yellow_red', pms.cards_yellow_red,
                            'aerials_lost', pms.aerials_lost,
                            'aerials_won', pms.aerials_won,
                            'aerials_won_pct', pms.aerials_won_pct
                        )
                    )
                    FROM player_match_stats pms
                    WHERE pms.player_id = l.player_id
                    AND pms.match_id = l.match_id
                    AND pms.team_id = l.team_id
                ) AS player_stats
            FROM lineups l
            JOIN matches m ON l.match_id = m.match_id
            JOIN teams ht ON m.home_id = ht.team_id
            JOIN teams at ON m.away_id = at.team_id
            JOIN leagues lg ON m.comp_id = lg.league_id
            WHERE l.player_id = {player_id}
            AND m.match_date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY m.match_date DESC
        )
        SELECT 
            json_build_object(
                'data', json_build_object(
                    'matches', COALESCE(json_agg(
                        json_build_object(
                            'xi', md.xi,
                            'lineup_id', md.lineup_id,
                            'team_id', md.team_id,
                            'match_info', json_build_object(
                                'match_id', md.match_id,
                                'match_date', md.match_date,
                                'date_time_utc', md.date_time_utc,
                                'round', md.round,
                                'season_year', md.season_year,
                                'draw', md.draw,
                                'et', md.et,
                                'pens', md.pens,
                                'result', md.result,
                                'comp_id', md.comp_id,
                                'comp', md.comp,
                                'comp_logo', md.comp_logo
                            ),
                            'teams', json_build_object(
                                'home', json_build_object(
                                    'team', md.home_team,
                                    'stats', md.home_stats
                                ),
                                'away', json_build_object(
                                    'team', md.away_team,
                                    'stats', md.away_stats
                                )
                            ),
                            'player_stats', COALESCE(md.player_stats, 'null'::json)
                        )
                    ), '[]'::json)
                )
            ) AS result
        FROM match_data md;
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
                    detail=f"No data found for player {player_id}"
                )
            
            return result
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error: {str(e)}"
            )

    def get_recent_ga(self, player_id: int):
        query = f"""
        WITH goal_contributions AS (
            SELECT 
                l.match_id,
                l.team_id,
                pms.goals,
                pms.assists,
                m.match_date
            FROM lineups l
            JOIN matches m ON l.match_id = m.match_id
            JOIN player_match_stats pms ON 
                pms.player_id = l.player_id AND 
                pms.match_id = l.match_id AND 
                pms.team_id = l.team_id
            WHERE l.player_id = {player_id}
            AND (pms.goals > 0 OR pms.assists > 0)
            ORDER BY m.match_date DESC
            LIMIT 25
        ),
        match_details AS (
            SELECT 
                json_build_object(
                    'teams', json_build_object(
                        'home', json_build_object(
                            'team', json_build_object(
                                'team_id', ht.team_id,
                                'team_name', ht.team_name,
                                'logo', COALESCE(ht.logo_url, NULL)
                            ),
                            'stats', json_build_object(
                                'goals', COALESCE(m.home_goals, NULL),
                                'pen_goals', COALESCE(m.pen_home_goals, NULL),
                                'ranking', COALESCE(m.home_ranking, NULL)
                            )
                        ),
                        'away', json_build_object(
                            'team', json_build_object(
                                'team_id', at.team_id,
                                'team_name', at.team_name,
                                'logo', COALESCE(at.logo_url, NULL)
                            ),
                            'stats', json_build_object(
                                'goals', COALESCE(m.away_goals, NULL),
                                'pen_goals', COALESCE(m.pen_away_goals, NULL),
                                'ranking', COALESCE(m.away_ranking, NULL)
                            )
                        )
                    ),
                    'match_info', json_build_object(
                        'match_id', m.match_id,
                        'match_date', m.match_date,
                        'date_time_utc', COALESCE(m.date_time_utc, NULL),
                        'round', m.round,
                        'season_year', m.season_year,
                        'draw', m."isDraw",
                        'et', m.extra_time,
                        'pens', m.pens,
                        'result', m.result,
                        'comp_id', m.comp_id,
                        'comp', lg.league_name,
                        'comp_logo', COALESCE(lg.logo_url, NULL)
                    ),
                    'player_stats', CASE WHEN pms.id IS NOT NULL THEN
                        json_build_object(
                            'id', pms.id,
                            'player_id', pms.player_id,
                            'match_id', pms.match_id,
                            'team_id', pms.team_id,
                            'minutes', pms.minutes,
                            'goals', pms.goals,
                            'assists', pms.assists,
                            'goals_assists', COALESCE(pms.goals_assists, NULL),
                            'pens_made', pms.pens_made,
                            'pens_att', pms.pens_att,
                            'age', pms.age,
                            'shots', pms.shots,
                            'shots_on_target', pms.shots_on_target,
                            'cards_yellow', pms.cards_yellow,
                            'cards_red', pms.cards_red,
                            'touches', pms.touches
                        )
                    ELSE NULL END
                ) AS match_json
            FROM goal_contributions gc
            JOIN matches m ON gc.match_id = m.match_id
            JOIN teams ht ON m.home_id = ht.team_id
            JOIN teams at ON m.away_id = at.team_id
            JOIN leagues lg ON m.comp_id = lg.league_id
            LEFT JOIN player_match_stats pms ON 
                pms.player_id = {player_id} AND
                pms.match_id = gc.match_id AND
                pms.team_id = gc.team_id
            ORDER BY m.match_date DESC
        )
        SELECT 
            json_build_object(
                'data', json_build_object(
                    'recent_ga', COALESCE(
                        (SELECT json_agg(match_json) FROM match_details),
                        '[]'::json
                    )
                )
            ) AS result;
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
                    detail=f"No data found for player {player_id}"
                )
            
            return result
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error: {str(e)}"
            )

    def get_recent_apps_bydate(self, player_id: int, start_date: str, end_date: str):
        query = f"""
        WITH goal_contributions AS (
            SELECT 
                l.match_id,
                l.team_id,
                pms.goals,
                pms.assists,
                m.match_date
            FROM lineups l
            JOIN matches m ON l.match_id = m.match_id
            JOIN player_match_stats pms ON 
                pms.player_id = l.player_id AND 
                pms.match_id = l.match_id AND 
                pms.team_id = l.team_id
            WHERE l.player_id = {player_id}
            -- AND (pms.goals > 0 OR pms.assists > 0)
            AND (pms.minutes is NOT NULL)
            AND m.match_date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY m.match_date DESC
            LIMIT 100
        ),
        match_details AS (
            SELECT 
                json_build_object(
                    'teams', json_build_object(
                        'home', json_build_object(
                            'team', json_build_object(
                                'team_id', ht.team_id,
                                'team_name', ht.team_name,
                                'logo', COALESCE(ht.logo_url, NULL)
                            ),
                            'stats', json_build_object(
                                'goals', COALESCE(m.home_goals, NULL),
                                'pen_goals', COALESCE(m.pen_home_goals, NULL),
                                'ranking', COALESCE(m.home_ranking, NULL)
                            )
                        ),
                        'away', json_build_object(
                            'team', json_build_object(
                                'team_id', at.team_id,
                                'team_name', at.team_name,
                                'logo', COALESCE(at.logo_url, NULL)
                            ),
                            'stats', json_build_object(
                                'goals', COALESCE(m.away_goals, NULL),
                                'pen_goals', COALESCE(m.pen_away_goals, NULL),
                                'ranking', COALESCE(m.away_ranking, NULL)
                            )
                        )
                    ),
                    'match_info', json_build_object(
                        'match_id', m.match_id,
                        'match_date', m.match_date,
                        'date_time_utc', COALESCE(m.date_time_utc, NULL),
                        'round', m.round,
                        'season_year', m.season_year,
                        'draw', m."isDraw",
                        'et', m.extra_time,
                        'pens', m.pens,
                        'result', m.result,
                        'comp_id', m.comp_id,
                        'comp', lg.league_name,
                        'comp_logo', COALESCE(lg.logo_url, NULL)
                    ),
                    'player_stats', CASE WHEN pms.id IS NOT NULL THEN
                        json_build_object(
                            'id', pms.id,
                            'player_id', pms.player_id,
                            'match_id', pms.match_id,
                            'team_id', pms.team_id,
                            'minutes', pms.minutes,
                            'goals', pms.goals,
                            'assists', pms.assists,
                            'goals_assists', COALESCE(pms.goals_assists, NULL),
                            'pens_made', pms.pens_made,
                            'pens_att', pms.pens_att,
                            'age', pms.age,
                            'shots', pms.shots,
                            'shots_on_target', pms.shots_on_target,
                            'cards_yellow', pms.cards_yellow,
                            'cards_red', pms.cards_red,
                            'touches', pms.touches
                        )
                    ELSE NULL END
                ) AS match_json
            FROM goal_contributions gc
            JOIN matches m ON gc.match_id = m.match_id
            JOIN teams ht ON m.home_id = ht.team_id
            JOIN teams at ON m.away_id = at.team_id
            JOIN leagues lg ON m.comp_id = lg.league_id
            LEFT JOIN player_match_stats pms ON 
                pms.player_id = {player_id} AND
                pms.match_id = gc.match_id AND
                pms.team_id = gc.team_id
            ORDER BY m.match_date DESC
        )
        SELECT 
            json_build_object(
                'data', json_build_object(
                    'recent_ga', COALESCE(
                        (SELECT json_agg(match_json) FROM match_details),
                        '[]'::json
                    )
                )
            ) AS result;
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
                    detail=f"No data found for player {player_id}"
                )
            
            return result
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error: {str(e)}"
            )

    def get_recent_ga_bydate(self, player_id: int, start_date: str, end_date: str):
        query = f"""
        WITH goal_contributions AS (
            SELECT 
                l.match_id,
                l.team_id,
                pms.goals,
                pms.assists,
                m.match_date
            FROM lineups l
            JOIN matches m ON l.match_id = m.match_id
            JOIN player_match_stats pms ON 
                pms.player_id = l.player_id AND 
                pms.match_id = l.match_id AND 
                pms.team_id = l.team_id
            WHERE l.player_id = {player_id}
            AND (pms.goals > 0 OR pms.assists > 0)
            -- AND (pms.minutes is NOT NULL)
            AND m.match_date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY m.match_date DESC
            LIMIT 100
        ),
        match_details AS (
            SELECT 
                json_build_object(
                    'teams', json_build_object(
                        'home', json_build_object(
                            'team', json_build_object(
                                'team_id', ht.team_id,
                                'team_name', ht.team_name,
                                'logo', COALESCE(ht.logo_url, NULL)
                            ),
                            'stats', json_build_object(
                                'goals', COALESCE(m.home_goals, NULL),
                                'pen_goals', COALESCE(m.pen_home_goals, NULL),
                                'ranking', COALESCE(m.home_ranking, NULL)
                            )
                        ),
                        'away', json_build_object(
                            'team', json_build_object(
                                'team_id', at.team_id,
                                'team_name', at.team_name,
                                'logo', COALESCE(at.logo_url, NULL)
                            ),
                            'stats', json_build_object(
                                'goals', COALESCE(m.away_goals, NULL),
                                'pen_goals', COALESCE(m.pen_away_goals, NULL),
                                'ranking', COALESCE(m.away_ranking, NULL)
                            )
                        )
                    ),
                    'match_info', json_build_object(
                        'match_id', m.match_id,
                        'match_date', m.match_date,
                        'date_time_utc', COALESCE(m.date_time_utc, NULL),
                        'round', m.round,
                        'season_year', m.season_year,
                        'draw', m."isDraw",
                        'et', m.extra_time,
                        'pens', m.pens,
                        'result', m.result,
                        'comp_id', m.comp_id,
                        'comp', lg.league_name,
                        'comp_logo', COALESCE(lg.logo_url, NULL)
                    ),
                    'player_stats', CASE WHEN pms.id IS NOT NULL THEN
                        json_build_object(
                            'id', pms.id,
                            'player_id', pms.player_id,
                            'match_id', pms.match_id,
                            'team_id', pms.team_id,
                            'minutes', pms.minutes,
                            'goals', pms.goals,
                            'assists', pms.assists,
                            'goals_assists', COALESCE(pms.goals_assists, NULL),
                            'pens_made', pms.pens_made,
                            'pens_att', pms.pens_att,
                            'age', pms.age,
                            'shots', pms.shots,
                            'shots_on_target', pms.shots_on_target,
                            'cards_yellow', pms.cards_yellow,
                            'cards_red', pms.cards_red,
                            'touches', pms.touches
                        )
                    ELSE NULL END
                ) AS match_json
            FROM goal_contributions gc
            JOIN matches m ON gc.match_id = m.match_id
            JOIN teams ht ON m.home_id = ht.team_id
            JOIN teams at ON m.away_id = at.team_id
            JOIN leagues lg ON m.comp_id = lg.league_id
            LEFT JOIN player_match_stats pms ON 
                pms.player_id = {player_id} AND
                pms.match_id = gc.match_id AND
                pms.team_id = gc.team_id
            ORDER BY m.match_date DESC
        )
        SELECT 
            json_build_object(
                'data', json_build_object(
                    'recent_ga', COALESCE(
                        (SELECT json_agg(match_json) FROM match_details),
                        '[]'::json
                    )
                )
            ) AS result;
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
                    detail=f"No data found for player {player_id}"
                )
            
            return result
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error: {str(e)}"
            )


    def get_recent_ga_against_team(self, player_id: int, opp_team_id: int):
        query = f"""
        WITH goal_contributions AS (
            SELECT 
                l.match_id,
                l.team_id,
                pms.goals,
                pms.assists,
                m.match_date
            FROM lineups l
            JOIN matches m ON l.match_id = m.match_id
            JOIN player_match_stats pms ON 
                pms.player_id = l.player_id AND 
                pms.match_id = l.match_id AND 
                pms.team_id = l.team_id
            WHERE l.player_id = {player_id}
            AND (pms.goals > 0 OR pms.assists > 0)
            AND (
                (l.team_id = m.home_id AND m.away_id = {opp_team_id}) OR
                (l.team_id = m.away_id AND m.home_id = {opp_team_id})
            )
            ORDER BY m.match_date DESC
            LIMIT 25
        ),
        match_details AS (
            SELECT 
                json_build_object(
                    'teams', json_build_object(
                        'home', json_build_object(
                            'team', json_build_object(
                                'team_id', ht.team_id,
                                'team_name', ht.team_name,
                                'logo', COALESCE(ht.logo_url, NULL)
                            ),
                            'stats', json_build_object(
                                'goals', COALESCE(m.home_goals, NULL),
                                'pen_goals', COALESCE(m.pen_home_goals, NULL),
                                'ranking', COALESCE(m.home_ranking, NULL)
                            )
                        ),
                        'away', json_build_object(
                            'team', json_build_object(
                                'team_id', at.team_id,
                                'team_name', at.team_name,
                                'logo', COALESCE(at.logo_url, NULL)
                            ),
                            'stats', json_build_object(
                                'goals', COALESCE(m.away_goals, NULL),
                                'pen_goals', COALESCE(m.pen_away_goals, NULL),
                                'ranking', COALESCE(m.away_ranking, NULL)
                            )
                        )
                    ),
                    'match_info', json_build_object(
                        'match_id', m.match_id,
                        'match_date', m.match_date,
                        'date_time_utc', COALESCE(m.date_time_utc, NULL),
                        'round', m.round,
                        'season_year', m.season_year,
                        'draw', m."isDraw",
                        'et', m.extra_time,
                        'pens', m.pens,
                        'result', m.result,
                        'comp_id', m.comp_id,
                        'comp', lg.league_name,
                        'comp_logo', COALESCE(lg.logo_url, NULL)
                    ),
                    'player_stats', CASE WHEN pms.id IS NOT NULL THEN
                        json_build_object(
                            'id', pms.id,
                            'player_id', pms.player_id,
                            'match_id', pms.match_id,
                            'team_id', pms.team_id,
                            'minutes', pms.minutes,
                            'goals', pms.goals,
                            'assists', pms.assists,
                            'goals_assists', COALESCE(pms.goals_assists, NULL),
                            'pens_made', pms.pens_made,
                            'pens_att', pms.pens_att,
                            'age', pms.age,
                            'shots', pms.shots,
                            'shots_on_target', pms.shots_on_target,
                            'cards_yellow', pms.cards_yellow,
                            'cards_red', pms.cards_red,
                            'touches', pms.touches
                        )
                    ELSE NULL END
                ) AS match_json
            FROM goal_contributions gc
            JOIN matches m ON gc.match_id = m.match_id
            JOIN teams ht ON m.home_id = ht.team_id
            JOIN teams at ON m.away_id = at.team_id
            JOIN leagues lg ON m.comp_id = lg.league_id
            LEFT JOIN player_match_stats pms ON 
                pms.player_id = {player_id} AND
                pms.match_id = gc.match_id AND
                pms.team_id = gc.team_id
            ORDER BY m.match_date DESC
        )
        SELECT 
            json_build_object(
                'data', json_build_object(
                    'recent_ga', COALESCE(
                        (SELECT json_agg(match_json) FROM match_details),
                        '[]'::json
                    )
                )
            ) AS result;
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
                    detail=f"No data found for player {player_id}"
                )
            
            return result
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error: {str(e)}"
            )

    def get_recent_apps_against_team(self, player_id: int, opp_team_id: int):
        query = f"""
        WITH goal_contributions AS (
            SELECT 
                l.match_id,
                l.team_id,
                pms.goals,
                pms.assists,
                m.match_date
            FROM lineups l
            JOIN matches m ON l.match_id = m.match_id
            JOIN player_match_stats pms ON 
                pms.player_id = l.player_id AND 
                pms.match_id = l.match_id AND 
                pms.team_id = l.team_id
            WHERE l.player_id = {player_id}
            AND (pms.minutes is NOT NULL)
            AND (
                (l.team_id = m.home_id AND m.away_id = {opp_team_id}) OR
                (l.team_id = m.away_id AND m.home_id = {opp_team_id})
            )
            ORDER BY m.match_date DESC
            LIMIT 100
        ),
        match_details AS (
            SELECT 
                json_build_object(
                    'teams', json_build_object(
                        'home', json_build_object(
                            'team', json_build_object(
                                'team_id', ht.team_id,
                                'team_name', ht.team_name,
                                'logo', COALESCE(ht.logo_url, NULL)
                            ),
                            'stats', json_build_object(
                                'goals', COALESCE(m.home_goals, NULL),
                                'pen_goals', COALESCE(m.pen_home_goals, NULL),
                                'ranking', COALESCE(m.home_ranking, NULL)
                            )
                        ),
                        'away', json_build_object(
                            'team', json_build_object(
                                'team_id', at.team_id,
                                'team_name', at.team_name,
                                'logo', COALESCE(at.logo_url, NULL)
                            ),
                            'stats', json_build_object(
                                'goals', COALESCE(m.away_goals, NULL),
                                'pen_goals', COALESCE(m.pen_away_goals, NULL),
                                'ranking', COALESCE(m.away_ranking, NULL)
                            )
                        )
                    ),
                    'match_info', json_build_object(
                        'match_id', m.match_id,
                        'match_date', m.match_date,
                        'date_time_utc', COALESCE(m.date_time_utc, NULL),
                        'round', m.round,
                        'season_year', m.season_year,
                        'draw', m."isDraw",
                        'et', m.extra_time,
                        'pens', m.pens,
                        'result', m.result,
                        'comp_id', m.comp_id,
                        'comp', lg.league_name,
                        'comp_logo', COALESCE(lg.logo_url, NULL)
                    ),
                    'player_stats', CASE WHEN pms.id IS NOT NULL THEN
                        json_build_object(
                            'id', pms.id,
                            'player_id', pms.player_id,
                            'match_id', pms.match_id,
                            'team_id', pms.team_id,
                            'minutes', pms.minutes,
                            'goals', pms.goals,
                            'assists', pms.assists,
                            'goals_assists', COALESCE(pms.goals_assists, NULL),
                            'pens_made', pms.pens_made,
                            'pens_att', pms.pens_att,
                            'age', pms.age,
                            'shots', pms.shots,
                            'shots_on_target', pms.shots_on_target,
                            'cards_yellow', pms.cards_yellow,
                            'cards_red', pms.cards_red,
                            'touches', pms.touches
                        )
                    ELSE NULL END
                ) AS match_json
            FROM goal_contributions gc
            JOIN matches m ON gc.match_id = m.match_id
            JOIN teams ht ON m.home_id = ht.team_id
            JOIN teams at ON m.away_id = at.team_id
            JOIN leagues lg ON m.comp_id = lg.league_id
            LEFT JOIN player_match_stats pms ON 
                pms.player_id = {player_id} AND
                pms.match_id = gc.match_id AND
                pms.team_id = gc.team_id
            ORDER BY m.match_date DESC
        )
        SELECT 
            json_build_object(
                'data', json_build_object(
                    'recent_ga', COALESCE(
                        (SELECT json_agg(match_json) FROM match_details),
                        '[]'::json
                    )
                )
            ) AS result;
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
                    detail=f"No data found for player {player_id}"
                )
            
            return result
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error: {str(e)}"
            )


    
    def get_player_goal_distribution(self, player_id: int, season: int):
        # 1. Fetch all goal events (which may include both goals and assists)
        event_response = self.supabase.table('match_events') \
            .select('*, matches!inner(season_year)') \
            .eq('matches.season_year', season) \
            .eq('event_type', 'goal') \
            .or_(f'active_player_id.eq.{player_id},passive_player_id.eq.{player_id}') \
            .execute()

        goal_events = event_response.data

        # Also fetch penalty goals separately (if needed)
        penalty_response = self.supabase.table('match_events') \
            .select('*, matches!inner(season_year)') \
            .eq('matches.season_year', season) \
            .eq('event_type', 'penalty goal') \
            .eq('active_player_id', player_id) \
            .execute()

        penalty_goals = penalty_response.data

        # Combine both event sets
        all_events = goal_events + penalty_goals

        # 2. Compute totals
        total_goals = sum(1 for e in goal_events if e['active_player_id'] == player_id)
        total_assists = sum(1 for e in goal_events if e['passive_player_id'] == player_id)
        total_ga = total_goals + total_assists
        total_pens = len(penalty_goals)

        # 3. Build per-team distribution
        team_contributions = {}
        for event in all_events:
            team_id = event['opp_team_id']
            if not team_id:
                continue

            if team_id not in team_contributions:
                team_contributions[team_id] = {
                    'goals_against': 0,
                    'assists_against': 0,
                    'ga_against': 0
                }

            # Count goal
            if event['event_type'] in ['goal', 'penalty goal'] and event['active_player_id'] == player_id:
                team_contributions[team_id]['goals_against'] += 1
                team_contributions[team_id]['ga_against'] += 1

            # Count assist (only from event_type = 'goal')
            if event['event_type'] == 'goal' and event['passive_player_id'] == player_id:
                team_contributions[team_id]['assists_against'] += 1
                team_contributions[team_id]['ga_against'] += 1

        # 4. Build team response list
        team_dists = []
        for team_id, stats in team_contributions.items():
            team_response = self.supabase.table('teams') \
                .select('team_id, team_name, logo_url') \
                .eq('team_id', team_id) \
                .single() \
                .execute()

            team_data = team_response.data

            team = Team(
                team_id=team_data['team_id'],
                team_name=team_data['team_name'],
                logo=team_data['logo_url']
            )

            stats_dist = StatsDist(
                ga_against=stats['ga_against'],
                ga_against_pct=round((stats['ga_against'] * 100.0 / total_ga), 1) if total_ga else None,
                goals_against=stats['goals_against'],
                goals_against_pct=round((stats['goals_against'] * 100.0 / total_goals), 1) if total_goals else None,
                assists_against=stats['assists_against'],
                assists_against_pct=round((stats['assists_against'] * 100.0 / total_assists), 1) if total_assists else None
            )

            team_dists.append(TeamDist(team=team, stats=stats_dist))

        # Sort by most impactful teams
        team_dists.sort(key=lambda x: x.stats.ga_against_pct or 0, reverse=True)

        # 5. Build response
        pens = Pens(
            pen_pct=round((total_pens * 100.0 / total_goals), 1) if total_goals > 0 else None,
            pens_scored=total_pens if total_pens > 0 else None
        )

        goal_dist_list = [GoalDist(teams=td) for td in team_dists]

        response = PlayerGADistResponse(
            data=PlayerGADistData(
                info=Comp2(
                    comp_id=9999,
                    comp_name="All Competitions",
                    comp_url=None,
                    season_year=season
                ),
                total=TotalGA(
                    goals=total_goals,
                    assists=total_assists,
                    ga=total_ga,
                    pens=total_pens if total_pens > 0 else None
                ),
                goal_dist=goal_dist_list,
                pens=pens
            )
        )

        return response


    def get_player_goal_dist_bydate(self, player_id: int, start_date: str, end_date: str):
        # 1. Fetch all goal events (which may include both goals and assists)
        event_response = self.supabase.table('match_events') \
            .select('*, matches!inner(match_date)') \
            .gte('matches.match_date', start_date) \
            .lte('matches.match_date', end_date) \
            .eq('event_type', 'goal') \
            .or_(f'active_player_id.eq.{player_id},passive_player_id.eq.{player_id}') \
            .execute()

        goal_events = event_response.data

        # Also fetch penalty goals separately (if needed)
        penalty_response = self.supabase.table('match_events') \
            .select('*, matches!inner(match_date)') \
            .gte('matches.match_date', start_date) \
            .lte('matches.match_date', end_date) \
            .eq('event_type', 'penalty goal') \
            .eq('active_player_id', player_id) \
            .execute()

        penalty_goals = penalty_response.data

        # Combine both event sets
        all_events = goal_events + penalty_goals

        # 2. Compute totals
        total_goals = sum(1 for e in goal_events if e['active_player_id'] == player_id)
        total_assists = sum(1 for e in goal_events if e['passive_player_id'] == player_id)
        total_ga = total_goals + total_assists
        total_pens = len(penalty_goals)

        # 3. Build per-team distribution
        team_contributions = {}
        for event in all_events:
            team_id = event['opp_team_id']
            if not team_id:
                continue

            if team_id not in team_contributions:
                team_contributions[team_id] = {
                    'goals_against': 0,
                    'assists_against': 0,
                    'ga_against': 0
                }

            # Count goal
            if event['event_type'] in ['goal', 'penalty goal'] and event['active_player_id'] == player_id:
                team_contributions[team_id]['goals_against'] += 1
                team_contributions[team_id]['ga_against'] += 1

            # Count assist (only from event_type = 'goal')
            if event['event_type'] == 'goal' and event['passive_player_id'] == player_id:
                team_contributions[team_id]['assists_against'] += 1
                team_contributions[team_id]['ga_against'] += 1

        # 4. Build team response list
        team_dists = []
        for team_id, stats in team_contributions.items():
            team_response = self.supabase.table('teams') \
                .select('team_id, team_name, logo_url') \
                .eq('team_id', team_id) \
                .single() \
                .execute()

            team_data = team_response.data

            team = Team(
                team_id=team_data['team_id'],
                team_name=team_data['team_name'],
                logo=team_data['logo_url']
            )

            stats_dist = StatsDist(
                ga_against=stats['ga_against'],
                ga_against_pct=round((stats['ga_against'] * 100.0 / total_ga), 1) if total_ga else None,
                goals_against=stats['goals_against'],
                goals_against_pct=round((stats['goals_against'] * 100.0 / total_goals), 1) if total_goals else None,
                assists_against=stats['assists_against'],
                assists_against_pct=round((stats['assists_against'] * 100.0 / total_assists), 1) if total_assists else None
            )

            team_dists.append(TeamDist(team=team, stats=stats_dist))

        # Sort by most impactful teams
        team_dists.sort(key=lambda x: x.stats.ga_against_pct or 0, reverse=True)

        # 5. Build response
        pens = Pens(
            pen_pct=round((total_pens * 100.0 / total_goals), 1) if total_goals > 0 else None,
            pens_scored=total_pens if total_pens > 0 else None
        )

        goal_dist_list = [GoalDist(teams=td) for td in team_dists]

        # Get the season year from the start_date (assuming it's in YYYY-MM-DD format)
        season_year = int(start_date[:4]) if start_date else None

        response = PlayerGADistResponse(
            data=PlayerGADistData(
                info=Comp2(
                    comp_id=9999,
                    comp_name="All Competitions",
                    comp_url=None,
                    season_year=season_year
                ),
                total=TotalGA(
                    goals=total_goals,
                    assists=total_assists,
                    ga=total_ga,
                    pens=total_pens if total_pens > 0 else None
                ),
                goal_dist=goal_dist_list,
                pens=pens
            )
        )

        return response
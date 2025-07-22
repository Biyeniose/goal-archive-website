from fastapi import HTTPException
from typing import List
from pydantic import BaseModel
from supabase import Client
from typing import Optional
import requests
from ..models.team import Transfer, PlayerNations, TeamBasicInfo
#from ..models.player import PlayerBasicInfo
from app.constants import GLOBAL_YEAR


# Define a Pydantic model
class TopStats(BaseModel):
    #player: PlayerBasicInfo
    team: TeamBasicInfo
    age: Optional[int] = None
    position: Optional[str] = None
    goals: Optional[int] = None
    penalty_goals: Optional[int] = None
    assists: Optional[int] = None
    ga: Optional[int] = None
    gp: Optional[int] = None
    minutes: Optional[int] = None  # Added minutes field
    season_year: int
    stats_id: int

class TeamPlayersStatsData(BaseModel):
    stats: List[TopStats]

class TeamPlayersStatsResponse(BaseModel):
    data: TeamPlayersStatsData

class TeamService:
    def __init__(self, supabase_client: Client):
        self.supabase = supabase_client
        self.key = supabase_client.supabase_key
        self.url = f"{supabase_client.rest_url}/rpc/execute_sql"
        self.headers = {
            "Authorization": f"Bearer {self.key}",
            "apikey": self.key,
            "Content-Type": "application/json"
        }

    def most_stats_by_team(self, team_id: int, season: int, stat: str, age: int):
        query = f"""
        WITH team_stats AS (
            SELECT
                json_build_object(
                    'player', json_build_object(
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
                    ),
                    'team', json_build_object(
                        'team_id', ps.team_id,
                        'team_name', t.team_name,
                        'team_logo', t.logo_url,
                        'league_id', t.league_id
                    ),
                    'age', ps.age,
                    'position', pl.position,
                    'goals', ps.goals,
                    'penalty_goals', ps.penalty_goals,
                    'assists', ps.assists,
                    'minutes', ps.minutes,
                    'ga', ps.ga,
                    'gp', ps.gp,
                    'season_year', ps.season_year,
                    'stats_id', ps.stats_id
                ) as stats
            FROM player_stats ps
            JOIN players pl ON ps.player_id = pl.player_id
            LEFT JOIN teams t ON ps.team_id = t.team_id
            LEFT JOIN teams n1 ON pl.nation1_id = n1.team_id
            LEFT JOIN teams n2 ON pl.nation2_id = n2.team_id
            WHERE ps.season_year = {season} 
                AND ps.team_id = {team_id} 
                AND ps.comp_id = 9999
                AND ps.age <= {age}
            ORDER BY ps.{stat} DESC
            LIMIT 10
        )
        SELECT json_build_object(
            'data', json_build_object(
                'stats', (SELECT coalesce(json_agg(stats), '[]'::json) FROM team_stats)
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
            # Debug prints (keep these for troubleshooting)
            #print(f"Supabase raw response status: {response.status_code}")
            #print(f"Supabase raw response text: {response.text}")
            #print(f"Supabase response: {result}")
            if not result or not result.get("data"):
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for"
                )
            return TeamPlayersStatsResponse(data=result["data"])
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error: {str(e)}"
            )

    def get_team_squads_per_year(self, team_id: int, season: int):
        query = f"""
        WITH team_squad AS (
            SELECT
                sq.id as squad_id,
                sq.player_id,
                p.player_name,
                p.pic_url,
                sq.team_id,
                sq.number,
                ps.age,
                p.position,
                json_build_object(
                    'nation1_id', p.nation1_id,
                    'nation2_id', p.nation2_id,
                    'nation1', p.nation1,
                    'nation2', p.nation2,
                    'nation1_url', n1.logo_url,
                    'nation2_url', n2.logo_url
                ) as nations,
                p.contract_end,
                json_build_object(
                    'ga', ps.ga,
                    'goals', ps.goals,
                    'assists', ps.assists,
                    'penalty_goals', ps.penalty_goals,
                    'gp', ps.gp,
                    'minutes', ps.minutes,
                    'subbed_on', ps.subbed_on,
                    'subbed_off', ps.subbed_off,
                    'yellows', ps.yellows,
                    'yellows2', ps.yellows2,
                    'reds', ps.reds
                ) as stats,
                sq.season_year 
            FROM squads sq
            LEFT JOIN players p ON sq.player_id = p.player_id
            LEFT JOIN player_stats ps ON sq.player_id = ps.player_id
                AND ps.season_year = {season}
                AND ps.comp_id = 9999
            LEFT JOIN teams n1 ON p.nation1_id = n1.team_id
            LEFT JOIN teams n2 ON p.nation2_id = n2.team_id
            WHERE sq.team_id = {team_id} 
            AND sq.season_year = {season}
        )
        SELECT json_build_object(
            'data', json_build_object(
                'squad', (SELECT coalesce(json_agg(row_to_json(team_squad)), '[]'::json) FROM team_squad)
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
            if not result or not result.get("data"):
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for match "
                )
                
            # Parse the response according to the actual structure
            return result
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error: {str(e)}"
            )


    def get_team_info(self, team_id: str):
        query = f"""
        WITH team_info AS (
            SELECT 
                t.team_id,
                t.team_name,
                t.team_name2,
                t.logo_url,
                t.league_id as curr_league_id,
                l.league_name as current_league_name,
                l.country_id as nation_id,
                nt.logo_url as nation_url,
                t.city,
                t.stadium
            FROM teams t
            LEFT JOIN leagues l ON t.league_id = l.league_id
            LEFT JOIN teams nt ON l.country_id = nt.team_id
            WHERE t.team_id = '{team_id}'
        ),
        team_transfers AS (
            SELECT
                tr.transfer_id,
                p.player_id,
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
                tr."isLoan" as "isLoan",
                tr.fee,
                tr.value,
                tr.date,
                tr.season
            FROM transfers tr
            JOIN players p ON tr.player_id = p.player_id
            JOIN teams ft ON tr.from_team_id = ft.team_id
            JOIN teams tt ON tr.to_team_id = tt.team_id
            JOIN leagues fl ON ft.league_id = fl.league_id
            JOIN leagues tl ON tt.league_id = tl.league_id
            JOIN teams fn ON fl.country_id = fn.team_id
            JOIN teams tn ON tl.country_id = tn.team_id
            WHERE tr.from_team_id = '{team_id}' OR tr.to_team_id = '{team_id}'
            ORDER BY tr.date DESC
            LIMIT 30
        ),
        played_matches AS (
            SELECT 
                json_build_object(
                    'home', json_build_object(
                        'stats', json_build_object(
                            'goals', m.home_goals,
                            'pen_goals', m.pen_home_goals,
                            'ranking', NULL
                        ),
                        'team', json_build_object(
                            'team_id', ht.team_id,
                            'team_name', ht.team_name,
                            'logo', ht.logo_url
                        )
                    ),
                    'away', json_build_object(
                        'stats', json_build_object(
                            'goals', m.away_goals,
                            'pen_goals', m.pen_away_goals,
                            'ranking', NULL
                        ),
                        'team', json_build_object(
                            'team_id', at.team_id,
                            'team_name', at.team_name,
                            'logo', at.logo_url
                        )
                    )
                ) as teams,
                json_build_object(
                    'match_id', m.match_id,
                    'match_date', m.match_date::text,
                    'date_time_utc', (m.match_date::text || ' ' || COALESCE(m.match_time::text, '00:00:00')),
                    'round', m.round,
                    'season_year', EXTRACT(YEAR FROM m.match_date)::int,
                    'draw', m."isDraw",
                    'et', m.extra_time,
                    'pens', m.pens,
                    'result', m.result,
                    'comp_id', m.comp_id,
                    'comp', l.league_name,
                    'comp_logo', l.logo_url
                ) as match_info
            FROM matches m
            JOIN teams ht ON m.home_id = ht.team_id
            JOIN teams at ON m.away_id = at.team_id
            JOIN leagues l ON m.comp_id = l.league_id
            WHERE (m.home_id = '{team_id}' OR m.away_id = '{team_id}')
            AND m.result IS NOT NULL
            ORDER BY m.match_date DESC, m.match_time DESC
            LIMIT 4
        ),
        upcoming_matches AS (
            SELECT 
                json_build_object(
                    'home', json_build_object(
                        'stats', json_build_object(
                            'goals', m.home_goals,
                            'pen_goals', m.pen_home_goals,
                            'ranking', NULL
                        ),
                        'team', json_build_object(
                            'team_id', ht.team_id,
                            'team_name', ht.team_name,
                            'logo', ht.logo_url
                        )
                    ),
                    'away', json_build_object(
                        'stats', json_build_object(
                            'goals', m.away_goals,
                            'pen_goals', m.pen_away_goals,
                            'ranking', NULL
                        ),
                        'team', json_build_object(
                            'team_id', at.team_id,
                            'team_name', at.team_name,
                            'logo', at.logo_url
                        )
                    )
                ) as teams,
                json_build_object(
                    'match_id', m.match_id,
                    'match_date', m.match_date::text,
                    'date_time_utc', (m.match_date::text || ' ' || COALESCE(m.match_time::text, '00:00:00')),
                    'round', m.round,
                    'season_year', EXTRACT(YEAR FROM m.match_date)::int,
                    'draw', m."isDraw",
                    'et', m.extra_time,
                    'pens', m.pens,
                    'result', m.result,
                    'comp_id', m.comp_id,
                    'comp', l.league_name,
                    'comp_logo', l.logo_url
                ) as match_info
            FROM matches m
            JOIN teams ht ON m.home_id = ht.team_id
            JOIN teams at ON m.away_id = at.team_id
            JOIN leagues l ON m.comp_id = l.league_id
            WHERE (m.home_id = '{team_id}' OR m.away_id = '{team_id}')
            AND m.result IS NULL
            ORDER BY m.match_date ASC, m.match_time ASC
            LIMIT 4
        ),
        team_matches AS (
            SELECT teams, match_info FROM played_matches
            UNION ALL
            SELECT teams, match_info FROM upcoming_matches
        ),
        team_stats AS (
            SELECT
                json_build_object(
                    'player_id', p.player_id,
                    'player_name', p.player_name,
                    'img', NULL
                ) as player,
                json_build_object(
                    'comp_id', ps.comp_id,
                    'comp_name', l.league_name,
                    'comp_url', l.logo_url
                ) as comp,
                json_build_object(
                    'team_id', t.team_id,
                    'team_name', t.team_name,
                    'logo', t.logo_url
                ) as team,
                ps.season_year,
                p.age,
                ps.ga,
                CASE WHEN ps.gp > 0 THEN ROUND((ps.ga::decimal / ps.gp), 2) ELSE NULL END as ga_pg,
                ps.goals,
                CASE WHEN ps.gp > 0 THEN ROUND((ps.goals::decimal / ps.gp), 2) ELSE NULL END as goals_pg,
                ps.assists,
                CASE WHEN ps.gp > 0 THEN ROUND((ps.assists::decimal / ps.gp), 2) ELSE NULL END as assists_pg,
                ps.penalty_goals,
                ps.gp,
                ps.minutes,
                CASE WHEN ps.gp > 0 THEN ROUND((ps.minutes::decimal / ps.gp), 2) ELSE NULL END as minutes_pg,
                NULL as cs,
                NULL as pass_compl_pg,
                NULL as passes_pg,
                NULL as errors_pg,
                NULL as shots_pg,
                NULL as shots_on_target_pg,
                NULL as sca_pg,
                NULL as gca_pg,
                NULL as take_ons_pg,
                NULL as take_ons_won_pg,
                NULL as goals_concede,
                NULL as yellows,
                NULL as yellows2,
                NULL as reds,
                NULL as own_goals,
                ps.stats_id
            FROM player_stats ps
            JOIN players p ON ps.player_id = p.player_id
            JOIN teams t ON ps.team_id = t.team_id
            JOIN leagues l ON ps.comp_id = l.league_id
            LEFT JOIN teams tn1 ON p.nation1_id = tn1.team_id
            LEFT JOIN teams tn2 ON p.nation2_id = tn2.team_id
            WHERE ps.team_id = '{team_id}' AND ps.comp_id = 9999 AND ps.season_year = {GLOBAL_YEAR}
            ORDER BY ps.ga DESC
        )
        SELECT json_build_object(
            'data', json_build_object(
                'info', (SELECT row_to_json(team_info) FROM team_info),
                'transfers', (SELECT coalesce(json_agg(
                    json_build_object(
                        'transfer_id', tt.transfer_id,
                        'player_id', tt.player_id,
                        'player_name', tt.player_name,
                        'from_team', tt.from_team,
                        'to_team', tt.to_team,
                        'isLoan', tt."isLoan",
                        'fee', tt.fee,
                        'value', tt.value,
                        'date', tt.date,
                        'season', tt.season
                    )
                ), '[]'::json) FROM team_transfers tt),
                'matches', (SELECT coalesce(json_agg(
                    json_build_object(
                        'teams', tm.teams,
                        'match_info', tm.match_info
                    )
                ), '[]'::json) FROM team_matches tm),
                'stats', (SELECT coalesce(json_agg(
                    json_build_object(
                        'player', ts.player,
                        'comp', ts.comp,
                        'team', ts.team,
                        'season_year', ts.season_year,
                        'age', ts.age,
                        'ga', ts.ga,
                        'ga_pg', ts.ga_pg,
                        'goals', ts.goals,
                        'goals_pg', ts.goals_pg,
                        'assists', ts.assists,
                        'assists_pg', ts.assists_pg,
                        'penalty_goals', ts.penalty_goals,
                        'gp', ts.gp,
                        'minutes', ts.minutes,
                        'minutes_pg', ts.minutes_pg,
                        'cs', ts.cs,
                        'pass_compl_pg', ts.pass_compl_pg,
                        'passes_pg', ts.passes_pg,
                        'errors_pg', ts.errors_pg,
                        'shots_pg', ts.shots_pg,
                        'shots_on_target_pg', ts.shots_on_target_pg,
                        'sca_pg', ts.sca_pg,
                        'gca_pg', ts.gca_pg,
                        'take_ons_pg', ts.take_ons_pg,
                        'take_ons_won_pg', ts.take_ons_won_pg,
                        'goals_concede', ts.goals_concede,
                        'yellows', ts.yellows,
                        'yellows2', ts.yellows2,
                        'reds', ts.reds,
                        'own_goals', ts.own_goals,
                        'stats_id', ts.stats_id
                    )
                ), '[]'::json) FROM team_stats ts)
            )
        ) as result
        """
        try:
            response = requests.post(
                self.url,
                headers=self.headers,
                json={"sql_query": query}
            )
            response.raise_for_status()
            result = response.json()
            #print(result)
            response_data = result.get('data')
            if not response_data:
                raise HTTPException(
                    status_code=404,
                    detail=f"No team data found"
                )
            return result

        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error: {str(e)}"
            )
        

    def get_team_matches_by_year(self, team_id: str, season: int):
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
            WHERE (m.home_id = {team_id} OR m.away_id = {team_id}) AND m.season_year = {season}
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
            #print(f"Supabase raw response status: {response.status_code}")
            #print(f"Supabase raw response text: {response.text}")
            # The response structure is {"data": {...}} not a list with result[0]
            if not result or not result.get("data"):
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for team {team_id}"
                )
            # Parse the response according to the actual structure
            return result
            
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

    def get_transfers_by_date(self, team_id: int, start_date: str, end_date:str):
        query = f"""
        WITH team_transfers AS (
            SELECT
                tr.transfer_id,
                p.player_id,
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
                tr."isLoan" as "isLoan",
                tr.fee,
                tr.value,
                tr.date,
                tr.season,
                CASE 
                    WHEN tr.to_team_id = {team_id} THEN 'in'
                    WHEN tr.from_team_id = {team_id} THEN 'out'
                END as transfer_direction
            FROM transfers tr
            JOIN players p ON tr.player_id = p.player_id
            JOIN teams ft ON tr.from_team_id = ft.team_id
            JOIN teams tt ON tr.to_team_id = tt.team_id
            JOIN leagues fl ON ft.league_id = fl.league_id
            JOIN leagues tl ON tt.league_id = tl.league_id
            JOIN teams fn ON fl.country_id = fn.team_id
            JOIN teams tn ON tl.country_id = tn.team_id
            WHERE (tr.from_team_id = {team_id} OR tr.to_team_id = {team_id})
            AND tr.date >= '{start_date}'
            AND tr.date <= '{end_date}'
            ORDER BY tr.date DESC
        ),
        transfers_summary AS (
            SELECT
                COUNT(CASE WHEN transfer_direction = 'in' THEN 1 END) as players_in,
                COUNT(CASE WHEN transfer_direction = 'out' THEN 1 END) as players_out,
                COALESCE(SUM(CASE WHEN transfer_direction = 'in' THEN fee END), 0) as total_fees_in,
                COALESCE(SUM(CASE WHEN transfer_direction = 'out' THEN fee END), 0) as total_fees_out,
                COALESCE(SUM(CASE WHEN transfer_direction = 'out' THEN fee END), 0) - 
                COALESCE(SUM(CASE WHEN transfer_direction = 'in' THEN fee END), 0) as net_fees
            FROM team_transfers
        )
        SELECT json_build_object(
            'data', json_build_object(
                'transfers', (SELECT coalesce(json_agg(
                    json_build_object(
                        'transfer_id', tt.transfer_id,
                        'player_id', tt.player_id,
                        'player_name', tt.player_name,
                        'from_team', tt.from_team,
                        'to_team', tt.to_team,
                        'isLoan', tt."isLoan",
                        'fee', tt.fee,
                        'value', tt.value,
                        'date', tt.date,
                        'season', tt.season
                    )
                ), '[]'::json) FROM team_transfers tt),
                'sum', (SELECT json_build_object(
                    'players_in', ts.players_in,
                    'players_out', ts.players_out,
                    'total_fees_in', ts.total_fees_in,
                    'total_fees_out', ts.total_fees_out,
                    'net_fees', ts.net_fees
                ) FROM transfers_summary ts)
            )
        ) as result
        """
        try:
            response = requests.post(
                self.url,
                headers=self.headers,
                json={"sql_query": query}
            )
            response.raise_for_status()
            result = response.json()
            #print(result)
            response_data = result.get('data')
            if not response_data:
                raise HTTPException(
                    status_code=404,
                    detail=f"No team data found"
                )
            return result

        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error: {str(e)}"
            )
        


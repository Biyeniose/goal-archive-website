from fastapi import HTTPException
from typing import List
from pydantic import BaseModel
from supabase import Client
from typing import Optional
import requests
from ..models.team import Transfer, PlayerStats, PlayerNations, TeamInfoResponse, TeamData, TeamSquadDataResponse


# Define a Pydantic model
class HighestStats(BaseModel):
    player_id: int
    season_year: int
    player_name: str
    age: Optional[int] = None
    team_name: Optional[str] = None
    ga: Optional[int] = None
    goals: Optional[int] = None
    assists: Optional[int] = None
    penalty_goals: Optional[int] = None
    gp: Optional[int] = None
    position: Optional[str] = None
    team_id: Optional[int] = None
    team_logo: Optional[str] = None
    nation1_id: Optional[int] = None
    nation2_id: Optional[int] = None
    nation1: Optional[str] = None
    nation2: Optional[str] = None
    nation1_url: Optional[str] = None
    nation2_url: Optional[str] = None
    stats_id: int

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
        SELECT json_agg(p)
        FROM (
            SELECT
                pl.player_name,
                pl.player_id,
                pl.position,
                ps.team_id,
                t.team_name,
                t.logo_url AS team_logo,
                pl.nation1_id,
                pl.nation2_id,
                n1.team_name AS nation1,
                n2.team_name AS nation2,
                n1.logo_url AS nation1_url,
                n2.logo_url AS nation2_url,
                ps.goals,
                ps.penalty_goals,
                ps.age,
                ps.assists,
                ps.minutes,
                ps.ga,
                ps.gp,
                ps.season_year,
                ps.stats_id
            FROM
                (SELECT
                    ps.player_id,
                    ps.team_id,
                    ps.goals,
                    ps.penalty_goals,
                    ps.age,
                    ps.minutes,
                    ps.assists,
                    ps.ga,
                    ps.gp,
                    ps.season_year,
                    ps.stats_id
                FROM player_stats ps
                JOIN players p ON ps.player_id = p.player_id AND ps.age <= {age}
                JOIN teams tm ON ps.team_id = tm.team_id

                WHERE ps.season_year = {season} AND ps.team_id = {team_id} AND ps.comp_id = 9999
                ORDER BY ps.{stat} DESC
                LIMIT 10) ps
            JOIN players pl ON ps.player_id = pl.player_id
            LEFT JOIN teams t ON ps.team_id = t.team_id
            LEFT JOIN teams n1 ON pl.nation1_id = n1.team_id
            LEFT JOIN teams n2 ON pl.nation2_id = n2.team_id
            ORDER BY ps.{stat} DESC
        ) p;
        """

        try:
            response = requests.post(self.url, headers=self.headers, json={"query": query})

            if response.status_code != 200:
                raise HTTPException(status_code=response.status_code, detail=response.text)

            results = response.json()  # Moved inside try block

            if results and isinstance(results[0].get("result"), list):
                player_list = results[0]["result"]
                data = [HighestStats(**entry) for entry in player_list]
                return data

            return []

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

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
            response = requests.post(self.url, headers=self.headers, json={"query": query})
            response.raise_for_status()
            
            result = response.json()
            
            if not result or not result[0] or not result[0].get("result"):
                raise HTTPException(
                    status_code=404,
                    detail=f"No squad data found for team {team_id} in season {season}"
                )
            
            # The result is a single object with the structure we defined in TeamSquadDataResponse
            return TeamSquadDataResponse(**result[0]["result"])

        except requests.exceptions.HTTPError as http_err:
            error_detail = response.text if hasattr(response, 'text') else str(http_err)
            raise HTTPException(status_code=500, detail=f"HTTP error occurred: {error_detail}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")



    def get_team_info(self, team_id: str) -> TeamInfoResponse:
        try:
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
            team_matches AS (
                SELECT 
                    m.match_id,
                    m.comp_id,
                    l.league_name as comp_name,
                    l.logo_url as comp_url,
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
                    ) as away_team,
                    json_build_object(
                        'result', m.result,
                        'win_team', m.win_team,
                        'loss_team', m.loss_team,
                        'isDraw', m."isDraw"
                    ) as result,
                    m.round,
                    m.match_date,
                    m.match_time,
                    json_build_object(
                        'extra_time', m.extra_time,
                        'pens', m.pens,
                        'pen_home_goals', m.pen_home_goals,
                        'pen_away_goals', m.pen_away_goals
                    ) as details
                FROM matches m
                JOIN teams ht ON m.home_id = ht.team_id
                JOIN teams at ON m.away_id = at.team_id
                JOIN leagues l ON m.comp_id = l.league_id
                WHERE m.home_id = '{team_id}' OR m.away_id = '{team_id}'
                ORDER BY m.match_date DESC, m.match_time DESC
                LIMIT 5
            ),
            team_stats AS (
                SELECT
                    ps.comp_id,
                    l.league_name as comp_name,
                    l.logo_url as comp_url,
                    ps.player_id,
                    ps.season_year,
                    p.player_name,
                    p.position,
                    p.age,
                    ps.team_id,
                    t.team_name,
                    t.logo_url as team_logo,
                    ps.ga,
                    ps.goals,
                    ps.assists,
                    ps.penalty_goals,
                    ps.gp,
                    ps.minutes,
                    json_build_object(
                        'nation1_id', p.nation1_id,
                        'nation2_id', p.nation2_id,
                        'nation1', p.nation1,
                        'nation2', p.nation2,
                        'nation1_url', tn1.logo_url,
                        'nation2_url', tn2.logo_url
                    ) as nations,
                    ps.stats_id
                FROM player_stats ps
                JOIN players p ON ps.player_id = p.player_id
                JOIN teams t ON ps.team_id = t.team_id
                JOIN leagues l ON ps.comp_id = l.league_id
                LEFT JOIN teams tn1 ON p.nation1_id = tn1.team_id
                LEFT JOIN teams tn2 ON p.nation2_id = tn2.team_id
                WHERE ps.team_id = '{team_id}' AND ps.comp_id = 9999
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
                    'matches', (SELECT coalesce(json_agg(row_to_json(team_matches)), '[]'::json) FROM team_matches),
                    'stats', (SELECT coalesce(json_agg(
                        json_build_object(
                            'comp_id', ts.comp_id,
                            'comp_name', ts.comp_name,
                            'comp_url', ts.comp_url,
                            'player_id', ts.player_id,
                            'season_year', ts.season_year,
                            'player_name', ts.player_name,
                            'position', ts.position,
                            'age', ts.age,
                            'team_id', ts.team_id,
                            'team_name', ts.team_name,
                            'team_logo', ts.team_logo,
                            'ga', ts.ga,
                            'goals', ts.goals,
                            'assists', ts.assists,
                            'penalty_goals', ts.penalty_goals,
                            'gp', ts.gp,
                            'minutes', ts.minutes,
                            'nations', ts.nations,
                            'stats_id', ts.stats_id
                        )
                    ), '[]'::json) FROM team_stats ts)
                )
            ) as result
            """
            
            response = requests.post(self.url, headers=self.headers, json={"query": query})
            response.raise_for_status()
            
            result = response.json()
            
            if not result or not result[0] or not result[0].get("result"):
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for team {team_id}"
                )
            
            return TeamInfoResponse(**result[0]["result"])

        except requests.exceptions.HTTPError as http_err:
            error_detail = response.text if hasattr(response, 'text') else str(http_err)
            raise HTTPException(status_code=500, detail=f"HTTP error occurred: {error_detail}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
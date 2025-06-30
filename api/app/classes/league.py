from fastapi import HTTPException
#from typing import List
from pydantic import BaseModel
from supabase import Client
import requests, json
from typing import Optional
#from ..models.league import TeamRank, LeagueRanking, TopLeaguesResponse

class LeagueRanking(BaseModel):
    league_name: str

class StatsRanking(BaseModel):
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
    minutes: Optional[int] = None
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

class AllTimeRank(BaseModel):
    player_id: int
    player_name: str
    total_pen_goals: int
    position: Optional[str] = None
    #nation1_id: Optional[int] = None
    #nation2_id: Optional[int] = None
    nation1: Optional[str] = None
    nation2: Optional[str] = None
    nation1_url: Optional[str] = None
    nation2_url: Optional[str] = None


class LeagueRankings(BaseModel):
    comp_id: int
    team_id: int
    season_year: int
    rank: int
    points: int
    gp: int
    wins: int
    losses: int
    draws: int
    goals_f: int
    goals_a: int
    gd: int
    rank_id: int
    team_name: str
    logo_url: str

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


    def most_stats_league(self, league_id: int, season: int, stat: str, age: int):
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

                WHERE ps.season_year = {season} AND ps.comp_id = {league_id}
                ORDER BY ps.{stat} DESC
                LIMIT 40) ps
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
                data = [StatsRanking(**entry) for entry in player_list]
                return data

            return []

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

    def teams_most_stats_league(self, league_id: int, team_id: int, season: int, stat: str, age: int):
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

                WHERE ps.season_year = {season} AND ps.comp_id = {league_id} AND
                ps.team_id = {team_id}
                ORDER BY ps.{stat} DESC
                LIMIT 30) ps
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
                data = [StatsRanking(**entry) for entry in player_list]
                return data

            return []

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

    # All time Most Stats
    def alltime_league_stats_per_season(self, league_id: int, stat: str, age: int):
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
                    WHERE ps.comp_id = {league_id} 
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
                data = [StatsRanking(**entry) for entry in player_list]
                return data

            return []

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


    def alltime_league_penalty_goals(self, league_id: int):
        query = f"""
            SELECT json_agg(p)
            FROM (
                SELECT
                    pl.player_name,
                    pl.player_id,
                    pl.position,
                    n1.team_name AS nation1,
                    n2.team_name AS nation2,
                    n1.logo_url AS nation1_url,
                    n2.logo_url AS nation2_url,
                    SUM(ps.penalty_goals) AS total_pen_goals
                FROM
                    player_stats ps
                JOIN players pl ON ps.player_id = pl.player_id
                LEFT JOIN teams n1 ON pl.nation1_id = n1.team_id
                LEFT JOIN teams n2 ON pl.nation2_id = n2.team_id
                WHERE ps.comp_id = {league_id}
                GROUP BY
                    pl.player_name,
                    pl.player_id,
                    pl.position,
                    n1.team_name,
                    n2.team_name,
                    n1.logo_url,
                    n2.logo_url
                ORDER BY total_pen_goals DESC
                LIMIT 10
            ) p;
            """

        try:
            response = requests.post(self.url, headers=self.headers, json={"query": query})

            if response.status_code != 200:
                raise HTTPException(status_code=response.status_code, detail=response.text)

            results = response.json()

            if results and isinstance(results[0].get("result"), list):
                player_list = results[0]["result"]
                data = [AllTimeRank(**entry) for entry in player_list]
                return data

            return []

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


    def most_stats_league_all(self, league_id: int, season: int, stat: str):
        query = f"""
        SELECT json_agg(p)
        FROM (
            SELECT
                pl.player_name,
                pl.player_id,
                pl.age,
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
                ps.assists,
                ps.minutes,
                ps.ga,
                ps.gp
            FROM
                (SELECT
                    ps.player_id,
                    ps.team_id,
                    ps.goals,
                    ps.minutes,
                    ps.assists,
                    ps.ga,
                    ps.gp
                FROM player_stats ps
                JOIN players p ON ps.player_id = p.player_id
                {"JOIN teams tm ON ps.team_id = tm.team_id AND tm.league_id IN (1,2,3,4,5,7,8,9,10)" if league_id == 9999 else f"JOIN teams tm ON ps.team_id = tm.team_id AND tm.league_id IN ({league_id})"}
                WHERE ps.season_year = {season}
                ORDER BY ps.{stat} DESC
                LIMIT 10) ps
            JOIN players pl ON ps.player_id = pl.player_id
            {"LEFT JOIN teams t ON ps.team_id = t.team_id AND t.league_id IN (1,2,3,4,5,7,8,9,10)" if league_id == 9999 else f"LEFT JOIN teams t ON ps.team_id = t.team_id AND t.league_id IN ({league_id})"}
            LEFT JOIN teams n1 ON pl.nation1_id = n1.team_id
            LEFT JOIN teams n2 ON pl.nation2_id = n2.team_id
            ORDER BY ps.{stat} DESC
        ) p;
        """

        try:
            response = requests.post(self.url, headers=self.headers, json={"query": query})
            if response.status_code == 200:
                results = response.json()
            if results and isinstance(results[0].get("result"), list):
                player_list = results[0]["result"]
                data = [StatsRanking(**entry) for entry in player_list]
                return data
            else:
                return []
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

    # GET the top 5 rankings of Top Leagues
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
            response = requests.post(self.url, headers=self.headers, json={"query": query})
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


    def get_past_3years(self, comp_id: int, season: int):
        try:
            query = f"""
            WITH filtered_ranks AS (
                SELECT 
                    lr.team_id,
                    lr.info,
                    COALESCE(lr.points, 0) as points,
                    COALESCE(lr.gp, 0) as gp,
                    COALESCE(lr.gd, 0) as gd,
                    COALESCE(lr.wins, 0) as wins,
                    COALESCE(lr.losses, 0) as losses,
                    COALESCE(lr.draws, 0) as draws,
                    COALESCE(lr.goals_f, 0) as goals_f,
                    COALESCE(lr.goals_a, 0) as goals_a,
                    t.team_name,
                    t.logo_url as team_logo
                FROM league_ranks lr
                JOIN teams t ON lr.team_id = t.team_id
                WHERE lr.comp_id = '{comp_id}' 
                AND lr.season_year IN (2022, 2023, 2024)
            ),
            aggregated_stats AS (
                SELECT
                    team_id,
                    MAX(team_name) as team_name,
                    MAX(team_logo) as team_logo,
                    MAX(info) as info,
                    SUM(points) as points,
                    SUM(gp) as gp,
                    SUM(gd) as gd,
                    SUM(wins) as wins,
                    SUM(losses) as losses,
                    SUM(draws) as draws,
                    SUM(goals_f) as goals_f,
                    SUM(goals_a) as goals_a
                FROM filtered_ranks
                GROUP BY team_id
            ),
            ranked_stats AS (
                SELECT
                    ROW_NUMBER() OVER (ORDER BY points DESC, gd DESC) as rank,
                    team_id,
                    team_name,
                    team_logo,
                    NULLIF(info, '') as info,
                    points,
                    gp,
                    gd,
                    wins,
                    losses,
                    draws,
                    goals_f,
                    goals_a
                FROM aggregated_stats
            )
            SELECT json_build_object(
                'data', json_build_object(
                    'past_ranks', COALESCE((
                        SELECT json_agg(
                            json_build_object(
                                'rank', CAST(r.rank AS TEXT),
                                'team_id', r.team_id,
                                'team_name', r.team_name,
                                'team_logo', r.team_logo,
                                'info', r.info,
                                'points', r.points,
                                'gp', r.gp,
                                'gd', r.gd,
                                'wins', r.wins,
                                'losses', r.losses,
                                'draws', r.draws,
                                'goals_f', r.goals_f,
                                'goals_a', r.goals_a
                            )
                        )
                        FROM ranked_stats r
                    ), '[]'::json)
                )
            ) as result
            """
            
            response = requests.post(self.url, headers=self.headers, json={"query": query})
            response.raise_for_status()
            
            result = response.json()
            
            if not result or not result[0] or not result[0].get("result"):
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for league {comp_id}"
                )
            
            return result[0]["result"]

        except requests.exceptions.HTTPError as http_err:
            error_detail = response.text if hasattr(response, 'text') else str(http_err)
            raise HTTPException(status_code=500, detail=f"HTTP error occurred: {error_detail}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
        

    def get_league_info(self, comp_id: int, season: int):
        try:
            # Query to get league info and rankings with type check
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
                                                'team_id', lr.team_id,
                                                'team_name', t2.team_name,
                                                'team_logo', t2.logo_url,
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
                                                'match_id', m.match_id,
                                                'comp_id', m.comp_id,
                                                'home_team_name', ht.team_name,
                                                'home_id', m.home_id,
                                                'home_logo', ht.logo_url,
                                                'home_color', m.home_color,
                                                'home_formation', m.home_formation,
                                                'home_ranking', m.home_ranking,
                                                'home_goals', m.home_goals,
                                                'away_team_name', at.team_name,
                                                'away_id', m.away_id,
                                                'away_logo', at.logo_url,
                                                'away_color', m.away_color,
                                                'away_formation', m.away_formation,
                                                'away_ranking', m.away_ranking,
                                                'away_goals', m.away_goals,
                                                'result', m.result,
                                                'round', m.round,
                                                'match_date', m.match_date,
                                                'season_year', m.season_year,
                                                'win_team', m.win_team,
                                                'loss_team', m.loss_team,
                                                'isDraw', m."isDraw",
                                                'extra_time', m.extra_time,
                                                'pens', m.pens,
                                                'pen_home_goals', m.pen_home_goals,
                                                'pen_away_goals', m.pen_away_goals,
                                                'match_time', m.match_time
                                            ) as match_subq
                                        FROM matches m
                                        JOIN teams ht ON m.home_id = ht.team_id
                                        JOIN teams at ON m.away_id = at.team_id
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
            
            response = requests.post(self.url, headers=self.headers, json={"query": query})
            response.raise_for_status()
            
            result = response.json()
            
            if not result or not result[0].get("result"):
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for league {comp_id}"
                )

            # Check if we got an error about competition type
            if isinstance(result[0]["result"], dict) and 'error' in result[0]["result"]:
                raise HTTPException(
                    status_code=400,
                    detail=result[0]["result"]["error"]
                )
                
            return result[0]["result"]
            
        except requests.exceptions.HTTPError as http_err:
            error_detail = response.text if hasattr(response, 'text') else str(http_err)
            raise HTTPException(status_code=500, detail=f"HTTP error occurred: {error_detail}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
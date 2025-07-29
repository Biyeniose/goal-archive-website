from fastapi import HTTPException
from typing import List
from pydantic import BaseModel
from supabase import Client
import requests,json
from typing import Optional
import asyncio

# Define a Pydantic model
class StatsRanking(BaseModel):
    player_id: int
    player_name: str
    age: Optional[int] = None
    team_id: Optional[int] = None
    team_name: Optional[str] = None
    team_logo: Optional[str] = None
    nation1_id: Optional[int] = None
    nation2_id: Optional[int] = None
    nation1: Optional[str] = None
    nation2: Optional[str] = None
    nation1_url: Optional[str] = None
    nation2_url: Optional[str] = None
    goals: int
    assists: int
    ga: int
    gp: int

class LeagueStats(BaseModel):
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

class MatchInfo(BaseModel):
    match_id: int
    home_id: int
    home_team_name: str
    away_id: int
    away_team_name: str
    home_goals: Optional[int] = None  # Made optional
    away_goals: Optional[int] = None  # Made optional
    match_date: str
    result: Optional[str] = None  # Made optional
    win_team: Optional[int] = None  # Made optional
    loss_team: Optional[int] = None  # Made optional
    isDraw: Optional[bool] = None  # Made optional

class TeamRecentMatches(BaseModel):
    team_name: str
    team_id: int
    comp_id: int

class TeamMatches(BaseModel):
    match_id: int
    home_team_name: str  # New field
    home_id: int
    home_goals: Optional[int] = None  # Made optional
    away_team_name: str  # New field
    away_id: int
    away_goals: Optional[int] = None  # Made optional
    match_date: str
    result: Optional[str] = None  # Made optional
    win_team: Optional[int] = None  # Made optional
    loss_team: Optional[int] = None  # Made optional
    isDraw: Optional[bool] = None  # Made optional

class TeamMatch(BaseModel):
    match_id: int
    home_team_name: str
    home_id: int
    home_goals: Optional[int] = None
    away_team_name: str
    away_id: int
    away_goals: Optional[int] = None
    result: Optional[str] = None
    round: Optional[str] = None
    match_date: str
    win_team: Optional[int] = None
    loss_team: Optional[int] = None
    isDraw: Optional[bool] = None
    match_time: Optional[str] = None


class TeamMatchesResponse(BaseModel):
    matches: List[TeamMatch]
    points: int
    wins: int
    losses: int
    draws: int
    gf: int
    ga: int
    gd: int

class TeamFormStats(BaseModel):
    team_id: int
    team_name: str
    logo_url: str
    points: int
    wins: int
    losses: int
    draws: int
    gf: int
    ga: int
    gd: int


class StatsService:
    def __init__(self, supabase_client: Client):
        self.supabase = supabase_client
        self.key = supabase_client.supabase_key
        self.url = f"{supabase_client.rest_url}/rpc/execute_sql"
        self.headers = {
            "Authorization": f"Bearer {self.key}",
            "apikey": self.key,
            "Content-Type": "application/json"
        }

    def get_teams_h2h(self, team1_id: int, team2_id: int, num_matches: int, start_date: str, end_date: str):
        query = f"""
        WITH team_matches AS (
            SELECT 
                m.match_id,
                m.match_date as match_date,
                m.date_time_utc,
                m.round,
                m.season_year,
                m."isDraw" as draw,
                m.extra_time,
                m.pens,
                m.result,
                m.comp_id,
                l.league_name as comp,
                l.logo_url as comp_logo,
                m.home_id,
                m.away_id,
                m.home_goals,
                m.away_goals,
                m.win_team,
                m.loss_team,
                home_team.team_name as home_team_name,
                home_team.logo_url as home_team_logo,
                away_team.team_name as away_team_name,
                away_team.logo_url as away_team_logo
            FROM matches m
            JOIN leagues l ON m.comp_id = l.league_id
            JOIN teams home_team ON m.home_id = home_team.team_id
            JOIN teams away_team ON m.away_id = away_team.team_id
            WHERE 
                ((m.home_id = {team1_id} AND m.away_id = {team2_id}) OR 
                (m.home_id = {team2_id} AND m.away_id = {team1_id}))
                AND m."isPlayed" = true
                AND m.match_date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY m.match_date DESC
            LIMIT {num_matches}
        ),
        match_stats AS (
            SELECT
                match_id,
                home_id as team_id,
                home_goals as goals_f,
                away_goals as goals_a,
                CASE 
                    WHEN win_team = home_id THEN 'win'
                    WHEN loss_team = home_id THEN 'loss'
                    ELSE 'draw'
                END as outcome
            FROM team_matches
            UNION ALL
            SELECT
                match_id,
                away_id as team_id,
                away_goals as goals_f,
                home_goals as goals_a,
                CASE 
                    WHEN win_team = away_id THEN 'win'
                    WHEN loss_team = away_id THEN 'loss'
                    ELSE 'draw'
                END as outcome
            FROM team_matches
        ),
        team_records AS (
            SELECT
                t.team_id,
                t.team_name,
                t.logo_url as logo,
                COUNT(*) as gp,
                SUM(CASE WHEN ms.outcome = 'win' THEN 1 ELSE 0 END) as wins,
                ROUND(SUM(CASE WHEN ms.outcome = 'win' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as win_pct,
                SUM(CASE WHEN ms.outcome = 'loss' THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN ms.outcome = 'draw' THEN 1 ELSE 0 END) as draws,
                SUM(ms.goals_f) as goals_f,
                SUM(ms.goals_a) as goals_a
            FROM match_stats ms
            JOIN teams t ON ms.team_id = t.team_id
            WHERE t.team_id IN ({team1_id}, {team2_id})
            GROUP BY t.team_id, t.team_name, t.logo_url
        )
        SELECT json_build_object(
            'data', json_build_object(
                'matches', COALESCE(
                    (SELECT json_agg(
                        json_build_object(
                            'teams', json_build_object(
                                'home', json_build_object(
                                    'stats', json_build_object(
                                        'goals', tm.home_goals,
                                        'pen_goals', NULL,
                                        'ranking', NULL
                                    ),
                                    'team', json_build_object(
                                        'team_id', tm.home_id,
                                        'team_name', tm.home_team_name,
                                        'logo', tm.home_team_logo
                                    )
                                ),
                                'away', json_build_object(
                                    'stats', json_build_object(
                                        'goals', tm.away_goals,
                                        'pen_goals', NULL,
                                        'ranking', NULL
                                    ),
                                    'team', json_build_object(
                                        'team_id', tm.away_id,
                                        'team_name', tm.away_team_name,
                                        'logo', tm.away_team_logo
                                    )
                                )
                            ),
                            'match_info', json_build_object(
                                'match_id', tm.match_id,
                                'match_date', tm.match_date,
                                'date_time_utc', tm.date_time_utc,
                                'round', tm.round,
                                'season_year', tm.season_year,
                                'draw', tm.draw,
                                'et', tm.extra_time,
                                'pens', tm.pens,
                                'result', tm.result,
                                'comp_id', tm.comp_id,
                                'comp', tm.comp,
                                'comp_logo', tm.comp_logo
                            )
                        )
                    ) FROM team_matches tm),
                    '[]'::json
                ),
                'record', COALESCE(
                    (SELECT json_agg(
                        json_build_object(
                            'team', json_build_object(
                                'team_id', tr.team_id,
                                'team_name', tr.team_name,
                                'logo', tr.logo
                            ),
                            'gp', tr.gp,
                            'wins', tr.wins,
                            'win_pct', tr.win_pct,
                            'losses', tr.losses,
                            'draws', tr.draws,
                            'goals_f', tr.goals_f,
                            'goals_a', tr.goals_a
                        )
                    ) FROM team_records tr),
                    '[]'::json
                )
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


    def get_teams_recent_matches(self, comp_id: int, season_year: int) -> List[TeamRecentMatches]:
        try:
            # Get all unique team IDs in the competition
            home_teams = self.supabase.table('matches') \
                .select('home_id') \
                .eq('comp_id', comp_id) \
                .eq('season_year', season_year) \
                .execute()

            away_teams = self.supabase.table('matches') \
                .select('away_id') \
                .eq('comp_id', comp_id) \
                .eq('season_year', season_year) \
                .execute()

            team_ids = list({t['home_id'] for t in home_teams.data} | {t['away_id'] for t in away_teams.data})

            if not team_ids:
                raise HTTPException(status_code=404, detail="No teams found for this competition and season")

            # Get team details
            teams = self.supabase.table('teams') \
                .select('team_id,team_name') \
                .in_('team_id', team_ids) \
                .execute()

            team_map = {t['team_id']: t for t in teams.data}

            result = []
            for team_id, team in team_map.items():
                # Get matches where team was either home or away
                matches = self.supabase.table('matches') \
                    .select('*, home_team:teams!home_id(team_name), away_team:teams!away_id(team_name)') \
                    .or_(f'home_id.eq.{team_id},away_id.eq.{team_id}') \
                    .eq('comp_id', comp_id) \
                    .eq('season_year', season_year) \
                    .order('match_date', desc=True) \
                    .limit(6) \
                    .execute()

                recent_matches = []
                for match in matches.data:
                    # Handle potential None values
                    home_team = match.get('home_team', {})
                    away_team = match.get('away_team', {})

                    recent_matches.append(MatchInfo(
                        match_id=match.get('match_id', 0),
                        home_id=match.get('home_id', 0),
                        home_team_name=home_team.get('team_name'),
                        away_id=match.get('away_id', 0),
                        away_team_name=away_team.get('team_name'),
                        home_goals=match.get('home_goals'),
                        away_goals=match.get('away_goals'),
                        match_date=str(match.get('match_date', '')),
                        result=match.get('result'),
                        win_team=match.get('win_team'),
                        loss_team=match.get('loss_team'),
                        isDraw=match.get('isDraw')
                    ))

                # This was incorrectly indented before - now properly inside the team loop
                result.append(TeamRecentMatches(
                    team_name=team['team_name'],
                    team_id=team_id,
                    comp_id=comp_id,
                    recent_matches=recent_matches
                ))

            if not result:
                raise HTTPException(status_code=404, detail="No matches found for teams in this competition and season")

            return result

        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    def get_team_recent(
        self,
        comp_id: int,
        season_year: int,
        team_id: int
    ) -> List[TeamMatches]:
        query = f"""
        SELECT json_agg(recent_matches)
        FROM (
            SELECT
                m.match_id,
                m.home_id,
                m.away_id,
                m.home_goals,
                m.away_goals,
                m.match_date,
                m.result,
                m.win_team,
                m.loss_team,
                m."isDraw",
                m.match_time,
                ht.team_name AS home_team_name,
                at.team_name AS away_team_name
            FROM matches m
            JOIN teams ht ON m.home_id = ht.team_id
            JOIN teams at ON m.away_id = at.team_id
            WHERE m.comp_id = {comp_id}
              AND m.season_year = {season_year}
              AND (m.home_id = {team_id} OR m.away_id = {team_id})
              AND m.home_goals IS NOT NULL
              AND m.away_goals IS NOT NULL
            ORDER BY m.match_date DESC
            LIMIT 10
        ) recent_matches;
        """

        try:
            response = requests.post(self.url, headers=self.headers, json={"query": query})
            if response.status_code == 200:
                results = response.json()

                if results and len(results) > 0:
                    # Handle both string and direct JSON responses
                    if isinstance(results[0].get("result"), str):
                        match_data = json.loads(results[0]["result"])
                    else:
                        match_data = results[0].get("result", [])

                    if isinstance(match_data, list):
                        return [TeamMatches(**match) for match in match_data]
                    elif isinstance(match_data, dict):
                        return [TeamMatches(**match_data)]

                raise HTTPException(
                    status_code=404,
                    detail="No matches found for this team in the specified competition and season"
                )
            else:
                raise HTTPException(status_code=response.status_code, detail=response.text)
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=500, detail=f"JSON decode error: {str(e)}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

   
    def get_no_losses(self):
        query = f"""
        SELECT json_agg(top_teams)
        FROM (
            SELECT
                lr.comp_id,
                lr.team_id,
                lr.season_year,
                lr.rank,
                lr.points,
                lr.gp,
                lr.wins,
                lr.losses,
                lr.draws,
                lr.goals_f,
                lr.goals_a,
                lr.gd,
                lr.rank_id,
                t.team_name,
                t.logo_url
            FROM
                (SELECT *
                FROM league_ranks
                WHERE losses = 0
                ORDER BY points DESC
                LIMIT 20) lr
            JOIN teams t ON lr.team_id = t.team_id
            ORDER BY lr.points DESC
        ) top_teams;
        """

        try:
            response = requests.post(self.url, headers=self.headers, json={"query": query})
            if response.status_code == 200:
                results = response.json()

                # Debug print to see the raw response structure
                #print("Raw response:", results)

                if results and len(results) > 0:
                    # The result might be a JSON string that needs parsing
                    if isinstance(results[0].get("result"), str):
                        # Parse the JSON string if needed
                        team_data = json.loads(results[0]["result"])
                    else:
                        team_data = results[0].get("result", [])

                    # Ensure we have a list of dictionaries
                    if isinstance(team_data, list):
                        return [LeagueStats(**entry) for entry in team_data]
                    elif isinstance(team_data, dict):
                        return [LeagueStats(**team_data)]

                return []
            else:
                raise HTTPException(status_code=response.status_code, detail=response.text)
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=500, detail=f"JSON decode error: {str(e)}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

    def get_worst_winners(self):
        query = f"""
        SELECT json_agg(top_teams)
        FROM (
            SELECT
                lr.comp_id,
                lr.team_id,
                lr.season_year,
                lr.rank,
                lr.points,
                lr.gp,
                lr.wins,
                lr.losses,
                lr.draws,
                lr.goals_f,
                lr.goals_a,
                lr.gd,
                lr.rank_id,
                t.team_name,
                t.logo_url
            FROM
                (SELECT *
                FROM league_ranks
                WHERE rank = 1 and season_year != 2024 and comp_id != 9
                ORDER BY goals_a ASC
                LIMIT 20) lr
            JOIN teams t ON lr.team_id = t.team_id
            ORDER BY lr.goals_a ASC
        ) top_teams;
        """

        try:
            response = requests.post(self.url, headers=self.headers, json={"sql_query": query})
            if response.status_code == 200:
                results = response.json()

                # Debug print to see the raw response structure
                #print("Raw response:", results)

                if results and len(results) > 0:
                    # The result might be a JSON string that needs parsing
                    if isinstance(results[0].get("result"), str):
                        # Parse the JSON string if needed
                        team_data = json.loads(results[0]["result"])
                    else:
                        team_data = results[0].get("result", [])

                    # Ensure we have a list of dictionaries
                    if isinstance(team_data, list):
                        return [LeagueStats(**entry) for entry in team_data]
                    elif isinstance(team_data, dict):
                        return [LeagueStats(**team_data)]

                return []
            else:
                raise HTTPException(status_code=response.status_code, detail=response.text)
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=500, detail=f"JSON decode error: {str(e)}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

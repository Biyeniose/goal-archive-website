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

    def get_team_recent_form(
        self,
        comp_id: int,
        team_id: int,
        season_year: Optional[int] = None
    ) -> TeamMatchesResponse:
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
                m.round,
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
              AND (m.home_id = {team_id} OR m.away_id = {team_id})
              AND m.home_goals IS NOT NULL
              AND m.away_goals IS NOT NULL
              {"AND m.season_year = " + str(season_year) if season_year is not None else ""}
            ORDER BY m.match_date DESC
            LIMIT 6
        ) recent_matches;
        """

        try:
            response = requests.post(self.url, headers=self.headers, json={"query": query})
            response.raise_for_status()

            results = response.json()

            if not results or not results[0].get("result"):
                raise HTTPException(
                    status_code=404,
                    detail="No matches found for this team in the specified competition"
                )

            match_data = results[0]["result"]
            if isinstance(match_data, str):
                match_data = json.loads(match_data)
            if isinstance(match_data, dict):
                match_data = [match_data]

            # Calculate points and prepare matches
            points = 0
            gd = 0
            gf = 0
            ga = 0
            w = 0
            l = 0
            d = 0
            matches = []
            for match in match_data:
                # Create match object
                match_obj = TeamMatch(**match)
                matches.append(match_obj)

                # Calculate points
                if match_obj.isDraw:
                    d += 1
                    points += 1
                elif team_id == match_obj.win_team:
                    w += 1
                    points += 3
                else:
                    l += 1

                h_goals = match_obj.home_goals
                a_goals = match_obj.away_goals
                if team_id == match_obj.home_id:
                    gf = gf + h_goals
                    ga = ga + a_goals

                else:
                    gf = gf + a_goals
                    ga = ga + h_goals

            gd = gf - ga


            #print(f'{points} Points - {w}W {l}L {d}D - GD: {gd} GF: {gf} GA: {ga}')
            # Return the properly structured response
            return {
                "matches": matches,
                "points": points,
                "wins": w,
                "losses": l,
                "draws": d,
                "gf": gf,
                "ga": ga,
                "gd": gd
            }

        except requests.exceptions.HTTPError as http_err:
            error_detail = response.text if hasattr(response, 'text') else str(http_err)
            raise HTTPException(status_code=500, detail=f"HTTP error occurred: {error_detail}")
        except json.JSONDecodeError as json_err:
            raise HTTPException(status_code=500, detail=f"JSON decode error: {str(json_err)}")
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

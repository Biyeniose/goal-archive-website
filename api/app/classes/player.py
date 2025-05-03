from fastapi import HTTPException
from typing import List
from pydantic import BaseModel
from supabase import Client
import requests
from typing import Optional


# Define a Pydantic model
class PlayerBioInfo(BaseModel):
    player_id: int
    player_name: str
    isRetired: Optional[bool] = None
    curr_team_id: Optional[int] = None
    curr_team_name: Optional[str] = None
    curr_team_logo: Optional[str] = None
    curr_number: Optional[int] = None
    position: Optional[str] = None
    dob: str 
    age: Optional[int] = None

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
        
    def get_player_bio(self, player_id: int):
        query = f"""
        SELECT row_to_json(p) 
        FROM (
            SELECT 
                p.player_id,
                p.player_name,
                p."isRetired",
                p.curr_team_id,
                t.team_name2 AS curr_team_name,
                t.logo_url AS curr_team_logo,
                p.curr_number,
                p.position,
                p.dob,
                p.age,
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
                p.contract_end
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






from fastapi import APIRouter, HTTPException, Depends
from supabase import Client
from ..dependencies import get_supabase_client

router = APIRouter(
    prefix="/v1/leagues",
    tags=["leagues"],
    dependencies=[Depends(get_supabase_client)],
    responses={404: {"description": "Not found"}},
)

# GET All Leagues with details
@router.get("/dom")
async def get_teams_by_league(supabase: Client = Depends(get_supabase_client)):
    """
    Fetch all leagues and include the country's logo URL from the teams table.
    """
    try:
        # Define the list of league IDs to filter by
        league_ids = [1, 2, 3, 4, 5, 7, 8, 10, 11, 12]

        # Step 1: Fetch leagues data
        leagues_response = supabase.table("leagues").select("league_id, league_name, country").in_("league_id", league_ids).execute()

        if not leagues_response.data:
            return {"message": f"No leagues found for league_ids: {league_ids}"}

        # Step 2: Fetch logo URLs for each country
        countries = list(set(league["country"] for league in leagues_response.data))  # Get unique country names
        teams_response = supabase.table("teams").select("team_name, logo_url").in_("team_name", countries).execute()
        country_logo_map = {team["team_name"]: team["logo_url"] for team in teams_response.data}  # Create a mapping of country to logo_url

        # Step 3: Add country_url to each league's data
        enriched_data = []
        for league in leagues_response.data:
            league["country_url"] = country_logo_map.get(league["country"], None)  # Add country_url
            enriched_data.append(league)

        return {"data": enriched_data}

    except Exception as e:
        return {"error": str(e)}

# GET All teams of a certain League with rank
@router.get("/{league_id}/rank")
async def get_teams(league_id: str, supabase: Client = Depends(get_supabase_client)):
    try:
        response = supabase.table("teams").select("curr_league_rank, curr_league_points, team_name, team_id, logo_url, league_id").eq("league_id", league_id).order("curr_league_rank", desc=False).execute()

        # Check if there are any results
        if response.data:
            #print(response.data)
            return {"data": response.data}
        else:
            return {"message": f"No results found for league {league_id}"}
    except Exception as e:
        # Handle errors
        return {"error": str(e)}
    except Exception as e:
        # Handle errors
        return {"error": str(e)}
    





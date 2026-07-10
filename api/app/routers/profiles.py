from fastapi import APIRouter, HTTPException
from postgrest.exceptions import APIError
from supabase import Client

from ..dependencies import AppLoggerDep, CurrentUser, SupabaseClientDep
from ..models.profile import ProfileResponse, ProfileUpdateRequest

router = APIRouter(
    prefix="/v1/profiles",
    tags=["profiles"],
)


def _with_prediction_stats(client: Client, profile: dict) -> dict:
    """total_predictions/correct_predictions on the profiles row are static
    and nothing keeps them updated — compute both live from match_predictions
    instead so they're always accurate."""
    profile_id = profile["id"]
    total = (
        client.table("match_predictions")
        .select("id", count="exact")
        .eq("profile_id", profile_id)
        .execute()
    )
    correct = (
        client.table("match_predictions")
        .select("id", count="exact")
        .eq("profile_id", profile_id)
        .eq("correct", True)
        .execute()
    )
    profile["total_predictions"] = total.count or 0
    profile["correct_predictions"] = correct.count or 0
    return profile


@router.get("/me", response_model=ProfileResponse)
def get_my_profile(
    client: SupabaseClientDep,
    logger: AppLoggerDep,
    user_id: CurrentUser,
):
    logger.info(f"Fetching own profile for user_id={user_id}")
    try:
        response = (
            client.table("profiles").select("*").eq("id", user_id).single().execute()
        )
    except APIError:
        raise HTTPException(status_code=404, detail="Profile not found")
    return {"data": _with_prediction_stats(client, response.data)}


@router.patch("/me", response_model=ProfileResponse)
def update_my_profile(
    updates: ProfileUpdateRequest,
    client: SupabaseClientDep,
    logger: AppLoggerDep,
    user_id: CurrentUser,
):
    logger.info(f"Updating profile for user_id={user_id}")
    # exclude_unset (not exclude_none): a field the client omitted entirely
    # should be left alone, but a field explicitly sent as null (e.g.
    # clearing country_id/favourite_team_id) must still reach the update —
    # exclude_none would silently drop those and make clearing impossible.
    update_data = updates.model_dump(exclude_unset=True)
    if not update_data:
        raise HTTPException(status_code=400, detail="No fields to update")

    try:
        response = (
            client.table("profiles").update(update_data).eq("id", user_id).execute()
        )
    except APIError as e:
        if "profiles_username_key" in str(e):
            raise HTTPException(status_code=409, detail="Username already taken")
        raise HTTPException(status_code=400, detail=str(e))

    if not response.data:
        raise HTTPException(status_code=404, detail="Profile not found")
    return {"data": response.data[0]}


@router.delete("/me", status_code=204)
def delete_my_profile(
    client: SupabaseClientDep,
    logger: AppLoggerDep,
    user_id: CurrentUser,
):
    logger.info(f"Soft-deleting profile for user_id={user_id}")
    try:
        client.rpc("soft_delete_user", {"p_user_id": user_id}).execute()
    except APIError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{username}", response_model=ProfileResponse)
def get_profile(
    username: str,
    client: SupabaseClientDep,
    logger: AppLoggerDep,
):
    logger.info(f"Fetching profile for username={username}")
    try:
        response = (
            client.table("profiles")
            .select("*")
            .eq("username", username)
            .eq("email_confirmed", True)
            .single()
            .execute()
        )
    except APIError:
        raise HTTPException(status_code=404, detail="Profile not found")
    return {"data": _with_prediction_stats(client, response.data)}

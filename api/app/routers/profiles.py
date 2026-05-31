from fastapi import APIRouter, HTTPException
from postgrest.exceptions import APIError

from ..dependencies import AppLoggerDep, SupabaseClientDep, CurrentUser
from ..models.profile import ProfileResponse, ProfileUpdateRequest

router = APIRouter(
    prefix="/v1/profiles",
    tags=["profiles"],
)


@router.get("/me", response_model=ProfileResponse)
async def get_my_profile(
    client: SupabaseClientDep,
    logger: AppLoggerDep,
    user_id: CurrentUser,
):
    logger.info(f"Fetching own profile for user_id={user_id}")
    try:
        response = client.table("profiles").select("*").eq("id", user_id).single().execute()
    except APIError:
        raise HTTPException(status_code=404, detail="Profile not found")
    return {"data": response.data}


@router.patch("/me", response_model=ProfileResponse)
async def update_my_profile(
    updates: ProfileUpdateRequest,
    client: SupabaseClientDep,
    logger: AppLoggerDep,
    user_id: CurrentUser,
):
    logger.info(f"Updating profile for user_id={user_id}")
    update_data = updates.model_dump(exclude_none=True)
    if not update_data:
        raise HTTPException(status_code=400, detail="No fields to update")

    try:
        response = (
            client.table("profiles")
            .update(update_data)
            .eq("id", user_id)
            .single()
            .execute()
        )
    except APIError as e:
        if "profiles_username_key" in str(e):
            raise HTTPException(status_code=409, detail="Username already taken")
        raise HTTPException(status_code=400, detail=str(e))
    return {"data": response.data}


@router.get("/{username}", response_model=ProfileResponse)
async def get_profile(
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
            .single()
            .execute()
        )
    except APIError:
        raise HTTPException(status_code=404, detail="Profile not found")
    return {"data": response.data}

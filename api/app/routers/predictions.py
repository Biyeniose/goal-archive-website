from fastapi import APIRouter, HTTPException
from postgrest.exceptions import APIError
from sqlalchemy import text

from ..dependencies import AppLoggerDep, CurrentUser, DBSession, SupabaseClientDep
from ..models.prediction import (
    MatchPredictionListResponse,
    MatchPredictionRequest,
    MatchPredictionResponse,
    UserPredictionsResponse,
)

router = APIRouter(
    prefix="/v1/predictions",
    tags=["predictions"],
)

_PREDICTION_JSON_BUILD = """
    json_build_object(
        'id',          mp.id,
        'profile_id',  mp.profile_id,
        'match_id',    mp.match_id,
        'home_goals',  mp.home_goals,
        'away_goals',  mp.away_goals,
        'win_team',    mp.win_team,
        'loss_team',   mp.loss_team,
        'isdraw',      mp.isdraw,
        'pens_winner', mp.pens_winner,
        'is_settled',  mp.is_settled,
        'correct',     mp.correct,
        'created_at',  mp.created_at,
        'updated_at',  mp.updated_at,
        'match', json_build_object(
            'match_id',       m.match_id,
            'match_date',     m.match_date,
            'match_time_utc', m.match_time_utc,
            'isplayed',       m.isplayed,
            'is_live',        m.is_live,
            'home_goals',     m.home_goals,
            'away_goals',     m.away_goals,
            'win_team',       m.win_team,
            'isdraw',         m.isdraw,
            'pen_home_goals', m.pen_home_goals,
            'pen_away_goals', m.pen_away_goals,
            'round',          m.round,
            'can_go_to_pens', public.round_can_go_to_pens(m.round),
            'home_team', json_build_object(
                'team_id',  ht.team_id,
                'name',     coalesce(ht.common_name, ht.name),
                'logo_url', ht.logo_url
            ),
            'away_team', json_build_object(
                'team_id',  at.team_id,
                'name',     coalesce(at.common_name, at.name),
                'logo_url', at.logo_url
            )
        )
    )
"""


def _list_user_predictions_by_settled(
    session: DBSession, username: str, is_settled: bool, limit: int = 4
) -> list[dict]:
    # Settled predictions sort by match kickoff (most recently played match
    # first); pending ones sort by when the pick was made — there's no
    # "recently played" for a match that hasn't happened yet.
    order_column = "m.match_time_utc" if is_settled else "mp.created_at"
    query = text(f"""
        SELECT coalesce(json_agg(d ORDER BY d_sort DESC NULLS LAST), '[]'::json)
        FROM (
            SELECT {_PREDICTION_JSON_BUILD} AS d, {order_column} AS d_sort
            FROM match_predictions mp
            JOIN profiles p ON p.id = mp.profile_id
            JOIN matches m ON m.match_id = mp.match_id
            LEFT JOIN teams ht ON ht.team_id = m.home_id
            LEFT JOIN teams at ON at.team_id = m.away_id
            WHERE p.username = :username
              AND p.email_confirmed = true
              AND mp.is_settled = :is_settled
            ORDER BY {order_column} DESC NULLS LAST
            LIMIT :limit
        ) sub
    """)
    result = session.exec(
        query, params={"username": username, "is_settled": is_settled, "limit": limit}
    ).first()
    return result[0] if result else []


@router.get("/me", response_model=MatchPredictionListResponse)
def list_my_predictions(
    session: DBSession,
    logger: AppLoggerDep,
    user_id: CurrentUser,
):
    logger.info(f"Listing predictions for user_id={user_id}")
    query = text(f"""
        SELECT coalesce(json_agg(d ORDER BY (d->'match'->>'match_date') DESC NULLS LAST, (d->>'match_time_utc') DESC NULLS LAST), '[]'::json)
        FROM (
            SELECT {_PREDICTION_JSON_BUILD} AS d
            FROM match_predictions mp
            JOIN matches m ON m.match_id = mp.match_id
            LEFT JOIN teams ht ON ht.team_id = m.home_id
            LEFT JOIN teams at ON at.team_id = m.away_id
            WHERE mp.profile_id = :user_id
        ) sub
    """)
    result = session.exec(query, params={"user_id": user_id}).first()
    return {"data": result[0] if result else []}


@router.get("/user/{username}", response_model=UserPredictionsResponse)
def list_user_predictions(
    username: str,
    session: DBSession,
    logger: AppLoggerDep,
):
    logger.info(f"Listing public predictions for username={username}")
    pending = _list_user_predictions_by_settled(session, username, is_settled=False)
    settled = _list_user_predictions_by_settled(session, username, is_settled=True)
    return {"pending": pending, "settled": settled}


@router.get("/matches/{match_id}", response_model=MatchPredictionResponse)
def get_match_prediction(
    match_id: int,
    client: SupabaseClientDep,
    logger: AppLoggerDep,
    user_id: CurrentUser,
):
    logger.info(f"Fetching prediction for match_id={match_id} user_id={user_id}")
    response = (
        client.table("match_predictions")
        .select("*")
        .eq("match_id", match_id)
        .eq("profile_id", user_id)
        .execute()
    )
    data = response.data[0] if response.data else None
    return {"data": data}


@router.put("/matches/{match_id}", response_model=MatchPredictionResponse)
def upsert_match_prediction(
    match_id: int,
    payload: MatchPredictionRequest,
    client: SupabaseClientDep,
    logger: AppLoggerDep,
    user_id: CurrentUser,
):
    logger.info(f"Saving prediction for match_id={match_id} user_id={user_id}")
    try:
        response = (
            client.table("match_predictions")
            .upsert(
                {
                    "profile_id": user_id,
                    "match_id": match_id,
                    "home_goals": payload.home_goals,
                    "away_goals": payload.away_goals,
                    "pens_winner": payload.pens_winner,
                },
                on_conflict="profile_id,match_id",
            )
            .execute()
        )
    except APIError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not response.data:
        raise HTTPException(status_code=400, detail="Failed to save prediction")
    return {"data": response.data[0]}


@router.delete("/matches/{match_id}", status_code=204)
def delete_match_prediction(
    match_id: int,
    client: SupabaseClientDep,
    logger: AppLoggerDep,
    user_id: CurrentUser,
):
    logger.info(f"Deleting prediction for match_id={match_id} user_id={user_id}")
    try:
        response = (
            client.table("match_predictions")
            .delete()
            .eq("match_id", match_id)
            .eq("profile_id", user_id)
            .execute()
        )
    except APIError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not response.data:
        raise HTTPException(status_code=404, detail="Prediction not found")

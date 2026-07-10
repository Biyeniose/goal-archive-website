from typing import Optional

from fastapi import APIRouter, HTTPException
from postgrest.exceptions import APIError
from sqlalchemy import text

from ..dependencies import AppLoggerDep, CurrentUser, DBSession, OptionalCurrentUser, SupabaseClientDep
from ..models.user_league import (
    CreateUserLeagueRequest,
    JoinUserLeagueRequest,
    SetMemberAdminRequest,
    UpdateUserLeagueRequest,
    UserLeagueDetailResponse,
    UserLeagueListResponse,
    UserLeagueRankingsResponse,
    UserLeagueResponse,
    UserLeagueScoredMatchesResponse,
)

router = APIRouter(
    prefix="/v1/user-leagues",
    tags=["user_leagues"],
)


def _get_membership(client, user_league_id: int, user_id: str) -> Optional[dict]:
    response = (
        client.table("user_league_members")
        .select("*")
        .eq("user_league_id", user_league_id)
        .eq("profile_id", user_id)
        .execute()
    )
    return response.data[0] if response.data else None


def _require_member(client, user_league_id: int, user_id: str) -> dict:
    membership = _get_membership(client, user_league_id, user_id)
    if not membership:
        raise HTTPException(status_code=403, detail="Not a member of this user league")
    return membership


def _require_admin(client, user_league_id: int, user_id: str) -> dict:
    membership = _require_member(client, user_league_id, user_id)
    if not membership["is_admin"]:
        raise HTTPException(status_code=403, detail="Only league admins can do this")
    return membership


def _get_league_or_404(client, user_league_id: int) -> dict:
    response = client.table("user_leagues").select("*").eq("id", user_league_id).execute()
    if not response.data:
        raise HTTPException(status_code=404, detail="User league not found")
    return response.data[0]


@router.post("", response_model=UserLeagueResponse, status_code=201)
def create_user_league(
    payload: CreateUserLeagueRequest,
    client: SupabaseClientDep,
    logger: AppLoggerDep,
    user_id: CurrentUser,
):
    logger.info(f"Creating user league for user_id={user_id}")
    try:
        response = (
            client.table("user_leagues")
            .insert(
                {
                    "name": payload.name,
                    "creator_id": user_id,
                    "start_date": payload.start_date.isoformat(),
                    "end_date": payload.end_date.isoformat(),
                }
            )
            .execute()
        )
    except APIError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not response.data:
        raise HTTPException(status_code=400, detail="Failed to create user league")
    league = response.data[0]

    try:
        client.table("user_league_leagues").insert(
            [{"user_league_id": league["id"], "league_id": lid} for lid in payload.league_ids]
        ).execute()
    except APIError as e:
        # Don't leave a league behind with no scoring leagues attached.
        client.table("user_leagues").delete().eq("id", league["id"]).execute()
        raise HTTPException(status_code=400, detail=str(e))

    return {"data": league}


@router.post("/join", response_model=UserLeagueResponse)
def join_user_league(
    payload: JoinUserLeagueRequest,
    client: SupabaseClientDep,
    logger: AppLoggerDep,
    user_id: CurrentUser,
):
    logger.info(f"user_id={user_id} joining user league via code")
    league_response = (
        client.table("user_leagues")
        .select("*")
        .eq("league_code", payload.league_code.strip().upper())
        .execute()
    )
    if not league_response.data:
        raise HTTPException(status_code=404, detail="No user league found with that code")
    league = league_response.data[0]

    if _get_membership(client, league["id"], user_id):
        raise HTTPException(status_code=409, detail="Already a member of this user league")

    try:
        client.table("user_league_members").insert(
            {"user_league_id": league["id"], "profile_id": user_id}
        ).execute()
    except APIError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return {"data": league}


@router.get("/me", response_model=UserLeagueListResponse)
def list_my_user_leagues(
    session: DBSession,
    logger: AppLoggerDep,
    user_id: CurrentUser,
):
    logger.info(f"Listing user leagues for user_id={user_id}")
    query = text("""
        SELECT coalesce(json_agg(d ORDER BY (d->>'created_at') DESC), '[]'::json)
        FROM (
            SELECT json_build_object(
                'id',                   ul.id,
                'name',                 ul.name,
                'league_code',          ul.league_code,
                'creator_id',           ul.creator_id,
                'start_date',           ul.start_date,
                'end_date',             ul.end_date,
                'matches_start_date',   ul.matches_start_date,
                'matches_end_date',     ul.matches_end_date,
                'status',               ul.status,
                'rankings_updated_at',  ul.rankings_updated_at,
                'created_at',           ul.created_at,
                'updated_at',           ul.updated_at,
                'is_admin',             mine.is_admin,
                'member_count',         (
                    SELECT count(*) FROM user_league_members m2 WHERE m2.user_league_id = ul.id
                )
            ) AS d
            FROM user_leagues ul
            JOIN user_league_members mine
              ON mine.user_league_id = ul.id AND mine.profile_id = :user_id
        ) sub
    """)
    result = session.exec(query, params={"user_id": user_id}).first()
    return {"data": result[0] if result else []}


@router.get("/{user_league_id}", response_model=UserLeagueDetailResponse)
def get_user_league(
    user_league_id: int,
    client: SupabaseClientDep,
    session: DBSession,
    logger: AppLoggerDep,
    user_id: OptionalCurrentUser,
):
    # Public — anyone can view a league's details. Only the join code is
    # member-only; membership/admin status is computed if a token was sent.
    league = _get_league_or_404(client, user_league_id)

    membership = _get_membership(client, user_league_id, user_id) if user_id else None
    is_member = membership is not None
    is_admin = bool(membership and membership["is_admin"])

    if not is_member:
        league = {**league, "league_code": None}

    leagues_response = (
        client.table("user_league_leagues")
        .select("league_id")
        .eq("user_league_id", user_league_id)
        .execute()
    )
    league_ids = [row["league_id"] for row in leagues_response.data]

    query = text("""
        SELECT coalesce(json_agg(d ORDER BY (d->>'joined_at') ASC), '[]'::json)
        FROM (
            SELECT json_build_object(
                'profile_id',   ulm.profile_id,
                'is_admin',     ulm.is_admin,
                'joined_at',    ulm.joined_at,
                'username',     p.username,
                'display_name', p.display_name,
                'avatar_url',   p.avatar_url
            ) AS d
            FROM user_league_members ulm
            JOIN profiles p ON p.id = ulm.profile_id
            WHERE ulm.user_league_id = :user_league_id
        ) sub
    """)
    result = session.exec(query, params={"user_league_id": user_league_id}).first()
    members = result[0] if result else []

    return {
        "data": {
            **league,
            "league_ids": league_ids,
            "is_member": is_member,
            "is_admin": is_admin,
            "members": members,
        }
    }


@router.patch("/{user_league_id}", response_model=UserLeagueResponse)
def update_user_league(
    user_league_id: int,
    payload: UpdateUserLeagueRequest,
    client: SupabaseClientDep,
    logger: AppLoggerDep,
    user_id: CurrentUser,
):
    _require_admin(client, user_league_id, user_id)
    logger.info(f"Updating user_league_id={user_league_id} by user_id={user_id}")

    date_update: dict = {}
    if payload.end_date is not None:
        date_update["end_date"] = payload.end_date.isoformat()
    if payload.matches_start_date is not None:
        date_update["matches_start_date"] = payload.matches_start_date.isoformat()
    if payload.matches_end_date is not None:
        date_update["matches_end_date"] = payload.matches_end_date.isoformat()

    if date_update:
        try:
            client.table("user_leagues").update(date_update).eq("id", user_league_id).execute()
        except APIError as e:
            raise HTTPException(status_code=400, detail=str(e))

    if payload.league_ids is not None:
        try:
            client.table("user_league_leagues").delete().eq("user_league_id", user_league_id).execute()
            if payload.league_ids:
                client.table("user_league_leagues").insert(
                    [{"user_league_id": user_league_id, "league_id": lid} for lid in payload.league_ids]
                ).execute()
        except APIError as e:
            raise HTTPException(status_code=400, detail=str(e))

    return {"data": _get_league_or_404(client, user_league_id)}


@router.delete("/{user_league_id}", status_code=204)
def delete_user_league(
    user_league_id: int,
    client: SupabaseClientDep,
    logger: AppLoggerDep,
    user_id: CurrentUser,
):
    _require_admin(client, user_league_id, user_id)
    logger.info(f"Deleting user_league_id={user_league_id} by user_id={user_id}")

    # Cascades to user_league_members, user_league_leagues, user_league_rankings.
    try:
        client.table("user_leagues").delete().eq("id", user_league_id).execute()
    except APIError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.patch("/{user_league_id}/members/{profile_id}", status_code=204)
def set_member_admin(
    user_league_id: int,
    profile_id: str,
    payload: SetMemberAdminRequest,
    client: SupabaseClientDep,
    logger: AppLoggerDep,
    user_id: CurrentUser,
):
    _require_admin(client, user_league_id, user_id)
    logger.info(f"Setting is_admin={payload.is_admin} for profile_id={profile_id} in user_league_id={user_league_id}")

    if not _get_membership(client, user_league_id, profile_id):
        raise HTTPException(status_code=404, detail="That user is not a member of this user league")

    league = _get_league_or_404(client, user_league_id)
    if not payload.is_admin and profile_id == league["creator_id"]:
        raise HTTPException(status_code=400, detail="The league creator can't be removed as admin")

    try:
        client.table("user_league_members").update({"is_admin": payload.is_admin}).eq(
            "user_league_id", user_league_id
        ).eq("profile_id", profile_id).execute()
    except APIError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/{user_league_id}/members/{profile_id}", status_code=204)
def remove_member(
    user_league_id: int,
    profile_id: str,
    client: SupabaseClientDep,
    logger: AppLoggerDep,
    user_id: CurrentUser,
):
    # A member can remove themselves (leave); removing someone else requires
    # being an admin. Either way, an admin can't be removed at all — demote
    # them first (and the creator can never be demoted, see set_member_admin).
    requester = _require_member(client, user_league_id, user_id)
    is_self = profile_id == user_id
    if not is_self and not requester["is_admin"]:
        raise HTTPException(status_code=403, detail="Only league admins can remove other members")

    target = _get_membership(client, user_league_id, profile_id)
    if not target:
        raise HTTPException(status_code=404, detail="That user is not a member of this user league")
    if target["is_admin"]:
        raise HTTPException(status_code=400, detail="Admins can't be removed — remove their admin status first")

    logger.info(f"Removing profile_id={profile_id} from user_league_id={user_league_id} (requested by {user_id})")

    try:
        client.table("user_league_members").delete().eq("user_league_id", user_league_id).eq(
            "profile_id", profile_id
        ).execute()
    except APIError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{user_league_id}/rankings", response_model=UserLeagueRankingsResponse)
def get_user_league_rankings(
    user_league_id: int,
    session: DBSession,
    logger: AppLoggerDep,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    # Public. Always predictions-based (a member only scores matches they
    # actually submitted a prediction for — no "no prediction defaults to
    # 1-1" credit). Left-joined from members (not the cached rankings table)
    # so a member with zero scored matches in the window still shows up at 0
    # points instead of being absent.
    #
    # start_date/end_date optionally narrow the scoring window for this
    # request only (live, uncached) — clamped to the league's own
    # matches_start_date/matches_end_date so a caller can't pull in matches
    # outside the admin-configured scoring window.
    query = text("""
        WITH bounds AS (
            SELECT
                GREATEST(matches_start_date, COALESCE(CAST(:start_date AS date), matches_start_date)) AS window_start,
                LEAST(matches_end_date, COALESCE(CAST(:end_date AS date), matches_end_date)) AS window_end
            FROM user_leagues WHERE id = :user_league_id
        ),
        scored AS (
            SELECT
                mp.profile_id,
                m.match_id,
                (
                    CASE WHEN
                        (CASE WHEN mp.isdraw THEN 'D'
                              WHEN mp.win_team = m.home_id THEN 'H'
                              WHEN mp.win_team = m.away_id THEN 'A'
                              ELSE 'D' END)
                        =
                        (CASE WHEN m.isdraw THEN 'D'
                              WHEN m.win_team = m.home_id THEN 'H'
                              WHEN m.win_team = m.away_id THEN 'A'
                              ELSE 'D' END)
                    THEN 3 ELSE 0 END
                )
                + (CASE WHEN mp.home_goals = m.home_goals THEN 1 ELSE 0 END)
                + (CASE WHEN mp.away_goals = m.away_goals THEN 1 ELSE 0 END)
                + (CASE WHEN mp.home_goals = m.home_goals AND mp.away_goals = m.away_goals THEN 1 ELSE 0 END)
                AS points
            FROM match_predictions mp
            JOIN user_league_members mem ON mem.profile_id = mp.profile_id AND mem.user_league_id = :user_league_id
            JOIN matches m ON m.match_id = mp.match_id
            JOIN competitions c ON c.competition_id = m.comp_id
            JOIN user_league_leagues ull ON ull.user_league_id = :user_league_id AND ull.league_id = c.league_id
            CROSS JOIN bounds
            WHERE m.match_date BETWEEN bounds.window_start AND bounds.window_end
              AND m.isplayed = true
              AND m.home_goals IS NOT NULL
              AND m.away_goals IS NOT NULL
        )
        SELECT coalesce(json_agg(d ORDER BY (d->>'points')::int DESC, d->>'username' ASC), '[]'::json)
        FROM (
            SELECT json_build_object(
                'profile_id',              mem.profile_id,
                'points',                  coalesce(sum(scored.points), 0),
                'matches_scored',          count(scored.match_id),
                'username',                p.username,
                'display_name',            p.display_name,
                'avatar_url',              p.avatar_url,
                'country_flag_url',        c.circle_url,
                'favourite_team_logo_url', t.logo_url
            ) AS d
            FROM user_league_members mem
            JOIN profiles p ON p.id = mem.profile_id
            LEFT JOIN scored ON scored.profile_id = mem.profile_id
            LEFT JOIN countries c ON c.country_id = p.country_id
            LEFT JOIN teams t ON t.team_id = p.favourite_team_id
            WHERE mem.user_league_id = :user_league_id
            GROUP BY mem.profile_id, p.username, p.display_name, p.avatar_url, c.circle_url, t.logo_url
        ) sub
    """)

    result = session.exec(
        query,
        params={"user_league_id": user_league_id, "start_date": start_date, "end_date": end_date},
    ).first()
    return {"data": result[0] if result else []}


@router.get("/{user_league_id}/rankings/{profile_id}/matches", response_model=UserLeagueScoredMatchesResponse)
def get_user_league_scored_matches(
    user_league_id: int,
    profile_id: str,
    session: DBSession,
    logger: AppLoggerDep,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    # Public. Every match a given member predicted and scored within the
    # (optionally narrowed) window — same clamping/scoring logic as the
    # rankings endpoint above, just per-match instead of summed.
    query = text("""
        WITH bounds AS (
            SELECT
                GREATEST(matches_start_date, COALESCE(CAST(:start_date AS date), matches_start_date)) AS window_start,
                LEAST(matches_end_date, COALESCE(CAST(:end_date AS date), matches_end_date)) AS window_end
            FROM user_leagues WHERE id = :user_league_id
        )
        SELECT coalesce(json_agg(d ORDER BY d->>'match_date' DESC), '[]'::json)
        FROM (
            SELECT json_build_object(
                'match_id',       m.match_id,
                'match_date',     m.match_date,
                'home_goals',     m.home_goals,
                'away_goals',     m.away_goals,
                'predicted_home_goals', mp.home_goals,
                'predicted_away_goals', mp.away_goals,
                'outcome_correct', (
                    (CASE WHEN mp.isdraw THEN 'D'
                          WHEN mp.win_team = m.home_id THEN 'H'
                          WHEN mp.win_team = m.away_id THEN 'A'
                          ELSE 'D' END)
                    =
                    (CASE WHEN m.isdraw THEN 'D'
                          WHEN m.win_team = m.home_id THEN 'H'
                          WHEN m.win_team = m.away_id THEN 'A'
                          ELSE 'D' END)
                ),
                'points', (
                    (CASE WHEN
                        (CASE WHEN mp.isdraw THEN 'D'
                              WHEN mp.win_team = m.home_id THEN 'H'
                              WHEN mp.win_team = m.away_id THEN 'A'
                              ELSE 'D' END)
                        =
                        (CASE WHEN m.isdraw THEN 'D'
                              WHEN m.win_team = m.home_id THEN 'H'
                              WHEN m.win_team = m.away_id THEN 'A'
                              ELSE 'D' END)
                    THEN 3 ELSE 0 END)
                    + (CASE WHEN mp.home_goals = m.home_goals THEN 1 ELSE 0 END)
                    + (CASE WHEN mp.away_goals = m.away_goals THEN 1 ELSE 0 END)
                    + (CASE WHEN mp.home_goals = m.home_goals AND mp.away_goals = m.away_goals THEN 1 ELSE 0 END)
                ),
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
            ) AS d
            FROM match_predictions mp
            JOIN matches m ON m.match_id = mp.match_id
            JOIN competitions c ON c.competition_id = m.comp_id
            JOIN user_league_leagues ull ON ull.user_league_id = :user_league_id AND ull.league_id = c.league_id
            LEFT JOIN teams ht ON ht.team_id = m.home_id
            LEFT JOIN teams at ON at.team_id = m.away_id
            CROSS JOIN bounds
            WHERE mp.profile_id = :profile_id
              AND m.match_date BETWEEN bounds.window_start AND bounds.window_end
              AND m.isplayed = true
              AND m.home_goals IS NOT NULL
              AND m.away_goals IS NOT NULL
        ) sub
    """)

    result = session.exec(
        query,
        params={
            "user_league_id": user_league_id,
            "profile_id": profile_id,
            "start_date": start_date,
            "end_date": end_date,
        },
    ).first()
    return {"data": result[0] if result else []}

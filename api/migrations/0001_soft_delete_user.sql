-- Soft-deletes a user: anonymizes their profiles row in place (same id, same
-- row, so any future predictions/friendships FKs keep resolving) and bans
-- the matching auth.users account (banned_until, not a delete — reversible,
-- doesn't touch anything).
--
-- Deliberately NOT granted to anon/authenticated: it takes an arbitrary
-- p_user_id with no ownership check baked in, so only the service-role
-- backend (which derives the id from the caller's own verified JWT) may
-- call it. Exposing this via PostgREST rpc/ to authenticated users would let
-- anyone soft-delete anyone.

\set ON_ERROR_STOP on

begin;

create or replace function public.soft_delete_user(p_user_id uuid)
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  update public.profiles
  set username = 'deleted_user_' || substr(replace(p_user_id::text, '-', ''), 1, 17),
      display_name = null,
      bio = null,
      avatar_url = null,
      favourite_team_id = null,
      favourite_player_id = null,
      deleted_at = now()
  where id = p_user_id
    and deleted_at is null;

  update auth.users
  set banned_until = 'infinity'
  where id = p_user_id;
end;
$$;

revoke all on function public.soft_delete_user(uuid) from public;
grant execute on function public.soft_delete_user(uuid) to service_role;

commit;

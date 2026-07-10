-- soft_delete_user() set banned_until = 'infinity', which is valid Postgres
-- syntax for timestamptz but not safely representable by Go's time.Time
-- (which GoTrue uses internally to scan/marshal auth.users rows). That can
-- break GoTrue's own admin/auth code paths for a banned row. Supabase's
-- Admin API avoids this by computing a concrete far-future timestamp
-- instead of the literal infinity sentinel — matching that here.

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
  set banned_until = '9999-12-31 23:59:59+00'
  where id = p_user_id;
end;
$$;

revoke all on function public.soft_delete_user(uuid) from public;
grant execute on function public.soft_delete_user(uuid) to service_role;

commit;

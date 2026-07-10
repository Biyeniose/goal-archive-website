-- One-time backfill: handle_new_user() only fires on new auth.users INSERTs
-- going forward. Accounts created before public.profiles/its trigger
-- existed (e.g. signups from before 2026-07-02) never got a profiles row,
-- so GET /v1/profiles/me 404s for them. This creates the missing rows using
-- the same logic as the trigger.

\set ON_ERROR_STOP on

begin;

insert into public.profiles (id, username, display_name, email_confirmed)
select
  u.id,
  lower(coalesce(nullif(trim(u.raw_user_meta_data->>'username'), ''), split_part(u.email, '@', 1))),
  nullif(trim(u.raw_user_meta_data->>'display_name'), ''),
  u.email_confirmed_at is not null
from auth.users u
left join public.profiles p on p.id = u.id
where p.id is null;

commit;

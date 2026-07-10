-- Creates the profiles table (extends auth.users) plus its triggers, RLS
-- policies, and grants. Written against a self-hosted Supabase instance
-- where public.profiles did not exist yet (auth.users / GoTrue is already
-- provisioned; standard roles anon/authenticated/service_role already
-- exist).
--
-- email_confirmed is included from the start (rather than as a later ALTER)
-- so newly-signed-up, unconfirmed users are never publicly readable —
-- see api/app/routers/profiles.py for the corresponding API-layer check.

\set ON_ERROR_STOP on

begin;

create table public.profiles (
  id                  uuid primary key references auth.users(id) on delete cascade,
  username            varchar(30) unique not null,
  display_name        varchar(100),
  bio                 text,
  avatar_url          text,
  favourite_team_id   integer references public.teams(team_id) on delete set null,
  favourite_player_id bigint references public.players(player_id) on delete set null,
  total_predictions   integer not null default 0,
  correct_predictions integer not null default 0,
  overall_rank        integer,
  last_seen_at        timestamptz,
  email_confirmed     boolean not null default false,
  deleted_at          timestamptz,
  created_at          timestamptz not null default now(),
  updated_at          timestamptz not null default now()
);

comment on column public.profiles.email_confirmed is
  'Mirrors auth.users.email_confirmed_at IS NOT NULL, kept in sync by sync_profile_email_confirmed(). Gates public visibility.';

-- Creates a profiles row whenever a new auth.users row is inserted.
-- Username defaults to the email's local part if not supplied via signup
-- metadata; falls back further to a short id-based handle on collision.
create or replace function public.handle_new_user()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  base_username text;
  final_username text;
  suffix int := 0;
begin
  base_username := lower(coalesce(
    nullif(trim(new.raw_user_meta_data->>'username'), ''),
    split_part(new.email, '@', 1)
  ));
  base_username := regexp_replace(base_username, '[^a-z0-9_]', '_', 'g');
  base_username := left(base_username, 30);
  final_username := base_username;

  while exists (select 1 from public.profiles where username = final_username) loop
    suffix := suffix + 1;
    final_username := left(base_username, 30 - length(suffix::text) - 1) || '_' || suffix::text;
  end loop;

  insert into public.profiles (id, username, display_name, email_confirmed)
  values (
    new.id,
    final_username,
    nullif(trim(new.raw_user_meta_data->>'display_name'), ''),
    new.email_confirmed_at is not null
  );
  return new;
end;
$$;

drop trigger if exists on_auth_user_created on auth.users;
create trigger on_auth_user_created
  after insert on auth.users
  for each row
  execute function public.handle_new_user();

-- Keeps profiles.email_confirmed in sync with auth.users.email_confirmed_at
-- on every subsequent change (e.g. clicking the confirmation link).
create or replace function public.sync_profile_email_confirmed()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  update public.profiles
  set email_confirmed = (new.email_confirmed_at is not null)
  where id = new.id;
  return new;
end;
$$;

drop trigger if exists on_auth_user_email_confirmed on auth.users;
create trigger on_auth_user_email_confirmed
  after update of email_confirmed_at on auth.users
  for each row
  execute function public.sync_profile_email_confirmed();

create trigger update_profiles_updated_at
  before update on public.profiles
  for each row
  execute function public.update_updated_at_column();

alter table public.profiles enable row level security;

-- Anyone can read a non-deleted, confirmed profile.
create policy profiles_public_read
  on public.profiles
  for select
  using (deleted_at is null and email_confirmed = true);

-- Owners can always read their own profile, confirmed or not.
create policy profiles_owner_read
  on public.profiles
  for select
  using (auth.uid() = id);

-- Owners can update their own profile.
create policy profiles_owner_update
  on public.profiles
  for update
  using (auth.uid() = id);

grant select on public.profiles to anon;
grant select, update on public.profiles to authenticated;
grant all on public.profiles to service_role;

commit;

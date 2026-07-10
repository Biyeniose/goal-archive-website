-- User-created prediction leagues ("user_leagues"), joinable only via a
-- unique league_code. Standings are computed from public.matches whose
-- competition's league_id is one of the user league's selected
-- public.leagues rows, and whose match_date falls within [start_date,
-- end_date]. Scoring uses the match's own final score (isplayed +
-- home_goals/away_goals), not match_predictions.is_settled/correct — this
-- keeps league scoring independent of when settle_match_predictions() last
-- ran, and lets the "no prediction = auto 1-1" rule apply cleanly.
--
-- Table chain to public.leagues: matches.comp_id -> competitions.competition_id
-- -> competitions.league_id -> leagues.league_id. There is no direct FK from
-- matches to leagues.

\set ON_ERROR_STOP on

begin;

create table public.user_leagues (
  id                    bigserial primary key,
  name                  text not null,
  league_code           text unique,
  creator_id            uuid not null references public.profiles(id) on delete restrict,
  start_date            date not null,
  end_date              date not null,
  status                text not null default 'active' check (status in ('active', 'finished')),
  rankings_updated_at   timestamptz,
  created_at            timestamptz not null default now(),
  updated_at            timestamptz not null default now()
);

create table public.user_league_members (
  id              bigserial primary key,
  user_league_id  bigint not null references public.user_leagues(id) on delete cascade,
  profile_id      uuid not null references public.profiles(id) on delete cascade,
  is_admin        boolean not null default false,
  joined_at       timestamptz not null default now(),
  unique (user_league_id, profile_id)
);

create index idx_user_league_members_profile on public.user_league_members(profile_id);

-- Which public.leagues' matches count toward this user league's scoring.
create table public.user_league_leagues (
  user_league_id  bigint not null references public.user_leagues(id) on delete cascade,
  league_id       integer not null references public.leagues(league_id),
  primary key (user_league_id, league_id)
);

create index idx_user_league_leagues_league on public.user_league_leagues(league_id);

-- Fully recomputed on every recalc — see recalculate_user_league_rankings().
create table public.user_league_rankings (
  user_league_id  bigint not null references public.user_leagues(id) on delete cascade,
  profile_id      uuid not null references public.profiles(id) on delete cascade,
  points          integer not null default 0,
  matches_scored  integer not null default 0,
  primary key (user_league_id, profile_id)
);

create index idx_user_leagues_creator_status on public.user_leagues(creator_id, status);

create trigger on_user_leagues_updated_at
  before update on public.user_leagues
  for each row
  execute function public.update_updated_at_column();

-- ── league_code generation ──────────────────────────────────────────────────
-- Auto-assigned on insert if not supplied. Excludes visually ambiguous
-- characters (0/O, 1/I/L).

create or replace function public.generate_league_code()
returns text
language plpgsql
as $$
declare
  chars text := 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
  code text;
  i integer;
begin
  loop
    code := '';
    for i in 1..6 loop
      code := code || substr(chars, floor(random() * length(chars) + 1)::int, 1);
    end loop;
    exit when not exists (select 1 from public.user_leagues where league_code = code);
  end loop;
  return code;
end;
$$;

create or replace function public.set_user_league_code()
returns trigger
language plpgsql
as $$
begin
  if new.league_code is null then
    new.league_code := public.generate_league_code();
  end if;
  return new;
end;
$$;

create trigger on_user_league_set_code
  before insert on public.user_leagues
  for each row
  execute function public.set_user_league_code();

-- ── date rules: 2-10 months long, end_date >= 1 day in the future ─────────
-- Not a check constraint because "1 day in the future" is relative to
-- current_date, which check constraints (immutable-only) can't reference.

create or replace function public.enforce_user_league_dates()
returns trigger
language plpgsql
as $$
begin
  if new.end_date <= new.start_date then
    raise exception 'User league end date must be after the start date';
  end if;

  if age(new.end_date, new.start_date) < interval '2 months' then
    raise exception 'User league must be at least 2 months long';
  end if;

  if age(new.end_date, new.start_date) > interval '10 months' then
    raise exception 'User league cannot be more than 10 months long';
  end if;

  if new.end_date < current_date + 1 then
    raise exception 'User league end date must be at least 1 day in the future';
  end if;

  return new;
end;
$$;

create trigger on_user_league_dates
  before insert or update of start_date, end_date on public.user_leagues
  for each row
  execute function public.enforce_user_league_dates();

-- ── max 3 active leagues per creator ────────────────────────────────────────

create or replace function public.enforce_max_active_user_leagues()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  active_count integer;
  v_username text;
begin
  select count(*) into active_count
  from public.user_leagues
  where creator_id = new.creator_id
    and status = 'active';

  if active_count >= 3 then
    select username into v_username from public.profiles where id = new.creator_id;
    raise exception 'Too many active user leagues for user %: 3', coalesce(v_username, new.creator_id::text);
  end if;

  return new;
end;
$$;

create trigger on_user_league_insert_limit
  before insert on public.user_leagues
  for each row
  execute function public.enforce_max_active_user_leagues();

-- ── creator auto-joins as admin ──────────────────────────────────────────────

create or replace function public.add_creator_as_admin_member()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  insert into public.user_league_members (user_league_id, profile_id, is_admin)
  values (new.id, new.creator_id, true);
  return new;
end;
$$;

create trigger on_user_league_insert_add_creator
  after insert on public.user_leagues
  for each row
  execute function public.add_creator_as_admin_member();

-- ── only join active leagues ─────────────────────────────────────────────────

create or replace function public.enforce_active_league_for_join()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_status text;
begin
  select status into v_status from public.user_leagues where id = new.user_league_id;
  if v_status is distinct from 'active' then
    raise exception 'User league % is not active and cannot accept new members', new.user_league_id;
  end if;
  return new;
end;
$$;

create trigger on_user_league_member_join_active_check
  before insert on public.user_league_members
  for each row
  execute function public.enforce_active_league_for_join();

-- ── ranking calculation ──────────────────────────────────────────────────────
-- +3 correct outcome, +1 per correctly-guessed team's goal count (independent
-- of outcome), +1 bonus if the exact scoreline is correct. Missing prediction
-- defaults to 1-1.

create or replace function public.recalculate_user_league_rankings(p_user_league_id bigint)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_start date;
  v_end   date;
begin
  select start_date, end_date into v_start, v_end
  from public.user_leagues
  where id = p_user_league_id;

  if not found then
    raise exception 'User league % does not exist', p_user_league_id;
  end if;

  delete from public.user_league_rankings where user_league_id = p_user_league_id;

  insert into public.user_league_rankings (user_league_id, profile_id, points, matches_scored)
  select
    p_user_league_id,
    scored.profile_id,
    sum(scored.points),
    count(*)
  from (
    select
      mem.profile_id,
      (
        case when
          (case when coalesce(mp.home_goals, 1) > coalesce(mp.away_goals, 1) then 'H'
                when coalesce(mp.away_goals, 1) > coalesce(mp.home_goals, 1) then 'A'
                else 'D' end)
          =
          (case when m.home_goals > m.away_goals then 'H'
                when m.away_goals > m.home_goals then 'A'
                else 'D' end)
        then 3 else 0 end
      )
      + (case when coalesce(mp.home_goals, 1) = m.home_goals then 1 else 0 end)
      + (case when coalesce(mp.away_goals, 1) = m.away_goals then 1 else 0 end)
      + (case when coalesce(mp.home_goals, 1) = m.home_goals
               and coalesce(mp.away_goals, 1) = m.away_goals
              then 1 else 0 end)
      as points
    from public.user_league_members mem
    join public.user_league_leagues ull on ull.user_league_id = p_user_league_id
    join public.competitions c on c.league_id = ull.league_id
    join public.matches m on m.comp_id = c.competition_id
    left join public.match_predictions mp
      on mp.match_id = m.match_id and mp.profile_id = mem.profile_id
    where mem.user_league_id = p_user_league_id
      and m.match_date between v_start and v_end
      and m.isplayed = true
      and m.home_goals is not null
      and m.away_goals is not null
  ) scored
  group by scored.profile_id;

  update public.user_leagues
  set rankings_updated_at = now()
  where id = p_user_league_id;
end;
$$;

-- Recalc when an admin edits the date range.
create or replace function public.trigger_recalc_on_user_league_dates_changed()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if old.start_date is distinct from new.start_date or old.end_date is distinct from new.end_date then
    perform public.recalculate_user_league_rankings(new.id);
  end if;
  return new;
end;
$$;

create trigger on_user_league_dates_changed_recalc
  after update of start_date, end_date on public.user_leagues
  for each row
  execute function public.trigger_recalc_on_user_league_dates_changed();

-- Recalc when the selected public.leagues change.
create or replace function public.trigger_recalc_on_league_selection_change()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  perform public.recalculate_user_league_rankings(coalesce(new.user_league_id, old.user_league_id));
  return coalesce(new, old);
end;
$$;

create trigger on_user_league_leagues_change_recalc
  after insert or update or delete on public.user_league_leagues
  for each row
  execute function public.trigger_recalc_on_league_selection_change();

-- Recalc when a member is added or removed (a new member's — or a removed
-- member's absence of — predictions must be reflected immediately, not just
-- on the next cron tick).
create or replace function public.trigger_recalc_on_member_change()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  perform public.recalculate_user_league_rankings(coalesce(new.user_league_id, old.user_league_id));
  return coalesce(new, old);
end;
$$;

create trigger on_user_league_members_change_recalc
  after insert or delete on public.user_league_members
  for each row
  execute function public.trigger_recalc_on_member_change();

-- ── status transition (cron, not trigger — depends on wall-clock date) ──────

create or replace function public.finish_ended_user_leagues()
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  finished_count integer;
begin
  update public.user_leagues
  set status = 'finished'
  where status = 'active'
    and end_date < current_date;

  get diagnostics finished_count = row_count;
  return finished_count;
end;
$$;

-- ── RLS (defense-in-depth — backend uses the service-role key) ──────────────

create or replace function public.is_user_league_member(p_user_league_id bigint, p_profile_id uuid)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select exists (
    select 1 from public.user_league_members
    where user_league_id = p_user_league_id and profile_id = p_profile_id
  );
$$;

create or replace function public.is_user_league_admin(p_user_league_id bigint, p_profile_id uuid)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select exists (
    select 1 from public.user_league_members
    where user_league_id = p_user_league_id and profile_id = p_profile_id and is_admin = true
  );
$$;

alter table public.user_leagues enable row level security;
alter table public.user_league_members enable row level security;
alter table public.user_league_leagues enable row level security;
alter table public.user_league_rankings enable row level security;

create policy user_leagues_member_read on public.user_leagues
  for select using (public.is_user_league_member(id, auth.uid()));

create policy user_leagues_owner_insert on public.user_leagues
  for insert with check (auth.uid() = creator_id);

create policy user_leagues_admin_update on public.user_leagues
  for update using (public.is_user_league_admin(id, auth.uid()));

create policy user_league_members_member_read on public.user_league_members
  for select using (public.is_user_league_member(user_league_id, auth.uid()));

create policy user_league_members_self_insert on public.user_league_members
  for insert with check (auth.uid() = profile_id);

create policy user_league_members_admin_delete on public.user_league_members
  for delete using (public.is_user_league_admin(user_league_id, auth.uid()));

create policy user_league_members_admin_update on public.user_league_members
  for update using (public.is_user_league_admin(user_league_id, auth.uid()));

create policy user_league_leagues_member_read on public.user_league_leagues
  for select using (public.is_user_league_member(user_league_id, auth.uid()));

create policy user_league_leagues_admin_write on public.user_league_leagues
  for all using (public.is_user_league_admin(user_league_id, auth.uid()))
  with check (public.is_user_league_admin(user_league_id, auth.uid()));

create policy user_league_rankings_member_read on public.user_league_rankings
  for select using (public.is_user_league_member(user_league_id, auth.uid()));

grant select, insert, update on public.user_leagues to authenticated;
grant select, insert, update, delete on public.user_league_members to authenticated;
grant select, insert, update, delete on public.user_league_leagues to authenticated;
grant select on public.user_league_rankings to authenticated;
grant all on public.user_leagues, public.user_league_members, public.user_league_leagues, public.user_league_rankings to service_role;
grant usage on sequence public.user_leagues_id_seq, public.user_league_members_id_seq to authenticated, service_role;
grant execute on function public.recalculate_user_league_rankings(bigint) to service_role;
grant execute on function public.finish_ended_user_leagues() to service_role;

commit;

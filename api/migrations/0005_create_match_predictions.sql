-- match_predictions: one row per user per match (upsert model), with
-- win_team/loss_team/isdraw auto-derived from home_goals/away_goals and
-- matches.home_id/away_id. Settlement (is_settled/correct) is NOT automatic
-- — run select settle_match_predictions(); manually whenever you want to
-- grade newly-finished matches. match_prediction_history is an append-only
-- log of genuine prediction changes (not settlement bookkeeping).

\set ON_ERROR_STOP on

begin;

create table public.match_predictions (
  id            bigserial primary key,
  profile_id    uuid not null references public.profiles(id) on delete restrict,
  match_id      bigint not null references public.matches(match_id),
  home_goals    smallint not null,
  away_goals    smallint not null,
  win_team      integer references public.teams(team_id),
  loss_team     integer references public.teams(team_id),
  isdraw        boolean,
  is_settled    boolean not null default false,
  correct       boolean,
  created_at    timestamptz not null default now(),
  updated_at    timestamptz not null default now(),
  unique (profile_id, match_id)
);

create table public.match_prediction_history (
  id             bigserial primary key,
  prediction_id  bigint not null references public.match_predictions(id) on delete cascade,
  profile_id     uuid not null references public.profiles(id) on delete restrict,
  match_id       bigint not null references public.matches(match_id),
  home_goals     smallint not null,
  away_goals     smallint not null,
  win_team       integer,
  loss_team      integer,
  isdraw         boolean,
  version        integer not null,
  changed_at     timestamptz not null default now()
);

-- Enforces the lock window (10 min pre-kickoff, no null match_time_utc/tfm_url,
-- not played, not live) and derives win_team/loss_team/isdraw. Scoped via the
-- trigger's WHEN clause to only fire when home_goals/away_goals actually
-- change, so it never fires for settle_match_predictions()'s is_settled/correct
-- -only update — which by definition targets already-played matches that this
-- same trigger would otherwise reject.
create or replace function public.enforce_and_derive_match_prediction()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  m record;
begin
  select match_time_utc, tfm_url, isplayed, is_live, home_id, away_id
  into m
  from matches
  where match_id = new.match_id;

  if not found then
    raise exception 'Match % does not exist', new.match_id;
  end if;

  if m.match_time_utc is null then
    raise exception 'Match % has no scheduled kickoff time and is not open for predictions', new.match_id;
  end if;

  if m.tfm_url is null then
    raise exception 'Match % is not eligible for predictions', new.match_id;
  end if;

  if m.isplayed then
    raise exception 'Match % has already been played', new.match_id;
  end if;

  if m.is_live then
    raise exception 'Match % is currently live', new.match_id;
  end if;

  if now() >= m.match_time_utc - interval '10 minutes' then
    raise exception 'Predictions for match % are locked (kickoff is within 10 minutes)', new.match_id;
  end if;

  if new.home_goals > new.away_goals then
    new.win_team := m.home_id;
    new.loss_team := m.away_id;
    new.isdraw := false;
  elsif new.away_goals > new.home_goals then
    new.win_team := m.away_id;
    new.loss_team := m.home_id;
    new.isdraw := false;
  else
    new.win_team := null;
    new.loss_team := null;
    new.isdraw := true;
  end if;

  -- A genuine new guess always resets settlement state.
  new.is_settled := false;
  new.correct := null;
  new.updated_at := now();

  return new;
end;
$$;

drop trigger if exists on_match_prediction_insert on public.match_predictions;
create trigger on_match_prediction_insert
  before insert on public.match_predictions
  for each row
  execute function public.enforce_and_derive_match_prediction();

drop trigger if exists on_match_prediction_update on public.match_predictions;
create trigger on_match_prediction_update
  before update on public.match_predictions
  for each row
  when (old.home_goals is distinct from new.home_goals or old.away_goals is distinct from new.away_goals)
  execute function public.enforce_and_derive_match_prediction();

-- Append-only history of genuine prediction changes only (same WHEN scoping
-- as above, for the same reason).
create or replace function public.log_match_prediction_history()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  next_version integer;
begin
  select coalesce(max(version), 0) + 1 into next_version
  from match_prediction_history
  where prediction_id = new.id;

  insert into match_prediction_history (
    prediction_id, profile_id, match_id, home_goals, away_goals, win_team, loss_team, isdraw, version
  ) values (
    new.id, new.profile_id, new.match_id, new.home_goals, new.away_goals, new.win_team, new.loss_team, new.isdraw, next_version
  );

  return new;
end;
$$;

drop trigger if exists on_match_prediction_history_insert on public.match_predictions;
create trigger on_match_prediction_history_insert
  after insert on public.match_predictions
  for each row
  execute function public.log_match_prediction_history();

drop trigger if exists on_match_prediction_history_update on public.match_predictions;
create trigger on_match_prediction_history_update
  after update on public.match_predictions
  for each row
  when (old.home_goals is distinct from new.home_goals or old.away_goals is distinct from new.away_goals)
  execute function public.log_match_prediction_history();

-- Manual settlement — run yourself: select settle_match_predictions();
-- Grades every unsettled prediction whose match has finished. Idempotent
-- (only touches is_settled = false rows), safe to run repeatedly.
create or replace function public.settle_match_predictions()
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  settled_count integer;
begin
  update match_predictions mp
  set is_settled = true,
      correct = (mp.isdraw = m.isdraw and (m.isdraw or mp.win_team = m.win_team)),
      updated_at = now()
  from matches m
  where mp.match_id = m.match_id
    and mp.is_settled = false
    and m.isplayed = true
    and m.is_live = false
    and m.home_goals is not null
    and m.away_goals is not null
    and m.isdraw is not null
    and (m.isdraw or (m.win_team is not null and m.loss_team is not null));

  get diagnostics settled_count = row_count;
  return settled_count;
end;
$$;

alter table public.match_predictions enable row level security;
alter table public.match_prediction_history enable row level security;

-- Owner-only for now. Friend-visibility to be added once the friends table
-- and are_friends() helper exist — this is a placeholder, not the final rule.
create policy match_predictions_owner_read
  on public.match_predictions
  for select
  using (auth.uid() = profile_id);

create policy match_predictions_owner_insert
  on public.match_predictions
  for insert
  with check (auth.uid() = profile_id);

create policy match_predictions_owner_update
  on public.match_predictions
  for update
  using (auth.uid() = profile_id);

create policy match_prediction_history_owner_read
  on public.match_prediction_history
  for select
  using (auth.uid() = profile_id);

grant select, insert, update on public.match_predictions to authenticated;
grant select on public.match_prediction_history to authenticated;
grant all on public.match_predictions to service_role;
grant all on public.match_prediction_history to service_role;
grant usage on sequence public.match_predictions_id_seq to authenticated, service_role;
grant usage on sequence public.match_prediction_history_id_seq to service_role;
grant execute on function public.settle_match_predictions() to service_role;

commit;

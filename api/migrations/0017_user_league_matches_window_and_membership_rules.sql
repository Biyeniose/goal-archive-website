-- 1) Separate the league's lifecycle dates (start_date/end_date — when it
--    auto-finishes) from the *scoring* window (matches_start_date/
--    matches_end_date — which public.matches count toward rankings).
--    Defaults to start_date/end_date at creation, then admins can move it
--    independently. Only the 10-month max is enforced here (no minimum, no
--    "must be in the future" — this is a scoring window, not the league's
--    own lifespan).
--
-- 2) Membership rules: an admin can't be removed (whether by themselves or
--    another admin) without being demoted first, and the league creator can
--    never be demoted. Enforced in the API layer for clearer error messages,
--    but the recalc wiring below is DB-level.

\set ON_ERROR_STOP on

begin;

alter table public.user_leagues
  add column matches_start_date date,
  add column matches_end_date date;

-- Backfill existing rows before the trigger below exists to catch new ones.
update public.user_leagues
set matches_start_date = start_date,
    matches_end_date = end_date
where matches_start_date is null;

create or replace function public.enforce_user_league_matches_dates()
returns trigger
language plpgsql
as $$
begin
  if new.matches_start_date is null then
    new.matches_start_date := new.start_date;
  end if;
  if new.matches_end_date is null then
    new.matches_end_date := new.end_date;
  end if;

  if new.matches_end_date <= new.matches_start_date then
    raise exception 'Matches end date must be after the matches start date';
  end if;

  if age(new.matches_end_date, new.matches_start_date) > interval '10 months' then
    raise exception 'Matches window cannot be more than 10 months long';
  end if;

  return new;
end;
$$;

create trigger on_user_league_matches_dates
  before insert or update of matches_start_date, matches_end_date on public.user_leagues
  for each row
  execute function public.enforce_user_league_matches_dates();

alter table public.user_leagues
  alter column matches_start_date set not null,
  alter column matches_end_date set not null;

-- Ranking calc now scores off the matches window, not the league lifecycle
-- dates.
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
  select matches_start_date, matches_end_date into v_start, v_end
  from public.user_leagues
  where id = p_user_league_id;

  if not found then
    return;
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
          (case when mp.id is null then 'D'
                when mp.isdraw then 'D'
                when mp.win_team = m.home_id then 'H'
                when mp.win_team = m.away_id then 'A'
                else 'D' end)
          =
          (case when m.isdraw then 'D'
                when m.win_team = m.home_id then 'H'
                when m.win_team = m.away_id then 'A'
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

-- Recalc now fires on the matches window changing, not start_date/end_date
-- (which no longer affect scoring at all).
drop trigger if exists on_user_league_dates_changed_recalc on public.user_leagues;

create or replace function public.trigger_recalc_on_user_league_matches_dates_changed()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if old.matches_start_date is distinct from new.matches_start_date
     or old.matches_end_date is distinct from new.matches_end_date then
    perform public.recalculate_user_league_rankings(new.id);
  end if;
  return new;
end;
$$;

create trigger on_user_league_matches_dates_changed_recalc
  after update of matches_start_date, matches_end_date on public.user_leagues
  for each row
  execute function public.trigger_recalc_on_user_league_matches_dates_changed();

commit;

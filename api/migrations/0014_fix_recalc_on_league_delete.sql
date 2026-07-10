-- Deleting a user_leagues row cascades to user_league_members and
-- user_league_leagues, which fire their AFTER DELETE recalc triggers — but by
-- then the parent user_leagues row is already gone (same statement), so
-- recalculate_user_league_rankings() raised "does not exist" instead of
-- being a harmless no-op. Found live while cleaning up test data.

\set ON_ERROR_STOP on

begin;

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
    -- The league itself may already be gone (e.g. cascade-delete triggered
    -- this via a member/league-selection row being removed alongside it) —
    -- nothing to recalculate.
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

commit;

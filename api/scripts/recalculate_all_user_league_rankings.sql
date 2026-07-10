-- Cron script: recompute rankings for every active user league. The trigger
-- coverage (date/league-selection/membership changes) keeps things fresh
-- in between runs; this is the periodic catch-all for newly-settled matches.
do $$
declare
  r record;
begin
  for r in select id from public.user_leagues where status = 'active' loop
    perform public.recalculate_user_league_rankings(r.id);
  end loop;
end $$;

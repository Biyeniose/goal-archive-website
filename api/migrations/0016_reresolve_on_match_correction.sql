-- If a played match's result-determining fields get corrected after
-- predictions for it were already settled, those predictions and any
-- user-league rankings that used them are now stale. This re-opens already-
-- settled predictions for the match (settle_match_predictions() re-grades
-- them on its next run, same manual-cron cadence as before — this does NOT
-- auto-settle) and immediately recalculates any user league whose date
-- range + selected public.leagues cover the match (that recalc is cheap and
-- already idempotent, so doing it immediately here is safe).
--
-- Scope: fires only when the match is (or remains) isplayed = true and one
-- of home_goals/away_goals/win_team/loss_team/isdraw actually changed. A
-- match transitioning to isplayed = true for the first time is also covered
-- (harmless no-op on the "reset settled predictions" side since nothing is
-- settled yet, but it does mean user-league rankings pick up a newly-final
-- score immediately instead of waiting for the next cron tick).

\set ON_ERROR_STOP on

begin;

create or replace function public.handle_match_result_correction()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if new.isplayed is distinct from true then
    return new;
  end if;

  if (old.home_goals is distinct from new.home_goals)
     or (old.away_goals is distinct from new.away_goals)
     or (old.win_team is distinct from new.win_team)
     or (old.loss_team is distinct from new.loss_team)
     or (old.isdraw is distinct from new.isdraw)
  then
    update match_predictions
    set is_settled = false,
        correct = null
    where match_id = new.match_id
      and is_settled = true;

    perform public.recalculate_user_league_rankings(ul.id)
    from user_leagues ul
    join user_league_leagues ull on ull.user_league_id = ul.id
    join competitions c on c.league_id = ull.league_id
    where c.competition_id = new.comp_id
      and new.match_date between ul.start_date and ul.end_date;
  end if;

  return new;
end;
$$;

drop trigger if exists on_match_result_correction on public.matches;
create trigger on_match_result_correction
  after update of home_goals, away_goals, win_team, loss_team, isdraw, isplayed on public.matches
  for each row
  execute function public.handle_match_result_correction();

commit;

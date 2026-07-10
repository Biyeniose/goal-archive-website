-- Allows a user to delete their own prediction, under the exact same lock
-- window as editing (not played, not live, more than 10 minutes before
-- kickoff) — enforced via a BEFORE DELETE trigger since the backend talks to
-- Postgres with the service-role key and bypasses RLS; RLS policy/grant below
-- are defense-in-depth only, same as the existing insert/update policies.

\set ON_ERROR_STOP on

begin;

create or replace function public.enforce_match_prediction_delete_lock()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  m record;
begin
  select match_time_utc, isplayed, is_live
  into m
  from matches
  where match_id = old.match_id;

  if not found then
    return old;
  end if;

  if m.isplayed then
    raise exception 'Match % has already been played', old.match_id;
  end if;

  if m.is_live then
    raise exception 'Match % is currently live', old.match_id;
  end if;

  if m.match_time_utc is not null and now() >= m.match_time_utc - interval '10 minutes' then
    raise exception 'Predictions for match % are locked (kickoff is within 10 minutes)', old.match_id;
  end if;

  return old;
end;
$$;

drop trigger if exists on_match_prediction_delete on public.match_predictions;
create trigger on_match_prediction_delete
  before delete on public.match_predictions
  for each row
  execute function public.enforce_match_prediction_delete_lock();

drop policy if exists match_predictions_owner_delete on public.match_predictions;
create policy match_predictions_owner_delete
  on public.match_predictions
  for delete
  using (auth.uid() = profile_id);

grant delete on public.match_predictions to authenticated;

commit;

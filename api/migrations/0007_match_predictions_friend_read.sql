-- Adds friend-visibility to match_predictions (full current state) and
-- match_prediction_history (last 2 versions only, per the original spec's
-- privacy scope). These are additive PERMISSIVE policies alongside the
-- existing owner-only ones — Postgres OR's them together, so this only
-- widens access, never narrows the owner's own.

\set ON_ERROR_STOP on

begin;

create policy match_predictions_friend_read
  on public.match_predictions
  for select
  using (public.are_friends(auth.uid(), profile_id));

-- A subquery against match_prediction_history from within its own RLS policy
-- re-triggers that same policy recursively. Routing the "latest version"
-- lookup through a security definer function (owned by the table owner, so
-- it's exempt from the table's own RLS internally) avoids the recursion.
create or replace function public.match_prediction_latest_version(p_prediction_id bigint)
returns integer
language sql
stable
security definer
set search_path = public
as $$
  select max(version) from public.match_prediction_history where prediction_id = p_prediction_id;
$$;

grant execute on function public.match_prediction_latest_version(bigint) to authenticated, service_role;

create policy match_prediction_history_friend_read
  on public.match_prediction_history
  for select
  using (
    public.are_friends(auth.uid(), profile_id)
    and version >= public.match_prediction_latest_version(prediction_id) - 1
  );

commit;

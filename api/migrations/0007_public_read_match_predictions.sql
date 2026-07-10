-- match_predictions was owner-only read; opening it up so any signed-in
-- user can view anyone's picks. Write access (insert/update, both already
-- scoped to auth.uid() = profile_id) is untouched — only the person who
-- made a prediction can change it.
--
-- Scoped to `authenticated`, not `anon`: the existing GRANT on this table
-- only covers `authenticated` (no SELECT grant to anon), so this policy
-- alone doesn't make predictions visible to signed-out visitors.
--
-- match_prediction_history (the edit-audit log) is left owner-only —
-- wasn't asked to be public, and is a different thing from the pick itself.

\set ON_ERROR_STOP on

begin;

drop policy if exists match_predictions_owner_read on public.match_predictions;
-- Superseded by the unconditional read policy below — kept from an earlier
-- pass that added friend-based visibility before this request replaced it.
drop policy if exists match_predictions_friend_read on public.match_predictions;

create policy match_predictions_read
  on public.match_predictions
  for select
  using (true);

commit;

-- Run this yourself, as often as you like throughout the day, to grade
-- predictions for matches that have finished since the last run. Nothing
-- runs this automatically — see api/migrations/0005_create_match_predictions.sql
-- for why (settling only touches matches.isplayed = true, which conflicts
-- with the prediction-lock trigger if it were allowed to fire automatically
-- on every matches update).
--
-- Safe to run repeatedly: settle_match_predictions() only touches rows
-- where is_settled = false, so already-graded predictions are left alone.
--
-- Usage:
--   psql "$SUPABASE_DB_URL" -f api/scripts/settle_predictions.sql
-- or, from this project's usual docker-based workflow:
--   docker run --rm postgres:16 psql "<connection string>" -f - < api/scripts/settle_predictions.sql

select settle_match_predictions() as newly_settled_count;

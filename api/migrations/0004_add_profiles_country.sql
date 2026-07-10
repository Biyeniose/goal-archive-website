-- Adds the user's country to profiles, matching the FK naming convention
-- used everywhere else in the schema (teams.country_id, players.country_id,
-- etc.) rather than a bare "country" column.

\set ON_ERROR_STOP on

begin;

alter table public.profiles
  add column if not exists country_id integer references public.countries(country_id) on delete set null;

commit;

-- User league names must be unique (globally, same as profiles.username).

\set ON_ERROR_STOP on

begin;

alter table public.user_leagues
  add constraint user_leagues_name_key unique (name);

commit;

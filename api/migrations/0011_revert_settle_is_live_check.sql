-- Reverting 0010: is_live = false is back as a settlement requirement. The
-- caller will keep the data feed's is_live flag accurate instead of having
-- settle_match_predictions() paper over a stuck/stale value.

\set ON_ERROR_STOP on

begin;

create or replace function public.settle_match_predictions()
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  settled_count integer;
begin
  update match_predictions mp
  set is_settled = true,
      correct = (mp.isdraw = m.isdraw and (m.isdraw or mp.win_team = m.win_team)),
      updated_at = now()
  from matches m
  where mp.match_id = m.match_id
    and mp.is_settled = false
    and m.isplayed = true
    and m.is_live = false
    and m.home_goals is not null
    and m.away_goals is not null
    and m.isdraw is not null
    and (m.isdraw or (m.win_team is not null and m.loss_team is not null));

  get diagnostics settled_count = row_count;
  return settled_count;
end;
$$;

commit;

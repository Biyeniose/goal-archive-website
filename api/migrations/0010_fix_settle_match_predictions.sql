-- settle_match_predictions() required matches.is_live = false, but the data
-- feed can leave is_live stuck true even after isplayed flips to true (seen
-- live on match_id 651651 — kicked off hours earlier, isplayed = true, but
-- is_live never cleared). isplayed is the authoritative "match is over"
-- signal; is_live is a live-ticker convenience field that can lag behind it,
-- so requiring both was blocking legitimate settlement. Drop the is_live
-- check.

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
    and m.home_goals is not null
    and m.away_goals is not null
    and m.isdraw is not null
    and (m.isdraw or (m.win_team is not null and m.loss_team is not null));

  get diagnostics settled_count = row_count;
  return settled_count;
end;
$$;

commit;

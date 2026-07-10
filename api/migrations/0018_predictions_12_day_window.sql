-- Predictions can now only be made for matches kicking off within the next
-- 12 days (in addition to the existing 10-minutes-before-kickoff lock).

\set ON_ERROR_STOP on

begin;

create or replace function public.enforce_and_derive_match_prediction()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  m record;
  v_days_until numeric;
begin
  select match_time_utc, tfm_url, isplayed, is_live, home_id, away_id, round
  into m
  from matches
  where match_id = new.match_id;

  if not found then
    raise exception 'Match % does not exist', new.match_id;
  end if;

  if m.match_time_utc is null then
    raise exception 'Match % has no scheduled kickoff time and is not open for predictions', new.match_id;
  end if;

  if m.tfm_url is null then
    raise exception 'Match % is not eligible for predictions', new.match_id;
  end if;

  if m.isplayed then
    raise exception 'Match % has already been played', new.match_id;
  end if;

  if m.is_live then
    raise exception 'Match % is currently live', new.match_id;
  end if;

  v_days_until := extract(epoch from (m.match_time_utc - now())) / 86400;
  if v_days_until > 12 then
    raise exception 'Match is in % days, exceeds 12 day window for predictions', round(v_days_until);
  end if;

  if now() >= m.match_time_utc - interval '10 minutes' then
    raise exception 'Predictions for match % are locked (kickoff is within 10 minutes)', new.match_id;
  end if;

  if new.home_goals = new.away_goals then
    if new.pens_winner is not null then
      if not public.round_can_go_to_pens(m.round) then
        raise exception 'Match % cannot go to penalties', new.match_id;
      end if;
      if new.pens_winner not in (m.home_id, m.away_id) then
        raise exception 'pens_winner must be one of the two teams in match %', new.match_id;
      end if;
    end if;
  else
    if new.pens_winner is not null then
      raise exception 'pens_winner can only be set when the predicted score is a draw';
    end if;
  end if;

  if new.home_goals > new.away_goals then
    new.win_team := m.home_id;
    new.loss_team := m.away_id;
    new.isdraw := false;
  elsif new.away_goals > new.home_goals then
    new.win_team := m.away_id;
    new.loss_team := m.home_id;
    new.isdraw := false;
  elsif new.pens_winner is not null then
    new.win_team := new.pens_winner;
    new.loss_team := case when new.pens_winner = m.home_id then m.away_id else m.home_id end;
    new.isdraw := false;
  else
    new.win_team := null;
    new.loss_team := null;
    new.isdraw := true;
  end if;

  -- A genuine new guess always resets settlement state.
  new.is_settled := false;
  new.correct := null;
  new.updated_at := now();

  return new;
end;
$$;

commit;

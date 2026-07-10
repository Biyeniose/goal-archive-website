-- Penalty-shootout winner pick for matches that can go to pens. Eligibility
-- is derived from matches.round (no new column — this is a "for now"
-- heuristic the caller said they'll refine later):
--   - round is purely numeric (e.g. "1", "14")        -> domestic league matchday, no pens
--   - round contains "1st Leg"                        -> first leg of a two-legged tie, no pens yet
--   - round contains "Group"                          -> group stage, no pens
--   - anything else, INCLUDING a null round            -> can go to pens
--
-- round IS NULL defaults to "can go to pens", not the more conservative
-- false: ~23% of unplayed matches in the last 30 days have a null round (a
-- broad, ongoing data gap, not just old backfilled rows), and of the 1363
-- historical matches that actually went to pens, 242 have a null round.
-- Defaulting null to false would silently hide the picker for roughly a
-- quarter of real knockout matches. Verified live: with null -> true, only 1
-- of 1363 real pens=true matches is misclassified (the same pre-existing
-- numeric-round outlier), and 0 real Group/1st-Leg/numeric-round matches are
-- ever incorrectly classified as pens-eligible.
--
-- pens_winner only makes sense when the user predicts a drawn scoreline —
-- it's who they think wins the shootout, not a score. When set, it's folded
-- into the same win_team/loss_team/isdraw derivation used for normal
-- predictions, because matches.win_team/isdraw already encode the *actual*
-- penalty winner (home_goals/away_goals stay the drawn regulation score,
-- but isdraw=false and win_team=<shootout winner> — confirmed against real
-- pens=true rows). That means the existing correctness comparisons in
-- settle_match_predictions() automatically reward a correct pens pick with
-- no changes needed there. recalculate_user_league_rankings() **is** updated
-- below, because it independently recomputed H/A/D from raw goals rather
-- than from win_team/isdraw, so it would otherwise ignore pens results
-- entirely (both for what the user predicted and for the actual outcome).

\set ON_ERROR_STOP on

begin;

create or replace function public.round_can_go_to_pens(p_round text)
returns boolean
language plpgsql
immutable
as $$
begin
  if btrim(p_round) ~ '^[0-9]+$' then
    return false;
  end if;

  if p_round ~* '1st Leg' then
    return false;
  end if;

  if p_round ~* 'Group' then
    return false;
  end if;

  return true;
end;
$$;

alter table public.match_predictions
  add column pens_winner integer references public.teams(team_id);

create or replace function public.enforce_and_derive_match_prediction()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  m record;
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

create or replace function public.recalculate_user_league_rankings(p_user_league_id bigint)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_start date;
  v_end   date;
begin
  select start_date, end_date into v_start, v_end
  from public.user_leagues
  where id = p_user_league_id;

  if not found then
    return;
  end if;

  delete from public.user_league_rankings where user_league_id = p_user_league_id;

  insert into public.user_league_rankings (user_league_id, profile_id, points, matches_scored)
  select
    p_user_league_id,
    scored.profile_id,
    sum(scored.points),
    count(*)
  from (
    select
      mem.profile_id,
      (
        case when
          -- Predicted outcome: a real prediction's win_team/isdraw already
          -- account for a pens pick (see enforce_and_derive_match_prediction);
          -- a missing prediction defaults to a 1-1 draw.
          (case when mp.id is null then 'D'
                when mp.isdraw then 'D'
                when mp.win_team = m.home_id then 'H'
                when mp.win_team = m.away_id then 'A'
                else 'D' end)
          =
          -- Actual outcome: m.isdraw/m.win_team already reflect the real
          -- penalty-shootout winner when the match went to pens.
          (case when m.isdraw then 'D'
                when m.win_team = m.home_id then 'H'
                when m.win_team = m.away_id then 'A'
                else 'D' end)
        then 3 else 0 end
      )
      + (case when coalesce(mp.home_goals, 1) = m.home_goals then 1 else 0 end)
      + (case when coalesce(mp.away_goals, 1) = m.away_goals then 1 else 0 end)
      + (case when coalesce(mp.home_goals, 1) = m.home_goals
               and coalesce(mp.away_goals, 1) = m.away_goals
              then 1 else 0 end)
      as points
    from public.user_league_members mem
    join public.user_league_leagues ull on ull.user_league_id = p_user_league_id
    join public.competitions c on c.league_id = ull.league_id
    join public.matches m on m.comp_id = c.competition_id
    left join public.match_predictions mp
      on mp.match_id = m.match_id and mp.profile_id = mem.profile_id
    where mem.user_league_id = p_user_league_id
      and m.match_date between v_start and v_end
      and m.isplayed = true
      and m.home_goals is not null
      and m.away_goals is not null
  ) scored
  group by scored.profile_id;

  update public.user_leagues
  set rankings_updated_at = now()
  where id = p_user_league_id;
end;
$$;

commit;

-- Goaliath: separate AI football chat service (goaliath.goal-archive.net).
--
-- goaliath_sessions tracks anonymous visitors via a UUID the frontend
-- generates and stores in localStorage. goaliath_messages logs every chat
-- turn for either a signed-in user (user_id) or an anonymous session
-- (session_id) — exactly one of the two is set per row. Rate limiting itself
-- is enforced in the API, not here; the indexes below just make those
-- rolling-window lookups fast.
--
-- goaliath_game_sessions is future work: transfer-guessing game state, also
-- keyed by either user_id or session_id.

\set ON_ERROR_STOP on

begin;

create table public.goaliath_sessions (
  session_id uuid primary key,
  created_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now()
);

create table public.goaliath_messages (
  id bigint generated always as identity primary key,
  user_id uuid references public.profiles(id) on delete cascade,
  session_id uuid references public.goaliath_sessions(session_id) on delete cascade,
  message_role text not null,
  message_text text not null,
  created_at timestamptz not null default now(),
  constraint goaliath_messages_role_check check (message_role in ('user', 'assistant')),
  constraint goaliath_messages_identity_check check (
    (user_id is not null and session_id is null) or
    (user_id is null and session_id is not null)
  )
);

create index idx_goaliath_messages_user_rate_limit
  on public.goaliath_messages (user_id, message_role, created_at);

create index idx_goaliath_messages_session_rate_limit
  on public.goaliath_messages (session_id, message_role, created_at);

create table public.goaliath_game_sessions (
  id bigint generated always as identity primary key,
  user_id uuid references public.profiles(id) on delete cascade,
  session_id uuid references public.goaliath_sessions(session_id) on delete cascade,
  player_id bigint not null references public.players(player_id),
  transfer_id integer not null references public.transfers(id),
  guesses_made integer not null default 0,
  guesses_remaining integer not null default 4,
  is_won boolean not null default false,
  is_active boolean not null default true,
  started_at timestamptz not null default now(),
  ended_at timestamptz,
  constraint goaliath_game_sessions_identity_check check (
    (user_id is not null and session_id is null) or
    (user_id is null and session_id is not null)
  ),
  constraint goaliath_game_sessions_guesses_remaining_check check (
    guesses_remaining >= 0 and guesses_remaining <= 4
  )
);

create index idx_goaliath_game_sessions_user
  on public.goaliath_game_sessions (user_id, is_active);

create index idx_goaliath_game_sessions_session
  on public.goaliath_game_sessions (session_id, is_active);

commit;

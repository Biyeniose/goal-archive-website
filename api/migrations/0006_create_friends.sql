-- friend_requests tracks the lifecycle (pending/accepted/rejected).
-- friendships holds confirmed mutual pairs only — never written directly,
-- only ever populated/removed by the trigger on friend_requests.status
-- changes. Always stored with the lesser UUID as user_id_1 so a pair can
-- never be duplicated in either order.

\set ON_ERROR_STOP on

begin;

create table public.friend_requests (
  id           serial primary key,
  sender_id    uuid not null references public.profiles(id) on delete cascade,
  receiver_id  uuid not null references public.profiles(id) on delete cascade,
  status       varchar(10) not null default 'pending' check (status in ('pending', 'accepted', 'rejected')),
  created_at   timestamptz not null default now(),
  updated_at   timestamptz not null default now(),
  unique (sender_id, receiver_id),
  check (sender_id != receiver_id)
);

create table public.friendships (
  id          serial primary key,
  user_id_1   uuid not null references public.profiles(id) on delete cascade,
  user_id_2   uuid not null references public.profiles(id) on delete cascade,
  created_at  timestamptz not null default now(),
  unique (user_id_1, user_id_2),
  check (user_id_1 < user_id_2)
);

-- Canonical friendship check, used in RLS policies elsewhere. Handles UUID
-- ordering internally so callers never need to worry about it.
create or replace function public.are_friends(a uuid, b uuid)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select exists (
    select 1 from public.friendships
    where user_id_1 = least(a, b) and user_id_2 = greatest(a, b)
  );
$$;

-- pending -> accepted creates the friendship. accepted -> rejected (unfriend)
-- removes it. Any other transition (e.g. pending -> rejected) is a no-op here.
create or replace function public.handle_friend_request_updated()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if new.status = 'accepted' and old.status is distinct from 'accepted' then
    insert into public.friendships (user_id_1, user_id_2)
    values (least(new.sender_id, new.receiver_id), greatest(new.sender_id, new.receiver_id))
    on conflict (user_id_1, user_id_2) do nothing;
  elsif old.status = 'accepted' and new.status = 'rejected' then
    delete from public.friendships
    where user_id_1 = least(new.sender_id, new.receiver_id)
      and user_id_2 = greatest(new.sender_id, new.receiver_id);
  end if;
  return new;
end;
$$;

drop trigger if exists on_friend_request_updated on public.friend_requests;
create trigger on_friend_request_updated
  after update of status on public.friend_requests
  for each row
  execute function public.handle_friend_request_updated();

drop trigger if exists update_friend_requests_updated_at on public.friend_requests;
create trigger update_friend_requests_updated_at
  before update on public.friend_requests
  for each row
  execute function public.update_updated_at_column();

alter table public.friend_requests enable row level security;
alter table public.friendships enable row level security;

create policy friend_requests_read
  on public.friend_requests
  for select
  using (auth.uid() = sender_id or auth.uid() = receiver_id);

create policy friend_requests_send
  on public.friend_requests
  for insert
  with check (auth.uid() = sender_id);

create policy friend_requests_respond
  on public.friend_requests
  for update
  using (auth.uid() = receiver_id);

create policy friend_requests_withdraw
  on public.friend_requests
  for delete
  using (auth.uid() = sender_id);

create policy friendships_read
  on public.friendships
  for select
  using (auth.uid() = user_id_1 or auth.uid() = user_id_2);

grant select, insert, update, delete on public.friend_requests to authenticated;
grant select on public.friendships to authenticated;
grant usage on sequence public.friend_requests_id_seq to authenticated, service_role;
grant usage on sequence public.friendships_id_seq to service_role;
grant all on public.friend_requests to service_role;
grant all on public.friendships to service_role;
grant execute on function public.are_friends(uuid, uuid) to authenticated, service_role, anon;

commit;

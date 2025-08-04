create schema if not exists fluxbbredir;
set search_path to fluxbbredir;

create table posts (
  id serial primary key,
  fluxbb_topic_id integer not null,
  fluxbb_post_id integer not null,
  discourse_topic_id integer not null,
  discourse_topic_post_number integer not null
);
create index posts_topic_idx on posts (fluxbb_topic_id);
create unique index posts_post_idx on posts (fluxbb_post_id);

create table users (
  id serial primary key,
  fluxbb_user_id integer not null,
  discourse_username text not null
);
create unique index users_idx on users (fluxbb_user_id);

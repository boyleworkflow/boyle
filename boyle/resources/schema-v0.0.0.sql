PRAGMA foreign_keys = ON;

-- state of task when run
-- create table task (
--   task_id text primary key,
--   definition text,
--   sysstate text
-- );

create table user (
  user_id text primary key,
  name text
);

create table calc (
  calc_id text primary key, -- id based on task and inputs
  task_id text,
  foreign key(task_id) references task(task_id) DEFERRABLE INITIALLY DEFERRED
);

create table input (
  calc_id text,
  uri text,
  digest text,
  foreign key(calc_id) references calc(calc_id) DEFERRABLE INITIALLY DEFERRED,
  primary key (calc_id, uri)
);

create table def (
  def_id text primary key,
  calc_id text,
  uri text,
  foreign key(calc_id) references calc(calc_id) DEFERRABLE INITIALLY DEFERRED
);

create table parent (
  def_id text,
  parent_id text,
  foreign key(def_id) references def(def_id) DEFERRABLE INITIALLY DEFERRED,
  foreign key(parent_id) references def(def_id) DEFERRABLE INITIALLY DEFERRED,
  primary key (def_id, parent_id)
);

create table trust (
  calc_id text,
  uri text,
  digest text,
  user_id text,
  -- time timestamp with time zone,
  correct boolean,
  foreign key(calc_id) references calc(calc_id) DEFERRABLE INITIALLY DEFERRED,
  foreign key(user_id) references user(user_id) DEFERRABLE INITIALLY DEFERRED,
  primary key (calc_id, uri, digest, user_id) --, time)
);

create table run (
  run_id text primary key,
  calc_id text,
  user_id text,
  -- info text,
  start_time timestamp with time zone,
  end_time timestamp with time zone,
  foreign key(user_id) references user(user_id) DEFERRABLE INITIALLY DEFERRED,
  foreign key(calc_id) references calc(calc_id) DEFERRABLE INITIALLY DEFERRED
);
create index run_calc on run (calc_id);

create table result (
  run_id text,
  uri text,
  digest text,
  foreign key(run_id) references run(run_id) DEFERRABLE INITIALLY DEFERRED,
  primary key (run_id, uri, digest)
);

create table requested (
  def_id text,
  digest text,
  user_id text,
  first_time timestamp with time zone,
  foreign key(user_id) references user(user_id) DEFERRABLE INITIALLY DEFERRED,
  foreign key(def_id) references def(def_id) DEFERRABLE INITIALLY DEFERRED,
  primary key (def_id, digest, user_id)
);
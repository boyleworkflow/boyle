-- state of task when run
create table task (
  task_id text primary key,
  definition text,
  sysstate text
);

create table calculation (
  calc_id text primary key, -- id based on task and inputs
  task_id text references task
);

create table composition (
  comp_id text primary key,
  calc_id text references calculation
);

create table subcomposition (
  comp_id text references composition,
  subcomp_id text references composition,
  primary key (comp_id, subcomp_id)
);

create table trust (
  path text,
  digest text,
  calc_id text references calculation,
  user_id text references user,
  time timestamp with time zone,
  correct boolean,
  primary key (path, digest, calc_id, user_id, time)
);

create table user (
  user_id text primary key,
  name text
);

create table run (
  run_id text primary key,
  user_id text references user,
  info text,
  time timestamp with time zone,
  calc_id text references calculation
);
create index run_calculation on run (calc_id);

create table created (
  run_id text references run,
  path text,
  digest text,
  primary key (run_id, path, digest)
);

create table uses (
  calc_id text references calculation,
  path text,
  digest text,
  primary key (calc_id, path, digest)
);

create table requested (
  path text,
  digest text,
  user_id text references user,
  firsttime timestamp with time zone,
  comp_id text references composition,
  primary key (path, digest, user_id, comp_id)
);
PRAGMA foreign_keys = ON;

-- state of task when run
create table task (
  task_id text primary key,
  definition text,
  sysstate text
);

create table calculation (
  calc_id text primary key, -- id based on task and inputs
  task_id text,
  foreign key(task_id) references task(task_id) DEFERRABLE INITIALLY DEFERRED
);

create table composition (
  comp_id text primary key,
  calc_id text,
  foreign key(calc_id) references calculation(calc_id) DEFERRABLE INITIALLY DEFERRED
);

create table subcomposition (
  comp_id text,
  subcomp_id text,
  foreign key(comp_id) references composition(comp_id) DEFERRABLE INITIALLY DEFERRED,
  foreign key(subcomp_id) references composition(comp_id) DEFERRABLE INITIALLY DEFERRED,
  primary key (comp_id, subcomp_id)
);

create table trust (
  path text,
  digest text,
  calc_id text,
  user_id text,
  time timestamp with time zone,
  correct boolean,
  foreign key(calc_id) references calculation(calc_id) DEFERRABLE INITIALLY DEFERRED,
  foreign key(user_id) references user(user_id) DEFERRABLE INITIALLY DEFERRED,
  primary key (path, digest, calc_id, user_id, time)
);

create table user (
  user_id text primary key,
  name text
);

create table run (
  run_id text primary key,
  user_id text,
  info text,
  time timestamp with time zone,
  calc_id text,
  foreign key(user_id) references user(user_id) DEFERRABLE INITIALLY DEFERRED,
  foreign key(calc_id) references calculation(calc_id) DEFERRABLE INITIALLY DEFERRED
);
create index run_calculation on run (calc_id);

create table created (
  run_id text,
  path text,
  digest text,
  foreign key(run_id) references run(run_id) DEFERRABLE INITIALLY DEFERRED,
  primary key (run_id, path, digest)
);

create table uses (
  calc_id text,
  path text,
  digest text,
  foreign key(calc_id) references calculation(calc_id) DEFERRABLE INITIALLY DEFERRED,
  primary key (calc_id, path, digest)
);

create table requested (
  path text,
  digest text,
  user_id text,
  firsttime timestamp with time zone,
  comp_id text,
  foreign key(user_id) references user(user_id) DEFERRABLE INITIALLY DEFERRED,
  foreign key(comp_id) references composition(comp_id) DEFERRABLE INITIALLY DEFERRED,
  primary key (path, digest, user_id, comp_id)
);
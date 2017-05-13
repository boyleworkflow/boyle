PRAGMA foreign_keys = ON;

-- -- state of rule when run
-- create table rule (
--   rule_id text primary key,
--   definition text,
--   sysstate text
-- );

create table user (
  user_id text primary key,
  name text
);

create table op (
  op_id text primary key, -- id based on definition
  definition text
);

create table calc (
  calc_id text primary key, -- id based on task and inputs
  op_id text,
  foreign key(op_id) references op(op_id)
);

-- An input to a calculation, i.e., a (Calc, Resource) pair
create table input (
  calc_id text,
  loc text,
  digest text,
  foreign key(calc_id) references calc(calc_id),
  primary key (calc_id, loc)
);

-- composition
create table comp (
  comp_id text primary key,
  op_id text,
  loc text,
  foreign key(op_id) references op(op_id)
);

-- Parent of a composition, i.e., a pair (Comp child, Comp parent)
create table parent (
  comp_id text,
  parent_id text,
  foreign key(comp_id) references comp(comp_id),
  foreign key(parent_id) references comp(comp_id),
  primary key (comp_id, parent_id)
);

create table trust (
  calc_id text,
  loc text,
  digest text,
  user_id text,
  -- time timestamp,
  opinion boolean,
  foreign key(calc_id) references calc(calc_id),
  foreign key(user_id) references user(user_id),
  primary key (calc_id, loc, digest, user_id) --, time)
);

create table run (
  run_id text primary key,
  calc_id text,
  user_id text,
  -- info text,
  start_time timestamp,
  end_time timestamp,
  foreign key(user_id) references user(user_id),
  foreign key(calc_id) references calc(calc_id)
);
create index run_calc on run (calc_id);

-- A result created by a run, i.e., a (Resource, Run) pair
create table result (
  run_id text,
  loc text,
  digest text,
  foreign key(run_id) references run(run_id),
  primary key (run_id, loc)
);

-- A resource that was the response to a request of a composition,
-- i.e., a record of which results have been received by different users.
create table response (
  comp_id text,
  digest text,
  user_id text,
  first_time timestamp,
  foreign key(user_id) references user(user_id),
  foreign key(comp_id) references comp(comp_id),
  primary key (comp_id, digest, user_id)
);

PRAGMA foreign_keys = ON;

create table op (
  op_id text primary key, -- id based on definition
  definition text
);

create table calc (
  calc_id text primary key, -- id based on op and inputs
  op_id text,
  foreign key(op_id) references op(op_id)
);

-- An input to a calculation, i.e., a (Calc, Result) pair
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

-- An input to a composition, i.e., a tuple (Comp child, Comp parent)
create table parent (
  comp_id text,
  parent_comp_id text,
  foreign key(comp_id) references comp(comp_id),
  foreign key(parent_comp_id) references comp(comp_id),
  primary key (comp_id, parent_comp_id)
);

create table trust (
  calc_id text,
  loc text,
  digest text,
  opinion boolean,
  foreign key(calc_id) references calc(calc_id),
  primary key (calc_id, loc, digest) --, time)
);

create table run (
  run_id text primary key,
  calc_id text,
  -- info text,
  start_time timestamp,
  end_time timestamp,
  foreign key(calc_id) references calc(calc_id)
);
create index run_calc on run (calc_id);

-- A result created by a run, i.e., a (Run, Resource) pair
create table result (
  run_id text,
  loc text,
  digest text,
  foreign key(run_id) references run(run_id),
  primary key (run_id, loc)
);

-- A resource that was the response to a request of a composition,
-- i.e., a record of which results have been received.
create table response (
  comp_id text,
  digest text,
  first_time timestamp,
  foreign key(comp_id) references comp(comp_id),
  primary key (comp_id, digest)
);

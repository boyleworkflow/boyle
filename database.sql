-- drop database if exists gpc;
-- create database gpc;

drop table if exists depended;
drop table if exists uses;
drop index if exists trust_run_path;
drop table if exists trust;
drop table if exists created;
drop index if exists run_calculation;
drop table if exists run;
drop table if exists usr;
drop table if exists calculation;
drop table if exists task;

-- state of task when run
create table task (
  id text primary key,
  definition text,
  sysstate text
);

create table calculation (
  id text primary key, -- id based on task and inputs
  task text references task
);

create table fso (
  path text,
  digest text,
  primary key (path, digest)
);
-- also index the result table?

create table output (
  calculation text references calculation,
  fso text references fso,
  primary key (calculation, fso)
);

create table composition (
  id text primary key,
  calculation text references calculation
);

create table input (
  composition text references composition,
  inputcomposition text references composition,
  primary key (composition, inputcomposition)
);

create table trust (
  output text references output,
  usr text references usr,
  time timestamp with time zone,
  correct boolean,
  primary key (fso, usr, time)
);

create table usr (
  id text primary key,
  name text
);

-- log of each run of a task
create table run (
  id text primary key,
  usr text references usr,
  info text,
  time timestamp with time zone,
  composition text references composition
);
create index run_calculation on run (calculation);

create table created (
  run text references run,
  output text references output,
  primary key (run, output)
);

create table uses (
  calculation text references calculation,
  fso text references fso,
  primary key (calculation, fso)
);

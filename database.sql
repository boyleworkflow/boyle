-- drop database if exists gpc;
-- create database gpc;

drop table if exists depended;
drop table if exists used;
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
  id text primary key, -- mostly because we want a simple way to reference results
  calculation text references calculation,
  path text,
  digest text
);
-- also index the result table?

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
  fso text references fso,
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
  calculation text references calculation
);
create index run_calculation on run (calculation);

create table runresult (
  run text references run,
  fso text references fso,
  primary key (run, fso)
);

create table compositionresult (
  composition text references composition,
  fso text references fso,
  primary key (composition, fso)
);

create table used {
  calculation text references calculation,
  fso text references fso,
  primary key (calculation, fso)
};

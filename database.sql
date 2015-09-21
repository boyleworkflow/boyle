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
  id text primary key,
  task text references task
);

create table result (
  calculation text references calculation,
  path text,
  digest text,
  primary key (calculation, path, digest)
);
-- this table is in practice redundant since the key contains all columns

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
  calculation text,
  path text,
  digest,
  usr text references usr,
  time timestamp with time zone,
  correct boolean,
  constraint fk foreign key (calculation, path, digest) references result
);
-- is this the right index to use?
create index trust_run_path on trust (calculation, path, digest);

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

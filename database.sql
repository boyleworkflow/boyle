-- drop database if exists gpc;
-- create database gpc;

drop table if exists runrun;
drop table if exists runout;
drop table if exists run;
drop table if exists fso;

create table run (
  id text primary key,
  usr text,
  task text references task
  -- info
  -- timestamp
);

create table task (
  id text primary key,
  define text,
  sysstate text
);

create table fso (
  id text primary key,
  path text,
  digest text
);

create table runout (
  run text references run,
  fso text references fso,
  correct boolean, -- move to own action table
  primary key (run, fso)
);

-- create table runin (
--   run text,
--   fso text,
--   primary key (run, fso)
-- );

create table runrun (
  run text references run,
  inputrun text references run,
  primary key (run, inputrun)
);

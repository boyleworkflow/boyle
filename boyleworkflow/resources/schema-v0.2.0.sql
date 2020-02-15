PRAGMA foreign_keys = ON;

create table op (
  op_id text primary key not null,
  definition json not null,
) without rowid;

create table node (
  node_id text primary key not null,
  op_id text not null,
  index_node_id text not null,
  foreign key (index_node_id) references node(node_id),
  foreign key (op_id) references op(op_id)
) without rowid;

create table defn (
  node_id text not null,
  loc text not null,
  foreign key (node_id) references node(node_id),
  primary key (node_id, loc)
) without rowid;

create table node_input (
  node_id text not null,
  loc text not null,
  parent_node_id text not null,
  parent_loc text not null,
  foreign key (node_id) references defn(node_id),
  foreign key (parent_node_id, parent_loc) references defn(node_id, loc),
  primary key (node_id, loc)
) without rowid;

create table index_result (
  digest primary key not null,
  data json not null,
  foreign key (digest) references calc_result(digest)
) without rowid;

create table defn_result (
  node_id text not null,
  loc text not null,
  index_digest text default null,
  iloc int default null,
  explicit boolean not null,
  result_digest text not null,
  first_time timestamp not null,
  foreign key (node_id, loc) references defn(node_id, loc),
  foreign key (index_digest) references index_result(digest),
  foreign key (result_digest) references calc_result(digest),
  primary key (node_id, loc, index_digest, iloc, explicit, result_digest)
) without rowid;

create table calc (
  calc_id text primary key not null,
  op_id text not null,
  foreign key (op_id) references op(op_id)
) without rowid;

create table calc_input (
  calc_id text not null,
  loc text not null,
  digest text not null,
  foreign key (calc_id) references calc(calc_id),
  primary key (calc_id, loc)
) without rowid;

create table run (
  run_id text primary key not null,
  calc_id text not null,
  start_time timestamp not null,
  end_time timestamp not null,
  foreign key (calc_id) references calc(calc_id)
) without rowid;
create index run_calc on run (calc_id);

create table calc_result (
  run_id text not null,
  loc text not null,
  digest text not null,
  foreign key (run_id) references run(run_id),
  primary key (run_id, loc)
) without rowid;

create table trust (
  calc_id text not null,
  loc text not null,
  digest text not null,
  opinion boolean not null,
  foreign key (calc_id) references calc(calc_id),
  primary key (calc_id, loc, digest)
) without rowid;

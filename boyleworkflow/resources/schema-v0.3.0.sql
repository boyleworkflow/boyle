PRAGMA foreign_keys = ON;

create table tree_item (
  tree_id text not null,
  path_segment text not null,
  item_type text not null, -- tree or blob
  item_id text not null, -- the tree_id or blob_id of the item
  primary key (tree_id, path_segment)
) without rowid;

create table calc (
  calc_id text primary key not null,
  input_tree_id not null,
  op json not null
  foreign key input_tree_id references tree_item(tree_id),
) without rowid;

create table run (
  run_id text primary key not null,
  calc_id text not null,
  start_time timestamp not null,
  end_time timestamp not null,
  foreign key calc_id references calc(calc_id)
) without rowid;

-- Each result of a calculation is a file or dir placed at a given path.
create table result (
  run_id text not null,
  result_path text not null,
  tree_item_type text not null,
  tree_item_id text not null,
  trusted boolean,
  foreign key run_id references run(run_id),
  primary key (run_id, result_path)
) without rowid;

create table provenance (
  workflow_blob_id text not null,
  workflow_input_tree_id text not null,
  target_key text not null,
  request_time timestamp not null,
  calc_id text not null,
  result_path text not null,
  tree_item_type text not null,
  tree_item_id text not null,
  foreign key workflow_input_tree_id references tree_item(tree_id),
  foreign key calc_id references calc_item(calc_id),
  primary key (workflow_blob_id, workflow_input_tree_id, target_key, request_time)
) without rowid;

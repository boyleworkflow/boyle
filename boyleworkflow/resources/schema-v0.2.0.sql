PRAGMA foreign_keys = ON;

create table tree (
  tree_id text primary key not null
) without rowid;

create table tree_item (
  tree_id text not null,
  type text not null, -- tree or blob
  path_segment text not null,
  item_id text not null, -- the tree_id or blob_id of the item
  foreign key (tree_id) references tree(tree_id),
  primary key (tree_id, path_segment, item_id)
) without rowid;

create table op (
  op_id text primary key not null,
  definition json not null
) without rowid;

create table run (
  run_id text primary key not null,
  op_id text not null,
  input_tree_id text not null,
  output_tree_id text not null,
  start_time timestamp not null,
  end_time timestamp not null,
  foreign key op_id references op(op_id)
) without rowid;

create table trust (
  run_id text primary key not null,
  opinion boolean not null,
  foreign key (run_id) references run(run_id)
) without rowid;

create table node (
  node_id text primary key not null,
  op_id text not null,
  depth int not null,
  foreign key (op_id) references op(op_id)
) without rowid;

create table node_input (
  node_id text not null,
  loc text not null,
  parent_node_id text not null,
  parent_loc text not null,
  foreign key (node_id) references node(node_id),
  foreign key (parent_node_id) references node(node_id),
  primary key (node_id, loc)
);

create table defn (
  node_id text not null,
  loc text not null,
  foreign key (node_id) references node(node_id),
  primary key (node_id, loc)
) without rowid;

create table node_result (
  node_id text not null,
  output_tree_id text not null,
  explicit text not null,
  first_time datetime not null,
  foreign key (node_id) references node(node_id),
  primary key (node_id, output_tree_id, explicit)
) without rowid;

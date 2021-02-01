create table run (
    run_id text not null unique primary key
);

create table run_result (
    run_id text not null references run(run_id),
    calc_out_id text not null,
    tree_id text not null,
    primary key (run_id, calc_out_id)
);

create table tree (
    tree_id text not null unique primary key,
    data_ text
);

create table tree_child (
    parent_tree_id text not null references tree (tree_id),
    name_ text not null,
    tree_id text not null references tree (tree_id),
    primary key (parent_tree_id, name_, tree_id)
)

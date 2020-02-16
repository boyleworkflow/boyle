# Ideas

## Resource

A Resource is an abstract chunk of data. A resource can be a Blob or a Tree.

## Tree

A Tree is similar to a git tree, a recursive data structure mapping path segments to hashes that represent items in the tree. The items are Resources (Trees or Blobs). So this is an abstract way to represent something like a directory structure. But could also represent, e.g., contents in an s3 bucket.

## Location (Loc)

A Loc is an abstract location of a Resource. It could be a URI, but to start with Locs will simply be relative paths, interpreted as relative to the working directory on the local machine.

## Operation (Op)

The Op specifies some work to be done on an input Tree.

## Calculation (Calc)

A Calc is just a pair (Op, Tree), specifying an Op to be applied to a Tree.

## Definition (Defn)

A Defn is a recipe to create a Resource. Each Defn belongs to a Node; it can be seen as an "output terminal" of a Node.

## Node

A Node is a recipe to create a Tree of outputs. It is defined by the following parts:

* A (possibly empty) mapping of Locs to input Defns, which defines an input Tree to the Node. Each of the Defns recursively resolves to a Resource. When they are all resolved, they can be placed into the given Locs and thus form one Tree which is then used as input to the Node. The reason we use a Loc: Defn mapping and not just a set of Defns is that we want to rename the inputs to suitable names.
* An Op which is applied to the input Tree.
* A set of output Locs, where output is expected after running the Op. Each of the Locs corresponds to a Defn. After running the Calc for a Node, we construct the Node's output tree from the Loc: Resource pairs.
* An operation depth. The depth specifies the number of nested subtrees to descend into before applying the Op. E.g., with depth=0 the Op is run with the whole input Tree as input. With depth=1 the Op is run once with each subtree as input. The Tree therefore must consist only of Trees (no Blobs) down to this depth. Each subtree at the given depth should be a suitable input to the Op.

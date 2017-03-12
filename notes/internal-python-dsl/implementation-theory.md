## Some definitions

Requested is a set given by the user.
Concrete = {d: inp \in Known \forall inp \in parents(d)}
Unknown = Concrete \cap {d: |Log.trusted(calc(d), instrument(d))| = 0}
Known = Concrete \cap {d: |Log.trusted(calc(d), instrument(d))| = 1}
Restorable = Known \cap {d: Storage.can_restore(resource(d))}
Runnable = {d: parents(d) \subseteq Restorable}
RunNeeded = 
	(Requested \cap \compl{Restorable})
	\cup ({d: children(d) \cap RunNeeded \neq \varnothing} \cap \compl{Restorable})
	= (Requested \cup {d: children(d) \cap RunNeeded \neq \varnothing}) \setminus Restorable

Assume that we have a directed acyclic G = (V, E) where all leaf nodes are Requested.

Assume that |Log.trusted(calc(d), instrument(d))| \in {0, 1} \forall d \in V. (Otherwise there is a conflict, which in practice will raise an exception and thus be handled elsewhere.)

## To deliver the resources corresponding to a requested set of definitions

While Requested \nsubseteq Restorable, find and run a nonempty A \subseteq RunNeeded \cap Runnable. The vertices in A will then leave RunNeeded and thus not appear again. So eventually this procedure will have exhausted all of RunNeeded. When this happens, all resources can be restored.

## Algorithm for Known/Unknown/Undecidable

Partitions a set of definitions into {Known, Unknown, Undecidable}. The algorithm guarantees that Unknown = \varnothing \implies Undecidable = \varnothing. In other words, in a set of definitions, at least one will be Known or Unknown. They can not all be Undecidable.

Take C_0 = root nodes

Let K_0 = C_0 \cap Known and U_0 = C_0 \cap Unknown. They will all be decidable since they have no parents, so K_0 \cup U_0 = C_0.

For i = 1, 2, \ldots,

let C_{i+1} = {d: d \in \bigcup_{p\in K_i} children(p) \land parents(d) \subseteq \bigcup_{j=0}^i K_j}, and

K_{i+1} = C_{i+1} \cap Known
U_{i+1} = C_{i+1} \cap Unknown

This way, K_0 \cup K_1 \cup \cdots \cup K_N is the set of known nodes reached in N steps, and U_0 \cup \cdots \cup U_N are the unknown nodes. If C_N = \varnothing then so is K_N and therefore also C_{N+j} = \varnothing for j > 0.

If C_{N+1} = \varnothing, can we possibly have Known \setminus (K_0 \cup \ldots \cup K_N) \neq \varnothing ? Does not seem so, but can we prove it?

## Algorithm for finding nonempty A \subseteq (RunNeeded \cap Runnable)

Begin by coloring the nodes.

Note that Runnable \subseteq \compl{Undecidable} \implies A \subseteq \compl{Undecidable}.

We are done iff Requested \subseteq Restorable, and since Restorable \subseteq Known, we have Requested \subseteq Known if we are done. And Requested \subseteq Known can only hold if all nodes Unknown = \varnothing. And the only way to move an element from Unknown to Known is to run its calculation. Hence Unknown \subseteq RunNeeded. And since \forall d \in Unknown we have descendants(d) \cap Runnable = \varnothing, we also know A \cap descendants(d) = \varnothing. This is just a complicated way of saying we don't have to look below any Unknown node.

If Unknown = \varnothing, all nodes are known, and therefore it can be decided for each node whether it is restorable or not. And we are done exactly if Requested \subseteq Restorable. So Unknown \cup (Requested \cap \compl{Restorable}) = \varnothing \iff done.

Let C_0 = (Requested \cap \compl{Restorable}) \cup Unknown.

First of all, note that C_0 = \varnothing \iff we are done. Also note that C_0 \subseteq RunNeeded.

For i = 1, 2, \ldots, let
C_{i+1} = 
	\bigcup_{d\in C_i} parents(d) \cap \compl{Restorable}
	= \bigcup_{d\in C_i} parents(d) \setminus Restorable.

Assume that C_i \subseteq RunNeeded. Then, for each element d \in C_{i+1} the following will hold:
(1) children(d) \cap RunNeeded \neq \varnothing (because d is a parent of some e \in C_i \subseteq RunNeeded), and
(2) d \notin Restorable (by definition).

(1) and (2) together imply d \in RunNeeded by definition, so C_{i+1} \subseteq RunNeeded if C_i \subseteq RunNeeded.

Hence, C_0 \cup \ldots \cup C_i \subseteq RunNeeded for any i \geq 0.

And finally note that, by definition, C_{i+1} = \varnothing iff \forall d \in C_i we have parents(d) \subseteq Restorable. In plain English, if the C_{i+1} is empty, then at least one element in C_i is runnable. And clearly C_{i+1} will be empty at some point, when a root node of the graph is reached.

So the solution is to take:

C_0 = (Requested \cap \compl{Restorable}) \cup Unknown,

C_{i+1} = \bigcup_{d\in C_i} parents(d) \setminus Restorable for i = 0, 1, 2, \ldots, N such that C_{N+1} = \varnothing,

A = \left( C_0 \cup \ldots \cup C_i \right) \cap Runnable.

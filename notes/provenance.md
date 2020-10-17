# On recording and recalling provenance

2020-10-17

I have previously been convinced that the full provenance graph of each output should be recorded in the log. Now I am not so sure this is worth the effort.

If the Boyle DSL has support for things like multi-level indexing/mapping, fold/reduce operations, non-file parameters, etc., then the provenance information will likely be practically useful if it explicitly encodes all of that. The alternative would be to save the dependency graph in "rolled out" form, e.g., with indexed nodes represented as separate nodes, etc., in which case it would be very difficult to use the provenance information to make sense of the overall workflow.

This leads to some thougths about possible uses for the provenance info:

1. Finding and reproducing the calculation behind a given file.
2. Finding and making sense of the workflow(s) that have been used to produce a given file.
3. Finding the upstream input files behind a given file, most importantly the files used as inputs to the entire workflow (i.e., the set of files necessary to reproduce all the outputs using the given workflow).

For all these aims, it seems sufficient to just save the workflow definition file instead. The only downside I can see with this is that potentially very many similar workflow definition files will have the same outputs...with the result that a "provenance search" could yield very many hits which would need to be inspected manually to discern differences.

But all in all, I currently see no strong reason to just start with the following provenance info. For each output actually requested by the end user (e.g., through `boyle make [target]`), the log contains the following info:

* time of the request
* name of the target
* digest of the result blob/tree
* the workflow definition (e.g., a `Boylefile.py` or whatever)
* a list of input blobs/trees to the workflow

Then, to reproduce the result afterwards, just restore those input files and the workflow definition and rerun `boyle make [target]`. Could not be easier.

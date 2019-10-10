Let's separate the different things that Boyle does:

* Describe contents of a directory
* Maintain/interact with a file cache
* Maintain a log of runs
* Run single calculations
* Create calculations from task descriptions
* Run entire workflows
* etc

Let's define a concrete list of functionality. For now at least I'll think about it in terms of command line operations, similar to the git plumbing commands.

Operation: A specification of the work to be done when all inputs are in place. Concretely it can be thought of as a command to run.

Tree: A specification of directory contents, similar to a git tree.

Calculation = Operation x Tree

To run a Calculation c = (op, tree):

* Use some source of files to create the calculation dir as specified in the tree.
* Go to the calc dir and run the operation op.

## Assumptions

We are always in a boyle project, providing project-specific configuration, a project directory, etc

This also means that the boyle project can have something like a workspace, containing a current task (maybe under construction?), etc

## Concrete functionality: run something as if in the CWD, caching results

Can be described as:
* Setup the calc (place files in a temporary workdir etc)
* Describe the calc (describe tree and operation)
* Run the calc
* Save relevant data (e.g. outputs and metadata about the calc)
* Place the output(s) in the CWD

## Tasks are templates

A task can be thought of as a calc template, specifying:
* Input names
* Operation
* Outputs

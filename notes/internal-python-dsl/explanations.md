# Some loose explanations

## Resource

A resource is an output that results from a calculation. It might be a file, a value, an object in a database, or something else. Implementations of the `Resource` interface represent the expected nature of such resources. But a `Resource` object does *not* represent the actual content of the resource. For example, a `File` instance `File('path/to/file.ext')` basically just means "a file, with any content, placed at `'path/to/file.ext'`". And a `Value` instance called `Value('foo')` just means "a constant value with the symbol `'foo'`. Hence, the concrete meaning, or content, of that object will depend on where and when it is used.

'Resource` instances are used to specify the expected outputs of a calculation. And `Resource` instances are also used to do all the practical handling of the actual, concrete outputs. In fact, the Boyle system should not depend on or have to "know" anything about the concrete implementations of `Resource`.

## Operation

Operations are things that can be done. For example run a shell command or an R script. To be useful in a Boyle workflow, any operation should help to create one or more resources.

A "recipe" (to borrow a word from GNU Make) is simply a sequence of operations that together create some set of resources (as specified by `Resource` objects).

## Definition

A definition expresses an expectation that some resource (output) will be created if a recipe (sequence of operations) is run. A definition may depend on other definitions, which can then be seen as inputs to the recipe. In other words, a recipe requires zero or more resources defined in some upstream definition(s).

Definitions should be deterministic, in the sense that its recipe should always cause the same resource (file content, value, etc) to be created upon completion.

# Creating a resource according to definition

Assume there is an environment, or context, where resources (like files and values) can reside. Place the input resources of a definition in this environment. Run the recipe. Now the output resources are expected to exist in the same context. In practice, it has to be up to the resources to "take care" of themselves, i.e., to restore inputs in the environment before running the recipe, and to save away the outputs after running the recipe.

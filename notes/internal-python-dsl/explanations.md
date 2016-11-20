# Some loose explanations

## Resource

A resource is an output that results from a calculation. It might be a file, a value, an object in a database, or something else. Implementations of the `Instrument` interface are the tools to handle such resources. An `Instrument` implementation does *not* represent the actual content of the resource. For example, a `File` instance `File('path/to/file.ext')` is merely an instrument to handle the corresponding resource, which can be described as "a file, with any content, placed at `'path/to/file.ext'`". And a `Value` instance called `Value('foo')` is a tool to handle "a constant value with the symbol `'foo'`. Hence, the concrete meaning, or content, of the resource is one thing, and the instrument that will handle it is another.

Thus, 'Instrument` instances are used to specify the expected outputs of a calculation. And `Instrument` instances are also used to do all the practical handling of the actual, concrete outputs. In fact, the Boyle system should not depend on or have to "know" anything about the concrete implementations of `Instrument`.

## Procedure

Procedures are things that can be done. For example run a shell command or an R script. To be useful in a Boyle workflow, any operation should help to create one or more resources. The Procedure of a definition is the equivalent of the "recipe" in GNU Make.

## Definition

A definition expresses an expectation that some resource (output) will be created if a Procedure is run. A definition may depend on other definitions, which can then be seen as inputs to the recipe. In other words, a recipe requires zero or more resources defined in some upstream definition(s).

Definitions should be deterministic, in the sense that its recipe should always cause the same resource (file content, value, etc) to be created upon completion.

# Creating a resource according to definition

Assume there is an environment, or context, where resources (like files and values) can reside. Use the proper `Instrument`s to place the input resources in this environment. Run the recipe. Now the output resources are expected to exist in the same context, and the `Instrument`s corresponding to output resources can save away the output resources.


Definition is essentially a tuple
    (Instrument instr, Definition[] dependencies, Procedure recipe)

Resource is a tuple
    (Instrument instr, String digest)

What is run is a Calculation, a tuple
    (Resource[] inputs, Procedure recipe)

Result is a tuple
    (Calculation calc, Resource r)


A Definition, before running, must be resolved into a Calculation. The process of calculating something with Boyle is all about successively transforming the Definitions in a graph into Calculations and running them to resolve further Definitions.

So to "make" a Definition, i.e., to produce the Resource defined by a Definition, what has to be done?

* Intelligent list here...

# How to bring source code along automatically?

We want to be able to define something like the following:

```python
a = define(
    Value(),
    some_input,
    Python('''
        import mymodule
        out = mymodule.do_something(inp)
        ''')
    )
```

A problem, then, is what do we do about the source code files (`mymodule.py` etc)?

* The simplest solution is to disallow this type of definition: The end user must somehow specify that `mymodule.py` is a dependency, etc, etc. This solution is so inconvenient that it is out of the question.

* An alternative solution, more complicated but also more convenient, is that we create a notion of a "home directory" somehow. Each definition has a home directory, which is the directory that the definition (or at least its Procedure) is instantiated in. In that case we might do the following. (1) Analyze the two-line script (`import mymodule` etc) and thereby find `mymodule.py` or whatever. (2) Recursively find all the dependencies of `mymodule` that can be found under the home directory. (3) Somehow include this code as a part of the definition, perhaps as a `Resource` dependency...

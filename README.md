# Cookbooks

Collection of cookbooks to automate and orchestrate operations in the WMF infrastructure.

The cookbooks will be executed by the `cookbook` entry point script of the `spicerack` package.

## Cookbooks hierarchy

The cookbooks must be structured in a tree, as they can be run also from an interactive menu that shows the tree from
an arbitrary entry point downwards.

Each cookbook filename must be a valid Python module name, hence all lowercase, with underscore if that improves
readability and that doesn't start with a number.

Given that the cookbooks are imported dynamically, a broader set of characters like dashes and starting with a number
are technically allowed, although its use is discouraged unless really needed.

Example of cookbooks tree:
```
cookbooks
|-- __init__.py
|-- top_level_cookbook.py
|-- group1
|   |-- __init__.py
|   `-- important_cookbook.py
`-- group2
    |-- __init__.py
    `-- subgroup1
        |-- __init__.py
        `-- some_task.py
```

## Cookbook interface

Each cookbook must define:

* A title in one of the two following ways:
  * Setting a string variable `__title__` at the module-level with the desired static value.
  * Defining a `get_title(args)` function in the module that accepts a parameter that is the list of CLI arguments
    specific to the cookbook and dynamically returns the string with the desired title.

  In case both are present, `get_title(args)` will be called.

* A `main(args, spicerack)` function that accept two parameters and returns an integer or None:
  * Parameter `args`: the list of CLI arguments specific to the cookbook. Cookbooks are encouraged to use
    `argparse.ArgumentParser` to parse the arguments, so that an help is automatically available with `-h/--help` and
    it can be shown both when running a cookbook directly or in the interactive menu.
  * Parameter `spicerack`: a pre-initialized instance of `spicerack.Spicerack` with the generic CLI arguments parsed
    by the `cookbook` entry point script. It allows to access all the libraries available in the `spicerack` package.
  * Return value: it must be `0` or `None` on success and a positive integer smaller than `128` on failure. The exit
    codes `90-99` are reserved by the `cookbook` entry point script and should be avoided.

### Logging

The logging is already pre-setup by the `cookbook` entry point script that initialize the root logger, so that each
cookbook can just initliaze its own `logging` instance and log. A special logger to send notification to the
`#wikimedia-operations` IRC channel is also available through the `spicerack` instance passed to `main()`.

Example of logging:
```
import logging

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name

logger.info('message')
```

## Spicerack library

### Available modules

All the available modules in the Spicerack package are exposed to the cookbooks through the `spicerack` parameter to
the `main()` function, that offers helper methods to obtain pre-initialized instances of all the available libraries.
It exposes also some of the global CLI arguments parsed by the `cookbook` entry point script such as `dry_run` and
`verbose` as getters. See the Spicerack documentation for more details.

### Exception catching

In general each module in the `spicerack` package has its own exception class to raise specific errors, but all are
derived from `spicerack.exceptions.SpicerackError`.

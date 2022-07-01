#!/bin/bash -e

fail() {
    echo "The code is not formatted according to the current style. You can autoformat your code running:"
    echo "    tox -e py3-format"
    echo "See also https://doc.wikimedia.org/spicerack/master/development.html#code-style"
    exit 1
}

black \
    --check \
    --diff \
    cookbooks/wmcs \
    tests/unit/wmcs \
|| fail

isort \
    --check-only \
    --diff \
    cookbooks/wmcs \
    tests/unit/wmcs \
|| fail

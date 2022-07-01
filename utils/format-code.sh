#!/bin/bash -e
black \
    cookbooks/wmcs \
    tests/unit/wmcs

isort \
    cookbooks/wmcs \
    tests/unit/wmcs

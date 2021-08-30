#!/usr/bin/env python3

import subprocess
from sys import argv

def call(linter, source, args):
    return subprocess.call([linter, source] + args.split())

arg = argv[1]
print("Code formatting of with Black")
ret = call("black", arg, "--config .black.toml")

print("Import sorting with isort")
ret = ret | call("isort", arg, "-p faaskeeper -l 88 --profile black")

print("flake8 linting")
ret = ret | call("flake8", arg, "--config=.flake8.cfg --black-config=.black.toml")

print("Check static typing")
ret = ret | call("mypy", arg, "--config-file=.mypy.ini")

exit(ret)

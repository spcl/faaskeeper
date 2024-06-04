#!/usr/bin/env python3

import argparse
import os
import subprocess

parser = argparse.ArgumentParser(description="Install FK and dependencies.")
parser.add_argument('--venv', metavar='dir', type=str, default="python-venv", help='destination of local python and nodejs virtual environment')
parser.add_argument('--without-client-library', action='store_true', default=False, help='skip installation of client library')
args = parser.parse_args()

def execute(cmd):
    ret = subprocess.run(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True
    )
    if ret.returncode:
        raise RuntimeError(
            "Running {} failed!\n Output: {}".format(cmd, ret.stdout.decode("utf-8"))
        )
    return ret.stdout.decode("utf-8")

python_env_dir = args.venv

print("Creating Python virtualenv at {}".format(python_env_dir))
execute("python3 -mvenv {}".format(python_env_dir))

print("Install Python dependencies with pip")
execute(". {}/bin/activate && pip3 install -r requirements.txt".format(python_env_dir))

print("Configure mypy extensions")
execute(". {}/bin/activate && mypy_boto3".format(python_env_dir))

print("Creating Node virtualenv at {}".format(python_env_dir))
execute(". {}/bin/activate && nodeenv -p".format(python_env_dir))

print("Install Node dependencies with npm")
execute(". {}/bin/activate && npm install -g serverless arabold/serverless-export-env serverless-python-requirements serverless-iam-roles-per-function serverless-google-cloudfunctions".format(python_env_dir))

if not args.without_client_library:
    print("Install FaaSKeeper Python library")
    execute(". {}/bin/activate && pip install git+https://github.com/spcl/faaskeeper-python --upgrade".format(python_env_dir))


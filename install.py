#!/usr/bin/env python3

import argparse
import os
import subprocess

parser = argparse.ArgumentParser(description="Install Lambda's ZK and dependencies.")
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

env_dir="zk-virtualenv"

print("Creating Python virtualenv at {}".format(env_dir))
execute("python3 -mvenv {}".format(env_dir))

print("Install Python dependencies with pip")
execute(". {}/bin/activate && pip3 install -r requirements.txt".format(env_dir))

print("Configure mypy extensions")
execute(". {}/bin/activate && mypy_boto3".format(env_dir))


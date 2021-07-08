# FaaSKeeper Python

**The Python client library for the serverless coordination service FaaSKeeper.**

[![GH Actions](https://github.com/mcopik/faaskeeper/actions/workflows/build.yml/badge.svg)](https://github.com/mcopik/faaskeeper/actions/workflows/build.yml)

The main implementation of the serverless FaaSKeeper service.
At the moment we support an AWS deployment only.
Adding support for other commercial clouds (Azure, GCP)
is planned in the future.

## Dependencies

* Python >= 3.6
* Node.js **<= 15.4.0**
* Cloud credentials to deploy the service

We use the [serverless](https://www.serverless.com/) framework to manage the deployment of serverless
functions and cloud resources. The framework with all dependencies and plugins is installed automatically.
Configure the provider credentials according to instructions provided in the serverless framework,
e.g., [instructions for AWS](https://www.serverless.com/framework/docs/providers/aws/guide/credentials/).


Currently, serverless has a bug causing it to generate empty deployment packages ([bug 1](https://github.com/serverless-heaven/serverless-webpack/issues/682), [bug 2](https://github.com/serverless/serverless/issues/8794)),
and the `node 15.4.0` is confirmed to work - any newer version is NOT guaranteed to work.

## Installation & Deployment

To install the local development environment with all necessary packages, please use the `install.py`
script. The script takes one optional argument `--venv` with a path to the Python and Node.js virtual
environment, and the default path is `python-venv`. Use `source {venv-path}/bin/activate` to use it.

The deployment with `serverless` framework is wrapped with a helper executable `fk.py`. Use this
to deploy the service with all functions, storage, and queue services:

```
./fk.py deploy service --provider aws
```

To update functions, they can be redeployed separately:

```
./fk.py deploy functions --provider aws
```

The existing deployment can be cleared by removing entire service before redeployment:

```
./fk.py deploy service --provider aws --clean
```

To enable verbose debugging of functions, use the flag `--verbose`.

## Using CLI

A CLI for FaaSKeeper is available in `bin/fkCli.py`. It allows to run interactive FaaSKeeper session,
and it includes history and command suggestions.

```console
bin/fkCli.py aws faaskeeper-dev
[fk: aws:faaskeeper-dev(CONNECTED) session:f3c1ba70 0] create /root/test1 "test_data" false false
```

## Development

We use `black` and `flake8` for code linting. Before commiting and pushing changes,
please run `tools/linting.py functions` to verify that there are no issues with your code.

We use Python type hints ([PEP](https://www.python.org/dev/peps/pep-0484/), [docs](https://docs.python.org/3/library/typing.html))
to enhance the readability of our code. When adding new interfaces and types, please use type hints.
The linting helper `tools/linting.py` includes a call to `mypy` to check for static typing errors.


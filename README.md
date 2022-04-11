# FaaSKeeper Python

**The Python client library for the serverless coordination service FaaSKeeper.**

[![GH Actions](https://github.com/mcopik/faaskeeper/actions/workflows/build.yml/badge.svg)](https://github.com/mcopik/faaskeeper/actions/workflows/build.yml)

The main implementation of the serverless FaaSKeeper service.
At the moment we support an AWS deployment only.
Adding support for other commercial clouds (Azure, GCP)
is planned in the future.

To use a deployed FaaSKeeper instance, check our Python client library: [spcl/faaskeeper-python](https://github.com/spcl/faaskeeper-python).

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

The deployment with `serverless` framework is wrapped with a helper executable `fk.py`.
Use the JSON config example in `config/user_config.json` to change the deployment name and parameters.
Use this to deploy the service with all functions, storage, and queue services:

```
./fk.py deploy service --provider aws --config config/user_config.json
```

To update functions, they can be redeployed separately:

```
./fk.py deploy functions --provider aws --config config/user_config.json
```

The existing deployment can be cleared by removing entire service before redeployment:

```
./fk.py deploy service --provider aws --clean --config config/user_config.json
```

To enable verbose debugging of functions, set the flag `verbose` in the config.

## Using CLI

A CLI for FaaSKeeper is available in `bin/fkCli.py`. It allows to run interactive FaaSKeeper session,
and it includes history and command suggestions.

```console
bin/fkCli.py aws faaskeeper-dev
[fk: aws:faaskeeper-dev(CONNECTED) session:f3c1ba70 0] create /root/test1 "test_data" false false
```

## Design

### Watches

To register a watch, the client performs the following sequence of operations: read node, add
watch in storage, read node again.
If the node has not been update, then we have inserted
the node correctly.
This is guaranteed by the fact that system first updates the data, and then
it sends watch notifications.
If there's a concurrent update happening in the background, then
interleaving between update and watch setting can happen.
If the client manages to add notification before the update, it will be notified.
However, if the data is updated, then client has no guarantee that it managed
to create watch before watch function started delivering notifications - thus, watch creation failed.
The watch might have been created correctly, but this case is managed by adding
timestamps to let `watch` function detect when the watch is new enough that
it shouldn't be trigerred.
Bad interleaving - entire watch process happens between system writing data
and starting notifications. The system should not trigger the watch and
retain them for future usage.

#### GetData

The watch is triggered by `set_data` and `delete` operations on the node.
To detect if the watch is not set on an older version of the node, the timestamp
is compared.

#### Exists

The watch is triggered by `create`, `set_data`, and `delete` call on the node.
To detect if the watch is not set on an older version of the node, the timestamp
is compared.
When the node does not exist, a "none" timestamp is set - such watch is
retained when the update was `delete` and triggered when the update was `create`.

#### GetChildren

Triggered by delete on the node and `create` and `delete` on its children.
To detect if the watch is not set on an older version of the node, we
use the `children` timestamp.

## Development

We use `black` and `flake8` for code linting. Before commiting and pushing changes,
please run `tools/linting.py functions` to verify that there are no issues with your code.

We use Python type hints ([PEP](https://www.python.org/dev/peps/pep-0484/), [docs](https://docs.python.org/3/library/typing.html))
to enhance the readability of our code. When adding new interfaces and types, please use type hints.
The linting helper `tools/linting.py` includes a call to `mypy` to check for static typing errors.


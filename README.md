# FaaSKeeper

**The ZooKeeper-like serverless coordination service FaaSKeeper.**

[![GH Actions](https://github.com/spcl/faaskeeper/actions/workflows/build.yml/badge.svg)](https://github.com/spcl/faaskeeper/actions/workflows/build.yml)

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

## Configuration.

The FaaSKeeper architecture can be customized with the following settings:
* **User storage** - the permitted values are *key-value* (DynamoDB on AWS), and *persistent* (S3 on AWS).
* **Worker queue** - there are two types of *writer* and *distributor* queues - *dynamodb* using DynamoDB streams, and *sqs* using the SQS queue.
* **Client channel** - functions deliver the client notification with a *tcp* and *sqs* channel. The former requires the client to accept incoming TCP connections, i.e., it needs to have public IP.
See the JSON config example in `config/user_config.json` for details.

## Installation & Deployment

To install the local development environment with all necessary packages, please use the `install.py`
script. The script takes one optional argument `--venv` with a path to the Python and Node.js virtual
environment, and the default path is `python-venv`. Use `source {venv-path}/bin/activate` to use it.

The deployment with `serverless` framework is wrapped with a helper executable `fk.py`. To enable verbose debugging of functions, set the flag `verbose` in the config.
### AWS
Use the JSON config example in `config/user_config.json` to change the deployment name and parameters.
Use this to deploy the service with all functions, storage, and queue services:

```
./fk.py deploy service config/user_config_final.json --provider aws --config config/user_config.json
```

The script will generate a new version of the config in `config/user_config_final.json`, which includes
data needed for clients, such as the location of S3 data bucket - its name is partially randomized to
generate unique S3 bucket names.
To update functions, they can be redeployed separately:

```
./fk.py deploy functions --provider aws --config config/user_config.json
```

The existing deployment can be cleared by removing entire service before redeployment:

```
./fk.py deploy service --provider aws --clean --config config/user_config.json
```
### GCP
Use the JSON config example in `config/user_config_gcp.json` to change the deployment name and parameters.

Before running the script, please create a project and do the follwing, the steps 1-5 are in GCP console, the rest are in local terminal:

1. enabling the datastore mode, deployment manager, pubsub, cloud functions and cloud run. To enable cloud functions and cloud run, you need to click the create button in console.
2. By enabling cloud run service, a service account in the format of XXXXX-compute@developer.gserviceaccount.com will be automatically created.
3. Go to Service Accounts under IAM & Admin and get a key pair of the XXXXX-compute@developer.gserviceaccount.com by clicking the three dots and selecting the manage key. Put the key file into the config folder, and execute the following cmd to store gcp credentials in your local environment.
```
 export GOOGLE_APPLICATION_CREDENTIALS="<ABSOLUTE_PATH_TO_KEY>"
```
4. fill the project details in `config/user_config_gcp.json`.
5. Goto GCP IAM, grant XXXXX-compute@developer.gserviceaccount.com the role of owner and Storage Admin; grant GOOGLE ACCOUNT the Service Account Token Creator role.
6. In terminal, login the gcloud cmd tool.
```
gcloud auth login <GOOGLE_ACCOUNT_EMAIL>
```
7. set current project
```
gcloud config set project <PROJECT_ID>
```
8. generate a temp token (valid for 1 hour), and copy the token into `gcp_config_auth.yml`.
```
gcloud auth print-access-token --impersonate-service-account=XXXXX-compute@developer.gserviceaccount.com
```
9. If the token expires, generate a new one and paste the token into `gcp_config_auth.yml` and delete the type-provider datastore-final. The new datastore-final type-provider will then be generated the next time you run deploy.
```
gcloud beta deployment-manager type-providers delete datastore-final
```
10. Uncomment the dependencies under gcp.
11. Use this to deploy the service with all functions, storage, and queue services. If you ever come across an error about permissions, be sure to verify that step 1 has been executed correctly first.

```
./fk.py deploy service config/user_config_gcp_final.json --provider gcp --config config/user_config_gcp.json
```
## Using CLI

A CLI for FaaSKeeper is available in `bin/fkCli.py`. It allows to run interactive FaaSKeeper session,
and it includes history and command suggestions.

```console
bin/fkCli.py <config-file>
[fk: aws:faaskeeper-dev(CONNECTED) session:f3c1ba70 0] create /root/test1 "test_data" false false
```

## Design

### Storage

FaaSKeeper uses two types of storage - system and user.
For system, we require strong consistency, as different writer functions must be able to safely
modify data in parallel.
For user data storage, we require strong consistency as well to prevent stale reads that would
violate ZooKeeper consistency principles.

### Components

All resources are allocated with the help of Serverless framework. See `config/aws.yml` for an example on the AWS platform.

#### Storage

We use key-value tables to store system data, user store, and to function as queus for incoming requests. Furthermore, we allocate persistent object storage buckets to store user data as well.

#### Functions

We create four basic functions: `heartbeat`, `watch`, `writer` and `distributor`. Users invoke `witer` indirectly via a queue or a table to proces a write request, and this function in turn invokes `distributor` and `watch` to process updated data nad new watch events. Heartbeat is invoked periodically by the system.

#### Communication

We use queues to process incoming requests and invoke functions. Queues must uphold FIFO ordering in FaaSKeeper.

## Functionalities

#### Watches

To register a watch, the client performs the following sequence of operations: read node, add
watch in storage, read node again.
If the node has not been update, then we have inserted the node correctly.
This is guaranteed by the fact that system first updates the data, and then it sends watch notifications.

If there's a concurrent update happening in the background, then interleaving between update and watch setting can happen.
If the client manages to add notification before the update, it will be notified.
However, if the data is updated, then client has no guarantee that it managed to create watch before watch function started delivering notifications - thus, watch creation failed.
The watch might have been created correctly, but this case is managed by adding timestamps to let `watch` function detect when the watch is new enough that
it shouldn't be trigerred.

Bad interleaving - entire watch process happens between system writing data and starting notifications. The system should not trigger the watch and
retain them for future usage.

##### GetData

The watch is triggered by `set_data` and `delete` operations on the node.
To detect if the watch is not set on an older version of the node, the timestamp
is compared.

##### Exists

The watch is triggered by `create`, `set_data`, and `delete` call on the node.
To detect if the watch is not set on an older version of the node, the timestamp
is compared.
When the node does not exist, a "none" timestamp is set - such watch is
retained when the update was `delete` and triggered when the update was `create`.

##### GetChildren

Triggered by delete on the node and `create` and `delete` on its children.
To detect if the watch is not set on an older version of the node, we
use the `children` timestamp.


## Development

We use `black` and `flake8` for code linting. Before commiting and pushing changes,
please run `tools/linting.py functions` to verify that there are no issues with your code.

We use Python type hints ([PEP](https://www.python.org/dev/peps/pep-0484/), [docs](https://docs.python.org/3/library/typing.html))
to enhance the readability of our code. When adding new interfaces and types, please use type hints.
The linting helper `tools/linting.py` includes a call to `mypy` to check for static typing errors.

## Authors

* [Marcin Copik (ETH Zurich)](https://github.com/mcopik/) - main author.
* [Ziad Hany](https://github.com/ziadhany) - SQS client channel.

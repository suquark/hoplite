# Setup AWS Cluster for Hoplite Microbenchmarks on AWS.

## Setup Local Environment _(About 2 min)_

On your local machine, make sure Python (>=3.6) is installed on the local machine. Then install Ray version `1.3` and boto with:

~~~bash
pip install ray==1.3 boto3  # if failed, use "pip -V" to check if you are using python3
~~~

## Start the Cluster _(About 3 min)_

Start the cluster and connect to the head node via:

~~~bash
export AWS_ACCESS_KEY_ID="Your Access Key ID"
export AWS_SECRET_ACCESS_KEY="Your Secret Acess Key"
ray up example.yaml
ray attach example.yaml
~~~

Visit the directory with pre-built binaries via `cd ~/efs/hoplite`

Remember to take down the cluster using `ray down example.yaml` on the local machine after evaluation.

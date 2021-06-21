# Setup Hoplite ML serving experiments on AWS.

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

Visit the directory with pre-built binaries and results via `cd ~/efs/hoplite-with-results/`

Remember to take down the cluster using `ray down example.yaml` on the local machine after evaluation.

## Access results

### ML model serving experiments (Figure 11)

The results are collected dynamically, so if you want to get the result numbers, you need to run the experiments with [this cluster setup](../cluster-config-with-ami).

### ML Model Serving fault tolerance experiments (Figure 12a)

After logging into the cluster, `cd ~/efs/hoplite-with-results/app/ray_serve`. `hoplite_ray_serve_log.json` and `ray_serve_log.json` contain the raw trajectory during failure. 

Run `python analyze_fault_tolerance.py` to compare the failure detection latency (see section 5.5 in the paper).

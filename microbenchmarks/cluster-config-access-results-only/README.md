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

Visit the directory with pre-built binaries via `cd ~/efs/hoplite-with-results/`

Remember to take down the cluster using `ray down example.yaml` on the local machine after evaluation.

## Access results

You can download results from the cluster to your local machine by executing

~~~bash
ray rsync-down example.yaml <remote file path> <local destination>
~~~

Here is how you could download main results:

### Roundtrip Microbenchmarks (Figure 6 at Section 5.1)

**Raw data for Figure 6**

~~~bash
ray rsync-down example.yaml /home/ubuntu/efs/hoplite-with-results/microbenchmarks/roundtrip-results.csv .
~~~

**Figure 6 (a)**

~~~bash
ray rsync-down example.yaml /home/ubuntu/efs/hoplite-with-results/microbenchmarks/RTT1K.pdf .
~~~

**Figure 6 (b)**

~~~bash
ray rsync-down example.yaml /home/ubuntu/efs/hoplite-with-results/microbenchmarks/RTT1M.pdf .
~~~

**Figure 6 (c)**

~~~bash
ray rsync-down example.yaml /home/ubuntu/efs/hoplite-with-results/microbenchmarks/RTT1G.pdf .
~~~

## Collective Communication Microbenchmarks (Figure 7 at Section 5.1, Figure 13 at Appendix A)

**Raw data for Figure 7 & Figure 13**

~~~bash
ray rsync-down example.yaml /home/ubuntu/efs/hoplite-with-results/microbenchmarks/mpi-cpp/mpi_results.csv .
ray rsync-down example.yaml /home/ubuntu/efs/hoplite-with-results/microbenchmarks/hoplite-cpp/hoplite_results.csv .
ray rsync-down example.yaml /home/ubuntu/efs/hoplite-with-results/microbenchmarks/gloo-cpp/gloo_results.csv .
ray rsync-down example.yaml /home/ubuntu/efs/hoplite-with-results/microbenchmarks/ray-python/ray-microbenchmark.csv .
ray rsync-down example.yaml /home/ubuntu/efs/hoplite-with-results/microbenchmarks/dask-python/dask_results.csv .
~~~

**Figure 7, Section 5.1**

~~~bash
ray rsync-down example.yaml /home/ubuntu/efs/hoplite-with-results/microbenchmarks/microbenchmarks-large.pdf .
~~~

**Figure 13, Appendix A**

~~~bash
ray rsync-down example.yaml /home/ubuntu/efs/hoplite-with-results/microbenchmarks/microbenchmarks-small.pdf .
~~~

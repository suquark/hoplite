# Reproducing RLLib experiments in Hoplite on AWS (with AMI).

## Setup Local Environment _(About 2 min)_

On your local machine, make sure Python 3 is installed on the local machine.Then install Ray version `0.8.0` and boto with:
   ~~~bash
   pip install ray==0.8.0 boto3
   ~~~

## Start the Cluster and Evaluate _(About 30 min)_

1. Launch the cluster and logging in:
   ~~~bash
   export AWS_ACCESS_KEY_ID="Your Access Key ID"
   export AWS_SECRET_ACCESS_KEY="Your Secret Acess Key"
   ray up example.yaml
   ray attach example.yaml
   ~~~
3. Move to the running scripts directory:
   ~~~bash
   cd ~/hoplite-rllib/hoplite-scripts
   ~~~
4. Generate the cluster configuration:
   ~~~bash
   python a3c_generate_config.py
   python impala_generate_config.py
   ~~~
5. Test all configurations:
   ~~~bash
   ./test_all_generated.sh
   ~~~
6. After all experiments finished, we can get the results via:
   ~~~bash
   python a3c_parse_log.py
   python impala_parse_log.py
   ~~~
   The results will be in the format of:
   ~~~
   #nodes / - / Hoplite or Ray / Throughput (mean) / Throughput (std)
   ~~~

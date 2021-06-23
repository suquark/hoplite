# Reproducing RLLib experiments in Hoplite on AWS.

If you already have access to the cluster and AMI, see [README-with-ami](README-with-ami.md) instead.

## Setup Local Environment _(About 5 min)_

 First, on your local machine:

1. Make sure Python 3 is installed on the local machine.Then install Ray version `0.8.0` and boto with:
   ~~~bash
   pip install ray==0.8.0 boto3
   ~~~
2. Configure your AWS credentials (`aws_access_key_id` and `aws_secret_access_key`) in `~/.aws/credentials` as described [here](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#guide-credentials). Your `~/.aws/credentials` should look like the following:
   ~~~
   [default]
   aws_access_key_id=XXXXXXXX
   aws_secret_access_key=YYYYYYYY
   ~~~
   Change the permission of this file:
   ~~~bash
   chmod 600 ~/.aws/credentials
   ~~~

## Setup AMI _(About 20 min)_

Please contact Zhuohan Li (zhuohan@berkeley.edu) for configured AMI. See following for the instructions to setup the cluster from scratch:

Start an AWS node with `initial.yaml` and connect to the node:
   ~~~bash
   ray up initial.yaml
   ray attach initial.yaml
   ~~~

You should have sshed into an AWS instance now, the following commands are executed on the AWS instance:

1. Clone Hoplite, install dependancies, and then compile Hoplite:
   ~~~bash
   cd ~
   git clone https://github.com/suquark/hoplite.git
   cd hoplite
   ./install_dependencies.sh
   mkdir build
   cd build
   cmake -DCMAKE_BUILD_TYPE=Release ..
   make -j
   ~~~
   Note that Hoplite should be compiled before activating conda environment, otherwise the Protobuf library in the conda environment will cause compilation errors.
2. Activate conda environment:
   ~~~bash
   conda activate
   echo "conda activate" >> ~/.bashrc
   ~~~
3. Install ray and tensorflow:
   ~~~bash
   pip install ray[all]==0.8.0 tensorflow==2.0.0
   ~~~
4. Install Hoplite Python library:
   ~~~bash
   cd ~/hoplite
   pip install -e python
   cp build/notification python/hoplite/
   ./python/setup.sh
   ~~~
5. Install the modified rllib library:
   ~~~bash
   cd ~
   git clone https://github.com/zhuohan123/hoplite-rllib.git
   cd hoplite-rllib
   git checkout artifact
   python python/ray/setup-dev.py # Setup symlink for rllib. Only set symlink for RLLib and don't set symlink for any other components (reply Y for the first option and reply n for all other).
   ~~~
6. Setup ssh key:
   ~~~bash
   ssh-keygen
   cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
   ~~~
7. Create an AMI on [AWS console](aws.amazon.com/console). See EC2 -> Instances -> Actions -> Image and templates -> Create image. Set the image name (e.g. `hoplite-artifact-rllib-ami`) and then create image.
8.  Go to AMIs tab on AWS console. When the AMI is ready, turn off the instance via:
   ~~~bash
   ray down initial.yaml
   ~~~

## Start the Cluster and Evaluate _(About 30 min)_

1. Create a [placement group](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/placement-groups.html) on the AWS Management Console. See EC2 -> Placement Groups. Choose the `Cluster` placement strategy. This can make sure the interconnection bandwidth among different nodes in the cluster are high.
2. Replace the `{image-id}` in `cluster.yaml` with the AMI-id you just created and `{group-name}` with the placement group name you just created. Start the cluster and connect to the head node via:
   ~~~bash
   ray up cluster.yaml
   ray attach cluster.yaml
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

# Setup AWS Cluster for Hoplite on AWS.

## Setup Local Environment _(About 5 min)_

 First, on your local machine:

1. Make sure Python 3 is installed on the local machine. Then install Ray version `1.1.0` and boto with:
   ~~~bash
   pip install ray==1.1.0 boto3
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

Please contact Siyuan Zhuang (s.z@berkeley.edu) for configured AMI. See following for the instructions to setup the cluster from scratch:

Start an AWS node with `initial.yaml` and connect to the node:
   ~~~bash
   ray up initial.yaml
   ray attach initial.yaml
   ~~~

You should have sshed into an AWS instance now, the following commands are executed on the AWS instance:

0. Create an [EFS](https://console.aws.amazon.com/efs) on region `us-east-1`. This is used as an NFS for all nodes in the cluster. Please add the security group ID of the node you just started (can be found on the AWS Management Console) to the EFS to make sure your node can access the EFS. After that, you need to install the [efs-utils](https://docs.aws.amazon.com/efs/latest/ug/installing-other-distro.html) to mount the EFS on the node:
   ~~~bash
   git clone https://github.com/aws/efs-utils
   cd efs-utils
   ./build-deb.sh
   sudo apt-get -y install ./build/amazon-efs-utils*deb
   ~~~
   Then mount the EFS on the node by:
   ~~~bash
   mkdir -p ~/efs
   sudo mount -t efs {Your EFS file system ID}:/ ~/efs
   sudo chmod 777 ~/efs
   ~~~
   If this takes forever, make sure you configure the sercurity groups right.
1. Install dependancies, clone Hoplite, and then compile Hoplite:
   ~~~bash
   ./install_dependencies.sh
   cd ~/efs
   # You must clone it under EFS.
   git clone https://github.com/suquark/hoplite.git
   cd hoplite
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
3. Install python libraries:
   ~~~bash
   pip install -r requirements.txt
   ~~~
4. Install Hoplite Python library:
   ~~~bash
   cd ~/hoplite
   pip install -e python
   cp build/notification python/hoplite/
   ./python/setup.sh
   ~~~
5. Config ssh for MPI:
   ~~~bash
   echo "Host *\n    StrictHostKeyChecking no" >> ~/.ssh/config
   sudo chmod 400 ~/.ssh/config
   ~~~
6. Setup ssh key:
   ~~~bash
   ssh-keygen
   cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
   ~~~

7. Create an AMI on [AWS console](aws.amazon.com/console). See EC2 -> Instances -> Actions -> Image and templates -> Create image. Set the image name (e.g. `hoplite-artifact-ami`) and then create image.
8. Go to AMIs tab on AWS console. When the AMI is ready, turn off the instance via:
   ~~~bash
   ray down initial.yaml
   ~~~

## Start the Cluster and Evaluate _(About 30 min)_

1. Create a [placement group](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/placement-groups.html) on the AWS Management Console. See EC2 -> Placement Groups. Choose the `Cluster` placement strategy. This can make sure the interconnection bandwidth among different nodes in the cluster are high.
2. Replace the `{image-id}` in `cluster.yaml` with the AMI-id you just created and `{group-name}` with the placement group name you just created.
3. Replace `{efs-id}` with your [EFS file system ID](https://console.aws.amazon.com/efs/home?region=us-east-1).
4. Start the cluster and connect to the head node via:
   ~~~bash
   ray up cluster.yaml
   ray attach cluster.yaml
   ~~~
   If the node fails to connect to EFS, check the security group ID as mentioned earlier.

# Setup AWS Cluster for Hoplite on AWS.

## Setup Local Environment _(About 10 min)_

 First, on your local machine:

1. Make sure Python 3 is installed on the local machine. Then install Ray version `1.3` and boto with:
   ~~~bash
   pip install ray==1.3 boto3
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

## Spin up AMI _(About 5 min)_

Please contact Siyuan Zhuang (s.z@berkeley.edu) for configured AMI. See following for the instructions to setup the cluster from scratch:

Start an AWS node with `initial.yaml` and connect to the node:
   ~~~bash
   ray up initial.yaml
   ray attach initial.yaml # ssh into the AWS instance
   ~~~

## Setup EFS _(About 10 min)_

Some experiments require a shared files system for proper logging. Here use AWS EFS.

1. Create an [EFS](https://console.aws.amazon.com/efs) on region `us-east-1`. This is used as an NFS for all nodes in the cluster.
2. Check your created EFS on [https://console.aws.amazon.com/efs/home?region=us-east-1#/file-systems/](https://console.aws.amazon.com/efs/home?region=us-east-1#/file-systems/). You can see the EFS File system ID ("fs-********") on the page.
3. Please add the security group ID of the node you just started (can be found on the AWS Management Console) to the EFS to make sure your node can access the EFS (link to manage EFS network access: https://console.aws.amazon.com/efs/home?region=us-east-1#/file-systems/{Your EFS file system ID}/network-access).

## Setup AMI _(About 20 min)_

You should have sshed into an AWS instance now, the following commands are executed on the AWS instance:

0. Install the [efs-utils](https://docs.aws.amazon.com/efs/latest/ug/installing-other-distro.html) to mount the EFS on the node:
   ~~~bash
   git clone https://github.com/aws/efs-utils
   cd efs-utils
   ./build-deb.sh
   sudo apt-get -y install ./build/amazon-efs-utils*deb
   ~~~
   It is normal to see
   ~~~
   E: Could not get lock /var/lib/dpkg/lock-frontend - open (11: Resource temporarily unavailable)
   E: Unable to acquire the dpkg frontend lock (/var/lib/dpkg/lock-frontend), is another process using it?
   ~~~
   when installing the package. This means the machine is still booting up and installing packages, you need to wait until the package manager is ready (usually 2-4 min) and install again.

   Then mount the EFS on the node by:
   ~~~bash
   mkdir -p ~/efs
   sudo mount -t efs {Your EFS file system ID}:/ ~/efs
   sudo chmod 777 ~/efs
   ~~~
   If this takes forever or connection timeout, make sure you configure the sercurity groups right.
1. Install dependancies, clone Hoplite, and then compile Hoplite:
   ~~~bash
   # You **must** the repo under EFS.
   cd ~/efs && git clone https://github.com/suquark/hoplite.git
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
3. Install python libraries (as in `requirements.txt`):
   ~~~bash
   pip install ray[all]==1.3 torchvision==0.8.2 mpi4py efficientnet_pytorch
   ~~~
4. Install Hoplite Python library:
   ~~~bash
   cd ~/efs/hoplite
   pip install -e python
   cp build/notification python/hoplite/
   ./python/setup.sh
   ~~~
5. Config ssh for MPI:
   ~~~bash
   echo -e "Host *\n    StrictHostKeyChecking no" >> ~/.ssh/config
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

## Start the Cluster _(About 10 min)_

1. Create a [placement group](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/placement-groups.html) on the AWS Management Console. See EC2 -> Placement Groups. Choose the `Cluster` placement strategy. This can make sure the interconnection bandwidth among different nodes in the cluster are high.
2. Replace the `{image-id}` in `cluster.yaml` with the AMI-id you just created and `{group-name}` with the placement group name you just created.
3. Replace `{efs-id}` with your [EFS file system ID](https://console.aws.amazon.com/efs/home?region=us-east-1).
4. Replace `SecurityGroupIds` with the security ID created by `initial.yaml`.
5. Start the cluster and connect to the head node via:
   ~~~bash
   ray up cluster.yaml
   ray attach cluster.yaml
   ~~~
   If the node fails to connect to EFS (or the cluster takes forever to spin up), check if the security group ID in EFS as mentioned earlier.

If everything is ok, take down the cluster using `ray down cluster.yaml` and remember to save your `cluster.yaml`.

`siyuan.yaml` is an example of configurated cluster file.

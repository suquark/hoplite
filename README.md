# Hoplite: Efficient and Fault-Tolerant Collective Communication for Task-Based Distributed Systems

This is the repo for the artifact evaluataion for the SIGCOMM 2021 paper: _Hoplite: Efficient and Fault-Tolerant Collective Communication for Task-Based Distributed Systems_. For any questions or related issue, please feel free to contact Siyuan Zhuang (s.z@berkeley.edu) and Zhuohan Li (zhuohan@berkeley.edu).

## Setup AWS Cluster & Hoplite

All the experiments in the paper are evaluated on AWS. We use [Ray cluster launcher](https://docs.ray.io/en/latest/cluster/launcher.html) to lanuch the cluster for all the experiments in the paper. We highly recommend using Ray cluster launcher to launch the cluster as it will automatically setup the execution environment we required in the experiments.

For every experiment, we include detailed instruction for setting up a cluster and reproducing the results in the paper.

## Microbenchmarks (Section 5.1)

Please see [microbenchmarks/](microbenchmarks) to reproduce the microbenchmark experiments in the paper.

## Asynchronous SGD (Section 5.2)

Please see [app/parameter-server/](parameter-server) to reproduce the Asynchronous SGD experiments in the paper.

## Reinforcement Learning (Section 5.3)

Please see [app/rllib/](app/rllib/) to reproduce the rllib experiments in the paper.

## ML Model Serving Experiments (Section 5.4)

Please see [app/ray_serve/](app/ray_serve) to reproduce the Ray serve experiments and the Ray serve fault tolerance experiments (Section 5.5, Figure 12a) in the paper.

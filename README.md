# Hoplite: Efficient Collective Communication for Task-Based Distributed Systems

## TODOs

- [ ] Refactor `src/reduce_dependency.cc` to switch from chain, binary tree, and star.

- [ ] Fix fault-tolerance for reduce. (figure out why sometimes it fails)

- [ ] Improve documentation coverage.

## Setup AWS Cluster & Hoplite _(45-60 min)_

See [cluster-config](cluster-config). This step is necessary for reproducing all experiments.

## Reinforcement Learning (Section 5.3)

See [RLLib experiments](rllib). Note that this experiment requires a local environment different from others.

## ML Model Serving Experiments (Section 5.4)

See [app/ray_serve](app/ray_serve). It also includes the fault tolerance experiments related to model serving in section 5.5.

## Microbenchmarks (Section 5.1)

* Figure 6 at Section 5.1 - See ??????.

* Figure 7 at Section 5.1, Figure 13 at Appendix A - See [Hoplite Microbenchmarks](microbenchmarks).

## Lint

`./format.sh`

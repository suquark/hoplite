// Author: Wes Kendall
// Copyright 2013 www.mpitutorial.com
// This code is provided freely with the tutorials on mpitutorial.com. Feel
// free to modify it for your own use. Any distribution of the code must
// either provide a link to www.mpitutorial.com or keep this header intact.
//
// Program that computes the average of an array of elements in parallel using
// MPI_Reduce.
//
#include <assert.h>
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

// Creates an array of random numbers. Each number has a value from 0 - 1
float *create_rand_nums(int num_elements) {
  float *rand_nums = (float *)malloc(sizeof(float) * num_elements);
  assert(rand_nums != NULL);
  int i;
  for (i = 0; i < num_elements; i++) {
    rand_nums[i] = (rand() / (float)RAND_MAX);
  }
  return rand_nums;
}

int main(int argc, char **argv) {
  if (argc != 2) {
    fprintf(stderr, "Usage: avg num_elements_per_proc\n");
    exit(1);
  }

  int num_elements_per_proc = atoi(argv[1]);
  double time = 0;

  MPI_Init(NULL, NULL);

  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  int world_size;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  // Create a random array of elements on all processes.
  srand(world_rank); // Seed the random number generator to get different
                     // results each time for each processor
  float *rand_nums = NULL;
  rand_nums = create_rand_nums(num_elements_per_proc);
  float *global_nums = (float *)malloc(sizeof(float) * num_elements_per_proc * world_size);

  MPI_Barrier(MPI_COMM_WORLD);
  // Reduce all of the local sums into the global sum
  time -= MPI_Wtime();
  MPI_Allgather(rand_nums, num_elements_per_proc, MPI_FLOAT, global_nums, num_elements_per_proc, MPI_FLOAT,
                MPI_COMM_WORLD);
  time += MPI_Wtime();

  // Print the result
  if (world_rank == 0) {
    printf("MPI_Allgather time = %lf\n", time);
  }

  // Clean up
  free(rand_nums);

  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Finalize();
}

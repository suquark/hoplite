// Author: Wes Kendall
// Copyright 2011 www.mpitutorial.com
// This code is provided freely with the tutorials on mpitutorial.com. Feel
// free to modify it for your own use. Any distribution of the code must
// either provide a link to www.mpitutorial.com or keep this header intact.
//
// Comparison of MPI_Bcast with the my_bcast function
//
#include <assert.h>
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char **argv) {
  if (argc != 2) {
    fprintf(stderr, "Usage: ./multicast num_elements\n");
    exit(1);
  }

  int num_elements = atoi(argv[1]);
  int num_trials = 1;

  MPI_Init(NULL, NULL);

  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

  double total_mpi_bcast_time = 0.0;
  int *data = (int *)malloc(sizeof(int) * num_elements);
  assert(data != NULL);

  // Time MPI_Bcast
  MPI_Barrier(MPI_COMM_WORLD);
  total_mpi_bcast_time -= MPI_Wtime();
  MPI_Bcast(data, num_elements, MPI_INT, 0, MPI_COMM_WORLD);
  MPI_Barrier(MPI_COMM_WORLD);
  total_mpi_bcast_time += MPI_Wtime();

  // Print off timing information
  if (world_rank == 0) {
    printf("MPI_Bcast duration = %lf\n", total_mpi_bcast_time);
  }

  free(data);
  MPI_Finalize();
}

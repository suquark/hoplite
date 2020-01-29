#include <assert.h>
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

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
  // Initialize the MPI environment
  MPI_Init(NULL, NULL);
  // Find out rank, size
  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  int world_size;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  // We are assuming at least 2 processes for this task
  if (world_size < 2) {
    fprintf(stderr, "World size must be greater than 1 for %s\n", argv[0]);
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  if (argc != 2) {
    fprintf(stderr, "Usage: send_recv num_elements\n");
    exit(1);
  }

  int num_elements = atoi(argv[1]);
  double time = 0;
  float *numbers = create_rand_nums(num_elements);
  if (world_rank == 0) {
    // If we are rank 0, set the number to -1 and send it to process 1
    MPI_Send(
        /* data         = */ numbers,
        /* count        = */ num_elements,
        /* datatype     = */ MPI_FLOAT,
        /* destination  = */ 1,
        /* tag          = */ 0,
        /* communicator = */ MPI_COMM_WORLD);
  } else if (world_rank == 1) {
    time -= MPI_Wtime();
    MPI_Recv(
        /* data         = */ numbers,
        /* count        = */ num_elements,
        /* datatype     = */ MPI_FLOAT,
        /* source       = */ 0,
        /* tag          = */ 0,
        /* communicator = */ MPI_COMM_WORLD,
        /* status       = */ MPI_STATUS_IGNORE);
    time += MPI_Wtime();

    printf("MPI_Recv time = %lf\n", time);
  }
  MPI_Finalize();
}

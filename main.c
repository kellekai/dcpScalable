#include <mpi.h>
#include <stdlib.h>
#include <stdio.h>
#include "dcp_lib.h"

int main() {

    MPI_Init(NULL,NULL);

    init( MPI_COMM_WORLD );
    
    MPI_Finalize();

    exit(EXIT_SUCCESS);
}

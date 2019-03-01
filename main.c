#include <mpi.h>
#include <stdlib.h>
#include <stdio.h>
#include "dcp_lib.h"

int main() {

    MPI_Init(NULL,NULL);

    init( MPI_COMM_WORLD );
    
    int data[512];
    int data2[8192];
    int i;
    for(i=0; i<512; i++) {
        data[i] = i+1;
    }
    for(i=0; i<8192; i++) {
        data2[i] = i+1;
    }

    protect( 0, data, 512, sizeof(int) );
    protect( 1, data2, 8192, sizeof(int) );
    checkpoint( 0 );
    
    data[0] = 2;
    
    for(i=0; i<8192; i++) {
        if(i>4096)
        data2[i] = i+2;
    }
    
    checkpoint( 1 );
    checkpoint( 2 );
    checkpoint( 3 );
    checkpoint( 4 );
    checkpoint( 5 );
    checkpoint( 6 );

    MPI_Finalize();

    exit(EXIT_SUCCESS);
}

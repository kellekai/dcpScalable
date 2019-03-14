#include <mpi.h>
#include <stdlib.h>
#include <stdio.h>
#include "dcp_lib.h"

#define BLOCKSIZE 16384L
#define ELEM_PER_BLOCK (BLOCKSIZE/sizeof(int))
#define SIZE_IN_BLOCKS( NUM ) (NUM*ELEM_PER_BLOCK)

int main() {

    MPI_Init(NULL,NULL);

    init( MPI_COMM_WORLD );
   
    int rank; 
    MPI_Comm_rank(MPI_COMM_WORLD, &rank); 
    
    unsigned long  nelems = SIZE_IN_BLOCKS(1024L) + SIZE_IN_BLOCKS(1)/2;
    //unsigned long  nelems = SIZE_IN_BLOCKS(1024L/16L*1024L) + SIZE_IN_BLOCKS(1)/2;
    unsigned long  size = nelems * sizeof(int);

    int *data = (int*) malloc( size );
    
    int i;
    for(i=0; i<nelems; i++) {
        data[i] = i+1;
    }

    if(rank==0) printf("nelem: d2->%lu | ELEM_PER_BLOCK: %lu\n", nelems, ELEM_PER_BLOCK);

    protect( 1, data, nelems, sizeof(int) );
    checkpoint( 0 ); // layer 0 
    
    // increase size
    unsigned long nelems_old = nelems;
    nelems += SIZE_IN_BLOCKS(256);
    size = nelems * sizeof(int);
    data = (int*) realloc( data, size );
    for(i=nelems_old; i<nelems; i++) {
        data[i] = i+1;
    }
    protect( 1, data, nelems, sizeof(int) );
    
    checkpoint( 1 );
    
    // decrease size
    nelems -= SIZE_IN_BLOCKS(256);
    size = nelems * sizeof(int);
    data = (int*) realloc( data, size );
    protect( 1, data, nelems, sizeof(int) );
    
    checkpoint( 2 );
    checkpoint( 3 );
    checkpoint( 4 );
    checkpoint( 5 ); // layer 0
    
    unsigned long j, cnt=0;
    for(j=0; j<size; j++) {
        int idx = j/sizeof(int);
        if(j%((unsigned long)(2*BLOCKSIZE)) == 0) {
            data[idx] = -1;
            //*(unsigned long*)((void*)data + j) = (unsigned long)-1;
        }
    }
    
    checkpoint( 6 );
    
    for(j=0; j<size; j++) {
        int idx = j/sizeof(int);
        if(j%(2*BLOCKSIZE) == 0)
            data[idx] = idx+1;
    }
    
    checkpoint( 7 );

    memset(data, 0x0, size);

    recover();

    unsigned long check = 0;
    for(i=0; i<nelems; i++) {
        check += data[i];
    }

    unsigned long check_cmpt = ((nelems*(nelems+1))/2);

    bool success = (check == check_cmpt);
    
    if(rank==0) printf( "[%s] -> check:[%lu|%lu]\n", (success)?"SUCCESS":"FAILURE", check, check_cmpt );
    MPI_Finalize();

    exit(EXIT_SUCCESS);
}

#include <mpi.h>
#include <stdlib.h>
#include <stdio.h>
#include "dcp_lib.h"

#define BLOCKSIZE 128
#define ELEM_PER_BLOCK (BLOCKSIZE/sizeof(int))
#define SIZE_IN_BLOCKS( NUM ) (NUM*ELEM_PER_BLOCK)

int main() {

    MPI_Init(NULL,NULL);

    init( MPI_COMM_WORLD );
   
    int rank,size; 
    MPI_Comm_rank(MPI_COMM_WORLD, &rank); 
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    
    unsigned long  nelems_1 = SIZE_IN_BLOCKS(20);
    unsigned long  nelems_2 = SIZE_IN_BLOCKS(40);
    unsigned long  size_1 = nelems_1 * sizeof(int);
    unsigned long  size_2 = nelems_2 * sizeof(int);

    int data_1[size_1];
    int data_2[size_2];
    
    int i;
    for(i=0; i<nelems_1; i++) {
        data_1[i] = i+1;
    }
    for(i=0; i<nelems_2; i++) {
        data_2[i] = i+1;
    }

    if(rank==0) printf("nelem: d1->%lu, d2->%lu | ELEM_PER_BLOCK: %lu\n", nelems_1, nelems_2, ELEM_PER_BLOCK);

    protect( 0, data_1, nelems_1, sizeof(int) );
    protect( 1, data_2, nelems_2, sizeof(int) );
    checkpoint( 0 ); // layer 0 
    checkpoint( 1 );
    checkpoint( 2 );
    checkpoint( 3 );
    checkpoint( 4 );
    checkpoint( 5 ); // layer 0

    data_1[0] = -1;
    data_2[ELEM_PER_BLOCK] = -1;
    data_2[10*ELEM_PER_BLOCK] = -1;
    
    checkpoint( 6 );
    
    data_1[0] = 1;
    data_2[ELEM_PER_BLOCK] = ELEM_PER_BLOCK+1;
    data_2[10*ELEM_PER_BLOCK] = 10*ELEM_PER_BLOCK+1;
    
    checkpoint( 7 );

    memset(data_1, 0x0, size_1);
    memset(data_2, 0x0, size_2);

    recover();

    unsigned long check_1 = 0;
    unsigned long check_2 = 0;
    for(i=0; i<nelems_2; i++) {
        if( i<nelems_1 ) check_1 += data_1[i];
        check_2 += data_2[i];
    }

    unsigned long check_1_cmpt = ((nelems_1*(nelems_1+1))/2);
    unsigned long check_2_cmpt = ((nelems_2*(nelems_2+1))/2);

    bool success = (check_1 == check_1_cmpt) && (check_2 == check_2_cmpt);
    
    if(rank==0) printf( "[%s] -> check_1:[%lu|%lu] , check_2:[%lu|%lu]\n", (success)?"SUCCESS":"FAILURE", check_1, check_1_cmpt, check_2, check_2_cmpt );
    MPI_Finalize();

    exit(EXIT_SUCCESS);
}

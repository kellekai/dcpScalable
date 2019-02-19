#include "dcp_lib_int.h"

#define SCES 0
#define NSCS -1

// API FUNCTIONS

int init( MPI_Comm comm );
int protect( int id, void* ptr, size_t nElem, size_t elemSize );
int Checkpoint( int id );

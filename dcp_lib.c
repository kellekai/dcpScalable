#include "dcp_lib.h"

//----------------------------------------------------------------------------------------------
// LOCAL LIBRARY VARIABLES
//----------------------------------------------------------------------------------------------

static dataInfo Data[BUFF];
static confInfo Conf;
static execInfo Exec;

//----------------------------------------------------------------------------------------------
// FUNCTION DEFINITIONS
//----------------------------------------------------------------------------------------------

int init( MPI_Comm comm )
{
    Exec.comm = comm;
    MPI_Comm_size( comm, &Exec.commSize );
    MPI_Comm_rank( comm, &Exec.commRank );

    if ( registerEnvironment( &Conf, &Exec ) != SCES ) {
        ERR_EXT( comm, "Errorous configuration environment!", rank );
    }

    unsigned long timestamp_ = timestamp();
    MPI_Bcast( &timestamp_, 1, MPI_UNSIGNED_LONG, 0, comm ); 
    snprintf( Exec.id, BUFF, "%lu", timestamp_ );
    
    if (mkdir(Exec.id, 0777) == -1) {
        if (errno != EEXIST) {
            ERR_EXT( comm, "unable to create checkpoint directory '%s'", Exec.commRank, Exec.id ); 
        }
    }
    
    Exec.nbVar = 0;
    
    Exec.nodeId = Exec.commRank/Exec.nodeSize;
    // create node comm
    MPI_Comm_split( Exec.comm, Exec.nodeId, Exec.commRank, &Exec.nodeComm );

    if( Exec.commRank == 0 ) {
        printConfiguration( Conf, Exec );
    }
}

int protect( int id, void* ptr, size_t nElem, size_t elemSize )
{
    if( ptr == NULL ) {
        ERR_MSG( Exec.comm, "invalid ptr (ptr == NULL).", Exec.commRank );
        return NSCS;
    }

    if( id < 0 ) {
        ERR_MSG( Exec.comm, "invalid ID '%d'. ID's have to be positive.", Exec.commRank, id );
        return NSCS;
    }
    
    int i;
    for( i=0; i<Exec.nbVar; i++) {
        if( id == Data[i].id ) {
            ERR_MSG( Exec.comm, "ID '%d' already taken.", Exec.commRank, id );
            return NSCS;
        }
    }
    
    Data[Exec.nbVar].elemSize = elemSize;
    Data[Exec.nbVar].id = id;    
    Data[Exec.nbVar].nElem = nElem;
    Data[Exec.nbVar].hashDataSize = 0;
    Data[Exec.nbVar].hashArray = NULL;
    Exec.nbVar++;
    
    return SCES;
}

int Checkpoint( int id )
{
    if( id < 0 ) {
        ERR_MSG( Exec.comm, "invalid ID '%d'. ID's have to be positive.", Exec.commRank, id );
        return NSCS;
    }
    
    char fn[BUFF], mfn[BUFF];
    snprintf( fn, BUFF, "%s/dcp-rank%d-part%d.fti", Exec.id, Exec.commRank, Exec.dcp.nbFiles );
    snprintf( mfn, BUFF, "%s/dcp-rank%d-meta.tmp", Exec.id, Exec.commRank );
}

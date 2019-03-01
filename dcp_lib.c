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

    // reset dcpStack
    Exec.dcp.dcpCounter = 0;

    // set dcp stack size to 5
    Conf.dcpStackSize = 5;
    Conf.dcpBlockSize = 128;

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
    Data[Exec.nbVar].ptr = ptr;
    Exec.nbVar++;
    
    return SCES;
}

int checkpoint( int id )
{
    if( id < 0 ) {
        ERR_MSG( Exec.comm, "invalid ID '%d'. ID's have to be positive.", Exec.commRank, id );
        return NSCS;
    }
    
    int dcpFileId = Exec.dcp.dcpCounter / Conf.dcpStackSize;
    int dcpLayer = Exec.dcp.dcpCounter % Conf.dcpStackSize;
    
    char fn[BUFF];
    snprintf( fn, BUFF, "%s/dcp-id%d-rank%d.fti", Exec.id, dcpFileId, Exec.commRank );

    FILE *fd;
    if( dcpLayer == 0 ) {
        fd = fopen( fn, "wb" );
        if( fd == NULL ) {
            ERR_MSG( Exec.comm, "Cannot create file '%s'!", Exec.commRank, fn );
            return NSCS;
        }
    } else {
        fd = fopen( fn, "ab" );
        if( fd == NULL ) {
            ERR_MSG( Exec.comm, "Cannot open file '%s' in append mode!", Exec.commRank, fn );
            return NSCS;
        }
    }

    unsigned char * block = (unsigned char*) malloc( Conf.dcpBlockSize );
    int i = 0;
    // init commitBlock. Commit all blocks to file for dcpLayer==0 (base layer).
    // bool commitBlock = !((bool)dcpLayer); // true for dcpLayer==0
    for(; i<Exec.nbVar; i++) {
        
        
        unsigned int varId = Data[i].id;
        unsigned long dataSize = Data[i].elemSize * Data[i].nElem;
        unsigned long nbHashes = dataSize/Conf.dcpBlockSize + (bool)(dataSize%Conf.dcpBlockSize);
        
        if( dataSize > (MAX_BLOCK_IDX*Conf.dcpBlockSize) ) {
            ERR_MSG( Exec.comm, "overflow in size of dataset with id: %d (datasize: %lu > MAX_DATA_SIZE: %lu)", Exec.commRank, Data[i].id, dataSize, ((unsigned long)MAX_BLOCK_IDX)*((unsigned long)Conf.dcpBlockSize) );
            return NSCS;
        }
        if( varId > MAX_VAR_ID ) {
            ERR_MSG( Exec.comm, "overflow in ID (id: %d > MAX_ID: %d)!", Exec.commRank, Data[i].id, (int)MAX_VAR_ID );
            return NSCS;
        }
        
        // allocate tmp hash array
        Data[i].hashArrayTmp = (unsigned char*) malloc( sizeof(unsigned char)*nbHashes*Conf.digestWidth );
        
        // create meta data buffer
        blockMetaInfo_t blockMeta;
        blockMeta.varId = Data[i].id;
        
        unsigned long pos = 0;
        unsigned char * ptr = Data[i].ptr;
        
        size_t dcpSize = 0;
        while( pos < dataSize ) {
            
            // hash index
            unsigned int blockId = pos/Conf.dcpBlockSize;
            unsigned int hashIdx = blockId*Conf.digestWidth;
            
            blockMeta.blockId = blockId;

            unsigned int chunkSize = ( (dataSize-pos) < Conf.dcpBlockSize ) ? dataSize-pos : Conf.dcpBlockSize;
            
            if( chunkSize < Conf.dcpBlockSize ) {
                // if block smaller pad with zeros
                memset( block, 0x0, Conf.dcpBlockSize );
                memcpy( block, ptr, chunkSize );
                Conf.hashFunc( block, Conf.dcpBlockSize, &Data[i].hashArrayTmp[hashIdx] );
                ptr = block;
            } else {
                Conf.hashFunc( ptr, Conf.dcpBlockSize, &Data[i].hashArrayTmp[hashIdx] );
            }
            
            bool commitBlock;
            // if old hash exists, compare. If datasize increased, there wont be an old hash to compare with.
            if( pos < Data[i].hashDataSize ) {
                commitBlock = memcmp( &Data[i].hashArray[hashIdx], &Data[i].hashArrayTmp[hashIdx], Conf.digestWidth );
            } else {
                commitBlock = true;
            }

            int WRITTEN = 1;
            if( commitBlock ) {
                fwrite( &blockMeta, 1, 6, fd );
                WRITTEN = fwrite( ptr, Conf.dcpBlockSize, 1, fd );
                dcpSize += WRITTEN*Conf.dcpBlockSize;
            }
            
            ptr = Data[i].ptr + Conf.dcpBlockSize*WRITTEN;
            pos += Conf.dcpBlockSize*WRITTEN;
           
        }

        free(Data[i].hashArray);
        Data[i].hashDataSize = dataSize;
        Data[i].hashArray = Data[i].hashArrayTmp;

    }
    free(block);
        
    fclose( fd );
    Exec.dcp.dcpCounter++;
    if( (dcpLayer == (Conf.dcpStackSize-1)) ) {
        int i = 0;
        for(; i<Exec.nbVar; i++) {
            //free(Data[i].hashArray);
            Data[i].hashDataSize = 0;
        }
    }
    if( (dcpLayer == 0) ) {
        char ofn[512];
        snprintf( ofn, BUFF, "%s/dcp-id%d-rank%d.fti", Exec.id, dcpFileId-1, Exec.commRank );
        if( (remove(ofn) < 0) && (errno != ENOENT) ) {
            char errstr[512];
            snprintf(errstr, 512, "cannot delete file '%s'", ofn );
            perror(errstr); 
        }
    }

    if(Exec.commRank==0)
        printf("[INFO] Checkpoint (id:%d) succeeded!\n", id);

}

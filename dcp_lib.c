#include "dcp_lib.h"

//----------------------------------------------------------------------------------------------
// LOCAL LIBRARY VARIABLES
//----------------------------------------------------------------------------------------------

static dataInfo Data[BUFF];
static confInfo Conf;
static execInfo Exec;

inline static int getIdx( int varId )
{
    int i=0;
    for(; i<Exec.nbVar; i++) {
        if(Data[i].id == varId) break;
    }
    if( i==Exec.nbVar ) {
        return -1;
    }
    return i;
}
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
    Exec.dcp.dcpFileSize = 0;

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
    Data[Exec.nbVar].size = elemSize*nElem;

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
    
    char fn[BUFF], mfn[BUFF], mfnt[BUFF];
    snprintf( fn, BUFF, "%s/dcp-id%d-rank%d.fti", Exec.id, dcpFileId, Exec.commRank );
    snprintf( mfnt, BUFF, "%s/dcp-rank%d.tmp", Exec.id, Exec.commRank );
    snprintf( mfn, BUFF, "%s/dcp-rank%d.meta", Exec.id, Exec.commRank );

    FILE *fd, *mfd;
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
    size_t dcpSize = 0;
    unsigned long glbDataSize = 0;
    if( dcpLayer == 0 ) Exec.dcp.dcpFileSize = 0;

    for(; i<Exec.nbVar; i++) {
         
        unsigned int varId = Data[i].id;
        unsigned long dataSize = Data[i].elemSize * Data[i].nElem;
        unsigned long nbHashes = dataSize/Conf.dcpBlockSize + (bool)(dataSize%Conf.dcpBlockSize);
        
        glbDataSize += dataSize;

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
       
        if( dcpLayer == 0 ) {
            while( !fwrite( &Data[i].id, sizeof(int), 1, fd ) ) {
                if(ferror(fd)) {
                    ERR_MSG( Exec.comm, "unable to write in file", rank );
                    return NSCS;
                }
            }
            while( !fwrite( &dataSize, sizeof(unsigned long), 1, fd ) ) {
                if(ferror(fd)) {
                    ERR_MSG( Exec.comm, "unable to write in file", rank );
                    return NSCS;
                }
            }
            Exec.dcp.dcpFileSize += (sizeof(int) + sizeof(long));
        }
        unsigned long pos = 0;
        unsigned char * ptr = Data[i].ptr;
        
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
                // we write plain data for dcpLayer = 0
                if( dcpLayer > 0 ) {
                    ptr = block;
                    chunkSize = Conf.dcpBlockSize;
                }
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

            bool success = true;
            int fileUpdate = 0;
            if( commitBlock ) {
                if( dcpLayer > 0 ) {
                    success = (bool)fwrite( &blockMeta, 6, 1, fd );
                    if( success) fileUpdate += 6;
                }
                if( success ) {
                    success = (bool)fwrite( ptr, chunkSize, 1, fd );
                    if( success ) fileUpdate += chunkSize;
                }
                dcpSize += success*chunkSize;
                Exec.dcp.dcpFileSize += success*fileUpdate;
            }
            
            ptr = Data[i].ptr + chunkSize*success;
            pos += chunkSize*success;
           
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
   
    // create meta data
    // - file size
    // - base size
    // - file id
    // - block size
    // - nb vars
    // - array of id and dataset size
    mfd = fopen( mfnt, "wb" );
    fwrite( &Exec.dcp.dcpFileSize, sizeof(unsigned long), 1, mfd );
    fwrite( &glbDataSize, sizeof(unsigned long), 1, mfd );
    fwrite( &dcpFileId, sizeof(int), 1, mfd );
    fwrite( &Conf.dcpBlockSize, sizeof(unsigned long), 1, mfd );
    fwrite( &Exec.nbVar, sizeof(int), 1, mfd);
    for(i=0; i<Exec.nbVar; i++) {
        unsigned long dataSize = Data[i].elemSize * Data[i].nElem;
        fwrite( &Data[i].id, sizeof(int), 1, mfd );
        fwrite( &dataSize, sizeof(unsigned long), 1, mfd );
    }
    fclose(mfd);

    rename( mfnt, mfn );

    if(Exec.commRank==0)
        printf("[INFO] Checkpoint (id:%d) succeeded (written %8lu of %8lu | file size:%8lu)\n", id, dcpSize, glbDataSize, Exec.dcp.dcpFileSize);

}

int recover()
{
    int dcpFileId;
    unsigned long glbDataSize;
    unsigned long dcpBlockSizeStored;

    char fn[BUFF], mfn[BUFF];
    snprintf( mfn, BUFF, "%s/dcp-rank%d.meta", Exec.id, Exec.commRank );
    
    FILE* mfd = fopen( mfn, "rb" );
    fread( &Exec.dcp.dcpFileSize, sizeof(unsigned long), 1, mfd );
    fread( &glbDataSize, sizeof(unsigned long), 1, mfd );
    fread( &dcpFileId, sizeof(int), 1, mfd );
    fread( &dcpBlockSizeStored, sizeof(unsigned long), 1, mfd );
    fread( &Exec.nbVar, sizeof(int), 1, mfd);
    int i;
    for(i=0; i<Exec.nbVar; i++) {
        unsigned int varId;
        fread( &varId, sizeof(int), 1, mfd );
        int idx = getIdx( varId );
        if( idx < 0 ) {
            ERR_MSG( Exec.comm, "id '%d' does not exist!", Exec.commRank, varId );
            return NSCS;
        }
        DBG_MSG(MPI_COMM_WORLD, "varId: %d, Data[%d].id:%d"  ,0, varId, idx, Data[idx].id );
        fread( &Data[idx].size, sizeof(unsigned long), 1, mfd );
        DBG_MSG(MPI_COMM_WORLD, "Data[%d].size:%lu"  ,0, idx, Data[i].size );
    }
    fclose(mfd);
    
    DBG_MSG(MPI_COMM_WORLD, "Exec.dcp.dcpFileSize:%lu"  ,0, Exec.dcp.dcpFileSize );
    DBG_MSG(MPI_COMM_WORLD, "glbDataSize:%lu"           ,0, glbDataSize );
    DBG_MSG(MPI_COMM_WORLD, "dcpFileId:%d"             ,0, dcpFileId );
    DBG_MSG(MPI_COMM_WORLD, "dcpBlockSizeStored:%lu"    ,0, dcpBlockSizeStored );
    DBG_MSG(MPI_COMM_WORLD, "Exec.nbVar:%d"            ,0, Exec.nbVar );

    snprintf( fn, BUFF, "%s/dcp-id%d-rank%d.fti", Exec.id, dcpFileId, Exec.commRank );
   
    // read base part of file
    FILE* fd = fopen( fn, "rb" );
    for(i=0; i<Exec.nbVar; i++) {
        unsigned int varId;
        unsigned long locDataSize;
        fread( &varId, sizeof(int), 1, fd );
        DBG_MSG(Exec.comm,"varId:%d",0,varId);
        fread( &locDataSize, sizeof(unsigned long), 1, fd );
        int idx = getIdx(varId);
        if( idx < 0 ) {
            ERR_MSG( Exec.comm, "id '%d' does not exist!", Exec.commRank, varId );
            return NSCS;
        }
        fread( Data[idx].ptr, locDataSize, 1, fd );
    }

    unsigned long pos = ftell( fd );
    
    DBG_MSG( MPI_COMM_WORLD, "pos:%lu"            ,0, pos );
    
    unsigned long offset;

    blockMetaInfo_t blockMeta;
    unsigned char *block = (unsigned char*) malloc( dcpBlockSizeStored );

    while( pos < Exec.dcp.dcpFileSize ) {
        fread( &blockMeta, 6, 1, fd );
        int idx = getIdx(blockMeta.varId);
        if( idx < 0 ) {
            ERR_MSG( Exec.comm, "id '%d' does not exist!", Exec.commRank, blockMeta.varId );
            return NSCS;
        }
        
        
        offset = blockMeta.blockId * dcpBlockSizeStored;
        void* ptr = &Data[idx] + offset;
        unsigned int chunkSize = ( (Data[idx].size-offset) < dcpBlockSizeStored ) ? Data[idx].size-offset : dcpBlockSizeStored; 

        fread( ptr, chunkSize, 1, fd );
        if( chunkSize < dcpBlockSizeStored ) {
            fread( block, dcpBlockSizeStored-chunkSize, 1, fd );
        }
        
        pos += (dcpBlockSizeStored+6);
        DBG_MSG( MPI_COMM_WORLD, "varId:%d blockId: %d, chunkSize:%lu, fileSize: %lu, pos: %lu",0, blockMeta.varId, blockMeta.blockId, chunkSize, Exec.dcp.dcpFileSize, pos );
    }

    fclose(fd);
}


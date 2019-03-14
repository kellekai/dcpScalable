#include "dcp_lib.h"

//----------------------------------------------------------------------------------------------
// LOCAL LIBRARY VARIABLES
//----------------------------------------------------------------------------------------------

static dataInfo Data[BUFF];
static confInfo Conf;
static execInfo Exec;

int getIdx( int varId, dataInfo *Data )
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
    Conf.dcpBlockSize = 16384;

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
   
    bool update = false;
    int i;
    for( i=0; i<Exec.nbVar; i++) {
        if( id == Data[i].id ) {
            //DBG_MSG( Exec.comm, "variable id '%d' will be updated.", Exec.commRank, id );
            update = true;
            break;
        }
    }
    
    Data[i].elemSize = elemSize;
    Data[i].id = id;    
    Data[i].nElem = nElem;
    Data[i].ptr = ptr;
    Data[i].size = elemSize*nElem;

    DBG_MSG(Exec.comm, "id: %d, size: %lu, ptr: %p", 0, id, elemSize*nElem, ptr);
    if( !update ) {
        Data[i].hashDataSize = 0;
        Data[i].hashArray = NULL;
        Exec.nbVar++;
    }
    
    return SCES;
}

int checkpoint( int id )
{
    MPI_Barrier(Exec.comm);
    double t1 = MPI_Wtime();
    
    if( id < 0 ) {
        ERR_MSG( Exec.comm, "invalid ID '%d'. ID's have to be positive.", Exec.commRank, id );
        return NSCS;
    }
    
    // dcpFileId increments every dcpStackSize checkpoints.
    int dcpFileId = Exec.dcp.dcpCounter / Conf.dcpStackSize;

    // dcpLayer corresponds to the additional layers towards the base layer.
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
            
            unsigned long dcpOld = Exec.dcp.dcpFileSize;
            // hash index
            unsigned int blockId = pos/Conf.dcpBlockSize;
            unsigned int hashIdx = blockId*Conf.digestWidth;
            unsigned int intIdx = pos/sizeof(int);
            
            blockMeta.blockId = blockId;

            unsigned int chunkSize = ( (dataSize-pos) < Conf.dcpBlockSize ) ? dataSize-pos : Conf.dcpBlockSize;
           
            // compute hashes
            if( chunkSize < Conf.dcpBlockSize ) {
                // if block smaller pad with zeros
                memset( block, 0x0, Conf.dcpBlockSize );
                memcpy( block, ptr, chunkSize );
                Conf.hashFunc( block, Conf.dcpBlockSize, &Data[i].hashArrayTmp[hashIdx] );
                ptr = block;
                chunkSize = Conf.dcpBlockSize;
                //DBG_MSG(Exec.comm, "yepp",0);
            } else {
                Conf.hashFunc( ptr, Conf.dcpBlockSize, &Data[i].hashArrayTmp[hashIdx] );
                char hashstring[Conf.digestWidth*2+1];
                //if(i==1)//DBG_MSG(Exec.comm, "ptr: %p, hash: %s", 0, ptr, hashHex(&Data[i].hashArrayTmp[hashIdx], Conf.digestWidth, hashstring));
            }
            
            bool commitBlock;
            // if old hash exists, compare. If datasize increased, there wont be an old hash to compare with.
            if( pos < Data[i].hashDataSize ) {
                commitBlock = memcmp( &Data[i].hashArray[hashIdx], &Data[i].hashArrayTmp[hashIdx], Conf.digestWidth );
                //if(i==1) //DBG_MSG(Exec.comm, "hash compare, commitBlock:%d, Data[%d]:%d", 0, commitBlock, intIdx, ((int*)Data[i].ptr)[intIdx] );
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
                //DBG_MSG(Exec.comm, "dcpFileSize (delta: %lu): %lu", 0, Exec.dcp.dcpFileSize-dcpOld, Exec.dcp.dcpFileSize );
            }
            
            pos += chunkSize*success;
            ptr = Data[i].ptr + pos; //chunkSize*success;
           
        }

        // swap hash arrays and free old one
        free(Data[i].hashArray);
        Data[i].hashDataSize = dataSize;
        Data[i].hashArray = Data[i].hashArrayTmp;

    }

    free(block);

    fsync(fileno(fd));
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
    MPI_Barrier(Exec.comm);
    double t2 = MPI_Wtime();
   
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

    MPI_Barrier(Exec.comm);
    if(Exec.commRank==0)
        printf("[INFO] Checkpoint (id:%d) succeeded (written %8lu of %8lu | file size:%8lu | time: %lf seconds.)\n", id, dcpSize, glbDataSize, Exec.dcp.dcpFileSize, t2-t1);

}

int recover()
{
    int ii;
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
        int idx = getIdx( varId, Data );
        if( idx < 0 ) {
            ERR_MSG( Exec.comm, "id '%d' does not exist!", Exec.commRank, varId );
            return NSCS;
        }
        //DBG_MSG(MPI_COMM_WORLD, "varId: %d, Data[%d].id:%d"  ,0, varId, idx, Data[idx].id );
        fread( &Data[idx].size, sizeof(unsigned long), 1, mfd );
        //DBG_MSG(MPI_COMM_WORLD, "Data[%d].size:%lu"  ,0, idx, Data[i].size );
    }
    fclose(mfd);
    
    //DBG_MSG(MPI_COMM_WORLD, "Exec.dcp.dcpFileSize:%lu"  ,0, Exec.dcp.dcpFileSize );
    //DBG_MSG(MPI_COMM_WORLD, "glbDataSize:%lu"           ,0, glbDataSize );
    //DBG_MSG(MPI_COMM_WORLD, "dcpFileId:%d"             ,0, dcpFileId );
    //DBG_MSG(MPI_COMM_WORLD, "dcpBlockSizeStored:%lu"    ,0, dcpBlockSizeStored );
    //DBG_MSG(MPI_COMM_WORLD, "Exec.nbVar:%d"            ,0, Exec.nbVar );

    snprintf( fn, BUFF, "%s/dcp-id%d-rank%d.fti", Exec.id, dcpFileId, Exec.commRank );
   
    // read base part of file
    FILE* fd = fopen( fn, "rb" );
    for(i=0; i<Exec.nbVar; i++) {
        unsigned int varId;
        unsigned long locDataSize;
        fread( &varId, sizeof(int), 1, fd );
        fread( &locDataSize, sizeof(unsigned long), 1, fd );
        int idx = getIdx(varId, Data);
        if( idx < 0 ) {
            ERR_MSG( Exec.comm, "id '%d' does not exist!", Exec.commRank, varId );
            return NSCS;
        }
        fread( Data[idx].ptr, locDataSize, 1, fd );
        int overflow;
        if( (overflow=locDataSize%dcpBlockSizeStored) != 0 ) {
            void *buffer = (void*) malloc( dcpBlockSizeStored - overflow ); 
            fread( buffer, dcpBlockSizeStored - overflow, 1, fd );
            free(buffer);
        }
    }
    
    unsigned long pos = ftell( fd );
    
    unsigned long offset;

    blockMetaInfo_t blockMeta;
    unsigned char *block = (unsigned char*) malloc( dcpBlockSizeStored );

    //DBG_MSG(Exec.comm,"pos:%lu",0,pos);
    unsigned long check = pos;
    unsigned long check_exp = pos;
    while( pos < Exec.dcp.dcpFileSize ) {
        
        //DBG_MSG(Exec.comm,"pos:%lu, check:%lu, exp: %lu",0,pos, check, check_exp);
        check += dcpBlockSizeStored+6;
        check_exp += fread( &blockMeta, 6, 1, fd )*6;


        int idx = getIdx(blockMeta.varId, Data);
        if( idx < 0 ) {
            ERR_MSG( Exec.comm, "id '%d' does not exist!", Exec.commRank, blockMeta.varId );
            return NSCS;
        }
        
        
        offset = blockMeta.blockId * dcpBlockSizeStored;
        void* ptr = Data[idx].ptr + offset;
        unsigned int chunkSize = ( (Data[idx].size-offset) < dcpBlockSizeStored ) ? Data[idx].size-offset : dcpBlockSizeStored; 

        //DBG_MSG(Exec.comm, "Data[0].id:%d, offset:%lu, chunksize: %lu", 0, Data[0].id, offset, chunkSize);
        check_exp += chunkSize*fread( ptr, chunkSize, 1, fd );
        if( chunkSize < dcpBlockSizeStored ) {
            check_exp += (dcpBlockSizeStored-chunkSize)*fread( block, dcpBlockSizeStored-chunkSize, 1, fd );
        }
        unsigned long posOld = pos;
        pos += (dcpBlockSizeStored+6);
        //DBG_MSG(Exec.comm, "pos (delta: %lu): %lu", 0, pos-posOld, pos );
        //DBG_MSG( MPI_COMM_WORLD, "varId:%d blockId: %d, chunkSize:%lu, fileSize: %lu, pos: %lu",0, blockMeta.varId, blockMeta.blockId, chunkSize, Exec.dcp.dcpFileSize, pos );
    }

    fclose(fd);
}


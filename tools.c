#include "dcp_lib.h"

unsigned long timestamp() 
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (unsigned long) (ts.tv_sec*1000L + ts.tv_nsec/1000000);
}

// have the same for for MD5 and CRC32
unsigned char* CRC32( const unsigned char *d, unsigned long nBytes, unsigned char *hash )
{
    static unsigned char hash_[CRC32_DIGEST_LENGTH];
    if( hash == NULL ) {
        hash = hash_;
    }
    
    uint32_t digest = crc32( 0L, Z_NULL, 0 );
    digest = crc32( digest, d, nBytes );

    memcpy( hash, &digest, CRC32_DIGEST_LENGTH );

    return hash;
}

char* hashHex( const unsigned char* hash, int digestWidth, char* hashHexStr )
{       
    if( hashHexStr == NULL ) {
        ERR_MSG( MPI_COMM_WORLD, "'hashHexStr == NULL'", rank );
        return NULL;
    }

    int i;
    for(i = 0; i < digestWidth; i++) {
        sprintf(&hashHexStr[2*i], "%02x", hash[i]);
    }

    return hashHexStr;
}
void printConfiguration( confInfo Conf, execInfo Exec ) 
{
    printf(
            "## CONFIGURATION ##\n"
            "execution id: \t\t\t%s\n"
            "number of processes: \t\t%d\n"
            "number of processes per node: \t%d\n"
            "number of nodes: \t\t%d\n"
            "dcp hashing method: \t\t%s\n"
            "## CONFIGURATION ##\n",
            Exec.id, 
            Exec.commSize, 
            Exec.nodeSize,
            Exec.commSize / Exec.nodeSize,
            (Conf.digestWidth==MD5_DIGEST_LENGTH)?"MD5":"CRC32"
          );
}

int registerEnvironment( confInfo *Conf, execInfo *Exec ) 
{
    char * envString;
    if( (envString = getenv("DCP_HASH_METHOD")) != 0 ) {
        if( strcmp( envString, "MD5" ) == 0 ) {
            Conf->hashFunc = MD5;
            Conf->digestWidth = MD5_DIGEST_LENGTH;
        } else if( strcmp( envString, "CRC32" ) == 0 ) {
            Conf->hashFunc = CRC32;
            Conf->digestWidth = CRC32_DIGEST_LENGTH;
        } else {
            ERR_MSG( MPI_COMM_WORLD, "'DCP_HASH_METHOD' has to be either 'MD5' or 'CRC32'", -1 );
            return NSCS;
        }
    } else {
        Conf->hashFunc = MD5;
        Conf->digestWidth = MD5_DIGEST_LENGTH;
    }
    if( (envString = getenv("NODE_SIZE")) != 0 ) {
        if( Exec->commSize%atoi(envString) != 0 ) {
            ERR_MSG( MPI_COMM_WORLD, "Number of processes '%d' has to be a multiple of the nodesize '%d'", Exec->commRank, Exec->commSize, atoi(envString) );
            return NSCS;
        } else {
            Exec->nodeSize = atoi(envString);
        }
    } else {
        if( Exec->commSize%2 != 0 ) {
            ERR_MSG( MPI_COMM_WORLD, "Number of processes '%d' has to be a multiple of the nodesize '%d'", Exec->commRank, Exec->commSize, 2 );
            return NSCS;
        }
        Exec->nodeSize = 2;
    }
    return SCES;
}

#include <openssl/md5.h>
#include <zlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

#ifndef MD5_DIGEST_LENGTH
#   define MD5_DIGEST_LENGTH 16 // 128 bits
#endif
#define MD5_DIGEST_STRING_LENGTH 2*MD5_DIGEST_LENGTH // hex string representation
#ifndef CRC32_DIGEST_LENGTH
#   define CRC32_DIGEST_LENGTH 4  // 32 bits
#endif
#define CRC32_DIGEST_STRING_LENGTH 2*CRC32_DIGEST_LENGTH // hex string representation

#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

#define DBG_MSG(COMM,MSG,RANK,...) do { \
    int rank; \
    MPI_Comm_rank(COMM,&rank); \
    if ( rank == RANK ) \
        printf( "%s:%d[DEBUG-%d] " MSG "\n", __FILENAME__,__LINE__,rank, ##__VA_ARGS__); \
    if ( RANK == -1 ) \
        printf( "%s:%d[DEBUG-%d] " MSG "\n", __FILENAME__,__LINE__,rank, ##__VA_ARGS__); \
    fflush(stdout); \
} while (0)

#define ERR_MSG(COMM,MSG,RANK,...) do { \
    int rank; \
    MPI_Comm_rank(COMM,&rank); \
    if ( rank == RANK ) \
        printf( "%s:%d[ERROR-%d] " MSG "\n", __FILENAME__,__LINE__,rank, ##__VA_ARGS__); \
    if ( RANK == -1 ) \
        printf( "%s:%d[ERROR-%d] " MSG "\n", __FILENAME__,__LINE__,rank, ##__VA_ARGS__); \
    fflush(stdout); \
} while (0)

#define ERR_EXT(COMM,MSG,RANK,...) do { \
    int rank; \
    MPI_Comm_rank(COMM,&rank); \
    if ( rank == RANK ) \
        printf( "%s:%d[ERROR-%d] " MSG "\n", __FILENAME__,__LINE__,rank, ##__VA_ARGS__); \
    if ( RANK == -1 ) \
        printf( "%s:%d[ERROR-%d] " MSG "\n", __FILENAME__,__LINE__,rank, ##__VA_ARGS__); \
    fflush(stdout); \
    MPI_Abort( COMM, -1 ); \
} while (0)

#define BUFF 512
#define MAX_BLOCK_IDX 0x3fffffff
#define MAX_VAR_ID 0x3ffff
#define BLOCK_META_LENGTH 48
#define BLOCK_IDX_OFFSET 18 

// TYPES

typedef struct blockMetaInfo_t
{
    unsigned long varId : 18;
    unsigned long blockId : 30;
} blockMetaInfo_t;

typedef struct MSTRM
{
    bool allocated;
    void* basePtr;
    void* pos;
    size_t length;
} MSTRM;

typedef struct confInfo 
{
    unsigned int digestWidth;
    unsigned char* (*hashFunc)( const unsigned char *data, unsigned long nBytes, unsigned char *hash );
    unsigned int dcpStackSize;
    unsigned long dcpBlockSize;
} confInfo;

typedef struct dcpInfo
{
    int dcpCounter;
    unsigned long dcpFileSize;
} dcpInfo;

typedef struct execInfo
{
    char id[BUFF];
    int nbVar;
    MPI_Comm comm;
    MPI_Comm nodeComm;
    int commSize;
    int commRank;
    int nodeSize;
    int nodeId;
    struct dcpInfo dcp;
} execInfo;

typedef struct dataInfo 
{
    int id;
    size_t elemSize;
    size_t size;
    size_t nElem;
    size_t hashDataSize;
    void *ptr;
    unsigned char *hashArray;
    unsigned char *hashArrayTmp;
} dataInfo;

typedef struct profInfo
{
    size_t hashArrayCur;
    size_t hashArrayCmp;
    size_t metaFileIdx;
} profInfo;

// HELPER FUNCTIONS

char* hashHex( const unsigned char* hash, int digestWidth, char* hashHexStr );
unsigned char* CRC32( const unsigned char *d, unsigned long nBytes, unsigned char *hash );
int registerEnvironment( confInfo * Conf, execInfo * Exec );
void printConfiguration( confInfo Conf, execInfo Exec );
unsigned long timestamp();
MSTRM* mcreate( void** ptr, size_t size );
size_t madd( void* ptr, size_t size, size_t nmemb, MSTRM* mstream );
void* mseek( MSTRM* mstream, size_t offset );
int mdestroy( MSTRM* mstream );

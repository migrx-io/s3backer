#ifndef _SWAL_H_
#define _SWAL_H_

#include <stdint.h>
#include <stdio.h>

#define SWAL_ERR_MALLOC_FAIL  -100
#define SWAL_ERR_META_OPEN_FAIL  -101
#define SWAL_ERR_LOG_OPEN_FAIL  -102
#define SWAL_ERR_MISMATCH_LOG_SIZE  -103
#define SWAL_ERR_INVALID_OFFSET  -104
#define SWAL_ERR_FLUSH_EXISTS     -4

#if defined(__cplusplus)
extern "C"
{
#endif
    typedef struct swal_options_t
    {
            int create_ifnotexist;
            size_t max_file_size;
            int debug;
            const char* log_prefix;
    } swal_options_t;
    swal_options_t* swal_options_create(void);
    void swal_options_destroy(swal_options_t* options);

    typedef struct swal_t swal_t;
    typedef size_t swal_replay_logfunc(
            const void* log, 
            size_t loglen, 
            uint64_t offset);

    int swal_open(
        const char* dir,
        const swal_options_t* options,
        swal_t** wal,
        swal_replay_logfunc* replay_cb
    );

    int swal_append(
        swal_t* wal,
        const void* log,
        size_t loglen,
        uint64_t offset
    );

    int swal_sync(swal_t* wal);
    int swal_replay(swal_t* wal);
    int swal_close(swal_t* wal);

#if defined(__cplusplus)
}
#endif
#endif /* _SWAL_H_ */

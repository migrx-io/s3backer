#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <time.h>
#include <stdarg.h>
#include <sys/uio.h>
#include "swal.h"

#define SWAL_FLUSH_WAIT_MS 100     // wait 100ms between checks
#define SWAL_FLUSH_WAIT_MAX 5000   // max total wait 5s
#define SWAL_FLUSH_MS 30000

struct swal_record_hdr {
    uint64_t offset;
    uint32_t size;
    uint32_t reserved;
} __attribute__((packed));

struct sync_arg {
    int fd;
    char path[1024];
    int result;
};

struct swal_t
{
    char* dir;
    swal_options_t options;

    int fd_hot;       // current hot write file
    char hot_path[1024];
    size_t hot_pos;
    struct timespec hot_last_write;

    int fd_flush;     // flush file being replayed
    char flush_path[1024];
    size_t flush_pos;

    swal_replay_logfunc* replay_cb;
    void* replay_data;

    pthread_t flush_thread;
    int flush_thread_running;

    pthread_mutex_t hot_lock;
    pthread_mutex_t flush_lock;

};

static void debug_log(swal_t *wal, const char *fmt, ...)
{
    if (!wal || !wal->options.debug)
        return;

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);

    char buf[64];
    struct tm tm;
    localtime_r(&ts.tv_sec, &tm);
    strftime(buf, sizeof(buf), "%H:%M:%S", &tm);

    fprintf(stderr, "[DEBUG]: %s.%03ld ", buf, ts.tv_nsec / 1000000);

    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);

    fprintf(stderr, "\n");
}

// -------------------- Options --------------------
swal_options_t* swal_options_create()
{
    swal_options_t* options = (swal_options_t*) malloc(sizeof(swal_options_t));
    memset(options, 0, sizeof(swal_options_t));
    options->create_ifnotexist = 1;
    options->max_file_size = 1ULL * 1024 * 1024 * 1024; // 1GB
    options->debug = 1;
    return options;
}

void swal_options_destroy(swal_options_t* options)
{
    if (options) free(options);
}

// -------------------- Helpers --------------------

static int open_hot_file(swal_t* wal)
{
    if (wal->fd_hot >= 0) return 0;

    sprintf(wal->hot_path, "%s/hot.log", wal->dir);

    debug_log(wal,"open hot_path %s", wal->hot_path);

    int fd = open(wal->hot_path, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    if (fd < 0)
    {
        fprintf(stderr, "[ERROR]: cannot open hot file '%s': %s\n", wal->hot_path, strerror(errno));
        return SWAL_ERR_LOG_OPEN_FAIL;
    }

    wal->fd_hot = fd;
    wal->hot_pos = 0;

    clock_gettime(CLOCK_REALTIME, &wal->hot_last_write);

    return 0;
}

// -------------------- Rotation --------------------

static int rotate_hot_to_flush(swal_t* wal)
{
    pthread_mutex_lock(&wal->hot_lock);

    if (wal->hot_pos == 0) {
        pthread_mutex_unlock(&wal->hot_lock);
        return 0; // nothing to rotate
    }

    sprintf(wal->flush_path, "%s/flush.log", wal->dir);
    if (access(wal->flush_path, F_OK) == 0) {
        pthread_mutex_unlock(&wal->hot_lock);
        return SWAL_ERR_FLUSH_EXISTS;
    }

    debug_log(wal,"rotate_hot_to_flush flush_path %s", wal->flush_path);

    // rename current hot file to flush file
    if (rename(wal->hot_path, wal->flush_path) < 0) {
        fprintf(stderr, "[ERROR]: cannot rename hot -> flush: %s\n", strerror(errno));
        pthread_mutex_unlock(&wal->hot_lock);
        return SWAL_ERR_LOG_OPEN_FAIL;
    }

    // now open new hot file
    int fd_hot = open(wal->hot_path, O_CREAT | O_RDWR | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fd_hot < 0) {
        fprintf(stderr, "[ERROR]: cannot create new hot file '%s': %s\n", wal->hot_path, strerror(errno));
        pthread_mutex_unlock(&wal->hot_lock);
        return SWAL_ERR_LOG_OPEN_FAIL;
    }

    wal->fd_hot = fd_hot;
    wal->hot_pos = 0;

    clock_gettime(CLOCK_REALTIME, &wal->hot_last_write);

    pthread_mutex_unlock(&wal->hot_lock);
    return 0;
}

// -------------------- Replay flush file --------------------

static void swal_replay_flush(swal_t* wal, swal_replay_logfunc func)
{
    debug_log(wal,"swal_replay_flush start..");

    pthread_mutex_lock(&wal->flush_lock);

    if (wal->fd_flush == -1 && access(wal->flush_path, F_OK) == 0) {
        wal->fd_flush = open(wal->flush_path, O_RDONLY);
        wal->flush_pos = 0;
    }

    if (wal->fd_flush != -1) {
        while (1) {
            struct swal_record_hdr hdr;

            ssize_t n = pread(
                wal->fd_flush,
                &hdr,
                sizeof(hdr),
                wal->flush_pos
            );

            if (n == 0) break;              // EOF
            if (n != sizeof(hdr)) break;   // partial / corrupt

            wal->flush_pos += sizeof(hdr);

            void *payload = malloc(hdr.size);
            if (!payload) break;

            n = pread(
                wal->fd_flush,
                payload,
                hdr.size,
                wal->flush_pos
            );

            if (n != (ssize_t)hdr.size) {
                free(payload);
                break;
            }

            wal->flush_pos += hdr.size;

            debug_log(
                wal,
                "replay record offset=%lu size=%u",
                hdr.offset,
                hdr.size
            );

            if (func && func(payload, hdr.size, hdr.offset) != 0) {
                free(payload);
                break;
            }

            free(payload);
        }

        off_t end = lseek(wal->fd_flush, 0, SEEK_END);
        if (wal->flush_pos == (size_t)end) {
            close(wal->fd_flush);
            wal->fd_flush = -1;
            wal->flush_pos = 0;
            unlink(wal->flush_path);
        }
    }

    debug_log(wal,"swal_replay_flush finish..");
    pthread_mutex_unlock(&wal->flush_lock);
}

// -------------------- Background flush thread --------------------
static void* flush_thread_func(void* arg)
{
    swal_t* wal = (swal_t*)arg;
    while (wal->flush_thread_running) {
        usleep(SWAL_FLUSH_MS * 1000);

        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);

        int rotate_needed = 0;

        pthread_mutex_lock(&wal->hot_lock);

        if (wal->hot_pos > 0) {
            rotate_needed = 1;
        }

        pthread_mutex_unlock(&wal->hot_lock);

        debug_log(wal,"flush_thread_func rotate_needed: %d", rotate_needed);

        if (rotate_needed) rotate_hot_to_flush(wal);

        // Replay flush log in background
        if (wal->replay_cb)
            swal_replay_flush(wal, wal->replay_cb);
    }
    return NULL;
}


// -------------------- Open / Close --------------------
int swal_open(const char* dir, const swal_options_t* options, swal_t** wal, swal_replay_logfunc* replay_cb)
{
    if (!dir || !options || !wal) return SWAL_ERR_MALLOC_FAIL;

    swal_t* w = (swal_t*) malloc(sizeof(swal_t));
    if (!w) return SWAL_ERR_MALLOC_FAIL;
    memset(w, 0, sizeof(swal_t));

    // Create WAL directory if it does not exist
    struct stat st;
    if (stat(dir, &st) != 0) {
        // Directory does not exist — try to create it
        if (mkdir(dir, 0755) != 0) {
            fprintf(stderr, "[ERROR]: cannot create WAL directory '%s': %s\n",
                    dir, strerror(errno));
            free(w->dir);
            free(w);
            return SWAL_ERR_LOG_OPEN_FAIL;
        }
    }


    w->fd_hot = -1;
    w->fd_flush = -1;
    w->flush_thread_running = 0;

    w->replay_cb = replay_cb;

    pthread_mutex_init(&w->hot_lock, NULL);
    pthread_mutex_init(&w->flush_lock, NULL);

    w->dir = strdup(dir);
    if (!w->dir)
    {
        free(w);
        return SWAL_ERR_MALLOC_FAIL;
    }

    w->options = *options;

    if (open_hot_file(w) != 0)
    {
        free(w->dir);
        free(w);
        return SWAL_ERR_LOG_OPEN_FAIL;
    }

    w->flush_thread_running = 1;
    if (pthread_create(&w->flush_thread, NULL, flush_thread_func, w) != 0) {
        fprintf(stderr, "[ERROR]: cannot create worker: %s\n", strerror(errno));
        w->flush_thread_running = 0;
    }


    *wal = w;
    return 0;
}

int swal_close(swal_t* wal)
{
    debug_log(wal,"swal_close..");

    if (!wal) return -1;

    wal->flush_thread_running = 0;
    pthread_join(wal->flush_thread, NULL);

    if (wal->fd_hot != -1) close(wal->fd_hot);
    if (wal->fd_flush != -1) close(wal->fd_flush);

    pthread_mutex_destroy(&wal->hot_lock);
    pthread_mutex_destroy(&wal->flush_lock);

    free(wal->dir);
    free(wal);
    return 0;
}

// -------------------- Append --------------------
int swal_append(swal_t* wal, const void* log, size_t loglen, uint64_t offset)
{
    debug_log(wal,"swal_append..");

    if (!wal || !log) return -1;

    struct swal_record_hdr hdr = {
        .offset = offset,
        .size   = (uint32_t)loglen,
        .reserved = 0
    };

    size_t record_size = sizeof(hdr) + loglen;

retry:
    pthread_mutex_lock(&wal->hot_lock);

    size_t remain = wal->options.max_file_size - wal->hot_pos;
    debug_log(wal,"swal_append remain %d", remain);

    if (remain < record_size) {
        pthread_mutex_unlock(&wal->hot_lock);

        int waited = 0;
        while (1) {
            usleep(SWAL_FLUSH_WAIT_MS * 1000);
            waited += SWAL_FLUSH_WAIT_MS;
            if (waited > SWAL_FLUSH_WAIT_MAX) {
                fprintf(stderr, "[ERROR]: hot file full, waited %d ms\n", waited);
                return SWAL_ERR_FLUSH_EXISTS;
            }

            pthread_mutex_lock(&wal->hot_lock);
            remain = wal->options.max_file_size - wal->hot_pos;
            pthread_mutex_unlock(&wal->hot_lock);

            if (remain >= record_size)
                goto retry;
        }
    }

    struct iovec iov[2] = {
        { .iov_base = &hdr,           .iov_len = sizeof(hdr) },
	{ .iov_base = (void *)(uintptr_t)log, .iov_len = loglen }
    };

    ssize_t n = writev(wal->fd_hot, iov, 2);
    if (n != (ssize_t)record_size) {
        pthread_mutex_unlock(&wal->hot_lock);
        return -1;
    }

    wal->hot_pos += record_size;
    clock_gettime(CLOCK_REALTIME, &wal->hot_last_write);

    pthread_mutex_unlock(&wal->hot_lock);

    debug_log(wal,"swal_append wrote record offset=%lu size=%zu",
              offset, loglen);

    return 0;
}

// -------------------- Replay --------------------

int swal_replay(swal_t* wal)
{
    if (!wal) return -1;

    // Replay flush file synchronously
    if (wal->replay_cb)
        swal_replay_flush(wal, wal->replay_cb);

    return 0;
}


static void* sync_thread(void* arg)
{
    struct sync_arg* a = (struct sync_arg*)arg;
    if (a->fd != -1) {
        if (fdatasync(a->fd) != 0) {
            fprintf(stderr, "[ERROR]: cannot fdatasync '%s': %s\n",
                    a->path, strerror(errno));
            a->result = -1;
        }
    }
    return NULL;
}


int swal_sync(swal_t* wal)
{
    if (!wal) return -1;

    debug_log(wal, "swal_sync start..");

    // Extract both fds WITHOUT syncing while holding locks
    struct sync_arg hot = { .fd = -1, .result = 0 };
    struct sync_arg flush = { .fd = -1, .result = 0 };

    pthread_mutex_lock(&wal->hot_lock);
    if (wal->fd_hot != -1) {
        hot.fd = wal->fd_hot;
        strcpy(hot.path, wal->hot_path);
    }
    pthread_mutex_unlock(&wal->hot_lock);

    pthread_mutex_lock(&wal->flush_lock);
    if (wal->fd_flush != -1) {
        flush.fd = wal->fd_flush;
        strcpy(flush.path, wal->flush_path);
    }
    pthread_mutex_unlock(&wal->flush_lock);

    // Launch parallel fsyncs
    pthread_t th1, th2;
    if (hot.fd != -1)
        pthread_create(&th1, NULL, sync_thread, &hot);
    if (flush.fd != -1)
        pthread_create(&th2, NULL, sync_thread, &flush);

    // Wait for both
    if (hot.fd != -1)
        pthread_join(th1, NULL);
    if (flush.fd != -1)
        pthread_join(th2, NULL);

    debug_log(wal, "swal_sync finish..");

    return (hot.result || flush.result) ? -1 : 0;
}

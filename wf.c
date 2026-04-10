#include "liburing.h"
#include <fcntl.h>
#include <liburing/io_uring.h>
#include <stdio.h>
#include <string.h>

#define MAX_ENTRIES 8192

int main(int argc, char **argv) {
    const char *filename = "test_file.txt";
    const char *buf = "this is a poem\n" "well not exactly...\n";

    struct io_uring ring;
    int flags = IORING_SETUP_SQPOLL | IORING_SETUP_SINGLE_ISSUER;
    int ret = 0;
    if ((ret = io_uring_queue_init(MAX_ENTRIES, &ring, flags))) {
        printf("init err: %s\n", strerror(-ret));
        return 1;
    }

    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    memset(sqe, 0, sizeof(*sqe));
    io_uring_prep_open(sqe, filename, O_CREAT | O_TRUNC | O_APPEND | O_WRONLY, 0644);

    if ((ret = io_uring_submit(&ring)) < 0) {
        printf("submit err: %s\n", strerror(-ret));
        return 1;
    }

    struct io_uring_cqe *cqe = NULL;
    if ((ret = io_uring_wait_cqe(&ring, &cqe))) {
        printf("wait err: %s\n", strerror(-ret));
        return 1;
    }
    io_uring_cqe_seen(&ring, cqe);

    int fd = cqe->res;
    if (fd < 0) {
        printf("open error: %s\n", strerror(-fd));
        return 0;
    }
    printf("Opened file: %d\n", fd);
    sqe = io_uring_get_sqe(&ring);
    io_uring_prep_write(sqe, fd, buf, strlen(buf), 0);
    sqe->flags |= IOSQE_IO_LINK;

    sqe = io_uring_get_sqe(&ring);
    io_uring_prep_close(sqe, fd);

    if ((ret = io_uring_submit(&ring)) < 0) {
        printf("submit err: %s\n", strerror(-ret));
        return 1;
    }

    if ((ret = io_uring_wait_cqe_nr(&ring, &cqe, 2))) {
        printf("wait err: %s\n", strerror(-ret));
        return 1;
    }
    io_uring_cqe_seen(&ring, cqe);
    io_uring_cqe_seen(&ring, cqe + 1);

    io_uring_queue_exit(&ring);
    return 0;
}

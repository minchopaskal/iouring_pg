#include "liburing.h"

#include "cmd_msg.h"

#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <bits/types/struct_timeval.h>
#include <liburing/io_uring.h>
#include <netinet/in.h>

#include <string>
#include <thread>
#include <unistd.h>
#include <vector>
#include <queue>
#include <unordered_map>

/*-------------------------------------------------- */
/* IO_Uring                                          */
/* ------------------------------------------------- */
using ioring = struct io_uring;

#define IO_CHECK(expr) \
    do { \
        int ret = 0; \
        if ((ret = (expr)) < 0) { \
            printf("error at %s:%d (%s): %s\n", __FILE__, __LINE__, #expr, strerror(-ret)); \
            return -ret; \
        } \
    } while (false);

#define FATAL_CHECK(expr) \
    do { \
        int ret = 0; \
        if ((ret = (expr)) < 0) { \
            printf("error at %s:%d (%s): %s\n", __FILE__, __LINE__, #expr, strerror(-ret)); \
            exit(1); \
        } \
    } while (false);

#define MAX_ENTRIES 8192

#define REQTYPE_ACCEPT 1
#define REQTYPE_READ 2
#define REQTYPE_WRITE 3
#define REQTYPE_CLOSE 4
struct IOUringReq {
    int reqType;
    int clientid;
};

uint64_t reqToUserData(IOUringReq r) {
    return r.reqType | (uint64_t(r.clientid) << 32);
}

int getReqType(uint64_t r) {
    return (r & 0xffffffff);
}

int getClientId(uint64_t r) {
    return (r >> 32);
}

struct io_uring_sqe *getSqe(ioring *ring) {
    auto sqe = io_uring_get_sqe(ring);
    while (sqe == nullptr) {
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(1ms);
        sqe = io_uring_get_sqe(ring);
    }

    return sqe;
}

/*-------------------------------------------------- */
/* Database                                          */
/* ------------------------------------------------- */
struct Data {
    uint8_t *data;
    uint64_t len;
};

/*-------------------------------------------------- */
/* Request                                           */
/* ------------------------------------------------- */
#define REPLTYPE_OK 1
#define REPLYTYPE_VALUE 2
struct ReplyMsg {
    uint8_t msgtype;
    char *data;
    uint64_t datalen;
};

static constexpr char ok[] = { (char)REPLTYPE_OK };
const char *prepMsg(ReplyMsg msg, bool *allocated) {
    switch (msg.msgtype) {
    case REPLTYPE_OK:
        *allocated = false;
        return ok;
    case REPLYTYPE_VALUE: {
        *allocated = true;
        char *val = (char*)malloc(msg.datalen + 1);
        val[0] = REPLYTYPE_VALUE;
        memcpy(val+1, msg.data, msg.datalen);
        return val;
    }
    default:
        assert(false);
        break;
    }
    return nullptr;
}

/*-------------------------------------------------- */
/* Connection                                        */
/* ------------------------------------------------- */
struct connection {
    int fd;
};

struct buf {
    char *buf;
    int len;
    int consumed;
};

struct client {
    connection conn;

    buf reqbuf;
    /* wait* variables used when we need to read more from socket so we have
     * the full request */
    bool wait;
    uint64_t waitRem;
    uint64_t waitStart; /* start position of request we are waiting for */

    buf replybuf;
    uint64_t writeRem;
};

struct srv {
    std::vector<client> clients;
    std::queue<int> clientpool;

    /* We are just a hashmap :)) */
    std::unordered_map<std::string, Data> db;
} server;

int setupRecv(ioring *ring, int cid) {
    client *c = &server.clients[cid];

    /* make sure we have enough space for the header */
    if (!c->wait && c->reqbuf.len - c->reqbuf.consumed < 11) {
        c->reqbuf.consumed = 0;
    }
    if (c->wait && c->waitRem > (c->reqbuf.len - c->reqbuf.consumed)) {
        c->reqbuf.len += c->waitRem - (c->reqbuf.len - c->reqbuf.consumed);
        c->reqbuf.buf = (char*)realloc(c->reqbuf.buf, c->reqbuf.len);
    }

    auto sqe = getSqe(ring);
    int recvflags = 0;
    io_uring_prep_recv(sqe, c->conn.fd, c->reqbuf.buf + c->reqbuf.consumed, c->reqbuf.len - c->reqbuf.consumed, recvflags);
    sqe->user_data = reqToUserData(IOUringReq{.reqType=REQTYPE_READ, .clientid=cid});
    return io_uring_submit(ring);
}

int setupWrite(ioring *ring, int cid, const char *buf, int buflen) {
    client *c = &server.clients[cid];

    if (c->writeRem == 0) {
        buflen += sizeof(int);
        c->writeRem = buflen;
        if (buflen > c->replybuf.len - c->replybuf.consumed) {
            c->replybuf.len = buflen;
            c->replybuf.buf = (char*)realloc(c->replybuf.buf, c->replybuf.len);
            c->replybuf.consumed = 0;
        }
        assert(c->replybuf.len - c->replybuf.consumed >= buflen);
        memcpy(c->replybuf.buf + c->replybuf.consumed, &buflen, sizeof(int));
        memcpy(c->replybuf.buf + c->replybuf.consumed + sizeof(int), buf, buflen - sizeof(int));
    }

    auto sqe = getSqe(ring);
    io_uring_prep_write(sqe, c->conn.fd, c->replybuf.buf + c->replybuf.consumed, c->writeRem, 0);
    sqe->user_data = reqToUserData(IOUringReq{.reqType=REQTYPE_WRITE, .clientid=cid});
    return io_uring_submit(ring);
}

int setupConnection(ioring *ring, int fd) {
    client c;
    c.conn.fd = fd;
 
    c.reqbuf.buf = (char*)malloc(16 * 1024);
    c.reqbuf.len = 16 * 1024;
    c.reqbuf.consumed = 0;
    c.wait = false;
    c.waitRem = 0;
    c.waitStart = 0;
    c.writeRem = 0;

    c.replybuf.buf = (char*)malloc(16 * 1024);
    c.replybuf.len = 16 * 1024;
    c.replybuf.consumed = 0;

    int cid = -1;
    if (server.clientpool.empty()) {
        cid = server.clients.size();
        server.clients.push_back(c);
    } else {
        cid = server.clientpool.front();
        server.clientpool.pop();
        server.clients[cid] = c;
    }

    printf("new client #%d on fd %d\n", cid, fd);

    /* TODO: multishot recv */
    return setupRecv(ring, cid);
}

int setupSocket(int port) {
    int sock;
    struct sockaddr_in srv_addr;

    FATAL_CHECK(sock = socket(AF_INET, SOCK_STREAM, 0));

    int enable = 1;
    FATAL_CHECK(setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)));

    memset(&srv_addr, 0, sizeof(srv_addr));
    srv_addr.sin_family = AF_INET;
    srv_addr.sin_port = htons(port);
    srv_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    FATAL_CHECK(bind(sock, (const struct sockaddr *)&srv_addr, sizeof(srv_addr)));
    FATAL_CHECK(listen(sock, 10));
    return sock;
}

int setupMultishotAccept(ioring *ring, int socketfd) {
    struct sockaddr_in addr;
    socklen_t client_addr_len = sizeof(addr);
    int acceptflags = 0;
    auto sqe = getSqe(ring);
    io_uring_prep_multishot_accept(sqe, socketfd, (struct sockaddr*)&addr, &client_addr_len, acceptflags);
    sqe->user_data = reqToUserData(IOUringReq{.reqType = REQTYPE_ACCEPT});
    return io_uring_submit(ring);
}

void dbSet(const std::string &key, char *buf, uint64_t buflen) {
    Data data;
    data.data = (uint8_t*)malloc(buflen);
    data.len = buflen;
    memcpy(data.data, buf, buflen);
    if (auto it = server.db.find(key); it != server.db.end()) {
        free(it->second.data);
        it->second = data;
    } else {
        server.db.insert({key, data});
    }
}

bool dbGet(const std::string &key, Data *res) {
    if (auto it = server.db.find(key); it != server.db.end()) {
        *res = it->second;
        return true;
    }
    return false;
}

int main(int argc, char **argv) {
    if (argc != 2) {
        printf("Usage: %s <port>\n", argv[0]);
        return 1;
    }

    /* Setup the socket we'll listen on */
    int port = std::stoi(argv[1]);
    int socketfd = setupSocket(port);

    /* Init io uring */
    ioring ring;
    int ringflags = IORING_SETUP_SQPOLL | IORING_SETUP_SINGLE_ISSUER;
    IO_CHECK(io_uring_queue_init(MAX_ENTRIES, &ring, ringflags));

    /* Setup multishot accept. We wont need to setup accept after each connection
     * unless kernel disabled multishot accept after long time with no new connections */
    IO_CHECK(setupMultishotAccept(&ring, socketfd));

    /* Main loop */
    while (true) {
        // before sleep...
        struct __kernel_timespec ts;
        ts.tv_nsec = 1000000; /* 1ms */
        ts.tv_sec = 0;
        struct io_uring_cqe *cqe = nullptr;
        int ret = io_uring_wait_cqes(&ring, &cqe, 1024, &ts, nullptr);
        // after sleep...
        if (ret == -ETIME) {
            continue;
        }
        IO_CHECK(ret);

        unsigned head = 0;
        int i = 0;
        io_uring_for_each_cqe(&ring, head, cqe) {
            int cid = getClientId(cqe->user_data);
            client *c = nullptr;
            if (cid >= 0 && cid < server.clients.size()) {
                c = &server.clients[cid];
            }

            switch (getReqType(cqe->user_data)) {
            case REQTYPE_ACCEPT:
                if (cqe->res < 0) {
                    IO_CHECK(setupMultishotAccept(&ring, socketfd));
                    break;
                }
                IO_CHECK(setupConnection(&ring, cqe->res));
                if (!(cqe->flags & IORING_CQE_F_MORE)) {
                    IO_CHECK(setupMultishotAccept(&ring, socketfd));
                }
                break;
            case REQTYPE_READ: {
                if (cqe->res <= 0) {
                    if (cqe->res < 0) {
                        printf("Error %s for client #%d on fd %d\n", strerror(-cqe->res), cid, c->conn.fd);
                    } else {
                        printf("EOF for client #%d on fd %d\n", cid, c->conn.fd);
                    }
                    printf("Closing the client...\n");
                    auto sqe = getSqe(&ring);
                    io_uring_prep_close(sqe, c->conn.fd);
                    sqe->user_data = reqToUserData(IOUringReq{.reqType = REQTYPE_CLOSE, .clientid=cid});
                    IO_CHECK(io_uring_submit(&ring));
                    break;
                }

                c->reqbuf.consumed += cqe->res;

                /* If we are waiting for data and we havent received all the data we are waiting for
                 * just setup a new recv and conitnue... */
                if (c->wait && c->waitRem > 0 && cqe->res < c->waitRem) {
                    c->waitRem -= cqe->res;
                    IO_CHECK(setupRecv(&ring, cid));
                    break;
                }

                char *buf = c->reqbuf.buf + c->reqbuf.consumed - cqe->res;
                uint64_t len = cqe->res;
                if (c->wait) {
                    assert(c->waitRem == 0 || c->waitRem <= cqe->res);
                    buf = c->reqbuf.buf + c->waitStart;
                    len = c->reqbuf.consumed - c->waitStart;
                    c->wait = false;
                    c->waitRem = 0;
                    c->waitStart = 0;
                }

                uint8_t type = *buf;
                buf += sizeof(uint8_t);
                len -= sizeof(uint8_t);
                uint16_t keylen = *(uint16_t*)buf;
                buf += sizeof(uint16_t);
                len -= sizeof(uint16_t);
                uint64_t vallen = 0;
                if (type == MSGTYPE_SET) {
                    vallen = *(uint64_t*)buf;
                    buf += sizeof(uint64_t);
                    len -= sizeof(uint64_t);
                }

                /* Header is now read fully */

                /* if remaining len is not enough to read the key and value, setup the
                 * wait mechanism */
                if (len < keylen + vallen) {
                    c->wait = true;
                    c->waitStart = c->reqbuf.consumed - cqe->res;
                    c->waitRem = (keylen + vallen) - len;
                    IO_CHECK(setupRecv(&ring, cid));
                    break;
                }

                std::string key {buf, buf + keylen};
                buf += keylen;
                if (type == MSGTYPE_GET) {
                    Data res;
                    if (dbGet(key, &res)) {
                        IO_CHECK(setupWrite(&ring, cid, (const char*)res.data, res.len));
                    } else {
                        IO_CHECK(setupWrite(&ring, cid, "nil", 3));
                    }
                } else if (type == MSGTYPE_SET) {
                    dbSet(key, buf, vallen);
                    IO_CHECK(setupWrite(&ring, cid, "ok", 2));
                } else {
                    assert(false);
                }

                break;
            }
            case REQTYPE_WRITE: {
                if (cqe->res < 0) {
                    auto sqe = getSqe(&ring);
                    io_uring_prep_close(sqe, c->conn.fd);
                    sqe->user_data = reqToUserData(IOUringReq{.reqType = REQTYPE_CLOSE, .clientid=cid});
                    IO_CHECK(io_uring_submit(&ring));
                    break;
                }
                c->replybuf.consumed += cqe->res;
                c->writeRem -= cqe->res;
                assert(c->replybuf.consumed <= c->replybuf.len);

                if (c->writeRem == 0) {
                    c->replybuf.consumed = 0;
                    /* We needed to extend the buffer. Let's shrink it back to
                     * save mem*/
                    if (c->replybuf.len > 16 * 1024) {
                        c->replybuf.len = 16 * 1024;
                        c->replybuf.buf = (char*)realloc(c->replybuf.buf, 16 * 1024);
                    }
                    IO_CHECK(setupRecv(&ring, cid));
                } else {
                    assert(c->writeRem > 0);
                    IO_CHECK(setupWrite(&ring, cid, nullptr, 0)); /* internally deals with writeRem */
                }
                break;
            }
            case REQTYPE_CLOSE: {
                printf("Client #%d closed.\n", cid);
                server.clientpool.push(cid);
                c->conn.fd = -1;
                free(c->reqbuf.buf);
                free(c->replybuf.buf);
                break;
            }
            default:
                assert(false);
                break;
            }

            ++i;
        }
        io_uring_cq_advance(&ring, i);
    }

    io_uring_queue_exit(&ring);
}


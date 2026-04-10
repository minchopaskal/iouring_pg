#include "cmd_msg.h"

#include <asm-generic/errno-base.h>
#include <assert.h>
#include <errno.h>
#include <time.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <netdb.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <arpa/inet.h>

#define MAXDATASIZE 16 * 1024

char *cmdToBuf(cmdMsg msg, int *len) {
    int header = sizeof(uint8_t) + sizeof(uint16_t) + (msg.msgtype == MSGTYPE_SET ? sizeof(uint64_t) : 0);
    int datasz = msg.keylen + (msg.msgtype == MSGTYPE_SET ? msg.vallen : 0);
    char *buf = (char*)malloc(header + datasz);

    uint64_t offset = 0;
    memcpy(buf, &msg.msgtype, sizeof(uint8_t));
    offset += sizeof(uint8_t);
    memcpy(buf + offset, &msg.keylen, sizeof(uint16_t));
    offset += sizeof(uint16_t);
    if (msg.msgtype == MSGTYPE_SET) {
        memcpy(buf + offset, &msg.vallen, sizeof(uint64_t));
        offset += sizeof(uint64_t);
    }
    memcpy(buf + offset, msg.key, msg.keylen);
    offset += msg.keylen;

    if (msg.msgtype == MSGTYPE_SET) {
        memcpy(buf + offset, msg.val, msg.vallen);
    }

    *len = header + datasz;
    return buf;
}

void test(int fd) {
    const char *keys[] = {"KEY_A", "KEY_B", "KEY_C"};
    const char *vals[5] = {
        "A",
        "B",
        "C",
        "D",
    };
    char *bigcmd = (char*)malloc(512 * 1024); /* some big ass value to test the partial read/write logic*/
    memset(bigcmd, 'Q', 512 * 1024);
    bigcmd[512 * 1024-1] = '\0'; /* strlen */
    vals[4] = bigcmd;

    int cmds[2] = {MSGTYPE_GET, MSGTYPE_SET};

    int len = 128;
    int pos = 0;
    int *lens = (int*)malloc(sizeof(int) * len);
    cmdMsg *msgs = (cmdMsg*)malloc(sizeof(cmdMsg) * len);
    char **cmdBufs = (char**)malloc(sizeof(char*) * len);
    memset(cmdBufs, 0, sizeof(char*)*len);

    srand(time(NULL));
    for (int i = 0; i < sizeof(keys)/sizeof(keys[0]); ++i) {
        int validx = rand() % (sizeof(vals)/sizeof(vals[0]));
        cmdMsg set = {
            .msgtype = MSGTYPE_SET,
            .key = keys[i],
            .keylen = strlen(keys[i]),
            .val = vals[validx],
            .vallen = strlen(vals[validx]),
        };
        msgs[pos] = set;
        cmdBufs[pos] = cmdToBuf(set, &lens[pos]);
        ++pos;
    }

    for (int i = 0; i < len - pos; ++i) {
        int cmd = rand() % 2;
        int key = rand() % (sizeof(keys)/sizeof(keys[0]));
        int val = rand() % (sizeof(vals)/sizeof(vals[0]));
        cmdMsg msg = {
            .msgtype = cmds[cmd],
            .key = keys[key],
            .keylen = strlen(keys[key]),
            .val = vals[val],
            .vallen = strlen(vals[val]),
        };
        msgs[pos] = msg;
        cmdBufs[pos] = cmdToBuf(msg, &lens[pos]);
        ++pos;
    }

    for (int j = 0; j < 100; ++j) {
        for (int i = 0; i < pos; ++i) {
            printf("Sending msg: %s for key %s (val: %s)\n", msgs[i].msgtype == MSGTYPE_SET ? "SET" : "GET", msgs[i].key,
                msgs[i].msgtype == MSGTYPE_SET ? (msgs[i].vallen > 100 ? "VERIBIG" : msgs[i].val) : "nil");
            int written = 0;
            int towrite = lens[i];
            while (written < towrite) {
                int res = write(fd, cmdBufs[i] + written, towrite - written);
                assert(res != 0);
                if (res < 0) {
                    printf("Connection error: %s\n", strerror(errno));
                    exit(1);
                }
                written += res;
            }

            int len = 16 * 1024;
            int nread = 0;
            char *buf = (char*)malloc(len);
            int curr = read(fd, buf, len);
            if (curr == 0) {
                printf("EOF from server... Connection closed...\n");
                exit(1);
            }
            if (curr < 0 && errno != EAGAIN) {
                printf("Connection error: %s\n", strerror(errno));
                exit(1);
            }
            if (curr < sizeof(int)) {
                printf("wtf\n");
                exit(1);
            }
            int toread = *((int*)buf) - sizeof(int);
            nread += (curr - sizeof(int));
            if (toread + sizeof(int) + 1 > len) {
                len = toread + sizeof(int) + 1;
                buf = (char*)realloc(buf, len);
            }
            while (nread < toread) {
                curr = read(fd, buf + nread, len - nread);
                if (curr == 0) {
                    printf("EOF from server(loop)... Connection closed...\n");
                    exit(1);
                }
                if (curr < 0 && errno != EAGAIN) {
                    printf("Connection error: %s\n", strerror(errno));
                    exit(1);
                }
                nread += curr;
            }
            assert(nread == toread);
            *(buf + sizeof(int) + toread) = '\0';
            printf("Resp: %s\n", toread > 100 ? "too big to show..." : buf + sizeof(int));

            free(buf);
        }
    }

    free(lens);
    free(bigcmd);
    free(msgs);
    for (int i = 0; i < len; ++i) {
        free(cmdBufs[i]);
    }
    free(cmdBufs);
}

int main(int argc, char **argv) {
    if (argc < 2) {
        printf("Usage: %s <srv_port>\n", argv[0]);
        return 1;
    }

    int sockfd, numbytes;
    struct addrinfo hints, *servinfo, *p;
    int rv;
    char s[INET_ADDRSTRLEN];

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    if ((rv = getaddrinfo("127.0.0.1", argv[1], &hints, &servinfo)) != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
        return 1;
    }

    for(p = servinfo; p != NULL; p = p->ai_next) {
        if ((sockfd = socket(p->ai_family, p->ai_socktype,
                p->ai_protocol)) == -1) {
            perror("client: socket");
            continue;
        }
        struct sockaddr_in *sa = (struct sockaddr_in *)p->ai_addr;
        inet_ntop(p->ai_family,
            &sa->sin_addr,
            s, sizeof s);
        printf("client: attempting connection to %s\n", s);

        if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
            perror("client: connect");
            close(sockfd);
            continue;
        }

        break;
    }

    if (p == NULL) {
        freeaddrinfo(servinfo); // all done with this structure
        fprintf(stderr, "client: failed to connect\n");
        return 2;
    }

    struct sockaddr_in *sa = (struct sockaddr_in *)p->ai_addr;
    inet_ntop(p->ai_family,
            &sa->sin_addr,
            s, sizeof s);
    printf("client: connected to %s\n", s);

    freeaddrinfo(servinfo); // all done with this structure

    test(sockfd);

    close(sockfd);

    return 0;
}

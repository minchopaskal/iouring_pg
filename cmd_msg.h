#pragma once

#include <stdint.h>

#define MSGTYPE_SET 1
#define MSGTYPE_GET 2
typedef struct {
    uint8_t msgtype;

    const char *key;
    uint16_t keylen;

    const char *val;
    uint64_t vallen;
} cmdMsg;


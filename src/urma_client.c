/* ==========================================================================
 * urma_client.c - URMA (umdk) transport for redis-cli / redis-benchmark
 * --------------------------------------------------------------------------
 * redis-server built with BUILD_URMA=yes accepts RDMA connections by first
 * establishing an out-of-band TCP channel, exchanging urma_rjetty_t over it
 * and then binding the URMA RC transport (see src/rdma.c).
 *
 * This module does the same from the client side and bridges the resulting
 * transport into hiredis through redisContextFuncs, so redis-cli and
 * redis-benchmark can talk the URMA transport transparently:
 *
 *   redisConnect(ip, port)          -> TCP channel (out-of-band)
 *   redisUrmaUpgrade(ctx, timeout)  -> URMA RC channel, context rewired
 *   normal hiredis read/write       -> URMA WRITE_IMM / RX buffer stream
 *
 * The data path mirrors the server side: streaming data is written onto the
 * peer registered RX buffer with WRITE_IMM (immediate data = byte count),
 * control commands are 64B SEND/RECV messages. When a RX buffer is fully
 * consumed, the owner re-registers a new one and re-announces it, which
 * resets the peer TX offset (see connRdmaImportRemoteRx in rdma.c).
 * ==========================================================================
 */

#include "urma_client.h"

#include <arpa/inet.h>
#include <stdio.h>
#include <unistd.h>

#if defined(USE_RDMA) && USE_RDMA == 1

#include <errno.h>
#include <poll.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <time.h>

#include <urma/urma_api.h>
#include <urma/urma_types.h>
#include <urma/urma_opcode.h>

/* The wire structures below must be kept in sync with src/rdma.c (server
 * side), both ends exchange them in raw form. */
#define VALKEY_BUILD_BUG_ON(cond) ((void)sizeof(char[1 - 2 * !!(cond)]))

#define VALKEY_RDMA_INVALID_OPCODE 0xffff
#define VALKEY_RDMA_JETTY_TOKEN 0xABCDEF

/* client resources are much smaller than the server ones */
#define URMA_CLI_MAX_WQE 128
/* jfc depth must cover jfs + jfr depths */
#define URMA_CLI_JFC_DEPTH (URMA_CLI_MAX_WQE * 2 * 2)
#define URMA_CLI_RX_SIZE (1024 * 1024)
#define URMA_CLI_WR_TIMEOUT_MS 10000

typedef enum ValkeyRdmaOpcode {
    GetServerFeature = 0,
    SetClientFeature = 1,
    Keepalive = 2,
    RegisterXferMemory = 3,
} ValkeyRdmaOpcode;

typedef struct ValkeyRdmaFeature {
    uint16_t opcode;
    uint16_t select;
    uint8_t rsvd[4];
    uint64_t features;
    uint8_t rsvd2[48];
} ValkeyRdmaFeature;

typedef struct ValkeyRdmaKeepalive {
    uint16_t opcode;
    uint8_t rsvd[62];
} ValkeyRdmaKeepalive;

typedef struct ValkeyRdmaMemory {
    uint16_t opcode;
    uint8_t rsvd[6];
    urma_seg_t seg;
    uint32_t token;
    uint8_t rsvd2[4];
} ValkeyRdmaMemory;

typedef union ValkeyRdmaCmd {
    ValkeyRdmaFeature feature;
    ValkeyRdmaKeepalive keepalive;
    ValkeyRdmaMemory memory;
} ValkeyRdmaCmd;

#ifndef MIN
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#endif

typedef struct urmaClient {
    urma_context_t *uctx;
    urma_jfce_t *jfce;
    urma_jfc_t *jfc;
    urma_jfr_t *jfr;
    urma_jetty_t *jetty;
    urma_target_jetty_t *tjetty;
    int bound;

    /* command buffers: 0 ~ WQE-1 recv, WQE ~ 2*WQE-1 send */
    ValkeyRdmaCmd *cmd_buf;
    urma_target_seg_t *cmd_tseg;

    /* RX: local buffer the server writes into */
    char *rx_addr;
    uint32_t rx_len, rx_off, rx_pos;
    uint32_t rx_token;
    urma_target_seg_t *rx_tseg;

    /* TX: staging buffer mirroring the remote RX buffer */
    char *tx_addr;
    uint32_t tx_len, tx_off;
    urma_target_seg_t *tx_tseg;
    urma_target_seg_t *remote_rx_tseg;
    urma_seg_t remote_rx_seg;
    uint64_t remote_addr;

    int eof; /* peer is gone */
    /* diagnostics of the last fatal CQ completion, reported via errstr */
    int last_cq_status, last_cq_opcode;
} urmaClient;

static long long urmaCliMstime(void) {
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void *page_aligned_zalloc(size_t size) {
    void *tmp;
    size_t aligned_size, page_size = sysconf(_SC_PAGESIZE);

    aligned_size = (size + page_size - 1) & (~(page_size - 1));
    if (posix_memalign(&tmp, page_size, aligned_size)) {
        return NULL;
    }

    memset(tmp, 0x00, aligned_size);
    return tmp;
}

/* wait for readability on the completion event fd */
static int urmaCliWaitEvent(urmaClient *uc, long long timeout_ms) {
    struct pollfd pfd = {.fd = uc->jfce->fd, .events = POLLIN, .revents = 0};
    int rc;

    if (timeout_ms < 0) timeout_ms = -1;
    rc = poll(&pfd, 1, (int)timeout_ms);
    if (rc <= 0) return 0;

    {
        urma_jfc_t *ev_jfc[1] = {NULL};
        return urma_wait_jfc(uc->jfce, 1, 0, ev_jfc) > 0;
    }
}

static int urmaCliPostRecv(urmaClient *uc, ValkeyRdmaCmd *cmd) {
    urma_jfr_wr_t wr = {0};
    urma_sge_t sge;
    urma_jfr_wr_t *bad_wr;
    int ret;

    sge.addr = (uint64_t)cmd;
    sge.len = sizeof(ValkeyRdmaCmd);
    sge.tseg = uc->cmd_tseg;
    sge.user_tseg = NULL;

    wr.src.sge = &sge;
    wr.src.num_sge = 1;
    wr.user_ctx = (uint64_t)cmd;
    wr.next = NULL;

    ret = urma_post_jetty_recv_wr(uc->jetty, &wr, &bad_wr);
    if (ret && ret != URMA_EAGAIN) {
        return -1;
    }

    return 0;
}

static int urmaCliSendCommand(urmaClient *uc, ValkeyRdmaCmd *cmd) {
    urma_jfs_wr_t wr = {0};
    urma_sge_t sge;
    urma_jfs_wr_t *bad_wr;
    int i;
    ValkeyRdmaCmd *_cmd = NULL;

    for (i = URMA_CLI_MAX_WQE; i < 2 * URMA_CLI_MAX_WQE; i++) {
        _cmd = uc->cmd_buf + i;
        if (_cmd->keepalive.opcode == VALKEY_RDMA_INVALID_OPCODE) break;
    }
    if (i == 2 * URMA_CLI_MAX_WQE) return -1; /* no free send buffer */

    memcpy(_cmd, cmd, sizeof(ValkeyRdmaCmd));
    sge.addr = (uint64_t)_cmd;
    sge.len = sizeof(ValkeyRdmaCmd);
    sge.tseg = uc->cmd_tseg;
    sge.user_tseg = NULL;

    wr.opcode = URMA_OPC_SEND;
    wr.tjetty = uc->tjetty;
    wr.user_ctx = (uint64_t)_cmd;
    wr.flag.bs.complete_enable = 1;
    wr.send.src.sge = &sge;
    wr.send.src.num_sge = 1;
    wr.next = NULL;

    if (urma_post_jetty_send_wr(uc->jetty, &wr, &bad_wr)) {
        return -1;
    }

    return 0;
}

/* announce a fresh RX buffer to the peer */
static int urmaCliRegisterRx(urmaClient *uc) {
    ValkeyRdmaCmd cmd = {0};

    cmd.memory.opcode = htons(RegisterXferMemory);
    cmd.memory.seg = uc->rx_tseg->seg;
    cmd.memory.token = htonl(uc->rx_token);

    uc->rx_off = 0;
    uc->rx_pos = 0;

    return urmaCliSendCommand(uc, &cmd);
}

/* import the RX buffer announced by the peer, resets the TX stream */
static int urmaCliImportRemoteRx(urmaClient *uc, ValkeyRdmaCmd *cmd) {
    urma_import_seg_flag_t flag = {0};
    urma_token_t token;

    if (uc->remote_rx_tseg) {
        urma_unimport_seg(uc->remote_rx_tseg);
        uc->remote_rx_tseg = NULL;
    }

    uc->remote_rx_seg = cmd->memory.seg;
    uc->remote_addr = uc->remote_rx_seg.ubva.va;
    fprintf(stderr, "URMA: import remote rx seg: va 0x%lx len %lu uasid %u token 0x%x\n",
            (unsigned long)uc->remote_rx_seg.ubva.va,
            (unsigned long)uc->remote_rx_seg.len,
            uc->remote_rx_seg.ubva.uasid, ntohl(cmd->memory.token));
    token.token = ntohl(cmd->memory.token);

    flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE;
    flag.bs.mapping = URMA_SEG_NOMAP;
    uc->remote_rx_tseg = urma_import_seg(uc->uctx, &uc->remote_rx_seg, &token, 0, flag);
    if (!uc->remote_rx_tseg) {
        return -1;
    }

    /* fresh remote buffer, restart the TX stream on a matching staging buf */
    uc->tx_off = 0;
    if (uc->tx_len != (uint32_t)uc->remote_rx_seg.len) {
        if (uc->tx_tseg) {
            urma_unregister_seg(uc->tx_tseg);
            uc->tx_tseg = NULL;
        }
        free(uc->tx_addr);
        uc->tx_addr = NULL;
        uc->tx_len = uc->remote_rx_seg.len;
        uc->tx_addr = page_aligned_zalloc(uc->tx_len);
        if (!uc->tx_addr) return -1;

        {
            urma_seg_cfg_t seg_cfg = {0};

            seg_cfg.va = (uint64_t)uc->tx_addr;
            seg_cfg.len = uc->tx_len;
            seg_cfg.token_id = NULL;
            seg_cfg.token_value.token = VALKEY_RDMA_JETTY_TOKEN;
            seg_cfg.flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE;
            uc->tx_tseg = urma_register_seg(uc->uctx, &seg_cfg);
            if (!uc->tx_tseg) return -1;
        }
    }

    return 0;
}

static int urmaCliHandleRecv(urmaClient *uc, ValkeyRdmaCmd *cmd) {
    switch (ntohs(cmd->keepalive.opcode)) {
    case GetServerFeature: {
        ValkeyRdmaCmd reply = {0};

        reply.feature.opcode = htons(GetServerFeature);
        /* currently no feature support, zero features in any byte order */
        if (urmaCliSendCommand(uc, &reply)) return -1;
        break;
    }
    case SetClientFeature: break;
    case Keepalive: break;
    case RegisterXferMemory:
        if (urmaCliImportRemoteRx(uc, cmd)) return -1;
        break;
    default:
        return -1;
    }

    return urmaCliPostRecv(uc, cmd);
}

/* poll the completion queue, returns 1 if something was processed,
 * 0 if idle (timeout), -1 on fatal error (uc->eof gets set) */
static int urmaCliPollCq(urmaClient *uc, long long timeout_ms) {
    urma_cr_t cr = {0};
    urma_jfc_t *ev_jfc[1] = {NULL};
    uint32_t nevents = 0;
    int polled = 0;
    int ret;

    if (!urmaCliWaitEvent(uc, timeout_ms)) return 0;

pollcq:
    ret = urma_poll_jfc(uc->jfc, 1, &cr);
    if (ret < 0) {
        uc->eof = 1;
        return -1;
    } else if (ret == 0) {
        /* confirm the jfc events and re-arm, the order is mandatory.
         * like urma_perftest, ack exactly 1 event per wait_jfc wakeup,
         * acking the polled CQE count over-acks and drops later events */
        nevents = 1;
        urma_ack_jfc(ev_jfc, &nevents, 1);
        if (urma_rearm_jfc(uc->jfc, false)) {
            uc->eof = 1;
            return -1;
        }
        return polled > 0;
    }

    polled++;
    if (cr.status != URMA_CR_SUCCESS) {
        /* keep the completion details for the error message */
        uc->last_cq_status = cr.status;
        uc->last_cq_opcode = cr.opcode;
        uc->eof = 1;
        return -1;
    }

    if (cr.flag.bs.s_r == 1) { /* receive side */
        switch (cr.opcode) {
        case URMA_CR_OPC_SEND:
        case URMA_CR_OPC_SEND_WITH_IMM:
            if (urmaCliHandleRecv(uc, (ValkeyRdmaCmd *)(uintptr_t)cr.user_ctx)) {
                uc->eof = 1;
                return -1;
            }
            break;
        case URMA_CR_OPC_WRITE_WITH_IMM:
            uc->rx_off += cr.completion_len;
            break;
        default:
            uc->eof = 1;
            return -1;
        }
    } else { /* send side: the driver does not fill cr.opcode for send CRs
              * (always URMA_CR_OPC_SEND), dispatch by user_ctx instead:
              * 0 is a void WRITE completion, otherwise a cmd SEND whose
              * buffer is recycled */
        if (cr.user_ctx) {
            ValkeyRdmaCmd *cmd = (ValkeyRdmaCmd *)(uintptr_t)cr.user_ctx;

            memset(cmd, 0x00, sizeof(*cmd));
            cmd->keepalive.opcode = VALKEY_RDMA_INVALID_OPCODE;
        }
    }

    goto pollcq;
}

static void urmaCliSetError(redisContext *c, urmaClient *uc, const char *what) {
    if (uc && uc->eof) {
        c->err = REDIS_ERR_EOF;
        if (uc->last_cq_status) {
            snprintf(c->errstr, sizeof(c->errstr),
                     "URMA connection is gone (%s, cq status %d, opcode 0x%x)",
                     what, uc->last_cq_status, uc->last_cq_opcode);
        } else {
            snprintf(c->errstr, sizeof(c->errstr), "URMA connection is gone (%s)", what);
        }
    } else {
        c->err = REDIS_ERR_IO;
        snprintf(c->errstr, sizeof(c->errstr), "URMA %s error: %s", what, strerror(errno));
    }
}

/* ---- hiredis bridge ---- */

static ssize_t urmaCliRead(redisContext *c, char *buf, size_t bufcap) {
    urmaClient *uc = c->privctx;
    long long deadline = -1;
    size_t n;

    if (uc->eof) {
        c->err = REDIS_ERR_EOF;
        snprintf(c->errstr, sizeof(c->errstr), "URMA connection is gone");
        return -1;
    }

    if (c->command_timeout) {
        deadline = urmaCliMstime() +
                   c->command_timeout->tv_sec * 1000LL + c->command_timeout->tv_usec / 1000;
    }

    while (uc->rx_pos == uc->rx_off) {
        long long wait = (deadline < 0) ? -1 : deadline - urmaCliMstime();
        int ret;

        if (wait <= 0) {
            c->err = REDIS_ERR_TIMEOUT;
            snprintf(c->errstr, sizeof(c->errstr), "URMA read timeout");
            return -1;
        }
        ret = urmaCliPollCq(uc, MIN(wait, 200));
        if (ret < 0) {
            urmaCliSetError(c, uc, "read");
            return -1;
        }
    }

    n = MIN((size_t)(uc->rx_off - uc->rx_pos), bufcap);
    memcpy(buf, uc->rx_addr + uc->rx_pos, n);
    uc->rx_pos += n;

    /* RX buffer fully consumed, announce a fresh one */
    if (uc->rx_pos == uc->rx_len && urmaCliRegisterRx(uc)) {
        urmaCliSetError(c, uc, "register rx");
        return -1;
    }

    return n;
}

static ssize_t urmaCliWrite(redisContext *c) {
    urmaClient *uc = c->privctx;
    size_t total = hi_sdslen(c->obuf);
    size_t written = 0;
    long long deadline = urmaCliMstime() + URMA_CLI_WR_TIMEOUT_MS;

    if (uc->eof) {
        c->err = REDIS_ERR_EOF;
        snprintf(c->errstr, sizeof(c->errstr), "URMA connection is gone");
        return -1;
    }

    while (written < total) {
        uint32_t avail = uc->tx_len - uc->tx_off;
        uint32_t chunk;

        if (!avail) {
            /* remote RX buffer exhausted, wait until the peer announces a
             * fresh one */
            int ret = urmaCliPollCq(uc, 100);

            if (ret < 0) {
                urmaCliSetError(c, uc, "write");
                return -1;
            }
            if (urmaCliMstime() >= deadline) {
                c->err = REDIS_ERR_IO;
                snprintf(c->errstr, sizeof(c->errstr), "URMA write timeout");
                return -1;
            }
            continue;
        }

        chunk = MIN((uint32_t)(total - written), avail);
        memcpy(uc->tx_addr + uc->tx_off, c->obuf + written, chunk);

        {
            urma_jfs_wr_t wr = {0};
            urma_sge_t sge, dsge;
            urma_jfs_wr_t *bad_wr;

            sge.addr = (uint64_t)(uc->tx_addr + uc->tx_off);
            sge.len = chunk;
            sge.tseg = uc->tx_tseg;
            sge.user_tseg = NULL;

            dsge.addr = uc->remote_addr + uc->tx_off;
            dsge.len = chunk;
            dsge.tseg = uc->remote_rx_tseg;
            dsge.user_tseg = NULL;

            wr.opcode = URMA_OPC_WRITE_IMM;
            wr.tjetty = uc->tjetty;
            wr.user_ctx = 0;
            wr.flag.bs.complete_enable = 1;
            /* place_order stays URMA_NO_ORDER(0), mirrors urma_perftest */
            wr.rw.src.sge = &sge;
            wr.rw.src.num_sge = 1;
            wr.rw.dst.sge = &dsge;
            wr.rw.dst.num_sge = 1;
            wr.rw.notify_data = chunk;
            wr.next = NULL;

            if (urma_post_jetty_send_wr(uc->jetty, &wr, &bad_wr)) {
                c->err = REDIS_ERR_IO;
                snprintf(c->errstr, sizeof(c->errstr), "URMA post write failed");
                return -1;
            }

            fprintf(stderr, "URMA: WRITE: src 0x%lx len %u, dst 0x%lx len %u, imm %u\n",
                    (unsigned long)(uintptr_t)(uc->tx_addr + uc->tx_off), chunk,
                    (unsigned long)(uc->remote_addr + uc->tx_off), chunk, chunk);
        }

        uc->tx_off += chunk;
        written += chunk;
    }

    /* drain completions so the send buffers are recycled early */
    urmaCliPollCq(uc, 0);

    return (ssize_t)total;
}

static void urmaCliNetClose(redisContext *c) {
    if (c && c->fd != -1) {
        close(c->fd);
        c->fd = -1;
    }
}

/* after the jetty is set to ERROR, pending WRs are flushed; poll until the
 * flush completions show up, mirrors urma_perftest drain_inflight_wr */
static void urmaCliDrainCq(urmaClient *uc) {
    urma_cr_t cr[16];
    int retry = 0;

    while (retry < 100) { /* 100 * 10ms upper bound, best effort */
        int n = urma_poll_jfc(uc->jfc, 16, cr);
        if (n <= 0) {
            usleep(10000);
            retry++;
            continue;
        }
        retry = 0;
        for (int i = 0; i < n; i++) {
            if (cr[i].status == URMA_CR_WR_FLUSH_ERR_DONE) return;
        }
    }
}

static void urmaCliFreePrivctx(void *privctx) {
    urmaClient *uc = privctx;

    if (!uc) return;

    /* graceful teardown, mirrors the urma_perftest destroy path: error the
     * jetty so HW flushes the pending WRs, drain completions, detach from
     * the peer, and only then free local resources. Freeing (unregistering)
     * segments while WRs still reference them causes remote LOC_LEN_ERR and
     * blocks the resource deletion below. */
    if (uc->bound) {
        urma_jetty_attr_t attr = {.mask = JETTY_STATE, .state = URMA_JETTY_STATE_ERROR};

        (void)urma_modify_jetty(uc->jetty, &attr);
        urmaCliDrainCq(uc);
    }

    /* detach from the peer first */
    if (uc->remote_rx_tseg) urma_unimport_seg(uc->remote_rx_tseg);
    if (uc->bound) urma_unbind_jetty(uc->jetty);
    if (uc->tjetty) urma_unimport_jetty(uc->tjetty);
    if (uc->jetty) urma_delete_jetty(uc->jetty);
    if (uc->jfr) urma_delete_jfr(uc->jfr);

    /* then release local memory */
    if (uc->tx_tseg) urma_unregister_seg(uc->tx_tseg);
    if (uc->rx_tseg) urma_unregister_seg(uc->rx_tseg);
    if (uc->cmd_tseg) urma_unregister_seg(uc->cmd_tseg);
    free(uc->tx_addr);
    free(uc->rx_addr);
    free(uc->cmd_buf);

    if (uc->jfc) urma_delete_jfc(uc->jfc);
    if (uc->jfce) urma_delete_jfce(uc->jfce);
    if (uc->uctx) urma_delete_context(uc->uctx);
    free(uc);
}

static redisContextFuncs redisContextUrmaFuncs = {
    .close = urmaCliNetClose,
    .free_privctx = urmaCliFreePrivctx,
    .async_read = redisAsyncRead,
    .async_write = redisAsyncWrite,
    .read = urmaCliRead,
    .write = urmaCliWrite,
};

/* ---- setup / upgrade ---- */

static int urmaCliCreateResource(urmaClient *uc) {
    urma_device_t **dev_list = NULL;
    urma_jfc_cfg_t jfc_cfg = {0};
    urma_jfr_cfg_t jfr_cfg = {0};
    urma_jetty_cfg_t jetty_cfg = {0};
    urma_seg_cfg_t seg_cfg = {0};
    int num_devices = 0;
    int i;
    int ret = -1;

    dev_list = urma_get_device_list(&num_devices);
    if (!dev_list || num_devices <= 0) {
        if (dev_list) urma_free_device_list(dev_list);
        return -1;
    }

    uc->uctx = urma_create_context(dev_list[0], 0);
    urma_free_device_list(dev_list);
    if (!uc->uctx) return -1;

    uc->jfce = urma_create_jfce(uc->uctx);
    if (!uc->jfce) goto out;

    jfc_cfg.depth = URMA_CLI_JFC_DEPTH;
    jfc_cfg.ceqn = 0;
    jfc_cfg.jfce = uc->jfce;
    uc->jfc = urma_create_jfc(uc->uctx, &jfc_cfg);
    if (!uc->jfc) goto out;

    jfr_cfg.depth = URMA_CLI_MAX_WQE;
    jfr_cfg.trans_mode = URMA_TM_RC;
    jfr_cfg.max_sge = 1;
    jfr_cfg.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
    jfr_cfg.jfc = uc->jfc;
    jfr_cfg.token_value.token = VALKEY_RDMA_JETTY_TOKEN;
    uc->jfr = urma_create_jfr(uc->uctx, &jfr_cfg);
    if (!uc->jfr) goto out;

    jetty_cfg.flag.bs.share_jfr = 1;
    jetty_cfg.jfs_cfg.depth = URMA_CLI_MAX_WQE;
    jetty_cfg.jfs_cfg.trans_mode = URMA_TM_RC;
    jetty_cfg.jfs_cfg.priority = URMA_MAX_PRIORITY;
    jetty_cfg.jfs_cfg.max_sge = 1;
    jetty_cfg.jfs_cfg.max_inline_data = 0;
    jetty_cfg.jfs_cfg.rnr_retry = URMA_TYPICAL_RNR_RETRY;
    jetty_cfg.jfs_cfg.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
    jetty_cfg.jfs_cfg.jfc = uc->jfc;
    jetty_cfg.shared.jfr = uc->jfr;
    uc->jetty = urma_create_jetty(uc->uctx, &jetty_cfg);
    if (!uc->jetty) goto out;

    /* command buffers */
    uc->cmd_buf = page_aligned_zalloc(sizeof(ValkeyRdmaCmd) * URMA_CLI_MAX_WQE * 2);
    if (!uc->cmd_buf) goto out;
    seg_cfg.va = (uint64_t)uc->cmd_buf;
    seg_cfg.len = sizeof(ValkeyRdmaCmd) * URMA_CLI_MAX_WQE * 2;
    seg_cfg.token_id = NULL;
    seg_cfg.token_value.token = 0;
    seg_cfg.flag.bs.access = URMA_ACCESS_LOCAL_ONLY;
    uc->cmd_tseg = urma_register_seg(uc->uctx, &seg_cfg);
    if (!uc->cmd_tseg) goto out;

    for (i = 0; i < URMA_CLI_MAX_WQE; i++) {
        if (urmaCliPostRecv(uc, uc->cmd_buf + i)) goto out;
    }
    for (i = URMA_CLI_MAX_WQE; i < URMA_CLI_MAX_WQE * 2; i++) {
        uc->cmd_buf[i].keepalive.opcode = VALKEY_RDMA_INVALID_OPCODE;
    }

    /* RX buffer */
    uc->rx_addr = page_aligned_zalloc(URMA_CLI_RX_SIZE);
    if (!uc->rx_addr) goto out;
    uc->rx_len = URMA_CLI_RX_SIZE;
    uc->rx_token = VALKEY_RDMA_JETTY_TOKEN;
    seg_cfg.va = (uint64_t)uc->rx_addr;
    seg_cfg.len = uc->rx_len;
    seg_cfg.token_id = NULL;
    seg_cfg.token_value.token = uc->rx_token;
    seg_cfg.flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE;
    uc->rx_tseg = urma_register_seg(uc->uctx, &seg_cfg);
    if (!uc->rx_tseg) goto out;

    if (urma_rearm_jfc(uc->jfc, false)) goto out;

    ret = 0;

out:
    return ret;
}

static int urmaCliWaitTcpDone(int fd, long long deadline) {
    for (;;) {
        int so_error = 0;
        socklen_t len = sizeof(so_error);
        struct pollfd pfd = {.fd = fd, .events = POLLOUT, .revents = 0};
        long long remain;
        int rc;

        if (!getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_error, &len) && !so_error) return 0;

        remain = deadline - urmaCliMstime();
        if (remain <= 0) return -1;

        rc = poll(&pfd, 1, (int)MIN(remain, 100));
        if (rc < 0 && errno != EINTR) return -1;
    }
}

static int urmaCliWriteAll(int fd, const void *buf, size_t len, long long deadline) {
    const char *p = buf;

    while ((size_t)(p - (const char *)buf) < len) {
        ssize_t n = write(fd, p, len - (p - (const char *)buf));

        if (n < 0) {
            if (errno == EINTR) continue;
            if (errno != EAGAIN) return -1;
        } else {
            p += n;
        }

        {
            struct pollfd pfd = {.fd = fd, .events = POLLOUT, .revents = 0};
            long long remain = deadline - urmaCliMstime();

            if (remain <= 0 || poll(&pfd, 1, (int)remain) <= 0) return -1;
        }
    }

    return 0;
}

static int urmaCliReadAll(int fd, void *buf, size_t len, long long deadline) {
    char *p = buf;

    while ((size_t)(p - (char *)buf) < len) {
        ssize_t n = read(fd, p, len - (p - (char *)buf));

        if (n < 0) {
            if (errno == EINTR) continue;
            if (errno != EAGAIN) return -1;
        } else if (n == 0) {
            return -1;
        } else {
            p += n;
        }

        {
            struct pollfd pfd = {.fd = fd, .events = POLLIN, .revents = 0};
            long long remain = deadline - urmaCliMstime();

            if (remain <= 0 || poll(&pfd, 1, (int)remain) <= 0) return -1;
        }
    }

    return 0;
}

int redisUrmaUpgrade(redisContext *c, long long timeout_ms) {
    urmaClient *uc = NULL;
    urma_rjetty_t local_rjetty, peer_rjetty;
    urma_token_t token = {.token = VALKEY_RDMA_JETTY_TOKEN};
    long long deadline = (timeout_ms < 0) ? -1 : urmaCliMstime() + timeout_ms;
    urma_status_t status;

    if (!c || c->err || c->fd < 0) {
        if (c && !c->err) {
            c->err = REDIS_ERR_IO;
            snprintf(c->errstr, sizeof(c->errstr), "URMA upgrade: invalid context");
        }
        return REDIS_ERR;
    }

    /* must init the urma environment before touching any resource, the
     * client process has nobody else doing it; tolerate URMA_EEXIST */
    {
        urma_init_attr_t init_attr = {
            .token = 0,
            .uasid = 0,
        };

        status = urma_init(&init_attr);
        if (status != URMA_SUCCESS && status != URMA_EEXIST) {
            c->err = REDIS_ERR_IO;
            snprintf(c->errstr, sizeof(c->errstr),
                     "URMA upgrade: urma init failed (status %d)", (int)status);
            return REDIS_ERR;
        }
    }

    uc = calloc(1, sizeof(*uc));
    if (!uc) {
        c->err = REDIS_ERR_IO;
        snprintf(c->errstr, sizeof(c->errstr), "URMA upgrade: out of memory");
        return REDIS_ERR;
    }

    /* non-blocking TCP connect may still be in progress */
    if (urmaCliWaitTcpDone(c->fd, deadline)) {
        c->err = REDIS_ERR_IO;
        snprintf(c->errstr, sizeof(c->errstr), "URMA upgrade: TCP connect failed");
        goto fail;
    }

    if (urmaCliCreateResource(uc)) {
        c->err = REDIS_ERR_IO;
        snprintf(c->errstr, sizeof(c->errstr), "URMA upgrade: no URMA device or resources");
        goto fail;
    }

    /* exchange jetty info over the out-of-band TCP channel */
    {
        urma_rjetty_t *rjetty = NULL;
        uint32_t length = 0;

        if (urma_get_rjetty(uc->jetty, &rjetty, &length) || !rjetty ||
            length < sizeof(urma_rjetty_t)) {
            if (rjetty) urma_put_rjetty(rjetty);
            c->err = REDIS_ERR_IO;
            snprintf(c->errstr, sizeof(c->errstr), "URMA upgrade: get rjetty failed");
            goto fail;
        }
        memcpy(&local_rjetty, rjetty, sizeof(urma_rjetty_t));
        urma_put_rjetty(rjetty);
    }

    if (urmaCliWriteAll(c->fd, &local_rjetty, sizeof(urma_rjetty_t), deadline) ||
        urmaCliReadAll(c->fd, &peer_rjetty, sizeof(urma_rjetty_t), deadline)) {
        c->err = REDIS_ERR_IO;
        snprintf(c->errstr, sizeof(c->errstr), "URMA upgrade: rjetty exchange failed");
        goto fail;
    }

    /* fix up the announced remote jetty transport attributes before
     * importing it, mirrors urma_perftest connect_jetty_default() */
    if (peer_rjetty.trans_mode == URMA_TM_RC && peer_rjetty.flag.bs.order_type == URMA_OT) {
        peer_rjetty.flag.bs.share_tp = 1;
    }
    peer_rjetty.tp_type = URMA_RTP;

    uc->tjetty = urma_import_jetty(uc->uctx, &peer_rjetty, &token);
    if (!uc->tjetty) {
        c->err = REDIS_ERR_IO;
        snprintf(c->errstr, sizeof(c->errstr), "URMA upgrade: import peer jetty failed");
        goto fail;
    }
    status = urma_bind_jetty(uc->jetty, uc->tjetty);
    if (status != URMA_SUCCESS && status != URMA_EEXIST) {
        c->err = REDIS_ERR_IO;
        snprintf(c->errstr, sizeof(c->errstr), "URMA upgrade: bind jetty failed (status %d)", (int)status);
        goto fail;
    }
    uc->bound = 1;

    /* announce our RX buffer and wait for the peer one, the RC channel is
     * usable after that */
    if (urmaCliRegisterRx(uc)) {
        c->err = REDIS_ERR_IO;
        snprintf(c->errstr, sizeof(c->errstr), "URMA upgrade: register rx failed");
        goto fail;
    }

    while (!uc->remote_rx_tseg) {
        int ret;

        if (deadline > 0 && urmaCliMstime() >= deadline) {
            c->err = REDIS_ERR_IO;
            snprintf(c->errstr, sizeof(c->errstr), "URMA upgrade: handshake timeout");
            goto fail;
        }
        ret = urmaCliPollCq(uc, 100);
        if (ret < 0) {
            c->err = REDIS_ERR_IO;
            snprintf(c->errstr, sizeof(c->errstr), "URMA upgrade: handshake failed");
            goto fail;
        }
    }

    /* switch the context onto the URMA transport */
    close(c->fd); /* the TCP channel is not needed anymore */
    c->fd = uc->jfce->fd;
    c->connection_type = REDIS_CONN_TCP;
    c->funcs = &redisContextUrmaFuncs;
    c->privctx = uc;

    return REDIS_OK;

fail:
    urmaCliFreePrivctx(uc);
    return REDIS_ERR;
}

#else /* !USE_RDMA */

int redisUrmaUpgrade(redisContext *c, long long timeout_ms) {
    (void)timeout_ms;

    if (c) {
        c->err = REDIS_ERR_IO;
        snprintf(c->errstr, sizeof(c->errstr), "URMA transport not built (BUILD_URMA=yes)");
    }
    return REDIS_ERR;
}

#endif

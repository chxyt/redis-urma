/* ==========================================================================
 * rdma.c - support RDMA protocol for transport layer, based on URMA (umdk).
 * --------------------------------------------------------------------------
 * Copyright (C) 2021-2024  zhenwei pi <pizhenwei@bytedance.com>
 *
 * This work is licensed under BSD 3-Clause, License 1 of the COPYING file in
 * the top-level directory.
 *
 * URMA port notes:
 * - ibv verbs   -> URMA: context/jfc/jetty/seg
 * - rdma_cm     -> replaced by an out-of-band TCP control channel, the two
 *                  peers exchange their urma_rjetty_t over TCP, then
 *                  urma_import_jetty() + urma_bind_jetty() to establish the
 *                  RC transport channel.
 * - data path   -> WRITE with immediate data onto the remote registered
 *                  transfer buffer, control commands via SEND/RECV.
 * ==========================================================================
 */

#define VALKEYMODULE_CORE_MODULE
#include "server.h"
#include "connection.h"

#if defined __linux__ /* currently RDMA is only supported on Linux */
#if (USE_RDMA == 1 /* BUILD_YES */) || ((USE_RDMA == 2 /* BUILD_MODULE */) && (BUILD_RDMA_MODULE == 2))
#include "connhelpers.h"

#include <assert.h>
#include <arpa/inet.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include <urma/urma_api.h>
#include <urma/urma_types.h>
#include <urma/urma_opcode.h>

#define CONN_TYPE_RDMA "rdma"

#ifndef MIN
#define MIN(a,b) ((a) < (b) ? (a) : (b))
#endif

typedef struct ValkeyRdmaFeature {
    /* defined as following Opcodes */
    uint16_t opcode;
    /* select features */
    uint16_t select;
    uint8_t rsvd[4];
    /* feature bits */
    uint64_t features;
    uint8_t rsvd2[48];
} ValkeyRdmaFeature;

typedef struct ValkeyRdmaKeepalive {
    /* defined as following Opcodes */
    uint16_t opcode;
    uint8_t rsvd[62];
} ValkeyRdmaKeepalive;

typedef struct ValkeyRdmaMemory {
    /* defined as following Opcodes */
    uint16_t opcode;
    uint8_t rsvd[6];
    /* the urma segment descriptor of a transfer buffer which is used to
     * receive remote streaming data, aka 'RX buffer'. The remote side should
     * import this segment and write into it. Note that the descriptor is
     * exchanged in raw form, both ends are assumed to run the same build. */
    urma_seg_t seg;
    /* the token of the 'RX buffer' segment */
    uint32_t token;
    uint8_t rsvd2[4];
} ValkeyRdmaMemory;

typedef union ValkeyRdmaCmd {
    ValkeyRdmaFeature feature;
    ValkeyRdmaKeepalive keepalive;
    ValkeyRdmaMemory memory;
} ValkeyRdmaCmd;

typedef enum ValkeyRdmaOpcode {
    GetServerFeature = 0,
    SetClientFeature = 1,
    Keepalive = 2,
    RegisterXferMemory = 3,
} ValkeyRdmaOpcode;

#define VALKEY_BUILD_BUG_ON(cond) ((void)sizeof(char[1 - 2 * !!(cond)]))
#define VALKEY_RDMA_MAX_WQE 128 /* EXPERIMENT: 1024 -> 128, align with the client */
#define VALKEY_RDMA_DEFAULT_RX_SIZE (1024 * 1024)
#define VALKEY_RDMA_MIN_RX_SIZE (64 * 1024)
#define VALKEY_RDMA_MAX_RX_SIZE (16 * 1024 * 1024)
#define VALKEY_RDMA_SYNCIO_RES 10
#define VALKEY_RDMA_INVALID_OPCODE 0xffff
#define VALKEY_RDMA_KEEPALIVE_MS 3000

/* token used locally for imported jets/segments protection table */
#define VALKEY_RDMA_JETTY_TOKEN 0xABCDEF

/* handshake state over the out-of-band TCP channel */
typedef enum RdmaHsState {
    HS_NONE = 0,      /* TCP connecting */
    HS_LOCAL_SENT,    /* our rjetty has been sent, waiting for peer rjetty */
    HS_ESTABLISHED,   /* RC transport channel established */
} RdmaHsState;

typedef struct rdma_connection {
    connection c;
    int cfd;               /* out-of-band TCP control channel, -1 after established */
    struct RdmaContext *ctx;
    int last_errno;
    listNode *pending_list_node;
} rdma_connection;

typedef struct RdmaXfer {
    char *addr;        /* address of transfer buffer in local memory */
    uint32_t length;   /* bytes of transfer buffer */
    uint32_t offset;   /* the offset of consumed transfer buffer */
    uint32_t pos;      /* the position in use of the transfer buffer */
    uint32_t token;    /* token of the registered segment */
} RdmaXfer;

typedef struct RdmaContext {
    connection *conn;
    char *ip;               /* peer address */
    int port;               /* peer port */
    char *lip;              /* local address */
    int lport;              /* local port */
    long long keepalive_te; /* RDMA has no transport layer keepalive */

    /* URMA resources */
    urma_context_t *uctx;
    urma_jfce_t *jfce;
    urma_jfc_t *jfc;
    urma_jfr_t *jfr;
    urma_jetty_t *jetty;
    urma_target_jetty_t *tjetty; /* remote jetty imported from peer */
    int bound;                   /* jetty bound with remote peer */
    int max_sge;                 /* device capability */
    int jetty_priority;          /* lowest priority supporting RTP */

    /* handshake state */
    RdmaHsState hs_state;
    urma_rjetty_t local_rjetty;
    urma_rjetty_t peer_rjetty;
    size_t hs_sent; /* bytes of local rjetty already sent */
    size_t hs_got;  /* bytes of peer rjetty already received */

    /* TX */
    RdmaXfer tx;
    urma_target_seg_t *tx_tseg;        /* locally registered transfer buffer */
    urma_target_seg_t *remote_rx_tseg; /* imported remote RX buffer */
    urma_seg_t remote_rx_seg;          /* raw descriptor of remote RX buffer */
    uint64_t tx_remote_addr;           /* remote transfer buffer address */
    uint32_t tx_key;                   /* remote transfer buffer token */
    uint32_t tx_length;                /* remote transfer buffer length */
    uint32_t tx_offset;                /* remote transfer buffer offset */
    uint32_t tx_ops;                   /* operations on remote transfer */

    /* RX */
    RdmaXfer rx;
    urma_target_seg_t *rx_tseg;

    /* CMD 0 ~ VALKEY_RDMA_MAX_WQE for recv buffer
     * VALKEY_RDMA_MAX_WQE ~ 2 * VALKEY_RDMA_MAX_WQE -1 for send buffer */
    ValkeyRdmaCmd *cmd_buf;
    urma_target_seg_t *cmd_tseg;
} RdmaContext;

typedef struct rdma_listener {
    int fd; /* TCP listening socket, acts as the out-of-band channel */
} rdma_listener;

/* RDMA connection is always writable, it has no POLLOUT event to drive the write handler, record available write
 * handler into pending list */
static list *pending_list;

static rdma_listener *rdma_listeners;
static serverRdmaContextConfig *rdma_config;

static ConnectionType CT_RDMA;

static void serverRdmaError(char *err, const char *fmt, ...) {
    va_list ap;

    if (!err) return;
    va_start(ap, fmt);
    vsnprintf(err, ANET_ERR_LEN, fmt, ap);
    va_end(ap);
}

static inline int connRdmaAllowCommand(void) {
    /* RDMA MR is not accessible in a child process, avoid segment fault due to
     * invalid MR access, close it rather than server random crash */
    if (server.in_fork_child != CHILD_TYPE_NONE) {
        return C_ERR;
    }

    return C_OK;
}

static inline int connRdmaAllowRW(connection *conn) {
    if (conn->state == CONN_STATE_ERROR || conn->state == CONN_STATE_CLOSED) {
        return C_ERR;
    }

    return connRdmaAllowCommand();
}

static int rdmaPostRecv(RdmaContext *ctx, ValkeyRdmaCmd *cmd) {
    urma_jfr_wr_t wr = {0};
    urma_sge_t sge;
    urma_jfr_wr_t *bad_wr;
    int ret;

    if (connRdmaAllowCommand()) {
        return C_ERR;
    }

    sge.addr = (uint64_t)cmd;
    sge.len = sizeof(ValkeyRdmaCmd);
    sge.tseg = ctx->cmd_tseg;
    sge.user_tseg = NULL;

    wr.src.sge = &sge;
    wr.src.num_sge = 1;
    wr.user_ctx = (uint64_t)cmd;
    wr.next = NULL;

    ret = urma_post_jetty_recv_wr(ctx->jetty, &wr, &bad_wr);
    if (ret && (ret != URMA_EAGAIN)) {
        serverLog(LL_WARNING, "RDMA: post recv failed: %d", ret);
        return C_ERR;
    }

    return C_OK;
}

/* To make Valkey forkable, buffer which is registered as RDMA
 * memory region should be aligned to page size. And the length
 * also need be aligned to page size.
 * Random segment-fault case like this:
 * 0x7f2764ac5000      -      0x7f2764ac7000
 * |ptr0 128| ... |ptr1 4096| ... |ptr2 512|
 *
 * After registering the range of 8K becomes DONTFORK. And the child
 * process will hit a segment fault during access ptr0/ptr2.
 * Note that the memory can be freed by libc free only.
 * TODO: move it to zmalloc.c if necessary
 */
static void *page_aligned_zalloc(size_t size) {
    void *tmp;
    size_t aligned_size, page_size = sysconf(_SC_PAGESIZE);

    aligned_size = (size + page_size - 1) & (~(page_size - 1));
    if (posix_memalign(&tmp, page_size, aligned_size)) {
        serverPanic("posix_memalign failed");
    }

    memset(tmp, 0x00, aligned_size);

    return tmp;
}

static void rdmaDestroyIoBuf(RdmaContext *ctx) {
    if (ctx->rx_tseg) {
        urma_unregister_seg(ctx->rx_tseg);
        ctx->rx_tseg = NULL;
    }

    zlibc_free(ctx->rx.addr);
    ctx->rx.addr = NULL;

    if (ctx->tx_tseg) {
        urma_unregister_seg(ctx->tx_tseg);
        ctx->tx_tseg = NULL;
    }

    zlibc_free(ctx->tx.addr);
    ctx->tx.addr = NULL;

    if (ctx->cmd_tseg) {
        urma_unregister_seg(ctx->cmd_tseg);
        ctx->cmd_tseg = NULL;
    }

    zlibc_free(ctx->cmd_buf);
    ctx->cmd_buf = NULL;
}

static int rdmaSetupIoBuf(RdmaContext *ctx) {
    urma_seg_cfg_t seg_cfg = {0};
    size_t length = sizeof(ValkeyRdmaCmd) * VALKEY_RDMA_MAX_WQE * 2;
    ValkeyRdmaCmd *cmd;
    int i;

    /* setup CMD buf & segment, only local access */
    ctx->cmd_buf = page_aligned_zalloc(length);
    seg_cfg.va = (uint64_t)ctx->cmd_buf;
    seg_cfg.len = length;
    seg_cfg.token_id = NULL; /* urma allocates and frees the token id */
    seg_cfg.token_value.token = 0;
    seg_cfg.flag.bs.access = URMA_ACCESS_LOCAL_ONLY;
    ctx->cmd_tseg = urma_register_seg(ctx->uctx, &seg_cfg);
    if (!ctx->cmd_tseg) {
        serverLog(LL_WARNING, "RDMA: register seg for CMD failed");
        goto destroy_iobuf;
    }

    for (i = 0; i < VALKEY_RDMA_MAX_WQE; i++) {
        cmd = ctx->cmd_buf + i;

        if (rdmaPostRecv(ctx, cmd) == C_ERR) {
            serverLog(LL_WARNING, "RDMA: post recv failed");
            goto destroy_iobuf;
        }
    }

    for (i = VALKEY_RDMA_MAX_WQE; i < VALKEY_RDMA_MAX_WQE * 2; i++) {
        cmd = ctx->cmd_buf + i;
        cmd->keepalive.opcode = VALKEY_RDMA_INVALID_OPCODE;
    }

    /* setup recv buf & segment, accessible by the remote peer */
    length = rdma_config->rx_size;
    ctx->rx.addr = page_aligned_zalloc(length);
    ctx->rx.length = length;
    ctx->rx.token = VALKEY_RDMA_JETTY_TOKEN;
    seg_cfg.va = (uint64_t)ctx->rx.addr;
    seg_cfg.len = length;
    seg_cfg.token_id = NULL;
    seg_cfg.token_value.token = ctx->rx.token;
    seg_cfg.flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE;
    ctx->rx_tseg = urma_register_seg(ctx->uctx, &seg_cfg);
    if (!ctx->rx_tseg) {
        serverLog(LL_WARNING, "RDMA: register seg for recv buffer failed");
        goto destroy_iobuf;
    }

    return C_OK;

destroy_iobuf:
    rdmaDestroyIoBuf(ctx);
    return C_ERR;
}


/* Each priority is bound to a TP type by the device (see urma_perftest
 * get_jetty_priority_by_tp_type), picking a priority whose TP type is not
 * provisioned makes urma_bind_jetty fail with 'get tp id list failed' in
 * kernel log. Bonding devices only provision CTP pools (perftest requires
 * --ctp on bonding_dev), so select the lowest priority supporting CTP. */
static int rdmaGetJettyPriority(const urma_device_attr_t *dev_attr) {
    int i;

    for (i = 0; i <= URMA_MAX_PRIORITY; i++) {
        if (dev_attr->dev_cap.priority_info[i].tp_type.bs.ctp) {
            return i;
        }
    }

    return -1;
}

/* URMA library messages (default rsyslog) are easy to miss, route them into
 * the server log so failures like urma_import_jetty print their reason. */
static void rdmaUrmaLog(int level, char *message) {
    serverLog(LL_NOTICE, "URMA lib(%d): %s", level, message);
}

static int rdmaCreateResource(RdmaContext *ctx) {
    urma_device_t **dev_list = NULL;
    urma_device_attr_t dev_attr = {0};
    urma_jfc_cfg_t jfc_cfg = {0};
    urma_jfr_cfg_t jfr_cfg = {0};
    urma_jetty_cfg_t jetty_cfg = {0};
    int num_devices = 0;
    int ceqn = rdma_config->completion_vector;
    int ret = C_ERR;

    /* negative number means a random one */
    if (ceqn < 0) {
        ceqn = 0;
    }

    dev_list = urma_get_device_list(&num_devices);
    if (!dev_list || num_devices <= 0) {
        serverLog(LL_WARNING, "RDMA: no urma device found");
        if (dev_list) urma_free_device_list(dev_list);
        return C_ERR;
    }
    ctx->max_sge = 1;

    (void)urma_register_log_func(rdmaUrmaLog);

    if (urma_query_device(dev_list[0], &dev_attr)) {
        serverLog(LL_WARNING, "RDMA: urma query device failed");
        goto out;
    }
    ctx->max_sge = dev_attr.dev_cap.max_jfs_sge;

    {
        int priority = rdmaGetJettyPriority(&dev_attr);

        if (priority < 0) {
            serverLog(LL_WARNING, "RDMA: no priority supports CTP on this device");
            goto out;
        }
        ctx->jetty_priority = priority;
        serverLog(LL_NOTICE, "RDMA: jetty priority %d (CTP)", priority);
    }

    ctx->uctx = urma_create_context(dev_list[0], 0);
    if (!ctx->uctx) {
        serverLog(LL_WARNING, "RDMA: urma create context failed");
        goto out;
    }

    ctx->jfce = urma_create_jfce(ctx->uctx);
    if (!ctx->jfce) {
        serverLog(LL_WARNING, "RDMA: urma create jfce failed");
        goto out;
    }

    /* jfc depth must cover jfs + jfr depths */
    jfc_cfg.depth = VALKEY_RDMA_MAX_WQE * 2;
    jfc_cfg.ceqn = ceqn;
    jfc_cfg.jfce = ctx->jfce;
    ctx->jfc = urma_create_jfc(ctx->uctx, &jfc_cfg);
    if (!ctx->jfc) {
        serverLog(LL_WARNING, "RDMA: urma create jfc failed");
        goto out;
    }

    jfr_cfg.depth = VALKEY_RDMA_MAX_WQE;
    jfr_cfg.trans_mode = URMA_TM_RC;
    jfr_cfg.max_sge = 1;
    jfr_cfg.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
    jfr_cfg.jfc = ctx->jfc;
    jfr_cfg.token_value.token = VALKEY_RDMA_JETTY_TOKEN;
    ctx->jfr = urma_create_jfr(ctx->uctx, &jfr_cfg);
    if (!ctx->jfr) {
        serverLog(LL_WARNING, "RDMA: urma create jfr failed");
        goto out;
    }

    jetty_cfg.flag.bs.share_jfr = 1;
    jetty_cfg.jfs_cfg.depth = VALKEY_RDMA_MAX_WQE;
    jetty_cfg.jfs_cfg.trans_mode = URMA_TM_RC;
    jetty_cfg.jfs_cfg.priority = (uint8_t)ctx->jetty_priority;
    jetty_cfg.jfs_cfg.max_sge = 1;
    jetty_cfg.jfs_cfg.max_inline_data = 0;
    jetty_cfg.jfs_cfg.rnr_retry = URMA_TYPICAL_RNR_RETRY;
    jetty_cfg.jfs_cfg.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
    jetty_cfg.jfs_cfg.jfc = ctx->jfc;
    jetty_cfg.shared.jfr = ctx->jfr;
    ctx->jetty = urma_create_jetty(ctx->uctx, &jetty_cfg);
    if (!ctx->jetty) {
        serverLog(LL_WARNING, "RDMA: urma create jetty failed");
        goto out;
    }

    if (rdmaSetupIoBuf(ctx)) {
        goto out;
    }

    /* arm the jfc, so the jfce fd is signaled on completion */
    if (urma_rearm_jfc(ctx->jfc, false)) {
        serverLog(LL_WARNING, "RDMA: rearm jfc failed");
        goto out;
    }

    /* get the exchangeable remote jetty information of ours */
    {
        urma_rjetty_t *rjetty = NULL;
        uint32_t length = 0;

        if (urma_get_rjetty(ctx->jetty, &rjetty, &length) || !rjetty ||
            length < sizeof(urma_rjetty_t)) {
            serverLog(LL_WARNING, "RDMA: get rjetty failed");
            if (rjetty) urma_put_rjetty(rjetty);
            goto out;
        }
        memcpy(&ctx->local_rjetty, rjetty, sizeof(urma_rjetty_t));
        urma_put_rjetty(rjetty);
    }

    ret = C_OK;

out:
    if (dev_list) {
        urma_free_device_list(dev_list);
    }

    return ret;
}

/* after the jetty is set to ERROR, pending WRs are flushed; poll until the
 * flush completions show up, mirrors urma_perftest drain_inflight_wr */
static void rdmaDrainCq(RdmaContext *ctx) {
    urma_cr_t cr[16];
    int retry = 0;

    while (retry < 100) { /* 100 * 10ms upper bound, best effort */
        int n = urma_poll_jfc(ctx->jfc, 16, cr);
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

static void rdmaReleaseResource(RdmaContext *ctx) {
    /* graceful teardown, mirrors the urma_perftest destroy path: error the
     * jetty so HW flushes the pending WRs and drain the completions, detach
     * from the peer, and only then release the local memory. Freeing
     * (unregistering) iobufs while WRs still reference them causes remote
     * access errors and may block the resource deletion below. */
    if (ctx->bound) {
        urma_jetty_attr_t attr = {.mask = JETTY_STATE, .state = URMA_JETTY_STATE_ERROR};

        (void)urma_modify_jetty(ctx->jetty, &attr);
        rdmaDrainCq(ctx);
    }

    /* detach from the peer first */
    if (ctx->bound) {
        urma_unbind_jetty(ctx->jetty);
        ctx->bound = 0;
    }

    if (ctx->tjetty) {
        urma_unimport_jetty(ctx->tjetty);
        ctx->tjetty = NULL;
    }

    if (ctx->jetty) {
        urma_delete_jetty(ctx->jetty);
        ctx->jetty = NULL;
    }

    if (ctx->jfr) {
        urma_delete_jfr(ctx->jfr);
        ctx->jfr = NULL;
    }

    /* then release local memory */
    rdmaDestroyIoBuf(ctx);

    if (ctx->jfc) {
        urma_delete_jfc(ctx->jfc);
        ctx->jfc = NULL;
    }

    if (ctx->jfce) {
        urma_delete_jfce(ctx->jfce);
        ctx->jfce = NULL;
    }

    if (ctx->uctx) {
        urma_delete_context(ctx->uctx);
        ctx->uctx = NULL;
    }
}

static int rdmaAdjustSendbuf(RdmaContext *ctx, unsigned int length) {
    urma_seg_cfg_t seg_cfg = {0};

    if (length == ctx->tx_length) {
        return C_OK;
    }

    /* try to free old segment & buffer */
    if (ctx->tx_length) {
        if (ctx->tx_tseg) {
            urma_unregister_seg(ctx->tx_tseg);
            ctx->tx_tseg = NULL;
        }
        zlibc_free(ctx->tx.addr);
        ctx->tx.addr = NULL;
        ctx->tx_length = 0;
    }

    /* create a new buffer & segment */
    ctx->tx.addr = page_aligned_zalloc(length);
    ctx->tx_length = length;
    ctx->tx.token = VALKEY_RDMA_JETTY_TOKEN;
    seg_cfg.va = (uint64_t)ctx->tx.addr;
    seg_cfg.len = length;
    seg_cfg.token_id = NULL;
    seg_cfg.token_value.token = ctx->tx.token;
    seg_cfg.flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE;
    ctx->tx_tseg = urma_register_seg(ctx->uctx, &seg_cfg);
    if (!ctx->tx_tseg) {
        serverRdmaError(server.neterr, "RDMA: register send seg failed");
        serverLog(LL_WARNING, "RDMA: FATAL error, recv corrupted cmd");
        zlibc_free(ctx->tx.addr);
        ctx->tx.addr = NULL;
        ctx->tx_length = 0;
        return C_ERR;
    }

    return C_OK;
}

static int rdmaSendCommand(RdmaContext *ctx, ValkeyRdmaCmd *cmd) {
    urma_jfs_wr_t wr = {0};
    urma_sge_t sge;
    urma_jfs_wr_t *bad_wr;
    ValkeyRdmaCmd *_cmd;
    int i, ret;

    /* find an unused cmd buffer */
    for (i = VALKEY_RDMA_MAX_WQE; i < 2 * VALKEY_RDMA_MAX_WQE; i++) {
        _cmd = ctx->cmd_buf + i;
        if (_cmd->keepalive.opcode == VALKEY_RDMA_INVALID_OPCODE) {
            break;
        }
    }

    assert(i < 2 * VALKEY_RDMA_MAX_WQE);

    memcpy(_cmd, cmd, sizeof(ValkeyRdmaCmd));
    sge.addr = (uint64_t)_cmd;
    sge.len = sizeof(ValkeyRdmaCmd);
    sge.tseg = ctx->cmd_tseg;
    sge.user_tseg = NULL;

    wr.opcode = URMA_OPC_SEND;
    wr.tjetty = ctx->tjetty;
    wr.user_ctx = (uint64_t)_cmd;
    wr.flag.bs.complete_enable = 1; /* recycle the cmd buffer on send completion */
    wr.send.src.sge = &sge;
    wr.send.src.num_sge = 1;
    wr.next = NULL;

    ret = urma_post_jetty_send_wr(ctx->jetty, &wr, &bad_wr);
    if (ret) {
        serverLog(LL_WARNING, "RDMA: post send failed: %d", ret);
        return C_ERR;
    }

    return C_OK;
}

static int connRdmaRegisterRx(RdmaContext *ctx) {
    ValkeyRdmaCmd cmd = {0};

    cmd.memory.opcode = htons(RegisterXferMemory);
    cmd.memory.seg = ctx->rx_tseg->seg;
    cmd.memory.token = htonl(ctx->rx.token);

    ctx->rx.offset = 0;
    ctx->rx.pos = 0;

    return rdmaSendCommand(ctx, &cmd);
}

static int connRdmaGetFeature(RdmaContext *ctx, ValkeyRdmaCmd *cmd) {
    ValkeyRdmaCmd _cmd = {0};

    _cmd.feature.opcode = htons(GetServerFeature);
    _cmd.feature.select = cmd->feature.select;
    _cmd.feature.features = htonu64(0); /* currently no feature support */

    return rdmaSendCommand(ctx, &_cmd);
}

static int connRdmaSetFeature(RdmaContext *ctx, ValkeyRdmaCmd *cmd) {
    UNUSED(ctx);

    /* currently no feature support */
    if (ntohu64(cmd->feature.features)) return C_ERR;

    return C_OK;
}

/* import the remote RX buffer segment announced by the peer */
static int connRdmaImportRemoteRx(RdmaContext *ctx, ValkeyRdmaCmd *cmd) {
    urma_import_seg_flag_t flag = {0};
    urma_token_t token;

    /* drop the previous imported seg on re-announce, otherwise the stale
     * remote mapping lingers in the device */
    if (ctx->remote_rx_tseg) {
        urma_unimport_seg(ctx->remote_rx_tseg);
        ctx->remote_rx_tseg = NULL;
    }

    ctx->remote_rx_seg = cmd->memory.seg;
    ctx->tx_remote_addr = ctx->remote_rx_seg.ubva.va;
    serverLog(LL_NOTICE,
              "RDMA: import remote rx seg: va 0x%lx len %lu uasid %u token 0x%x",
              (unsigned long)ctx->remote_rx_seg.ubva.va,
              (unsigned long)ctx->remote_rx_seg.len,
              ctx->remote_rx_seg.ubva.uasid, ntohl(cmd->memory.token));
    ctx->tx.length = ctx->remote_rx_seg.len;
    ctx->tx_key = ntohl(cmd->memory.token);
    ctx->tx.offset = 0;

    flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE;
    flag.bs.mapping = URMA_SEG_NOMAP;
    token.token = ctx->tx_key;
    ctx->remote_rx_tseg = urma_import_seg(ctx->uctx, &ctx->remote_rx_seg, &token, 0, flag);
    if (!ctx->remote_rx_tseg) {
        serverLog(LL_WARNING, "RDMA: import remote rx seg failed");
        return C_ERR;
    }

    return rdmaAdjustSendbuf(ctx, ctx->tx.length);
}

static inline void rdmaDelKeepalive(aeEventLoop *el, RdmaContext *ctx) {
    if (ctx->keepalive_te == AE_ERR) {
        return;
    }

    aeDeleteTimeEvent(el, ctx->keepalive_te);
    ctx->keepalive_te = AE_ERR;
}

/* peer is gone (flushed WR / CQ error / TCP channel closed during handshake),
 * mark this connection as closed and let the upper layer close it */
static void rdmaHandleDisconnect(aeEventLoop *el, RdmaContext *ctx) {
    connection *conn = ctx->conn;
    rdma_connection *rdma_conn;

    if (!conn) return;
    rdma_conn = (rdma_connection *)conn;

    rdmaDelKeepalive(el, ctx);
    conn->state = CONN_STATE_CLOSED;

    /* we can't close connection now, let's mark this connection as closed state */
    if (!rdma_conn->pending_list_node) {
        listAddNodeTail(pending_list, conn);
        rdma_conn->pending_list_node = listLast(pending_list);
    }
}

static int connRdmaHandleRecv(RdmaContext *ctx, ValkeyRdmaCmd *cmd, uint32_t byte_len) {
    if (unlikely(byte_len != sizeof(ValkeyRdmaCmd))) {
        serverLog(LL_WARNING, "RDMA: FATAL error, recv corrupted cmd");
        return C_ERR;
    }

    switch (ntohs(cmd->keepalive.opcode)) {
    case GetServerFeature: connRdmaGetFeature(ctx, cmd); break;

    case SetClientFeature: connRdmaSetFeature(ctx, cmd); break;

    case Keepalive: break;

    case RegisterXferMemory:
        if (connRdmaImportRemoteRx(ctx, cmd) == C_ERR) {
            return C_ERR;
        }
        break;

    default: serverLog(LL_WARNING, "RDMA: FATAL error, unknown cmd"); return C_ERR;
    }

    return rdmaPostRecv(ctx, cmd);
}

static int connRdmaHandleSend(ValkeyRdmaCmd *cmd) {
    /* clear cmd and mark this cmd has already sent */
    memset(cmd, 0x00, sizeof(*cmd));
    cmd->keepalive.opcode = VALKEY_RDMA_INVALID_OPCODE;

    return C_OK;
}

static int connRdmaHandleRecvImm(RdmaContext *ctx, uint32_t byte_len) {
    assert(byte_len + ctx->rx.offset <= ctx->rx.length);

    ctx->rx.offset += byte_len;

    return C_OK;
}


static int connRdmaHandleCq(rdma_connection *rdma_conn) {
    RdmaContext *ctx = rdma_conn->ctx;
    urma_cr_t cr = {0};
    urma_jfc_t *ev_jfc[1] = {NULL};
    uint32_t nevents = 0;
    int polled = 0;
    int has_event;
    int ret;

    has_event = urma_wait_jfc(ctx->jfce, 1, 0, ev_jfc) > 0;

pollcq:
    ret = urma_poll_jfc(ctx->jfc, 1, &cr);
    if (ret < 0) {
        serverLog(LL_WARNING, "RDMA: poll jfc error");
        return C_ERR;
    } else if (ret == 0) {
        if (has_event) {
            /* confirm the jfc events and re-arm, the order is mandatory.
             * like urma_perftest, ack exactly 1 event per wait_jfc wakeup,
             * acking the polled CQE count over-acks and drops later events */
            nevents = 1;
            urma_ack_jfc(ev_jfc, &nevents, 1);
            if (urma_rearm_jfc(ctx->jfc, false)) {
                serverLog(LL_WARNING, "RDMA: rearm jfc error");
                return C_ERR;
            }
        }
        return C_OK;
    }

    polled++;

    if (cr.status != URMA_CR_SUCCESS) {
        if (rdma_conn->c.state == CONN_STATE_CONNECTED) {
            /* user_ctx == 0 means a data WRITE, otherwise a control cmd SEND */
            serverLog(LL_WARNING,
                "RDMA: CQ handle error status: %d, opcode : 0x%x, s_r : %d, len : %u, "
                "user_ctx : %llu, tx_off : %u",
                cr.status, cr.opcode, cr.flag.bs.s_r, cr.completion_len,
                (unsigned long long)cr.user_ctx, ctx->tx.offset);
        }
        return C_ERR;
    }

    if (cr.flag.bs.s_r == 1) { /* receive side */
        switch (cr.opcode) {
        case URMA_CR_OPC_SEND:
        case URMA_CR_OPC_SEND_WITH_IMM:
            if (connRdmaHandleRecv(ctx, (ValkeyRdmaCmd *)(uintptr_t)cr.user_ctx, cr.completion_len) == C_ERR) {
                return C_ERR;
            }
            break;

        case URMA_CR_OPC_WRITE_WITH_IMM:
            if (connRdmaHandleRecvImm(ctx, cr.completion_len) == C_ERR) {
                rdma_conn->c.state = CONN_STATE_ERROR;
                return C_ERR;
            }

            break;
        default: serverLog(LL_WARNING, "RDMA: unexpected recv opcode 0x[%x]", cr.opcode); return C_ERR;
        }
    } else { /* send side: the driver does not fill cr.opcode for send CRs
              * (always URMA_CR_OPC_SEND), dispatch by user_ctx instead:
              * 0 is a data WRITE, otherwise a control cmd SEND */
        if (cr.user_ctx) {
            if (connRdmaHandleSend((ValkeyRdmaCmd *)(uintptr_t)cr.user_ctx) == C_ERR) {
                return C_ERR;
            }
        }
    }

    goto pollcq;
}

static int connRdmaAccept(connection *conn, ConnectionCallbackFunc accept_handler) {
    rdma_connection *rdma_conn = (rdma_connection *)conn;
    RdmaContext *ctx = rdma_conn->ctx;
    int ret = C_OK;

    if (conn->state != CONN_STATE_ACCEPTING) return C_ERR;

    conn->state = CONN_STATE_CONNECTED;

    connIncrRefs(conn);
    if (!callHandler(conn, accept_handler)) ret = C_ERR;
    connDecrRefs(conn);

    conn->iovcnt = min(ctx->max_sge, IOV_MAX);
    ctx->conn = conn; /* save conn into RdmaContext */

    return ret;
}

static connection *connCreateRdma(void) {
    rdma_connection *rdma_conn = zcalloc(sizeof(rdma_connection));
    rdma_conn->c.type = &CT_RDMA;
    rdma_conn->c.fd = -1;
    rdma_conn->cfd = -1;
    rdma_conn->c.iovcnt = 1; /* at least 1, overwrite this on connect */

    return (connection *)rdma_conn;
}

static connection *connCreateAcceptedRdma(int fd, void *priv) {
    rdma_connection *rdma_conn = (rdma_connection *)connCreateRdma();
    rdma_conn->c.fd = fd;
    rdma_conn->cfd = fd;
    rdma_conn->c.state = CONN_STATE_ACCEPTING;
    rdma_conn->ctx = priv;
    connNonBlock(&rdma_conn->c);

    return (connection *)rdma_conn;
}

/* import the peer jetty and construct the RC transport channel */
static int rdmaEstablish(RdmaContext *ctx) {
    urma_token_t token = {.token = VALKEY_RDMA_JETTY_TOKEN};
    urma_rjetty_t *rjetty = &ctx->peer_rjetty;
    urma_status_t status;
    int i;

    /* fix up the announced remote jetty transport attributes before
     * importing it, mirrors urma_perftest connect_jetty_default() */
    if (rjetty->trans_mode == URMA_TM_RC && rjetty->flag.bs.order_type == URMA_OT) {
        rjetty->flag.bs.share_tp = 1;
    }
    /* bonding devices only provision CTP TP pools, keep in sync with the
     * local jetty priority selected in rdmaCreateResource */
    rjetty->tp_type = URMA_CTP;

    /* both peers import each other's jetty concurrently over the fast TCP
     * exchange, a transient collision is possible, retry a few times */
    for (i = 0; i < 10; i++) {
        ctx->tjetty = urma_import_jetty(ctx->uctx, rjetty, &token);
        if (ctx->tjetty) break;
        usleep(100 * 1000);
    }
    if (!ctx->tjetty) {
        serverLog(LL_WARNING, "RDMA: import peer jetty failed");
        return C_ERR;
    }

    status = urma_bind_jetty(ctx->jetty, ctx->tjetty);
    if (status != URMA_SUCCESS && status != URMA_EEXIST) {
        serverLog(LL_WARNING, "RDMA: bind jetty failed: %d", (int)status);
        return C_ERR;
    }
    ctx->bound = 1;

    return C_OK;
}

static void rdmaSetEstablished(aeEventLoop *el, rdma_connection *rdma_conn) {
    connection *conn = &rdma_conn->c;
    RdmaContext *ctx = rdma_conn->ctx;

    /* the TCP channel is not needed anymore */
    if (rdma_conn->cfd != -1) {
        aeDeleteFileEvent(server.el, rdma_conn->cfd, AE_READABLE | AE_WRITABLE);
        close(rdma_conn->cfd);
        rdma_conn->cfd = -1;
    }

    ctx->hs_state = HS_ESTABLISHED;
    conn->fd = ctx->jfce->fd;
    if (aeCreateFileEvent(server.el, conn->fd, AE_READABLE, conn->type->ae_handler, conn) == AE_ERR) {
        conn->state = CONN_STATE_ERROR;
        return;
    }

    connRdmaRegisterRx(ctx);

    if (conn->state == CONN_STATE_CONNECTED && conn->conn_handler && ctx->conn == conn) {
        /* client side: kick the connect handler */
        callHandler(conn, conn->conn_handler);
    }
    UNUSED(el);
}

/* drive the out-of-band TCP handshake, works for both client and server side */
static void connRdmaTcpProcess(aeEventLoop *el, rdma_connection *rdma_conn) {
    connection *conn = &rdma_conn->c;
    RdmaContext *ctx = rdma_conn->ctx;
    char *buf = (char *)&ctx->peer_rjetty;
    ssize_t n;

    if (conn->state == CONN_STATE_ERROR || conn->state == CONN_STATE_CLOSED) return;

    if (ctx->hs_state == HS_NONE) {
        /* check whether the TCP connect has completed */
        int so_error = 0;
        socklen_t len = sizeof(so_error);

        if (getsockopt(rdma_conn->cfd, SOL_SOCKET, SO_ERROR, &so_error, &len) < 0 || so_error) {
            serverLog(LL_WARNING, "RDMA: tcp connect failed: %s", strerror(so_error ? so_error : errno));
            conn->state = CONN_STATE_ERROR;
            conn->last_errno = so_error ? so_error : ENETUNREACH;
            if (conn->conn_handler) callHandler(conn, conn->conn_handler);
            return;
        }
        ctx->hs_state = HS_LOCAL_SENT; /* reuse the state, our rjetty follows below */
    }

    /* send local rjetty */
    while (ctx->hs_sent < sizeof(urma_rjetty_t)) {
        n = write(rdma_conn->cfd, (char *)&ctx->local_rjetty + ctx->hs_sent, sizeof(urma_rjetty_t) - ctx->hs_sent);
        if (n < 0) {
            if (errno == EAGAIN || errno == EINTR) return;
            serverLog(LL_WARNING, "RDMA: send rjetty failed: %s", strerror(errno));
            goto tcp_fail;
        }
        ctx->hs_sent += n;
    }

    /* receive peer rjetty */
    while (ctx->hs_got < sizeof(urma_rjetty_t)) {
        n = read(rdma_conn->cfd, buf + ctx->hs_got, sizeof(urma_rjetty_t) - ctx->hs_got);
        if (n < 0) {
            if (errno == EAGAIN || errno == EINTR) return;
            serverLog(LL_WARNING, "RDMA: recv rjetty failed: %s", strerror(errno));
            goto tcp_fail;
        }
        if (n == 0) {
            serverLog(LL_WARNING, "RDMA: peer closed the TCP channel during handshake");
            goto tcp_fail;
        }
        ctx->hs_got += n;
    }

    if (ctx->hs_state != HS_ESTABLISHED) {
        if (rdmaEstablish(ctx) == C_ERR) {
            goto tcp_fail;
        }

        rdmaSetEstablished(el, rdma_conn);
    }

    return;

tcp_fail:
    conn->state = CONN_STATE_ERROR;
    conn->last_errno = ECONNRESET;
    if (conn->conn_handler && ctx->conn == conn) {
        /* client side: notify the connect handler directly */
        callHandler(conn, conn->conn_handler);
    } else {
        /* server side: no connect handler, schedule the close via pending
         * list, otherwise the TCP channel stays readable forever and burns
         * the event loop */
        rdmaHandleDisconnect(el, ctx);
    }
}

static void connRdmaTcpEventHandler(struct aeEventLoop *el, int fd, void *clientData, int mask) {
    rdma_connection *rdma_conn = (rdma_connection *)clientData;

    UNUSED(fd);
    UNUSED(mask);

    connRdmaTcpProcess(el, rdma_conn);
}

static void connRdmaEventHandler(struct aeEventLoop *el, int fd, void *clientData, int mask) {
    rdma_connection *rdma_conn = (rdma_connection *)clientData;
    connection *conn = &rdma_conn->c;
    RdmaContext *ctx = rdma_conn->ctx;
    int ret = 0;

    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);

    ret = connRdmaHandleCq(rdma_conn);
    if (ret == C_ERR) {
        rdmaHandleDisconnect(server.el, ctx);
        return;
    }

    /* uplayer should read all */
    while (ctx->rx.pos < ctx->rx.offset) {
        if (conn->read_handler && (callHandler(conn, conn->read_handler) == C_ERR)) {
            return;
        }
    }

    /* recv buf is full, register a new RX buffer */
    if (ctx->rx.pos == ctx->rx.length) {
        connRdmaRegisterRx(ctx);
    }

    /* RDMA comp channel has no POLLOUT event, try to send remaining buffer */
    if ((ctx->tx.offset < ctx->tx.length) && conn->write_handler) {
        callHandler(conn, conn->write_handler);
    }
}

static int rdmaKeepaliveTimeProc(struct aeEventLoop *el, long long id, void *clientData) {
    RdmaContext *ctx = clientData;
    connection *conn = ctx->conn;
    ValkeyRdmaCmd cmd = {0};

    UNUSED(el);
    UNUSED(id);
    if (!conn || conn->state != CONN_STATE_CONNECTED) {
        return AE_NOMORE;
    }

    cmd.keepalive.opcode = htons(Keepalive);
    if (rdmaSendCommand(ctx, &cmd) != C_OK) {
        return AE_NOMORE;
    }

    return VALKEY_RDMA_KEEPALIVE_MS;
}

/* listener side: a new TCP connection means a new RDMA connection request */
static int rdmaHandleConnect(aeEventLoop *el, char *err, int cfd, char *ip, size_t ip_len, int *port, void **priv) {
    RdmaContext *ctx = NULL;

    UNUSED(ip_len);
    UNUSED(err);

    ctx = zcalloc(sizeof(RdmaContext));
    ctx->ip = zstrdup(ip);
    ctx->port = *port;
    ctx->keepalive_te = AE_ERR;
    ctx->hs_state = HS_NONE;

    if (rdmaCreateResource(ctx) == C_ERR) {
        goto free_rdma;
    }

    /* capture local address of the TCP channel */
    {
        struct sockaddr_storage laddr;
        socklen_t llen = sizeof(laddr);
        char lip[NET_IP_STR_LEN] = {0};

        if (!getsockname(cfd, (struct sockaddr *)&laddr, &llen)) {
            if (laddr.ss_family == AF_INET) {
                inet_ntop(AF_INET, &((struct sockaddr_in *)&laddr)->sin_addr, lip, sizeof(lip));
                ctx->lport = ntohs(((struct sockaddr_in *)&laddr)->sin_port);
            } else {
                inet_ntop(AF_INET6, &((struct sockaddr_in6 *)&laddr)->sin6_addr, lip, sizeof(lip));
                ctx->lport = ntohs(((struct sockaddr_in6 *)&laddr)->sin6_port);
            }
            ctx->lip = zstrdup(lip);
        }
    }

    ctx->keepalive_te = aeCreateTimeEvent(el, VALKEY_RDMA_KEEPALIVE_MS, rdmaKeepaliveTimeProc, ctx, NULL);
    if (ctx->keepalive_te == AE_ERR) {
        goto free_rdma;
    }

    *priv = ctx;
    UNUSED(err);
    return C_OK;

free_rdma:
    rdmaReleaseResource(ctx);
    if (ctx->ip) zfree(ctx->ip);
    if (ctx->lip) zfree(ctx->lip);
    zfree(ctx);
    return C_ERR;
}

static rdma_listener *rdmaFdToListener(connListener *listener, int fd) {
    for (int i = 0; i < listener->count; i++) {
        if (listener->fd[i] != fd) continue;

        return &rdma_listeners[i];
    }

    return NULL;
}

/*
 * rdmaAccept, actually it works as accept handler for the listening socket.
 * A new TCP connection stands for a RDMA connection request, create URMA
 * resources and start the jetty exchange over this TCP channel.
 */
static int
rdmaAccept(aeEventLoop *el, connListener *listener, char *err, int fd, char *ip, size_t ip_len, int *port, void **priv) {
    int ret = C_OK;
    rdma_listener *rdma_listener;

    UNUSED(el);

    rdma_listener = rdmaFdToListener(listener, fd);
    if (!rdma_listener) {
        serverPanic("RDMA: unexpected listen file descriptor");
    }

    ret = anetTcpAccept(err, fd, ip, ip_len, port);
    if (ret == ANET_ERR) {
        if (errno != EWOULDBLOCK) {
            serverLog(LL_WARNING, "RDMA: accepting client connection failed, %s", strerror(errno));
        }
        return ANET_ERR;
    }

    if (rdmaHandleConnect(el, err, ret, ip, ip_len, port, priv) == C_ERR) {
        close(ret);
        return ANET_ERR;
    }

    return ret;
}

static void connRdmaAcceptHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cport = 0, cfd, max = 1000;
    int flags = 0;
    char cip[NET_IP_STR_LEN];
    void *connpriv = NULL;
    connListener *listener = (connListener *)privdata;
    UNUSED(el);
    UNUSED(mask);

    while (max--) {
        cfd = rdmaAccept(el, listener, server.neterr, fd, cip, sizeof(cip), &cport, &connpriv);
        if (cfd == ANET_ERR) {
            if (errno != EWOULDBLOCK) serverLog(LL_WARNING, "RDMA Accepting client connection: %s", server.neterr);
            return;
        }

        connection *conn = connCreateAcceptedRdma(cfd, connpriv);
        if (!conn) {
            serverLog(LL_WARNING, "[RDMA] connCreateAcceptedRdma returned NULL! Skipping connection from %s:%d", cip, cport);
            RdmaContext *ctx = connpriv;
            rdmaReleaseResource(ctx);
            if (ctx->ip) zfree(ctx->ip);
            if (ctx->lip) zfree(ctx->lip);
            zfree(ctx);
            close(cfd); // Avoid leaking open file descriptors
            continue;
        }
        anetNonBlock(NULL, cfd);
        anetCloexec(cfd);
        /* drive the jetty exchange over the TCP channel */
        if (aeCreateFileEvent(server.el, cfd, AE_READABLE | AE_WRITABLE, connRdmaTcpEventHandler, conn) == AE_ERR) {
            serverLog(LL_WARNING, "[RDMA] register TCP handshake event failed for %s:%d", cip, cport);
            close(cfd);
            continue;
        }
        acceptCommonHandler(conn, flags, cip);
    }
}

static int connRdmaSetRwHandler(connection *conn) {
    /* IB channel only has POLLIN event */
    if (conn->read_handler || conn->write_handler) {
        if (aeCreateFileEvent(server.el, conn->fd, AE_READABLE, conn->type->ae_handler, conn) == AE_ERR) {
            return C_ERR;
        }
    } else {
        aeDeleteFileEvent(server.el, conn->fd, AE_READABLE);
    }

    return C_OK;
}

static int connRdmaSetWriteHandler(connection *conn, ConnectionCallbackFunc func, int barrier) {
    rdma_connection *rdma_conn = (rdma_connection *)conn;

    if (conn->state != CONN_STATE_CONNECTED) {
        return C_OK;
    }

    conn->write_handler = func;
    if (barrier) {
        conn->flags |= CONN_FLAG_WRITE_BARRIER;
    } else {
        conn->flags &= ~CONN_FLAG_WRITE_BARRIER;
    }

    /* does this connection has pending write data? */
    if (func) {
        if (!rdma_conn->pending_list_node) {
            listAddNodeTail(pending_list, conn);
            rdma_conn->pending_list_node = listLast(pending_list);
        }
    } else if (rdma_conn->pending_list_node) {
        listDelNode(pending_list, rdma_conn->pending_list_node);
        rdma_conn->pending_list_node = NULL;
    }

    return connRdmaSetRwHandler(conn);
}

static int connRdmaSetReadHandler(connection *conn, ConnectionCallbackFunc func) {
    conn->read_handler = func;

    return connRdmaSetRwHandler(conn);
}

static const char *connRdmaGetLastError(connection *conn) {
    return strerror(conn->last_errno);
}

static inline void rdmaConnectFailed(rdma_connection *rdma_conn) {
    connection *conn = &rdma_conn->c;

    conn->state = CONN_STATE_ERROR;
    conn->last_errno = ENETUNREACH;
}

static int rdmaResolveAddr(rdma_connection *rdma_conn, const char *addr, int port, const char *src_addr) {
    struct addrinfo hints, *servinfo = NULL, *p = NULL;
    RdmaContext *ctx = NULL;
    struct sockaddr_storage saddr;
    char _port[6]; /* strlen("65535") */
    int availableAddrs = 0;
    int ret = C_ERR;

    UNUSED(src_addr);
    ctx = zcalloc(sizeof(RdmaContext));
    if (!ctx) {
        serverLog(LL_WARNING, "RDMA: Out of memory");
        goto out;
    }
    ctx->port = port;
    ctx->keepalive_te = AE_ERR;
    ctx->hs_state = HS_NONE;
    rdma_conn->ctx = ctx;

    /* create the out-of-band TCP channel */
    snprintf(_port, 6, "%d", port);
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(addr, _port, &hints, &servinfo)) {
        hints.ai_family = AF_INET6;
        if (getaddrinfo(addr, _port, &hints, &servinfo)) {
            serverLog(LL_WARNING, "RDMA: bad server addr info");
            goto out;
        }
    }

    for (p = servinfo; p != NULL; p = p->ai_next) {
        if (p->ai_family != PF_INET && p->ai_family != PF_INET6) {
            serverLog(LL_WARNING, "RDMA: Unsupported family");
            continue;
        }

        memcpy(&saddr, p->ai_addr, p->ai_addrlen);
        if (p->ai_family == PF_INET) {
            ((struct sockaddr_in *)&saddr)->sin_port = htons(port);
        } else {
            ((struct sockaddr_in6 *)&saddr)->sin6_port = htons(port);
        }

        rdma_conn->cfd = socket(p->ai_family, SOCK_STREAM, 0);
        if (rdma_conn->cfd < 0) {
            continue;
        }
        anetNonBlock(NULL, rdma_conn->cfd);
        anetCloexec(rdma_conn->cfd);
        if (connect(rdma_conn->cfd, (struct sockaddr *)&saddr, p->ai_addrlen) < 0 && errno != EINPROGRESS) {
            close(rdma_conn->cfd);
            rdma_conn->cfd = -1;
            continue;
        }

        ctx->ip = zstrdup(addr);
        availableAddrs++;
        break;
    }

    if (!availableAddrs) {
        serverLog(LL_WARNING, "RDMA: server addr not available");
        goto out;
    }

    if (rdmaCreateResource(ctx) == C_ERR) {
        goto out;
    }

    ctx->keepalive_te = aeCreateTimeEvent(server.el, VALKEY_RDMA_KEEPALIVE_MS, rdmaKeepaliveTimeProc, ctx, NULL);

    ret = C_OK;

out:
    if (servinfo) {
        freeaddrinfo(servinfo);
    }

    return ret;
}

static int connRdmaWait(connection *conn, long start, long timeout) {
    rdma_connection *rdma_conn = (rdma_connection *)conn;
    RdmaContext *ctx = rdma_conn->ctx;
    long long remaining = timeout, wait, elapsed = 0;
    int mask;

    remaining = timeout - elapsed;
    wait = (remaining < VALKEY_RDMA_SYNCIO_RES) ? remaining : VALKEY_RDMA_SYNCIO_RES;

    if (ctx->hs_state != HS_ESTABLISHED && rdma_conn->cfd != -1) {
        /* still in the TCP handshake phase */
        mask = aeWait(rdma_conn->cfd, AE_READABLE | AE_WRITABLE, wait);
        elapsed = mstime() - start;
        if (elapsed >= timeout) {
            errno = ETIMEDOUT;
            return C_ERR;
        }
        if (mask & (AE_READABLE | AE_WRITABLE)) {
            connRdmaTcpProcess(server.el, rdma_conn);
            if (conn->state == CONN_STATE_ERROR) return C_ERR;
        }
        return C_OK;
    }

    aeWait(conn->fd, AE_READABLE, wait);
    elapsed = mstime() - start;
    if (elapsed >= timeout) {
        errno = ETIMEDOUT;
        return C_ERR;
    }

    if (connRdmaHandleCq(rdma_conn) == C_ERR) {
        conn->state = CONN_STATE_ERROR;
        return C_ERR;
    }

    return C_OK;
}

static int connRdmaConnect(connection *conn,
                           const char *addr,
                           int port,
                           const char *src_addr,
                           ConnectionCallbackFunc connect_handler) {
    rdma_connection *rdma_conn = (rdma_connection *)conn;
    RdmaContext *ctx;

    if (rdmaResolveAddr(rdma_conn, addr, port, src_addr) == C_ERR) {
        return C_ERR;
    }

    ctx = rdma_conn->ctx;
    ctx->conn = conn;
    if (aeCreateFileEvent(server.el, rdma_conn->cfd, AE_READABLE | AE_WRITABLE, connRdmaTcpEventHandler, conn) ==
        AE_ERR) {
        return C_ERR;
    }

    conn->conn_handler = connect_handler;

    return C_OK;
}

static int connRdmaBlockingConnect(connection *conn, const char *addr, int port, long long timeout) {
    rdma_connection *rdma_conn = (rdma_connection *)conn;
    RdmaContext *ctx;
    long long start = mstime();

    if (rdmaResolveAddr(rdma_conn, addr, port, NULL) == C_ERR) {
        return C_ERR;
    }

    ctx = rdma_conn->ctx;
    ctx->conn = conn;
    conn->conn_handler = NULL;

    do {
        if (connRdmaWait(conn, start, timeout) == C_ERR) {
            return C_ERR;
        }
    } while (conn->state != CONN_STATE_CONNECTED);

    return C_OK;
}

static void connRdmaShutdown(connection *conn) {
    UNUSED(conn);
}

static void connRdmaClose(connection *conn) {
    rdma_connection *rdma_conn = (rdma_connection *)conn;
    RdmaContext *ctx;

    if (conn->fd != -1) {
        aeDeleteFileEvent(server.el, conn->fd, AE_READABLE);
        conn->fd = -1;
    }

    /* If called from within a handler, schedule the close but
     * keep the connection until the handler returns.
     */
    if (connHasRefs(conn)) {
        conn->flags |= CONN_FLAG_CLOSE_SCHEDULED;
        return;
    }

    ctx = rdma_conn->ctx;
    if (!ctx) {
        if (rdma_conn->cfd != -1) close(rdma_conn->cfd);
        zfree(conn);
        return;
    }

    rdmaDelKeepalive(server.el, ctx);

    /* poll all CR before close */
    if (ctx->hs_state == HS_ESTABLISHED && connRdmaAllowCommand() == C_OK) {
        connRdmaHandleCq(rdma_conn);
    }

    if (rdma_conn->cfd != -1) {
        aeDeleteFileEvent(server.el, rdma_conn->cfd, AE_READABLE | AE_WRITABLE);
        close(rdma_conn->cfd);
        rdma_conn->cfd = -1;
    }

    rdmaReleaseResource(ctx);

    if (ctx->ip) zfree(ctx->ip);
    if (ctx->lip) zfree(ctx->lip);
    zfree(ctx);
    rdma_conn->ctx = NULL;
    zfree(conn);
}

static size_t connRdmaSend(connection *conn, const void *data, size_t data_len) {
    rdma_connection *rdma_conn = (rdma_connection *)conn;
    RdmaContext *ctx = rdma_conn->ctx;
    urma_jfs_wr_t wr = {0};
    urma_sge_t sge, dsge;
    urma_jfs_wr_t *bad_wr;
    uint32_t off = ctx->tx.offset;
    char *addr = ctx->tx.addr + off;
    uint64_t remote_addr = ctx->tx_remote_addr + ctx->tx.offset;
    int ret;

    if (connRdmaAllowCommand()) {
        return C_ERR;
    }

    if (!ctx->remote_rx_tseg || !ctx->tx_tseg) {
        serverLog(LL_WARNING, "RDMA: FATAL error, remote rx buffer not registered");
        return C_ERR;
    }

    memcpy(addr, data, data_len);

    sge.addr = (uint64_t)addr;
    sge.len = data_len;
    sge.tseg = ctx->tx_tseg;
    sge.user_tseg = NULL;

    dsge.addr = remote_addr;
    dsge.len = data_len;
    dsge.tseg = ctx->remote_rx_tseg;
    dsge.user_tseg = NULL;

    wr.opcode = URMA_OPC_WRITE_IMM;
    wr.tjetty = ctx->tjetty;
    wr.user_ctx = 0;
    /* signal the send completion periodically to release TX buffer space */
    wr.flag.bs.complete_enable = (++ctx->tx_ops % (VALKEY_RDMA_MAX_WQE / 2)) ? 0 : 1;
    /* place_order stays URMA_NO_ORDER(0), mirrors urma_perftest */
    wr.rw.src.sge = &sge;
    wr.rw.src.num_sge = 1;
    wr.rw.dst.sge = &dsge;
    wr.rw.dst.num_sge = 1;
    wr.rw.notify_data = data_len; /* immediate data in host byte order */
    wr.next = NULL;

    ret = urma_post_jetty_send_wr(ctx->jetty, &wr, &bad_wr);
    if (ret) {
        serverLog(LL_WARNING, "RDMA: post send failed: %d", ret);
        conn->state = CONN_STATE_ERROR;
        return C_ERR;
    }

    if (ctx->tx_ops <= 2) { /* log the first writes, WQE level diagnostics */
        serverLog(LL_NOTICE,
                  "RDMA: WRITE #%lu: src 0x%lx len %zu (tseg off %u), dst 0x%lx len %zu, imm %zu",
                  (unsigned long)ctx->tx_ops, (unsigned long)(uintptr_t)addr, data_len, off,
                  (unsigned long)remote_addr, data_len, data_len);
    }

    ctx->tx.offset += data_len;

    return data_len;
}

static int connRdmaWrite(connection *conn, const void *data, size_t data_len) {
    rdma_connection *rdma_conn = (rdma_connection *)conn;
    RdmaContext *ctx = rdma_conn->ctx;
    uint32_t towrite;

    if (connRdmaAllowRW(conn)) {
        return C_ERR;
    }

    assert(ctx->tx.offset <= ctx->tx.length);
    towrite = MIN(ctx->tx.length - ctx->tx.offset, data_len);
    if (!towrite) {
        return 0;
    }

    return connRdmaSend(conn, data, towrite);
}

static int connRdmaWritev(connection *conn, const struct iovec *iov, int iovcnt) {
    int ret, nwritten = 0;

    for (int i = 0; i < iovcnt; i++) {
        ret = connRdmaWrite(conn, iov[i].iov_base, iov[i].iov_len);
        if (ret == C_ERR) return C_ERR;
        nwritten += ret;
    }

    return nwritten;
}

static inline uint32_t rdmaRead(RdmaContext *ctx, void *buf, size_t buf_len) {
    uint32_t toread;

    toread = MIN(ctx->rx.offset - ctx->rx.pos, buf_len);

    assert(ctx->rx.pos + toread <= ctx->rx.length);
    memcpy(buf, ctx->rx.addr + ctx->rx.pos, toread);

    ctx->rx.pos += toread;

    return toread;
}

static int connRdmaRead(connection *conn, void *buf, size_t buf_len) {
    rdma_connection *rdma_conn = (rdma_connection *)conn;
    RdmaContext *ctx = rdma_conn->ctx;

    if (connRdmaAllowRW(conn)) {
        return C_ERR;
    }

    /* No more data to read */
    if (ctx->rx.pos == ctx->rx.offset) {
        return -1;
    }

    assert(ctx->rx.pos < ctx->rx.offset);

    return rdmaRead(ctx, buf, buf_len);
}

static ssize_t connRdmaSyncWrite(connection *conn, char *ptr, ssize_t size, long long timeout) {
    rdma_connection *rdma_conn = (rdma_connection *)conn;
    RdmaContext *ctx = rdma_conn->ctx;
    ssize_t nwritten = 0;
    long long start = mstime();
    uint32_t towrite;

    if (connRdmaAllowRW(conn)) {
        return C_ERR;
    }

    assert(ctx->tx.offset <= ctx->tx.length);
    if (ctx->tx.offset < ctx->tx.length) {
        /* TX buffer is available */
        goto copy;
    }

wait:
    if (connRdmaWait(conn, start, timeout) == C_ERR) {
        return C_ERR;
    }

    if (unlikely(!ctx->tx_tseg)) {
        goto wait;
    }

copy:
    towrite = MIN(ctx->tx.length - ctx->tx.offset, size - nwritten);
    if (connRdmaSend(conn, ptr, towrite) == (size_t)C_ERR) {
        return C_ERR;
    } else {
        ptr += towrite;
        nwritten += towrite;
    }

    if (nwritten < size) {
        goto wait;
    }

    return size;
}

static ssize_t connRdmaSyncRead(connection *conn, char *ptr, ssize_t size, long long timeout) {
    rdma_connection *rdma_conn = (rdma_connection *)conn;
    RdmaContext *ctx = rdma_conn->ctx;
    ssize_t nread = 0;
    long long start = mstime();
    uint32_t toread;

    if (connRdmaAllowRW(conn)) {
        return C_ERR;
    }

    assert(ctx->rx.pos <= ctx->rx.offset);
    if (ctx->rx.pos < ctx->rx.offset) {
        goto copy;
    }

wait:
    if (connRdmaWait(conn, start, timeout) == C_ERR) {
        return C_ERR;
    }

copy:
    toread = rdmaRead(ctx, ptr, size - nread);
    ptr += toread;
    nread += toread;
    if (nread < size) {
        goto wait;
    }

    return size;
}

static ssize_t connRdmaSyncReadLine(connection *conn, char *ptr, ssize_t size, long long timeout) {
    rdma_connection *rdma_conn = (rdma_connection *)conn;
    RdmaContext *ctx = rdma_conn->ctx;
    ssize_t nread = 0;
    long long start = mstime();
    uint32_t toread;
    char *c;
    char nl = 0;

    if (connRdmaAllowRW(conn)) {
        return C_ERR;
    }

    assert(ctx->rx.pos <= ctx->rx.offset);
    if (ctx->rx.pos < ctx->rx.offset) {
        goto copy;
    }

wait:
    if (connRdmaWait(conn, start, timeout) == C_ERR) {
        return C_ERR;
    }

copy:
    for (toread = 0; toread <= ctx->rx.offset - ctx->rx.pos; toread++) {
        c = ctx->rx.addr + ctx->rx.pos + toread;
        if (*c == '\n') {
            *c = '\0';
            if (toread && *(c - 1) == '\r') {
                *(c - 1) = '\0';
            }
            nl = 1;
            break;
        }
    }

    toread = rdmaRead(ctx, ptr, MIN(toread + nl, size - nread));
    ptr += toread;
    nread += toread;
    if (nl) {
        return nread;
    }

    if (nread < size) {
        goto wait;
    }

    return size;
}

static const char *connRdmaGetType(connection *conn) {
    UNUSED(conn);

    return CONN_TYPE_RDMA;
}

static int rdmaServer(char *err, int port, char *bindaddr, int af, rdma_listener *rdma_listener) {
    int fd = ANET_ERR;

    if (af == AF_INET6) {
        fd = anetTcp6Server(err, port, bindaddr, 511);
    } else {
        fd = anetTcpServer(err, port, bindaddr, 511);
    }

    if (fd == ANET_ERR) {
        return ANET_ERR;
    }

    rdma_listener->fd = fd;
    serverLog(LL_NOTICE, "[RDMA] urma transport listening on %s:%d", bindaddr ? bindaddr : "*", port);
    return ANET_OK;
}

static int connRdmaIsLocal(connection *conn) {
    rdma_connection *rdma_conn = (rdma_connection *)conn;
    RdmaContext *ctx = rdma_conn->ctx;

    if (!ctx || !ctx->lip || !ctx->ip) return -1;

    return !strcmp(ctx->lip, ctx->ip);
}

int connRdmaListen(connListener *listener) {
    int j, ret;
    char **bindaddr = listener->bindaddr;
    int bindaddr_count = listener->bindaddr_count;
    int port = listener->port;
    rdma_listener *rdma_listener;

    assert(server.proto_max_bulk_len <= 512ll * 1024 * 1024);

    /* Force binding of 0.0.0.0 if no bind address is specified. */
    if (listener->bindaddr_count == 0) {
        static char *hardcoded_bindaddr[] = {"10.10.1.1"};
        bindaddr_count = 1;
        bindaddr = hardcoded_bindaddr;  // <== Hardcoded RDMA IP
    }

    rdma_listeners = rdma_listener = zcalloc_num(bindaddr_count, sizeof(*rdma_listener));
    for (j = 0; j < bindaddr_count; j++) {
        char *addr = bindaddr[j];
        int optional = *addr == '-';

        if (optional) addr++;
        if (strchr(addr, ':')) {
            /* Bind IPv6 address. */
            ret = rdmaServer(server.neterr, port, addr, AF_INET6, rdma_listener);
        } else {
            /* Bind IPv4 address. */
            ret = rdmaServer(server.neterr, port, addr, AF_INET, rdma_listener);
        }

        if (ret == ANET_ERR) {
            serverLog(LL_WARNING, "RDMA: Could not create server for %s:%d: %s", addr, port, server.neterr);

            return C_ERR;
        }

        int fd = rdma_listener->fd;
        anetNonBlock(NULL, fd);
        anetCloexec(fd);
        listener->fd[listener->count++] = fd;
        rdma_listener++;
    }

    rdma_config = listener->priv;
    return C_OK;
}

static void connRdmaCloseListener(connListener *listener) {
    /* Close old servers */
    for (int i = 0; i < listener->count; i++) {
        if (listener->fd[i] == -1) continue;

        aeDeleteFileEvent(server.el, listener->fd[i], AE_READABLE);
        close(listener->fd[i]);
        listener->fd[i] = -1;
    }

    listener->count = 0;
    zfree(rdma_listeners);
    rdma_listeners = NULL;
    rdma_config = NULL;
}

static int connRdmaAddr(connection *conn, char *ip, size_t ip_len, int *port, int remote) {
    rdma_connection *rdma_conn = (rdma_connection *)conn;
    RdmaContext *ctx = rdma_conn->ctx;
    const char *addr;
    int rport;

    if (!ctx) {
        goto error;
    }

    if (remote) {
        addr = ctx->ip;
        rport = ctx->port;
    } else {
        addr = ctx->lip;
        rport = ctx->lport;
    }

    if (!addr) {
        goto error;
    }

    if (ip) {
        if (ip_len < strlen(addr) + 1) {
            goto error;
        }
        memcpy(ip, addr, strlen(addr) + 1);
    }

    if (port) {
        *port = rport;
    }

    return 0;

error:
    if (ip) {
        if (ip_len >= 2) {
            ip[0] = '?';
            ip[1] = '\0';
        } else if (ip_len == 1) {
            ip[0] = '\0';
        }
    }

    if (port) *port = 0;

    return -1;
}

static void rdmaInit(void) {
    pending_list = listCreate();

    VALKEY_BUILD_BUG_ON(sizeof(ValkeyRdmaFeature) != 64);
    VALKEY_BUILD_BUG_ON(sizeof(ValkeyRdmaKeepalive) != 64);
    VALKEY_BUILD_BUG_ON(sizeof(ValkeyRdmaMemory) != 64);
    VALKEY_BUILD_BUG_ON(sizeof(ValkeyRdmaCmd) != 64);

    urma_init_attr_t init_attr = {
        .token = 0,
        .uasid = 0,
    };
    urma_status_t status = urma_init(&init_attr);

    /* tolerate URMA_EEXIST: urma may already be initialized in-process */
    if (status != URMA_SUCCESS && status != URMA_EEXIST) {
        serverLog(LL_WARNING, "RDMA: FATAL error, urma init failed (status %d)", (int)status);
    }
}

static int rdmaHasPendingData(void) {
    if (!pending_list) return 0;

    return listLength(pending_list) > 0;
}

static int rdmaProcessPendingData(void) {
    listIter li;
    listNode *ln;
    rdma_connection *rdma_conn;
    connection *conn;
    int processed;

    processed = listLength(pending_list);
    listRewind(pending_list, &li);
    while ((ln = listNext(&li))) {
        rdma_conn = listNodeValue(ln);
        conn = &rdma_conn->c;

        /* a connection can be disconnected by remote peer, error mark state as CONN_STATE_CLOSED, kick connection
         * read/write handler to close connection */
        if (conn->state == CONN_STATE_ERROR || conn->state == CONN_STATE_CLOSED) {
            listDelNode(pending_list, rdma_conn->pending_list_node);
            rdma_conn->pending_list_node = NULL;
            /* Invoke both read_handler and write_handler, unless read_handler
               returns 0, indicating the connection has closed, in which case
               write_handler will be skipped. */
            if (callHandler(conn, conn->read_handler)) {
                callHandler(conn, conn->write_handler);
            }

            continue;
        }

        connRdmaEventHandler(NULL, -1, rdma_conn, 0);
    }

    return processed;
}

static ConnectionType CT_RDMA = {
    /* connection type */
    .get_type = connRdmaGetType,

    /* connection type initialize & finalize & configure */
    .init = rdmaInit,
    .cleanup = NULL,

    /* ae & accept & listen & error & address handler */
    .ae_handler = connRdmaEventHandler,
    .accept_handler = connRdmaAcceptHandler,
    //.cluster_accept_handler = NULL,
    .is_local = connRdmaIsLocal,
    .listen = connRdmaListen,
    .closeListener = connRdmaCloseListener,
    .addr = connRdmaAddr,

    /* create/close connection */
    .conn_create = connCreateRdma,
    .conn_create_accepted = connCreateAcceptedRdma,
    .shutdown = connRdmaShutdown,
    .close = connRdmaClose,

    /* connect & accept */
    .connect = connRdmaConnect,
    .blocking_connect = connRdmaBlockingConnect,
    .accept = connRdmaAccept,

    /* IO */
    .write = connRdmaWrite,
    .writev = connRdmaWritev,
    .read = connRdmaRead,
    .set_write_handler = connRdmaSetWriteHandler,
    .set_read_handler = connRdmaSetReadHandler,
    .get_last_error = connRdmaGetLastError,
    .sync_write = connRdmaSyncWrite,
    .sync_read = connRdmaSyncRead,
    .sync_readline = connRdmaSyncReadLine,

    /* pending data */
    .has_pending_data = rdmaHasPendingData,
    .process_pending_data = rdmaProcessPendingData,
};

ConnectionType *connectionTypeRdma(void) {
    static ConnectionType *ct_rdma = NULL;

    if (ct_rdma != NULL) return ct_rdma;

    ct_rdma = connectionByType(CONN_TYPE_RDMA);
    serverAssert(ct_rdma != NULL);

    return ct_rdma;
}

int RegisterConnectionTypeRdma(void) {
    return connTypeRegister(&CT_RDMA);
}

#else


int RegisterConnectionTypeRdma(void) {
    return C_ERR;
}

#endif

#if BUILD_RDMA_MODULE == 2 /* BUILD_MODULE */

#include "release.h"


int ValkeyModule_OnLoad(void *ctx, ValkeyModuleString **argv, int argc) {
    UNUSED(argv);
    UNUSED(argc);

    /* Connection modules MUST be part of the same build as valkey. */
    if (strcmp(REDIS_BUILD_ID_RAW, serverBuildIdRaw())) {
        serverLog(LL_NOTICE, "Connection type %s was not built together with the valkey-server used.", CONN_TYPE_RDMA);
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_Init(ctx, CONN_TYPE_RDMA, 1, VALKEYMODULE_APIVER_1) == VALKEYMODULE_ERR) return VALKEYMODULE_ERR;

    /* Connection modules is available only bootup. */
    if ((ValkeyModule_GetContextFlags(ctx) & VALKEYMODULE_CTX_FLAGS_SERVER_STARTUP) == 0) {
        serverLog(LL_NOTICE, "Connection type %s can be loaded only during bootup", CONN_TYPE_RDMA);
        return VALKEYMODULE_ERR;
    }

    ValkeyModule_SetModuleOptions(ctx, VALKEYMODULE_OPTIONS_HANDLE_REPL_ASYNC_LOAD);

    if (connTypeRegister(&CT_RDMA) != C_OK) return VALKEYMODULE_ERR;

    return VALKEYMODULE_OK;
}

int ValkeyModule_OnUnload(void *arg) {
    UNUSED(arg);
    serverLog(LL_NOTICE, "Connection type %s can not be unloaded", CONN_TYPE_RDMA);
    return VALKEYMODULE_ERR;
}

#endif /* BUILD_RDMA_MODULE */

#else /* __linux__ */

int RegisterConnectionTypeRdma(void) {
    serverLog(LL_VERBOSE, "Connection type %s is supported on Linux only", CONN_TYPE_RDMA);
    return C_ERR;
}

#endif /* __linux__ */

/* ==========================================================================
 * urma_client.h - URMA (umdk) transport for hiredis based clients
 * --------------------------------------------------------------------------
 * Upgrades a hiredis redisContext (whose fd is the out-of-band TCP channel)
 * into the URMA transport expected by a redis-server built with
 * BUILD_URMA=yes.
 * ==========================================================================
 */
#ifndef __URMA_CLIENT_H
#define __URMA_CLIENT_H

#include <hiredis.h>
#include <async.h>

/* Upgrade an established (or in-progress, non-blocking) TCP connection to
 * the URMA transport: exchange jetty info over the TCP channel, bind the
 * RC channel, exchange the RX transfer buffers, then rewire the context to
 * the URMA completion-event fd.
 *
 * timeout_ms: overall timeout in milliseconds, < 0 means wait forever.
 *
 * Returns REDIS_OK on success. On failure the context is set into error
 * state (c->err / c->errstr) and must be freed by the caller. */
int redisUrmaUpgrade(redisContext *c, long long timeout_ms);

#endif

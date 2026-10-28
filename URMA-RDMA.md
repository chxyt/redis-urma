# URMA 版 RDMA 传输层使用说明

本仓库的 RDMA 传输层已从 ibverbs + rdma_cm 实现重写为基于 **umdk 的 URMA 接口**实现。
本文档说明其架构、编译方法、运行配置及已知限制。

## 1. 概述

- 源码：`src/rdma.c`（URMA 实现，原 ibv 实现已整体替换）
- 依赖库：umdk 提供的 `liburma` / `liburma_common`
- 参考文档：umdk 仓库 `doc/en/urma/URMA API Guide.md`
- 连接类型名仍为 `rdma`，对上层（redis/valkey 连接框架）接口不变

### 对象映射关系

| 原 ibverbs/rdma_cm | URMA |
| --- | --- |
| `ibv_context` + `ibv_pd` | `urma_context_t` |
| `ibv_cq` / `ibv_comp_channel` | `urma_jfc_t` / `urma_jfce_t`（`jfce->fd` 可 epoll） |
| `ibv_qp`（RC） | `urma_jetty_t`（jfs + jfr 对） |
| `ibv_mr` | `urma_target_seg_t`（内存段） |
| rdma_cm 建连 | 带外 TCP 通道交换 `urma_rjetty_t` |

### 建连流程（无 rdma_cm）

1. 服务端在普通 **TCP** socket 上监听（复用 `rdma-bind`/`rdma-port` 配置）。
2. 客户端向服务端发起 TCP 连接，双方通过该通道互发 `urma_get_rjetty()` 得到的
   `urma_rjetty_t`。
3. 各自执行 `urma_import_jetty()` + `urma_bind_jetty()` 建立 RC 通道，随后关闭
   TCP 通道，事件切换到 `jfce->fd`（完成事件驱动）。
4. 数据面：
   - 流量数据：`URMA_OPC_WRITE_IMM` 写入对端注册的 RX 缓冲，立即数（imm）承载本次
     写入字节数，接收侧由 `URMA_CR_OPC_WRITE_WITH_IMM` 完成记录获知数据长度；
   - 控制命令（特性协商 / keepalive / RX 缓冲注册）：SEND/RECV，64 字节定长命令。

> 交换的 `urma_seg_t`/token 等描述符为原始内存布局，要求主客两端使用同一构建版本。

## 2. 编译

### 2.1 编译并安装 umdk（URMA 部分）

```bash
git clone <umdk 仓库> ~/umdk
cd ~/umdk && mkdir build && cd build
# 只构建 URMA，跳过 tpsa/uvs 等无需组件
cmake .. -DBUILD_ALL=disable -DBUILD_URMA=enable -DBUILD_UDMA=disable
make -j$(nproc)

# 安装头文件与库到自定义目录（示例）
install -d ~/umdk-install/include/urma ~/umdk-install/lib
cp src/urma/lib/urma/core/include/urma_api.h \
   src/urma/lib/urma/core/include/urma_types.h \
   src/urma/lib/urma/core/include/urma_opcode.h \
   ~/umdk-install/include/urma/
cp -a lib/liburma.so* lib/liburma_common.so* ~/umdk-install/lib/
```

### 2.2 编译 redis-server

```bash
make BUILD_URMA=yes                 # 默认 URMA_PATH=/home/work/umdk-install
# 或指定 umdk 安装路径：
make BUILD_URMA=yes URMA_PATH=~/umdk-install
```

说明：

- `URMA_PATH` 需包含 `include/urma/*.h` 与 `lib/liburma.so*`。
- Makefile 链接参数使用 `-Wl,-rpath,$(URMA_PATH)/lib,--disable-new-dtags`，
  保证 `liburma_common.so` 这类传递依赖也能被解析（RUNPATH 不解析传递依赖）。
- 修改 Makefile 后若只重链不重编，请先 `rm -f src/redis-server`。
- **无需 UB 设备即可编译通过**；无设备时仅 RDMA 连接不可用，TCP 服务不受影响。

## 3. 运行配置

服务端配置项（与原 RDMA 实现一致）：

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `rdma-port` | 6380 | RDMA 服务监听端口（TCP 带外通道端口） |
| `rdma-bind` | 无 | 监听地址，可多个；**未配置时回退到硬编码 `10.10.1.1`**，部署时请显式配置为本机 UB 网卡 IP |
| `rdma-rx-size` | 1MB | RX 传输缓冲大小（64KB ~ 16MB） |
| `rdma-completion-vector` | -1 | 完成事件中断号，负数表示 0 |

示例 `redis.conf`：

```conf
port 6379
rdma-port 6380
rdma-bind 192.168.1.10        # 本机 UB 网卡地址
rdma-rx-size 1mb
```

启动：

```bash
./redis-server redis.conf
```

日志出现 `[RDMA] urma transport listening on <bind>:<port>` 即监听成功。

## 4. 客户端

### 4.1 redis-cli / redis-benchmark（`--urma`）

`redis-cli` 与 `redis-benchmark` 内置 URMA 传输支持（`src/urma_client.c`），通过
`--urma` 选项启用。客户端先与 `rdma-port` 建立 TCP 连接作为带外通道完成 RC 建连，
之后数据面切换到 URMA 通道：

```bash
# redis-cli：走 RDMA 连接 6380 端口
./redis-cli --urma -h 192.168.1.10 -p 6380 ping

# redis-benchmark：全部压测连接走 RDMA
./redis-benchmark --urma -h 192.168.1.10 -p 6380 -t set,get -n 100000 -c 50
```

说明：

- `-p` 指定的必须是服务端的 **`rdma-port`**（默认 6380），而非普通 TCP 端口。
- `--urma` 不与 `-s`（unix socket）、`--tls`、`--cluster` 兼容，组合使用会报错退出。
- 客户端运行环境同样需要 URMA 栈：`liburma.so` / `liburma_common.so` 可通过
  `ldd` 确认能被解析（编译时 `URMA_PATH` 已通过 rpath 指定）。
- 建连过程会阻塞直至服务端完成握手；服务端未启用 RDMA 或握手失败时，
  客户端报错退出并保留底层错误信息（如 `Connection timed out`）。

### 4.2 服务间连接（复制）

主从复制中，对端 redis-server 实例即客户端，同样需要 URMA 栈：

```bash
# replica 侧配置
replicaof 192.168.1.10 6379
# 复制连接默认走 TCP；RDMA 连接由连接框架按服务端宣告的类型协商
```

## 5. 验证状态与已知限制

- ✅ 本地（无 UB 设备）`make BUILD_URMA=yes` 编译、链接通过，
  `ldd redis-server` 可正确解析 `liburma.so` / `liburma_common.so`。
- ✅ `urma_init(NULL)` 在无 UB 设备的机器上立即返回 0（不阻塞启动）。
- ⚠️ **未做真实 UB 环境运行验证**：建连、读写、复制等数据面行为需在带 UB 网卡的
  机器上测试。建议的最小验证序列：
  1. 两台机器分别确认 `urma_get_device_list()` 能枚举到设备；
  2. 服务端配置 `rdma-bind <UB NIC IP>` 后启动，确认监听日志；
  3. 对端以 `replicaof` 发起复制，观察双方日志中 RDMA 连接建立与
     `Ready to accept connections`；
  4. 压测/长跑验证 keepalive 与断连回收（`rdmaHandleDisconnect` 路径）。
- ⚠️ 命令结构从 32B 扩展为 **64B**（容纳 `urma_seg_t` + token），与原 ibv 版
  RDMA 实现的线上协议**不兼容**，主客两端必须使用本构建。
- ⚠️ `rdma-bind` 未配置时的硬编码回退地址 `10.10.1.1`（`src/rdma.c`
  `connRdmaListen()`）为当前代码的显式行为，生产环境务必显式配置。

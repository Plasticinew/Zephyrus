#pragma once

#include <string>
#include <vector>
#include <cstdarg>
#include <unordered_map>
#include <chrono>
#include <iostream>
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <infiniband/mlx5dv.h>
#include <thread>
#include <sys/mman.h>
#include <fstream>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
#include <tbb/concurrent_hash_map.h>
#include <atomic>

using std::string;
using std::vector;
using std::unordered_map;
using std::thread;
using std::atomic;

namespace Zephyrus {

#define NOTIFY_WORK 0xFF
#define NOTIFY_IDLE 0x00
#define MAX_MSG_SIZE 16384
#define MAX_SERVER_WORKER 1
#define MAX_SERVER_CLIENT 4096
#define RESOLVE_TIMEOUT_MS 5000
#define RDMA_TIMEOUT_US (uint64_t)10000000
#define RETRY_TIMEOUT 1 
#define MAX_REQUESTOR_NUM 32768
#define MAX_REMOTE_SIZE (1UL << 25)

// Time
#define TIME_NOW (std::chrono::high_resolution_clock::now())
#define TIME_DURATION_US(start, end) (std::chrono::duration_cast<std::chrono::microseconds>((end) - (start)).count())

#define CHECK_RDMA_MSG_SIZE(T) \
    static_assert(sizeof(T) < MAX_MSG_SIZE, #T " msg size is too big!")


#define MAX_NIC_NUM 2

struct zDeviceConfig {
    uint16_t node_id;
    uint16_t num_devices;
    string eth_names[MAX_NIC_NUM];
    string mlx_names[MAX_NIC_NUM];
    string ports[MAX_NIC_NUM];
    string ips[MAX_NIC_NUM];
};

struct zTargetConfig {
    uint16_t num_nodes;
    string target_ports[16];
    string target_ips[16];
};

// Status Info
enum zStatus
{
    ZSTATUS_INIT,
    ZSTATUS_CONNECTED,
    ZSTATUS_ACCEPTED,
    ZSTATUS_ERROR
};

// Device Info
struct zDevice
{
    string mlx_name;
    string eth_name;
    string eth_ip;
    string port;
    ibv_context *context = NULL;
    zStatus status = ZSTATUS_INIT;
};

enum zQPType
{
    ZQP_ONESIDED,
    ZQP_RPC
};

enum ResStatus { RES_OK, RES_FAIL };

enum MsgType { MSG_REGISTER, MSG_UNREGISTER, MSG_FETCH, MSG_FETCH_FAST, MSG_MW_BIND, RPC_FUSEE_SUBTABLE, MSG_MW_REBIND, MSG_MW_CLASS_BIND, MSG_FREE_FAST, MSG_PRINT_INFO, MSG_MW_BATCH};

struct CNodeInit{
    uint16_t node_id;
    zQPType qp_type;
};


struct PData {
    uint64_t buf_addr;
    uint32_t buf_rkey[MAX_NIC_NUM];
    uint16_t qp_id;
    uint16_t conn_id;
    uint32_t size;
    uint16_t nic_num_;
    uint64_t gid1[MAX_NIC_NUM], gid2[MAX_NIC_NUM];
    uint16_t lid_[MAX_NIC_NUM];
    uint32_t dct_num_[MAX_NIC_NUM];
    char ip[MAX_NIC_NUM][16];
    char port[MAX_NIC_NUM][8];
    uint64_t qp_info_addr;
    uint32_t qp_info_rkey[MAX_NIC_NUM];
    uint64_t atomic_table_addr;
    uint32_t atomic_table_rkey[MAX_NIC_NUM];
};

struct CmdMsgBlock {
    uint8_t rsvd1[MAX_MSG_SIZE - 1];
    volatile uint8_t notify;
};

struct CmdMsgRespBlock {
    uint8_t rsvd1[MAX_MSG_SIZE - 1];
    volatile uint8_t notify;
};

class RequestsMsg {
public:
    uint64_t resp_addr;
    uint32_t resp_rkey;
    uint16_t id;
    uint8_t type;
};
CHECK_RDMA_MSG_SIZE(RequestsMsg);

class ResponseMsg {
public:
    uint8_t status;
};
CHECK_RDMA_MSG_SIZE(ResponseMsg);

class RegisterRequest : public RequestsMsg {
public:
    uint64_t size;
};
CHECK_RDMA_MSG_SIZE(RegisterRequest);

class RegisterResponse : public ResponseMsg {
public:
    uint64_t addr;
    uint32_t rkey[MAX_NIC_NUM];
};
CHECK_RDMA_MSG_SIZE(RegisterResponse);


#define WR_ENTRY_NUM 1024
#define MAX_QP_NUM 1024
#define ATOMIC_ENTRY_NUM 512

inline void z_debug(const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    vprintf(fmt, args);
    va_end(args);
}

struct zDCQP_requestor { 
    zDevice* device_;
    ibv_pd* pd_;
    ibv_qp* qp_;
    ibv_qp_ex* qp_ex_;
    mlx5dv_qp_ex* qp_mlx_ex_;
    ibv_cq* cq_;
    zStatus status_;
    uint8_t port_num_;
    uint16_t lid_;
    uint32_t dct_num_;
};


struct zDCQP_responder { 
    zDevice* device_;
    ibv_pd* pd_;
    ibv_qp* qp_;
    ibv_qp_ex* qp_ex_;
    mlx5dv_qp_ex* qp_mlx_ex_;
    ibv_cq* cq_;
    ibv_srq* srq_;
    ibv_ah* ah_;
    zStatus status_;
    uint64_t gid1, gid2, interface, subnet;
    uint8_t port_num_;
    uint16_t lid_;
    uint32_t dct_num_;
};


struct zPD {
    vector<ibv_pd*> m_pds;
    unordered_map<ibv_mr*, vector<ibv_mr*>> m_mrs;
    unordered_map<uint32_t, vector<uint32_t>> m_lkey_table;
    vector<vector<zDCQP_requestor*>> m_requestors;   
    vector<vector<zDCQP_responder*>> m_responders;
};

struct zWR_entry {
    uint64_t reserved;
    uint64_t wr_addr:48;
    uint64_t time_stamp:14;
    uint64_t finished:2;
};

struct zWR_header {
    uint64_t wr_addr:48;
    uint64_t time_stamp:14;
    uint64_t finished:2;
};

struct qp_info_table {
    uint64_t addr;
    uint32_t rkey[MAX_NIC_NUM];
};

struct zAtomic_entry {
    uint64_t offset:32;
    uint64_t qp_id:16;
    uint64_t time_stamp:16;
};

struct zAtomic_buffer{
    uint64_t buffer;
    uint64_t target_addr:48;
    uint64_t time_stamp:14;
    uint64_t finished:2;
};


struct zQP_requestor
{
    ibv_qp* qp_;
    ibv_cq* cq_;
    ibv_pd* pd_;
    rdma_event_channel* channel_;
    rdma_cm_id* cm_id_;
    zStatus status_;
    struct CmdMsgBlock *cmd_msg_;
    struct CmdMsgRespBlock *cmd_resp_;
    struct ibv_mr *msg_mr_;
    struct ibv_mr *resp_mr_;
    uint64_t server_cmd_msg_;
    uint32_t server_cmd_rkey_[MAX_NIC_NUM];
    uint64_t gid1, gid2, interface, subnet;
    uint16_t lid_;
    uint32_t dct_num_;
    uint32_t conn_id_;
    uint32_t qp_id_;
};

struct WorkerInfo {
    CmdMsgBlock *cmd_msg;
    CmdMsgRespBlock *cmd_resp_msg;
    struct ibv_mr *msg_mr;
    struct ibv_mr *resp_mr;
    rdma_cm_id *cm_id;
    struct ibv_cq *cq;
}; 

struct zQP_responder_connection{
    rdma_cm_id* cm_id_;
    struct ibv_cq *cq_;
    zStatus status_;
};

struct zTarget{
    string ip;
    string port;
    ibv_ah* ah;
    uint16_t lid_;
    uint32_t dct_num_;
};

typedef unordered_map<uint32_t, vector<uint32_t>> rkeyTable;

struct zEndpoint
{
    vector<zDevice*> m_devices;
    int m_device_num;
    int m_node_id;
    atomic<uint64_t> qp_num_ = 1;
};

struct zQP_responder
{
    rdma_event_channel* channel_;
    rdma_cm_id* cm_id_;
    ibv_pd* pd_;
    vector<zQP_responder_connection*> connections;
    uint32_t worker_num_ = 0;
    WorkerInfo** worker_info_;
    thread **worker_threads_;
};

struct zQP_listener {
    zPD *m_pd;
    zEndpoint *m_ep;
    unordered_map<int, zQP_responder*> listeners;
    // CmdMsgBlock* qp_log_[MAX_QP_NUM];
    // struct ibv_mr* qp_log_list_[MAX_QP_NUM];
    qp_info_table* qp_info;
    uint32_t qp_info_rkey[MAX_NIC_NUM];
    thread *flush_thread_;
};

struct zQP
{
    zPD *m_pd;
    zEndpoint *m_ep;
    rkeyTable *m_rkey_table;
    unordered_map<int, zQP_requestor*> m_requestors;
    tbb::concurrent_hash_map<uint64_t, ibv_wc_status> completed_table;
    unordered_map<int, zTarget*> m_targets;
    int primary_device = 0;
    int current_device = 0;
    uint16_t time_stamp;
    CmdMsgBlock *cmd_msg_;
    CmdMsgRespBlock *cmd_resp_;
    unordered_map<int, ibv_mr*> msg_mr_;
    unordered_map<int, ibv_mr*> resp_mr_;
    zWR_entry wr_entry_[WR_ENTRY_NUM];
    int entry_start_ = 0;
    int entry_end_ = 0;
    zQPType qp_type;
    int qp_id_ = 0;
    uint64_t remote_atomic_table_addr = 0;
    uint32_t remote_atomic_table_rkey[MAX_NIC_NUM];
    uint64_t remote_qp_info_addr = 0;
    uint32_t remote_qp_info_rkey[MAX_NIC_NUM];
};

zEndpoint* zEP_create(string config_file);
zPD* zPD_create(zEndpoint *ep, int pool_size);
zQP* zQP_create(zPD* pd, zEndpoint *ep, rkeyTable* table, zQPType qp_type);

zDCQP_requestor* zDCQP_create_requestor(zDevice *device, ibv_pd *pd);
int zQP_connect(zQP *qp, int nic_index, string ip, string port);

int zDCQP_read(zDCQP_requestor* requestor, ibv_ah* ah, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, uint32_t lid, uint32_t dct_num);
int zDCQP_write(zDCQP_requestor* requestor, ibv_ah* ah, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, uint32_t lid, uint32_t dct_num);
int zDCQP_CAS(zDCQP_requestor* requestor, ibv_ah* ah, void* local_addr, uint32_t lkey, uint64_t new_val, void* remote_addr, uint32_t rkey, uint32_t lid, uint32_t dct_num);

zDCQP_responder* zDCQP_create_responder(zDevice *device, ibv_pd *pd);
zQP_listener* zQP_listener_create(zPD* pd, zEndpoint *ep);
void zQP_flush(qp_info_table* qp_info);
int zQP_listen(zQP_listener *zqp, int nic_index, string ip, string port);
int zQP_accept(zQP_listener *zqp, int nic_index, rdma_cm_id *cm_id, zQPType qp_type, int node_id);
void zQP_worker(zPD *pd, zQP_responder *qp_instance, WorkerInfo *work_info, uint32_t num);

int zQP_read(zQP *zqp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, uint32_t time_stamp, vector<uint64_t> *wr_ids);
int zQP_write(zQP *zqp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, uint32_t time_stamp, bool use_log, vector<uint64_t> *wr_ids);
int zQP_CAS(zQP *zqp, void *local_addr, uint32_t lkey, uint64_t new_val, void* remote_addr, uint32_t rkey, uint32_t time_stamp);
int zQP_CAS_step2(zQP *zqp, uint64_t new_val, void* remote_addr, uint32_t rkey, uint32_t time_stamp, uint64_t offset);
int zQP_poll_thread(zQP *qp);

void zQP_RPC_Alloc(zQP* qp, uint64_t* addr, uint32_t* rkey, size_t size);
int worker_write(ibv_qp *qp, ibv_cq *cq, uint64_t local_addr, uint32_t lkey, uint32_t length, uint64_t remote_addr, uint32_t rkey);

int z_read(zQP *qp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey);
int z_write(zQP *qp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey);
int z_CAS(zQP *qp, void* local_addr, uint32_t lkey, uint64_t new_val, void* remote_addr, uint32_t rkey);
int z_recovery(zQP *qp);

int z_read_async(zQP *qp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, vector<uint64_t> *wr_ids);
int z_write_async(zQP *qp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, vector<uint64_t> *wr_ids);
int z_CAS_async(zQP *qp, void* local_addr, uint32_t lkey, uint64_t new_val, void* remote_addr, uint32_t rkey, vector<uint64_t> *wr_ids);
int z_poll_completion(zQP* qp, vector<uint64_t> *wr_ids);

int load_config(const char* fname, struct zDeviceConfig* config);
int load_config(const char* fname, struct zTargetConfig* config);

ibv_mr* mr_create(ibv_pd *pd, void *addr, size_t length);
ibv_mr* mr_malloc_create(zPD* pd, uint64_t &addr, size_t length);

}
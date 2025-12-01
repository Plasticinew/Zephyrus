
#pragma once

#include "common.h"

#include "zrdma.h"

namespace zrdma {

struct ibv_context * ib_get_ctx(uint32_t dev_id, uint32_t port_id);
struct ibv_qp * ib_create_rc_qp(struct ibv_pd * ib_pd, struct ibv_qp_init_attr * qp_init_attr);

int ib_connect_qp(struct ibv_qp * local_qp, 
    const struct QpInfo * local_qp_info, 
    const struct QpInfo * remote_qp_info, uint8_t conn_type, bool server);

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
    qp_info_table* qp_info;
    uint32_t qp_info_rkey[MAX_NIC_NUM];
    thread *flush_thread_;
};


struct zQP
{
    zPD *m_pd;
    zEndpoint *m_ep;
    rkeyTable *m_rkey_table;
    tbb::concurrent_vector<zQP_requestor*> m_requestors;
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
    std::atomic<int> entry_end_{0};
    std::atomic<uint64_t> size_counter_{0};
    zQPType qp_type;
    int qp_id_ = 0;
    uint64_t remote_atomic_table_addr = 0;
    uint32_t remote_atomic_table_rkey[MAX_NIC_NUM];
    uint64_t remote_qp_info_addr = 0;
    uint32_t remote_qp_info_rkey[MAX_NIC_NUM];
};

zQP* zQP_create(zPD* pd, zEndpoint *ep, rkeyTable* table, zQPType qp_type);
int zQP_connect(zQP *qp, int nic_index, string ip, string port);

zQP_listener* zQP_listener_create(zPD* pd, zEndpoint *ep);
void zQP_flush(qp_info_table* qp_info);
int zQP_listen(zQP_listener *zqp, int nic_index, string ip, string port);
int zQP_accept(zQP_listener *zqp, int nic_index, rdma_cm_id *cm_id, zQPType qp_type, int node_id);
void zQP_worker(zPD *pd, zQP_responder *qp_instance, WorkerInfo *work_info, uint32_t num);

int z_simple_read(zQP *zqp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey);
int z_simple_write(zQP *zqp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey);
int z_simple_write_async(zQP *zqp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, vector<uint64_t> *wr_ids);
int z_simple_read_async(zQP *zqp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, vector<uint64_t> *wr_ids);
int z_simple_CAS(zQP *zqp, void *local_addr, uint32_t lkey, uint64_t new_val, void* remote_addr, uint32_t rkey);
int zQP_read(zQP *zqp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, uint32_t time_stamp);
int zQP_write(zQP *zqp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, uint32_t time_stamp, bool use_log);
int zQP_CAS(zQP *zqp, void *local_addr, uint32_t lkey, uint64_t new_val, void* remote_addr, uint32_t rkey, uint32_t time_stamp);
int zQP_CAS_step2(zQP *zqp, uint64_t new_val, void* remote_addr, uint32_t rkey, uint32_t time_stamp, uint64_t offset);

int zQP_read_async(zQP *zqp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, uint32_t time_stamp, vector<uint64_t> *wr_ids);
int zQP_write_async(zQP *zqp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, uint32_t time_stamp, bool use_log, vector<uint64_t> *wr_ids);
int zQP_CAS_async(zQP *zqp, void *local_addr, uint32_t lkey, uint64_t new_val, void* remote_addr, uint32_t rkey, uint32_t time_stamp, vector<uint64_t> *wr_ids);
int zQP_CAS_step2_async(zQP *zqp, uint64_t new_val, void* remote_addr, uint32_t rkey, uint32_t time_stamp, uint64_t offset);
int zQP_poll_thread(zEndpoint *zep);
int zQP_post_send(zQP* zqp, ibv_send_wr *send_wr, ibv_send_wr **bad_wr, bool non_idempotent, uint32_t time_stamp, vector<uint64_t> *wr_ids);

void zQP_RPC_Alloc(zQP* qp, uint64_t* addr, uint32_t* rkey, size_t size);
int worker_write(ibv_qp *qp, ibv_cq *cq, uint64_t local_addr, uint32_t lkey, uint32_t length, uint64_t remote_addr, uint32_t rkey);

int z_read(zQP *qp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey);
int z_write(zQP *qp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey);
int z_CAS(zQP *qp, void* local_addr, uint32_t lkey, uint64_t new_val, void* remote_addr, uint32_t rkey);
int z_switch(zQP *qp);
int z_recovery(zQP *qp);

int z_read_async(zQP *qp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, vector<uint64_t> *wr_ids);
int z_write_async(zQP *qp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, vector<uint64_t> *wr_ids);
int z_CAS_async(zQP *qp, void* local_addr, uint32_t lkey, uint64_t new_val, void* remote_addr, uint32_t rkey, vector<uint64_t> *wr_ids);
int z_post_send_async(zQP* qp, ibv_send_wr *send_wr, ibv_send_wr **bad_wr, bool non_idempotent, uint32_t time_stamp, vector<uint64_t> *wr_ids);
int z_poll_completion(zQP* qp, vector<uint64_t> *wr_ids);
int z_poll_completion(zQP* qp, uint64_t wr_id);

}
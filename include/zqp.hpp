#pragma once

// #include "common.hpp"

#include "zrdma.hpp"

namespace zrdma {

struct ibv_context * ib_get_ctx(uint32_t dev_id, uint32_t port_id);
struct ibv_qp * ib_create_rc_qp(struct ibv_pd * ib_pd, struct ibv_qp_init_attr * qp_init_attr);

int ib_connect_qp(struct ibv_qp * local_qp, 
    const struct QpInfo * local_qp_info, 
    const struct QpInfo * remote_qp_info, uint8_t conn_type, bool server);

struct zqp_requestor
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

struct zTarget{
    string ip;
    string port;
    ibv_ah* ah;
    uint16_t lid_;
    uint32_t dct_num_;
};

class zqp
{
public:
    zqp(zendpoint* ep, zqpType qp_type) {
        m_ep = ep;
        m_pd = ep->m_pd;
        m_rkey_table = ep->m_rkey_table;
        qp_type = qp_type;
        time_stamp = random() % MAX_REQUESTOR_NUM;
        ibv_mr* msg_mr = m_pd->mr_malloc_create((uint64_t&)cmd_msg_, sizeof(CmdMsgBlock));
        memset(cmd_msg_, 0, sizeof(CmdMsgBlock));
        m_pd->fill_mr_list(msg_mr_, msg_mr);
        ibv_mr* resp_mr = m_pd->mr_malloc_create((uint64_t&)cmd_resp_, sizeof(CmdMsgRespBlock));
        memset(cmd_resp_, 0, sizeof(CmdMsgRespBlock));
        m_pd->fill_mr_list(resp_mr_, resp_mr);
    }

    int zqp_connect(int nic_index, string ip, string port) {
        auto start = TIME_NOW;
        if (nic_index < m_requestors.size() && (m_requestors[nic_index] != NULL && m_requestors[nic_index]->status_ == ZSTATUS_CONNECTED)){
            return -1;
        }
        zqpType qp_type = qp_type;
        zqp_requestor *qp_instance = new zqp_requestor();
        qp_instance->status_ = ZSTATUS_INIT;
        qp_instance->channel_ = rdma_create_event_channel();
        qp_instance->pd_ = m_pd->get_pd(nic_index);
        int result = rdma_create_id(qp_instance->channel_, &(qp_instance->cm_id_), NULL, RDMA_PS_TCP);
        assert(result == 0);

        rdma_addrinfo *t = NULL, *s = NULL;
        rdma_addrinfo *res, *src;
        int counter = 0;
        while( t == NULL && counter < 100) {
            counter += 1;
            struct sockaddr_in src_addr;   // 设置源地址（指定网卡设备）
            memset(&src_addr, 0, sizeof(src_addr));
            src_addr.sin_family = AF_INET;
            char ip_buf[16], port_buf[8];
            m_ep->get_address(nic_index, ip_buf, port_buf);
            inet_pton(AF_INET, ip_buf, &src_addr.sin_addr); // 本地网卡IP地址
            result = rdma_getaddrinfo(ip.c_str(), port.c_str(), NULL, &res);
            assert(result == 0);
            assert(result == 0);
            for(t = res; t; t = t->ai_next) {
                if(!rdma_resolve_addr(qp_instance->cm_id_, (struct sockaddr *)&src_addr, t->ai_dst_addr, RESOLVE_TIMEOUT_MS)) {
                    break;
                }
            }

        }
        assert(t != NULL);

        rdma_cm_event* event;
        result = rdma_get_cm_event(qp_instance->channel_, &event);
        if(result != 0) {
            perror("rdma_get_cm_event failed");
        }
        if(event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
            printf("rdma_get_cm_event addr resolve failed: %d\n", event->event);
        }
        assert(result == 0);
        assert(event->event == RDMA_CM_EVENT_ADDR_RESOLVED);
        rdma_ack_cm_event(event);
        // Addr resolve finished, make route resolve

        result = rdma_resolve_route(qp_instance->cm_id_, RESOLVE_TIMEOUT_MS);
        assert(result == 0);
        result = rdma_get_cm_event(qp_instance->channel_, &event);
        assert(result == 0);
        assert(event->event == RDMA_CM_EVENT_ROUTE_RESOLVED);
        rdma_ack_cm_event(event);
        ibv_cq* cq = NULL;
        ibv_comp_channel* comp_channel = ibv_create_comp_channel(m_ep->get_device_context(nic_index));
        assert(comp_channel != NULL);
        cq = ibv_create_cq(m_ep->get_device_context(nic_index), 1024, NULL, comp_channel, 0);
        assert(cq != NULL);
        result = ibv_req_notify_cq(cq, 0);

        m_ep->cq_list_.push_back(cq);
        assert(result == 0);
        
        ibv_qp_init_attr qp_init_attr;
        memset(&qp_init_attr, 0, sizeof(qp_init_attr));
        qp_init_attr.send_cq = cq;
        qp_init_attr.recv_cq = cq;
        qp_init_attr.qp_type = IBV_QPT_RC;
        qp_init_attr.cap.max_send_wr = 512;
        qp_init_attr.cap.max_recv_wr = 1;
        qp_init_attr.cap.max_send_sge = 16;
        qp_init_attr.cap.max_recv_sge = 16;
        qp_init_attr.cap.max_inline_data = 256;
        qp_init_attr.sq_sig_all = 0;
        result = rdma_create_qp(qp_instance->cm_id_, m_pd->get_pd(nic_index), &qp_init_attr);
        assert(result == 0);

        CNodeInit init_msg = {qp_id_, qp_type};
        rdma_conn_param conn_param;
        memset(&conn_param, 0, sizeof(conn_param));
        conn_param.responder_resources = 16;
        conn_param.initiator_depth = 16;
        conn_param.private_data = &init_msg;
        conn_param.private_data_len = sizeof(CNodeInit);
        conn_param.retry_count = RETRY_TIMEOUT;
        conn_param.rnr_retry_count = RETRY_TIMEOUT;
        while(event->event != RDMA_CM_EVENT_ESTABLISHED){
            result = rdma_connect(qp_instance->cm_id_, &conn_param);
            rdma_get_cm_event(qp_instance->channel_, &event);
        }
        
        struct PData server_pdata;
        memset(&server_pdata, 0, sizeof(server_pdata));
        memcpy(&server_pdata, event->param.conn.private_data, sizeof(server_pdata));

        if(server_pdata.nic_num_ > m_ep->get_device_num()){
            server_pdata.nic_num_ = m_ep->get_device_num();
        }
        qp_instance->server_cmd_msg_ = server_pdata.buf_addr;
        for(int i = 0; i < MAX_NIC_NUM; i ++) {
            qp_instance->server_cmd_rkey_[i] = server_pdata.buf_rkey[i];
        }
        // qp_instance->server_cmd_rkey_ = server_pdata.buf_rkey;
        qp_instance->conn_id_ = server_pdata.conn_id;
        qp_instance->qp_id_ = server_pdata.qp_id;
        if(qp_id_ == 0){
            qp_id_ = server_pdata.qp_id;
        }
        for(int i = 0; i < server_pdata.nic_num_; i++){
            zTarget* target;
            if(m_targets[i] != nullptr){
                target = m_targets[i];
                assert(m_targets[i] == target);
                assert(target->lid_ == server_pdata.lid_[i]);
                assert(target->dct_num_ == server_pdata.dct_num_[i]);
                assert(target->ip == string(server_pdata.ip[i]));
                assert(target->port == server_pdata.port[i]);
            }
            else{
                target = new zTarget();
                m_targets[i] = target;
                target->ah = m_pd->create_ah(i, server_pdata.gid1[i], server_pdata.gid2[i], server_pdata.gid2[i], server_pdata.gid1[i], server_pdata.lid_[i]);
                target->lid_ = server_pdata.lid_[i];
                target->dct_num_ = server_pdata.dct_num_[i];
                target->ip = string(server_pdata.ip[i]);
                target->port = server_pdata.port[i];
            }
        }
        
        assert(server_pdata.size == sizeof(CmdMsgBlock));
        qp_instance->cmd_msg_ = qp->cmd_msg_;
        qp_instance->msg_mr_ = qp->msg_mr_[nic_index];

        qp_instance->cmd_resp_ = qp->cmd_resp_;
        qp_instance->resp_mr_ = qp->resp_mr_[nic_index];

        if(qp->remote_atomic_table_addr == 0) {
            qp->remote_atomic_table_addr = server_pdata.atomic_table_addr;
            for(int i = 0; i < MAX_NIC_NUM; i ++) {
                qp->remote_atomic_table_rkey[i] = server_pdata.atomic_table_rkey[i];
            }
            for(int i = 0; i < MAX_NIC_NUM; i++){
                if(qp->m_rkey_table->find(qp->remote_atomic_table_rkey[0]) == qp->m_rkey_table->end())
                    (*qp->m_rkey_table)[qp->remote_atomic_table_rkey[0]] = std::vector<uint32_t>();
                qp->m_rkey_table->at(qp->remote_atomic_table_rkey[0]).push_back(qp->remote_atomic_table_rkey[i]);
            }
        }
        else if(qp->remote_atomic_table_addr != server_pdata.atomic_table_addr) {
            std::cerr << "Error, atomic table address mismatch" << std::endl;
            return -1;
        }
        if(qp->remote_qp_info_addr == 0) {
            qp->remote_qp_info_addr = server_pdata.qp_info_addr;
            for(int i = 0; i < MAX_NIC_NUM ; i ++) {
                qp->remote_qp_info_rkey[i] = server_pdata.qp_info_rkey[i];
            }
            for(int i = 0; i < MAX_NIC_NUM; i++){
                if(qp->m_rkey_table->find(qp->remote_qp_info_rkey[0]) == qp->m_rkey_table->end())
                    (*qp->m_rkey_table)[qp->remote_qp_info_rkey[0]] = std::vector<uint32_t>();
                qp->m_rkey_table->at(qp->remote_qp_info_rkey[0]).push_back(qp->remote_qp_info_rkey[i]);
            }
        }
        else if(qp->remote_qp_info_addr != server_pdata.qp_info_addr) {
            std::cerr << "Error, qp info address mismatch" << std::endl;
            return -1;
        }

        rdma_ack_cm_event(event);

        rdma_freeaddrinfo(res);

        // Connect finished
        qp_instance->cq_ = cq;
        qp_instance->qp_ = qp_instance->cm_id_->qp;

        qp->m_requestors.push_back(qp_instance);
        assert(qp->m_requestors[nic_index] == qp_instance);
        qp_instance->status_ = ZSTATUS_CONNECTED;
        // printf("QP connected: qp_id=%u, nic_index=%d, server_qp_id=%u\n", qp->qp_id_, nic_index, qp_instance->qp_id_);
        // ProfilerStop();
        return 0;
    }

private:
    zpd *m_pd;
    zendpoint *m_ep;
    rkeyTable *m_rkey_table;
    tbb::concurrent_vector<zqp_requestor*> m_requestors;
    zTarget* m_targets[MAX_NIC_NUM];
    int primary_device = 0;
    int current_device = 0;
    uint16_t time_stamp;
    CmdMsgBlock *cmd_msg_;
    CmdMsgRespBlock *cmd_resp_;
    ibv_mr* msg_mr_[MAX_NIC_NUM];
    ibv_mr* resp_mr_[MAX_NIC_NUM];
    zWR_entry wr_entry_[WR_ENTRY_NUM];
    int entry_start_ = 0;
    std::atomic<int> entry_end_{0};
    std::atomic<uint64_t> size_counter_{0};
    zqpType qp_type;
    int qp_id_ = 0;
    uint64_t remote_atomic_table_addr = 0;
    uint32_t remote_atomic_table_rkey[MAX_NIC_NUM];
    uint64_t remote_qp_info_addr = 0;
    uint32_t remote_qp_info_rkey[MAX_NIC_NUM];
};

}

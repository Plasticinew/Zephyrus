#pragma once

// #include "common.hpp"

#include "zrdma.hpp"

namespace zrdma {

struct zqp_responder_connection{
    rdma_cm_id* cm_id_;
    struct ibv_cq *cq_;
    zStatus status_;
};

struct zqp_responder
{
    rdma_event_channel* channel_;
    rdma_cm_id* cm_id_;
    ibv_pd* pd_;
    vector<zqp_responder_connection*> connections;
    uint32_t worker_num_ = 0;
    WorkerInfo** worker_info_;
    thread **worker_threads_;
};

class zserver{
public:
    zserver(string config_file){
        m_ep = new zendpoint(config_file);
        m_pd = new zpd(m_ep, MAX_SERVER_WORKER);
        ibv_mr* mr = m_pd->mr_malloc_create((uint64_t&)qp_info, sizeof(qp_info_table)*MAX_QP_NUM);
        for(int i = 0; i < m_pd->get_device_num(); i ++) {
            qp_info_rkey[i] = mr->rkey;
        }
        flush_thread_ = new std::thread(zqp_flush, qp_info);
    }
    ~zserver();
    bool create_listener(int nic_index, string ip, string port) {
        if(listeners.find(nic_index) != listeners.end()) {
            std::cout << "Listener already exists on nic " << nic_index << std::endl;
            return false;
        }
        zqp_responder *qp = new zqp_responder();
        qp->worker_info_ = new WorkerInfo *[MAX_SERVER_WORKER*MAX_SERVER_CLIENT];
        qp->worker_threads_ = new std::thread *[MAX_SERVER_WORKER];
        qp->pd_ = m_pd->get_pd(nic_index);
        for (uint32_t i = 0; i < MAX_SERVER_WORKER; i++) {
            qp->worker_info_[i] = nullptr;
            qp->worker_threads_[i] = nullptr;
        }
    
        listeners[nic_index] = qp;
        qp->channel_ = rdma_create_event_channel();
        int result = rdma_create_id(qp->channel_, &(qp->cm_id_), NULL, RDMA_PS_TCP);

        assert(result == 0);
        sockaddr_in sin;
        memset(&sin, 0, sizeof(sin));
        sin.sin_family = AF_INET;
        sin.sin_port = htons(atoi(port.c_str()));
        sin.sin_addr.s_addr = inet_addr(ip.c_str());
        result = rdma_bind_addr(qp->cm_id_, (sockaddr*)&sin);
        assert(result == 0);
        result = rdma_listen(qp->cm_id_, 1024);
        assert(result == 0);
        std::thread* listener_thread = new std::thread(zqp_listen, qp, nic_index, ip, port);
        return true;
    }
    int accept(int nic_index, rdma_cm_id *cm_id, zqpType qp_type, int node_id) {
        int id = node_id; 
        if(id == 0) {
            id = qp_num_.fetch_add(1);
        }

        zqp_responder_connection *conn = new zqp_responder_connection();
        conn->cm_id_ = cm_id;

        ibv_comp_channel* comp_channel = ibv_create_comp_channel(m_ep->get_device_context(nic_index));
        assert(comp_channel != NULL);
        ibv_cq* cq = ibv_create_cq(m_ep->get_device_context(nic_index), 1, NULL, comp_channel, 0);
        assert(cq != NULL);
        int result = ibv_req_notify_cq(cq, 0);
        assert(result == 0);
        ibv_qp_init_attr qp_init_attr;
        memset(&qp_init_attr, 0, sizeof(qp_init_attr));
        qp_init_attr.send_cq = cq;
        qp_init_attr.recv_cq = cq;
        qp_init_attr.qp_type = IBV_QPT_RC;    
        qp_init_attr.cap.max_send_wr = 1;
        qp_init_attr.cap.max_recv_wr = 1;
        qp_init_attr.cap.max_send_sge = 1;
        qp_init_attr.cap.max_recv_sge = 1;
        qp_init_attr.cap.max_inline_data = 256;
        qp_init_attr.sq_sig_all = 0;
        result = rdma_create_qp(cm_id, m_pd->get_pd(nic_index), &qp_init_attr);
        assert(result == 0);

        struct PData rep_pdata;
        memset(&rep_pdata, 0, sizeof(rep_pdata));
        CmdMsgBlock *cmd_msg = nullptr;
        CmdMsgRespBlock *cmd_resp = nullptr;
        struct ibv_mr *msg_mr = nullptr;
        struct ibv_mr *resp_mr = nullptr;
        cmd_msg = new CmdMsgBlock();
        memset(cmd_msg, 0, sizeof(CmdMsgBlock));
        msg_mr = m_pd->mr_create(nic_index, (void *)cmd_msg, sizeof(CmdMsgBlock));

        cmd_resp = new CmdMsgRespBlock();
        memset(cmd_resp, 0, sizeof(CmdMsgRespBlock));
        resp_mr = m_pd->mr_create(nic_index, (void *)cmd_resp, sizeof(CmdMsgRespBlock));
        rep_pdata.qp_id = id;
        
        if(qp_info[id].addr == 0) {
            ibv_mr* mr = m_pd->mr_malloc_create(qp_info[id].addr, sizeof(zAtomic_buffer)*WR_ENTRY_NUM);
            m_pd->fill_rkey_table(qp_info[id].rkey, mr);
        }
        // rep_pdata.id = -1;
        auto qp_instance = listeners[nic_index];
        rep_pdata.conn_id = -1;
        if(qp_type == ZQP_RPC){
            int num = qp_instance->worker_num_;
            if (num < MAX_SERVER_WORKER) {
                qp_instance->worker_info_[num] = new WorkerInfo();
                qp_instance->worker_info_[num]->cmd_msg = cmd_msg;
                qp_instance->worker_info_[num]->cmd_resp_msg = cmd_resp;
                qp_instance->worker_info_[num]->msg_mr = msg_mr;
                qp_instance->worker_info_[num]->resp_mr = resp_mr;
                qp_instance->worker_info_[num]->cm_id = cm_id;
                qp_instance->worker_info_[num]->cq = cq;
                qp_instance->worker_threads_[num] =
                new std::thread(&zqp_worker, m_pd, qp_instance, qp_instance->worker_info_[num], num);
            } else {
                qp_instance->worker_info_[num] = new WorkerInfo();
                qp_instance->worker_info_[num]->cmd_msg = cmd_msg;
                qp_instance->worker_info_[num]->cmd_resp_msg = cmd_resp;
                qp_instance->worker_info_[num]->msg_mr = msg_mr;
                qp_instance->worker_info_[num]->resp_mr = resp_mr;
                qp_instance->worker_info_[num]->cm_id = cm_id;
                qp_instance->worker_info_[num]->cq = cq;
            } 
            rep_pdata.conn_id = num;
            qp_instance->worker_num_ += 1;
        } 
        if(qp_type == ZQP_RPC){
            rep_pdata.buf_addr = (uintptr_t)cmd_msg;
            for(int i = 0; i < MAX_NIC_NUM; i++){
                rep_pdata.buf_rkey[i] = msg_mr->rkey;
            }
        }
        else{
            rep_pdata.buf_addr = qp_info[id].addr;
            for(int i = 0; i < MAX_NIC_NUM; i++){
                rep_pdata.buf_rkey[i] = qp_info[id].rkey[i];
            }
        }
        rep_pdata.size = sizeof(CmdMsgRespBlock);
        rep_pdata.nic_num_ = m_pd->m_responders.size();
        rep_pdata.qp_info_addr = (uint64_t)(qp_info);
        for(int i = 0; i < MAX_NIC_NUM; i++){
            rep_pdata.qp_info_rkey[i] = qp_info_rkey[i];
        }
        rep_pdata.atomic_table_addr = (uint64_t)(qp_info[id].addr);
        for(int i = 0; i < MAX_NIC_NUM; i++){
            rep_pdata.atomic_table_rkey[i] = qp_info[id].rkey[i];
        }
        for(int i = 0; i < rep_pdata.nic_num_; i++){
            rep_pdata.gid1[i] = m_pd->m_responders[i][0]->gid1;
            rep_pdata.gid2[i] = m_pd->m_responders[i][0]->gid2;
            rep_pdata.lid_[i] = m_pd->m_responders[i][0]->lid_;
            rep_pdata.dct_num_[i] = m_pd->m_responders[i][0]->dct_num_;
            m_ep->get_address(i, rep_pdata.ip[i], rep_pdata.port[i]);
        }
        rdma_conn_param conn_param;
        memset(&conn_param, 0, sizeof(conn_param));
        conn_param.responder_resources = 16;
        conn_param.initiator_depth = 16;
        conn_param.private_data = &rep_pdata;
        conn_param.private_data_len = sizeof(rep_pdata);
        conn_param.retry_count = RETRY_TIMEOUT;
        conn_param.rnr_retry_count = RETRY_TIMEOUT;
        result = rdma_accept(cm_id, &conn_param);
        assert(result == 0);

        conn->cq_ = cq;
        conn->cm_id_ = cm_id;
        conn->status_ = zStatus::ZSTATUS_ACCEPTED;
        qp_instance->connections.push_back(conn);

        return 0;
    }
private:
    zpd *m_pd;
    zendpoint *m_ep;
    unordered_map<int, zqp_responder*> listeners;
    qp_info_table* qp_info;
    uint32_t qp_info_rkey[MAX_NIC_NUM];
    thread *flush_thread_;
    std::atomic<uint64_t> qp_num_{1};
};
    
int zqp_listen(zserver* server, rdma_event_channel *channel, int nic_index) {
    while(true) {
        rdma_cm_event* event;
        int result = rdma_get_cm_event(channel, &event);
        assert(result == 0);
        if(event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
            rdma_cm_id* new_cm_id = event->id;
            CNodeInit msg = *(CNodeInit*)event->param.conn.private_data;
            rdma_ack_cm_event(event);
            server->accept(nic_index, new_cm_id, msg.qp_type, msg.node_id);
        } else {
            rdma_ack_cm_event(event);
        }
    }
    return 0;
}

void zqp_flush(qp_info_table* qp_info) {
    while(true) {
        for(int i = 0; i < MAX_QP_NUM; i ++) {
            if (qp_info[i].addr == 0){
                continue;
            }
            zAtomic_buffer* atomic_buffer = (zAtomic_buffer*)qp_info[i].addr;
            for(int j = 0; j < WR_ENTRY_NUM; j ++) {
                if(atomic_buffer[j].target_addr != 0 && atomic_buffer[j].finished == 0) {
                    uint64_t target_addr = atomic_buffer[j].target_addr;
                    zAtomic_entry entry = *(zAtomic_entry*)(uintptr_t)target_addr;
                    if(entry.offset != j || entry.qp_id != i || entry.offset != j) {
                        atomic_buffer[j].finished = 2;
                        continue;
                    }
                    *(volatile uint64_t*)target_addr = atomic_buffer[j].buffer;
                    atomic_buffer[j].finished = 1;
                    printf("flush qp %d entry %d: buffer=%lu\n", i, j, atomic_buffer[j].buffer);
                }
            }
        }
        // std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

void zqp_worker(zpd *pd, zqp_responder *qp_instance, WorkerInfo *work_info, uint32_t num) {
    printf("start worker %d\n", num);
    CmdMsgBlock *cmd_msg = work_info->cmd_msg;
    CmdMsgRespBlock *cmd_resp = work_info->cmd_resp_msg;
    struct ibv_mr *resp_mr = work_info->resp_mr;
    cmd_resp->notify = NOTIFY_WORK;
    int active_id = -1;
    int record = num;
    while (true) {
        for (int i = record; i < qp_instance->worker_num_; i+=MAX_SERVER_WORKER) {
            if (qp_instance->worker_info_[i]->cmd_msg->notify != NOTIFY_IDLE){
                active_id = i;
                cmd_msg = qp_instance->worker_info_[i]->cmd_msg;
                record = i + MAX_SERVER_WORKER;
                break;
            }
        }
        if (active_id == -1) {
            record = num;
            continue;
        }
        cmd_msg->notify = NOTIFY_IDLE;
        RequestsMsg *request = (RequestsMsg *)cmd_msg;
        if(active_id != request->id) {
            printf("find %d, receive from id:%d\n", active_id, request->id);
        }
        assert(active_id == request->id);
        work_info = qp_instance->worker_info_[request->id];
        cmd_resp = work_info->cmd_resp_msg;
        memset(cmd_resp, 0, sizeof(CmdMsgRespBlock));
        resp_mr = work_info->resp_mr;
        cmd_resp->notify = NOTIFY_WORK;
        active_id = -1;
        if (request->type == MSG_REGISTER) {

            RegisterRequest *reg_req = (RegisterRequest *)request;
            RegisterResponse *resp_msg = (RegisterResponse *)cmd_resp;
            ibv_mr* mr;
            if ((mr = pd->mr_malloc_create(resp_msg->addr, 
                                            reg_req->size)) == NULL) {
                resp_msg->status = RES_FAIL;
            } else {
                pd->fill_rkey_table(resp_msg->rkey, mr);
                resp_msg->status = RES_OK;
            }

            worker_write(work_info->cm_id->qp, work_info->cq, (uint64_t)cmd_resp, resp_mr->lkey,
                        sizeof(CmdMsgRespBlock), reg_req->resp_addr,
                        reg_req->resp_rkey);
        } else if (request->type == MSG_FETCH_FAST) {
           
        } else if (request->type == MSG_FREE_FAST) {
            
        } else if (request->type == MSG_MW_REBIND) {
            
        } else if (request->type == RPC_FUSEE_SUBTABLE){
            
        } else if (request->type == MSG_UNREGISTER) {
            
        } else if(request->type == MSG_PRINT_INFO){
           
        } else if(request->type == MSG_MW_BATCH){
            
        } else {
            printf("wrong request type\n");
        }
    } 
}

int worker_write(ibv_qp *qp, ibv_cq *cq, uint64_t local_addr, uint32_t lkey, uint32_t length, uint64_t remote_addr, uint32_t rkey) {
    struct ibv_sge sge;
    sge.addr = (uintptr_t)local_addr;
    sge.length = length;
    sge.lkey = lkey;

    struct ibv_send_wr send_wr = {};
    struct ibv_send_wr *bad_send_wr;
    send_wr.wr_id = 0;
    send_wr.num_sge = 1;
    send_wr.next = NULL;
    send_wr.opcode = IBV_WR_RDMA_WRITE;
    send_wr.sg_list = &sge;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.wr.rdma.remote_addr = remote_addr;
    send_wr.wr.rdma.rkey = rkey;
    int error_code;
    if (error_code = ibv_post_send(qp, &send_wr, &bad_send_wr)) {
        perror("ibv_post_send write fail");
        printf("error code %d\n", error_code);
        return -1;
    }

    auto start = TIME_NOW;
    struct ibv_wc wc;
    int ret = -1;
    while (true) {
        if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
            perror("remote write timeout");
            return -1;
        }
        int rc = ibv_poll_cq(cq, 1, &wc);
        if (rc > 0) {
        if (IBV_WC_SUCCESS == wc.status) {
            ret = 0;
            break;
        } else if (IBV_WC_WR_FLUSH_ERR == wc.status) {
            perror("cmd_send IBV_WC_WR_FLUSH_ERR");
            break;
        } else if (IBV_WC_RNR_RETRY_EXC_ERR == wc.status) {
            perror("cmd_send IBV_WC_RNR_RETRY_EXC_ERR");
            break;
        } else {
            perror("cmd_send ibv_poll_cq status error");
            printf("%d\n", wc.status);
            break;
        }
        } else if (0 == rc) {
            continue;
        } else {
            perror("ibv_poll_cq fail");
            break;
        }
    }
    return 0;
}

}
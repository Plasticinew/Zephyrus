#include "zqp.h"
#include <gperftools/profiler.h>

namespace zrdma {

int z_simple_write(zqp *zqp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey) {
    zqp_requestor *requestor = zqp->m_requestors[zqp->current_device];
    struct ibv_sge sge;
    struct ibv_send_wr send_wr;
    struct ibv_send_wr *bad_send_wr;
    
    sge.addr = (uint64_t)local_addr;
    sge.length = length;
    if(zqp->current_device != 0){
        tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
        zqp->m_pd->m_lkey_table.find(a, lkey);
        sge.lkey = a->second[zqp->current_device];
    } else {
        sge.lkey = lkey;
    }

    send_wr.wr_id = 0;
    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    send_wr.next = NULL;
    send_wr.opcode = IBV_WR_RDMA_WRITE;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.wr.rdma.remote_addr = (uint64_t)remote_addr;
    if(zqp->current_device != 0){
        send_wr.wr.rdma.rkey = zqp->m_rkey_table->at(rkey)[zqp->current_device];
    } else {
        send_wr.wr.rdma.rkey = rkey;
    }
    ibv_qp* qp = requestor->qp_;
    if (ibv_post_send(qp, &send_wr, &bad_send_wr)) {
        z_switch(zqp);
        return -1;
    }
    int result;
    auto start = TIME_NOW;
    struct ibv_wc wc;
    ibv_cq* cq = requestor->cq_;
    while(true) {
        if(TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
            z_switch(zqp);
            result = -1;
            // std::cerr << "Error, write timeout" << std::endl;
            break;
        }
        if(ibv_poll_cq(cq, 1, &wc) > 0) {
            if(wc.status != IBV_WC_SUCCESS) {
                z_switch(zqp);
                result = -1;
                break;
            }
            result = 0;
            break;
        }
    }
    zqp->size_counter_.fetch_add(length);
    return result;
}

int z_simple_write_async(zqp *zqp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, vector<uint64_t> *wr_ids) {
    zqp_requestor *requestor = zqp->m_requestors[zqp->current_device];
    struct ibv_sge sge;
    struct ibv_send_wr send_wr;
    struct ibv_send_wr *bad_send_wr;
    
    sge.addr = (uint64_t)local_addr;
    sge.length = length;
    if(zqp->current_device != 0){
        tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
        zqp->m_pd->m_lkey_table.find(a, lkey);
        sge.lkey = a->second[zqp->current_device];
    } else {
        sge.lkey = lkey;
    }

    send_wr.wr_id = ++(zqp->time_stamp);
    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    send_wr.next = NULL;
    send_wr.opcode = IBV_WR_RDMA_WRITE;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.wr.rdma.remote_addr = (uint64_t)remote_addr;
    if(zqp->current_device != 0){
        send_wr.wr.rdma.rkey = zqp->m_rkey_table->at(rkey)[zqp->current_device];
    } else {
        send_wr.wr.rdma.rkey = rkey;
    }
    ibv_qp* qp = requestor->qp_;
    if (ibv_post_send(qp, &send_wr, &bad_send_wr)) {
        z_switch(zqp);
        return -1;
    }
    wr_ids->push_back(send_wr.wr_id);
    zqp->size_counter_.fetch_add(length);
    return 0;
}

int z_simple_read(zqp *zqp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey) {
    zqp_requestor *requestor = zqp->m_requestors[zqp->current_device];
    struct ibv_sge *sge = new ibv_sge();
    struct ibv_send_wr *send_wr = new ibv_send_wr();
    struct ibv_send_wr *bad_send_wr;
    struct ibv_sge log_sge;
    struct ibv_send_wr log_wr;
    
    sge->addr = (uint64_t)local_addr;
    sge->length = length;
    if(zqp->current_device != 0){
        tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
        zqp->m_pd->m_lkey_table.find(a, lkey);
        sge->lkey = a->second[zqp->current_device];
    } else {
        sge->lkey = lkey;
    }

    send_wr->wr_id = 0;
    send_wr->sg_list = sge;
    send_wr->num_sge = 1;
    send_wr->next = NULL;
    send_wr->opcode = IBV_WR_RDMA_READ;
    send_wr->send_flags = IBV_SEND_SIGNALED;
    send_wr->wr.rdma.remote_addr = (uint64_t)remote_addr;
    if(zqp->current_device != 0){
        send_wr->wr.rdma.rkey = zqp->m_rkey_table->at(rkey)[zqp->current_device];
    } else {
        send_wr->wr.rdma.rkey = rkey;
    }
    ibv_qp* qp = requestor->qp_;
    if (ibv_post_send(qp, send_wr, &bad_send_wr)) {
        z_switch(zqp);
        return -1;
    }
    int result;
    auto start = TIME_NOW;
    struct ibv_wc wc;
    ibv_cq* cq = requestor->cq_;
    while(true) {
        if(TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
            // std::cerr << "Error, read timeout" << std::endl;
            break;
        }
        if(ibv_poll_cq(cq, 1, &wc) > 0) {
            if(wc.status != IBV_WC_SUCCESS) {
                result = -1;
                z_switch(zqp);
                break;
            }
            result = 0;
            break;
        }
    }
    return result;
}

int z_simple_read_async(zqp *zqp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, vector<uint64_t> *wr_ids) {
    zqp_requestor *requestor = zqp->m_requestors[zqp->current_device];
    struct ibv_sge sge;
    struct ibv_send_wr send_wr;
    struct ibv_send_wr *bad_send_wr;
    
    sge.addr = (uint64_t)local_addr;
    sge.length = length;
    if(zqp->current_device != 0){
        tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
        zqp->m_pd->m_lkey_table.find(a, lkey);
        sge.lkey = a->second[zqp->current_device];
    } else {
        sge.lkey = lkey;
    }

    send_wr.wr_id = ++(zqp->time_stamp);
    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    send_wr.next = NULL;
    send_wr.opcode = IBV_WR_RDMA_READ;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.wr.rdma.remote_addr = (uint64_t)remote_addr;
    if(zqp->current_device != 0){
        send_wr.wr.rdma.rkey = zqp->m_rkey_table->at(rkey)[zqp->current_device];
    } else {
        send_wr.wr.rdma.rkey = rkey;
    }
    ibv_qp* qp = requestor->qp_;
    if (ibv_post_send(qp, &send_wr, &bad_send_wr)) {
        z_switch(zqp);
        return -1;
    }
    wr_ids->push_back(send_wr.wr_id);
    zqp->size_counter_.fetch_add(length);
    return 0;
}

int z_simple_CAS(zqp *zqp, void *local_addr, uint32_t lkey, uint64_t new_val, void* remote_addr, uint32_t rkey) {
    zqp_requestor *requestor = zqp->m_requestors[zqp->current_device];
    struct ibv_sge *sge = new ibv_sge();
    struct ibv_send_wr *send_wr = new ibv_send_wr();
    struct ibv_send_wr *bad_send_wr;
    struct ibv_sge log_sge;
    struct ibv_send_wr log_wr;
    
    sge->addr = (uint64_t)local_addr;
    sge->length = sizeof(uint64_t);
    if(zqp->current_device != 0){
        tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
        zqp->m_pd->m_lkey_table.find(a, lkey);
        sge->lkey = a->second[zqp->current_device];
    } else {
        sge->lkey = lkey;
    }

    send_wr->wr_id = 0;
    send_wr->sg_list = sge;
    send_wr->num_sge = 1;
    send_wr->next = NULL;
    send_wr->opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    send_wr->send_flags = IBV_SEND_SIGNALED;
    send_wr->wr.atomic.remote_addr = (uint64_t)remote_addr;
    if(zqp->current_device != 0){
        send_wr->wr.atomic.rkey = zqp->m_rkey_table->at(rkey)[zqp->current_device];
    } else {
        send_wr->wr.atomic.rkey = rkey;
    }
    send_wr->wr.atomic.compare_add = *(uint64_t*)local_addr;
    send_wr->wr.atomic.swap = new_val;
    ibv_qp* qp = requestor->qp_;
    if (ibv_post_send(qp, send_wr, &bad_send_wr)) {
        std::cout << "Send error" << std::endl;
        z_switch(zqp);
        return -1;
    }
    int result;
    auto start = TIME_NOW;
    struct ibv_wc wc;
    ibv_cq* cq = requestor->cq_;
    while(true) {
        if(TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
            z_switch(zqp);
            result = -1;
            std::cout << "Error, read timeout" << std::endl;
            break;
        }
        if(ibv_poll_cq(cq, 1, &wc) > 0) {
            if(wc.status != IBV_WC_SUCCESS) {
                std::cout << "Wc error " << wc.status << std::endl;
                z_switch(zqp);
                result = -1;
                break;
            }
            result = 0;
            break;
        }
    }
    return result;
}

int z_simple_CAS_async(zqp *zqp, void *local_addr, uint32_t lkey, uint64_t new_val, void* remote_addr, uint32_t rkey, vector<uint64_t> *wr_ids) {
    zqp_requestor *requestor = zqp->m_requestors[zqp->current_device];
    struct ibv_sge *sge = new ibv_sge();
    struct ibv_send_wr *send_wr = new ibv_send_wr();
    struct ibv_send_wr *bad_send_wr;
    struct ibv_sge log_sge;
    struct ibv_send_wr log_wr;
    
    sge->addr = (uint64_t)local_addr;
    sge->length = sizeof(uint64_t);
    if(zqp->current_device != 0){
        tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
        zqp->m_pd->m_lkey_table.find(a, lkey);
        sge->lkey = a->second[zqp->current_device];
    } else {
        sge->lkey = lkey;
    }

    send_wr->wr_id = ++(zqp->time_stamp);
    send_wr->sg_list = sge;
    send_wr->num_sge = 1;
    send_wr->next = NULL;
    send_wr->opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    send_wr->send_flags = IBV_SEND_SIGNALED;
    send_wr->wr.atomic.remote_addr = (uint64_t)remote_addr;
    if(zqp->current_device != 0){
        send_wr->wr.atomic.rkey = zqp->m_rkey_table->at(rkey)[zqp->current_device];
    } else {
        send_wr->wr.atomic.rkey = rkey;
    }
    send_wr->wr.atomic.compare_add = *(uint64_t*)local_addr;
    send_wr->wr.atomic.swap = new_val;
    ibv_qp* qp = requestor->qp_;
    if (ibv_post_send(qp, send_wr, &bad_send_wr)) {
        z_switch(zqp);
        return -1;
    }
    wr_ids->push_back(send_wr->wr_id);
    return 0;
}

int zqp_read(zqp *zqp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, uint32_t time_stamp) {
    zqp_requestor *requestor = zqp->m_requestors[zqp->current_device];
    struct ibv_sge *sge = new ibv_sge();
    sge->addr = (uint64_t)local_addr;
    sge->length = length;
    sge->lkey = lkey;
    struct ibv_send_wr *send_wr = new ibv_send_wr();
    struct ibv_send_wr *bad_send_wr;
    
    int entry_index = zqp->entry_end_.fetch_add(1)%WR_ENTRY_NUM;
    zqp->wr_entry_[entry_index].time_stamp = time_stamp;
    zqp->wr_entry_[entry_index].wr_addr = (uint64_t)send_wr;
    zqp->wr_entry_[entry_index].finished = 0;
    uint64_t wr_id = *((uint64_t*)(&zqp->wr_entry_[entry_index])+1);
    vector<uint64_t> wr_ids;
    wr_ids.push_back(wr_id);

    send_wr->wr_id = wr_id;
    send_wr->sg_list = sge;
    send_wr->num_sge = 1;
    send_wr->next = NULL;
    send_wr->opcode = IBV_WR_RDMA_READ;
    send_wr->send_flags = IBV_SEND_SIGNALED;
    send_wr->wr.rdma.remote_addr = (uint64_t)remote_addr;
    send_wr->wr.rdma.rkey = rkey;

    ibv_qp* qp = requestor->qp_;
    if (ibv_post_send(qp, send_wr, &bad_send_wr)) {
        zqp->wr_entry_[entry_index].finished = 1;
        std::cerr << "Error, ibv_post_send failed" << std::endl;
        return -1;
    }

    send_wr->next = NULL;
#ifndef POLLTHREAD
    int result;
    auto start = TIME_NOW;
    struct ibv_wc wc;
    ibv_cq* cq = requestor->cq_;
    while(true) {
        if(TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
            z_switch(zqp);
            result = -1;
            // std::cerr << "Error, read timeout" << std::endl;
            break;
        }
        if(ibv_poll_cq(cq, 1, &wc) > 0) {
            if(wc.status != IBV_WC_SUCCESS) {
                z_switch(zqp);
                result = -1;
                break;
            }
            result = 0;
            break;
        }
    }
    zqp->size_counter_.fetch_add(length);
    return result;
#else
    return z_poll_completion(zqp, wr_id);
#endif
}

int zqp_write(zqp *zqp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, uint32_t time_stamp, bool use_log) {
    zqp_requestor *requestor = zqp->m_requestors[zqp->current_device];
    struct ibv_sge *sge = new ibv_sge();
    struct ibv_send_wr *send_wr = new ibv_send_wr();
    struct ibv_send_wr *bad_send_wr;
    struct ibv_sge log_sge;
    struct ibv_send_wr log_wr;
    
    int entry_index = zqp->entry_end_.fetch_add(1)%WR_ENTRY_NUM;
    zWR_entry entry;
    entry.time_stamp = time_stamp;
    entry.wr_addr = (uint64_t)send_wr;
    entry.finished = 1;

    zqp->wr_entry_[entry_index].time_stamp = entry.time_stamp;
    zqp->wr_entry_[entry_index].wr_addr = entry.wr_addr;
    zqp->wr_entry_[entry_index].finished = 0;
    // uint64_t wr_id = *((uint64_t*)(&zqp->wr_entry_[entry_index])+1);
    zWR_header header{entry.wr_addr, entry.time_stamp, 0};
    uint64_t wr_id = *(uint64_t*)&header;

    if(use_log){
        log_sge.addr = (uint64_t)(&entry);
        log_sge.length = sizeof(zWR_entry);
        log_sge.lkey = 0;
        log_wr.wr_id = wr_id;
        log_wr.sg_list = &log_sge;
        log_wr.num_sge = 1;
        log_wr.next = NULL;
        log_wr.opcode = IBV_WR_RDMA_WRITE;
        log_wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
        log_wr.wr.rdma.remote_addr = requestor->server_cmd_msg_ + entry_index * sizeof(zWR_entry);
        log_wr.wr.rdma.rkey = requestor->server_cmd_rkey_[zqp->current_device];
    }        

    sge->addr = (uint64_t)local_addr;
    sge->length = length;
    sge->lkey = lkey;

    if(use_log)
        send_wr->wr_id = 0;
    else
        send_wr->wr_id = wr_id;
    send_wr->sg_list = sge;
    send_wr->num_sge = 1;
    if(use_log)
        send_wr->next = &log_wr;
    else
        send_wr->next = NULL;
    send_wr->opcode = IBV_WR_RDMA_WRITE;
    if(use_log)
        send_wr->send_flags = 0;
    else
        send_wr->send_flags = IBV_SEND_SIGNALED;
    send_wr->wr.rdma.remote_addr = (uint64_t)remote_addr;
    send_wr->wr.rdma.rkey = rkey;
    ibv_qp* qp = requestor->qp_;
    if (ibv_post_send(qp, send_wr, &bad_send_wr)) {
        z_switch(zqp);
        zqp->wr_entry_[entry_index].finished = 1;
        // perror("Error, ibv_post_send failed");
        return -1;
    }

    send_wr->next = NULL;
#ifndef POLLTHREAD
    int result;
    auto start = TIME_NOW;
    struct ibv_wc wc;
    ibv_cq* cq = requestor->cq_;
    while(true) {
        if(TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
            z_switch(zqp);
            result = -1;
            // std::cerr << "Error, read timeout" << std::endl;
            break;
        }
        if(ibv_poll_cq(cq, 1, &wc) > 0) {
            if(wc.status != IBV_WC_SUCCESS) {
                z_switch(zqp);
                result = -1;
                break;
            }
            result = 0;
            break;
        }
    }
    zqp->size_counter_.fetch_add(length);
    return result;
#else
    return z_poll_completion(zqp, wr_id);
#endif
}

int zqp_CAS(zqp *zqp, void *local_addr, uint32_t lkey, uint64_t new_val, void* remote_addr, uint32_t rkey, uint32_t time_stamp) {
    zqp_requestor *requestor = zqp->m_requestors[zqp->current_device];
    uint64_t expected = *(uint64_t*)local_addr;

    struct ibv_sge *sge = new ibv_sge();
    sge->addr = (uint64_t)local_addr;
    sge->length = sizeof(uint64_t);
    sge->lkey = lkey;

    int offset = zqp->entry_end_.fetch_add(1)%WR_ENTRY_NUM;
    zAtomic_entry *entry = new zAtomic_entry();
    entry->time_stamp = time_stamp;
    entry->qp_id = zqp->qp_id_;
    entry->offset = offset;

    struct ibv_send_wr *send_wr = new ibv_send_wr();
    struct ibv_send_wr *bad_send_wr;
    zqp->wr_entry_[offset].time_stamp = time_stamp;
    zqp->wr_entry_[offset].wr_addr = (uint64_t)send_wr;
    zqp->wr_entry_[offset].finished = 0;
    zqp->wr_entry_[offset].reserved = new_val;
    uint64_t wr_id = *((uint64_t*)(&zqp->wr_entry_[offset])+1);

    send_wr->wr_id = 0;
    send_wr->sg_list = sge;
    send_wr->num_sge = 1;
    send_wr->next = NULL;
    send_wr->opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    send_wr->send_flags = 0;
    send_wr->wr.atomic.remote_addr = (uint64_t)remote_addr;
    send_wr->wr.atomic.rkey = rkey;
    send_wr->wr.atomic.compare_add = expected;
    send_wr->wr.atomic.swap = *(uint64_t*)entry;
    // printf("CAS: %lu --> %lu\n", expected, *(uint64_t*)entry);

    struct ibv_sge *buffer_sge = new ibv_sge();
    struct ibv_send_wr *buffer_wr = new ibv_send_wr();
    zAtomic_buffer* buffer = new zAtomic_buffer();
    buffer->buffer = new_val;
    buffer->finished = 0;
    buffer->target_addr = (uint64_t)(remote_addr);
    buffer->time_stamp = time_stamp;

    buffer_sge->addr = (uint64_t)(buffer);
    buffer_sge->length = sizeof(zAtomic_buffer);
    buffer_sge->lkey = 0;
    buffer_wr->wr_id = wr_id;
    buffer_wr->sg_list = buffer_sge;
    buffer_wr->num_sge = 1;
    buffer_wr->next = NULL;
    buffer_wr->opcode = IBV_WR_RDMA_WRITE;
    buffer_wr->send_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED;
    buffer_wr->wr.rdma.remote_addr = requestor->server_cmd_msg_ + offset * sizeof(zAtomic_buffer);
    buffer_wr->wr.rdma.rkey = requestor->server_cmd_rkey_[zqp->current_device];
    send_wr->next = buffer_wr;

    ibv_qp* qp = requestor->qp_;
    int result;
    if (result = ibv_post_send(qp, send_wr, &bad_send_wr)) {
        zqp->wr_entry_[offset].finished = 1;
        std::cerr << "Error, ibv_post_send failed:" << result << std::endl;
        return -1;
    }

    // zqp->entry_end_ = (zqp->entry_end_ + 1)%WR_ENTRY_NUM;

    delete entry;
    delete buffer;
    delete buffer_sge;
    delete buffer_wr;
    send_wr->next = NULL;
#ifndef POLLTHREAD
    auto start = TIME_NOW;
    struct ibv_wc wc;
    ibv_cq* cq = requestor->cq_;
    while(true) {
        if(TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
            z_switch(zqp);
            result = -1;
            // std::cerr << "Error, read timeout" << std::endl;
            break;
        }
        if(ibv_poll_cq(cq, 1, &wc) > 0) {
            if(wc.status != IBV_WC_SUCCESS) {
                z_switch(zqp);
                result = -1;
                break;
            }
            result = 0;
            break;
        }
    }
    if(result >= 0 && expected == *(uint64_t*)local_addr) {
        // zqp_CAS_step2(zqp, new_val, remote_addr, rkey, zqp->time_stamp, offset);
        // std::thread(zqp_CAS_step2, zqp, new_val, remote_addr, rkey, zqp->time_stamp, offset).detach();
    } else { 
        printf("CAS failed, expect %lu, get %lu\n", expected, *(uint64_t*)local_addr);
    }
    return result;
#else
    result = z_poll_completion(zqp, wr_id);
    if(result >= 0 && expected == *(uint64_t*)local_addr) {
        zqp_CAS_step2(zqp, new_val, remote_addr, rkey, zqp->time_stamp, offset);
    } else {
        printf("CAS failed, expect %lu, get %lu\n", expected, *(uint64_t*)local_addr);
    }
    return 0;
#endif
}

int zqp_CAS_step2(zqp *zqp, uint64_t new_val, void* remote_addr, uint32_t rkey, uint32_t time_stamp, uint64_t offset) {
    zqp_requestor *requestor = zqp->m_requestors[zqp->current_device];
    
    zAtomic_entry *entry = new zAtomic_entry();
    entry->time_stamp = time_stamp;
    entry->qp_id = zqp->qp_id_;
    entry->offset = offset;

    struct ibv_sge *sge = new ibv_sge();
    sge->addr = (uint64_t)requestor->cmd_resp_;
    sge->length = sizeof(uint64_t);
    sge->lkey = requestor->resp_mr_->lkey;

    struct ibv_send_wr *send_wr = new ibv_send_wr();
    struct ibv_send_wr *bad_send_wr;
    send_wr->wr_id = 0;
    send_wr->sg_list = sge;
    send_wr->num_sge = 1;
    send_wr->next = NULL;
    send_wr->opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    send_wr->send_flags = 0;
    send_wr->wr.atomic.remote_addr = (uint64_t)remote_addr;
    send_wr->wr.atomic.rkey = rkey;
    send_wr->wr.atomic.compare_add = *(uint64_t*)entry;
    send_wr->wr.atomic.swap = new_val;
    // printf("CAS: %lu --> %lu\n", *(uint64_t*)entry, new_val);

    struct ibv_sge *buffer_sge = new ibv_sge();
    struct ibv_send_wr *buffer_wr = new ibv_send_wr();
    zAtomic_buffer* buffer = new zAtomic_buffer();
    buffer->buffer = new_val;
    buffer->finished = 0;
    buffer->target_addr = (uint64_t)(remote_addr);
    buffer->time_stamp = time_stamp;
    uint64_t old_val = *((uint64_t*)(buffer)+1);
    buffer->finished = 1;
    uint64_t check_val = *((uint64_t*)(buffer)+1);
    // printf("old: %lu, check: %lu\n", old_val, check_val);

    buffer_sge->addr = (uint64_t)((uint64_t*)(requestor->cmd_resp_)+1);
    buffer_sge->length = sizeof(uint64_t);
    buffer_sge->lkey = requestor->resp_mr_->lkey;
    buffer_wr->wr_id = 0;
    buffer_wr->sg_list = buffer_sge;
    buffer_wr->num_sge = 1;
    buffer_wr->next = NULL;
    buffer_wr->opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    buffer_wr->send_flags = IBV_SEND_SIGNALED;
    buffer_wr->wr.atomic.remote_addr = requestor->server_cmd_msg_ + entry->offset * sizeof(zAtomic_buffer) + sizeof(uint64_t);
    buffer_wr->wr.atomic.rkey = requestor->server_cmd_rkey_[zqp->current_device];
    buffer_wr->wr.atomic.compare_add = old_val;
    buffer_wr->wr.atomic.swap = check_val;
    send_wr->next = buffer_wr;

    ibv_qp* qp = requestor->qp_;
    if (ibv_post_send(qp, send_wr, &bad_send_wr)) {
        std::cerr << "Error, ibv_post_send failed" << std::endl;
        return -1;
    }
#ifndef POLLTHREAD
    int result;    
    auto start = TIME_NOW;
    struct ibv_wc wc;
    ibv_cq* cq = requestor->cq_;
    while(true) {
        if(TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
            z_switch(zqp);
            result = -1;
            // std::cerr << "Error, read timeout" << std::endl;
            break;
        }
        if(ibv_poll_cq(cq, 1, &wc) > 0) {
            if(wc.status != IBV_WC_SUCCESS) {
                z_switch(zqp);
                result = -1;
                break;
            }
            result = 0;
            break;
        }
    }
    return result;
#endif
    return 0;
}

int zqp_post_send(zqp* zqp, ibv_send_wr *send_wr, ibv_send_wr **bad_wr, bool non_idempotent, uint32_t time_stamp, vector<uint64_t> *wr_ids) {
    ibv_send_wr* copy_wr = new ibv_send_wr();
    ibv_send_wr* p = send_wr;
    ibv_send_wr* q = copy_wr;
    bool retry = false;
    // deep copy
    zqp_requestor *requestor = zqp->m_requestors[zqp->current_device];
    while(p != NULL) {
        memcpy(q, p, sizeof(ibv_send_wr));
        ibv_sge *sge = (ibv_sge*)malloc(sizeof(ibv_sge)*p->num_sge);
        if(p->sg_list != NULL) {
            memcpy(sge, p->sg_list, sizeof(ibv_sge)*p->num_sge);
        } else {
            sge = NULL;
        }
        q->sg_list = sge;
        if(p->opcode == IBV_WR_SEND || p->opcode == IBV_WR_RDMA_WRITE || 
            p->opcode == IBV_WR_RDMA_WRITE_WITH_IMM || p->opcode == IBV_WR_ATOMIC_CMP_AND_SWP) {
            retry = true;
        }
        if(zqp->current_device != 0){
            for(int i = 0; i < p->num_sge; i++) {
                tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
                zqp->m_pd->m_lkey_table.find(a, p->sg_list[i].lkey);
                q->sg_list[i].lkey = a->second[zqp->current_device];
            }
            q->wr.rdma.rkey = zqp->m_rkey_table->at(p->wr.rdma.rkey)[zqp->current_device];
            p = p->next;
        }
        if(q->opcode == IBV_WR_ATOMIC_CMP_AND_SWP) {
            int entry_index = zqp->entry_end_.fetch_add(1)%WR_ENTRY_NUM;
            zAtomic_entry *entry = new zAtomic_entry();
            entry->time_stamp = time_stamp;
            entry->qp_id = zqp->qp_id_;
            entry->offset = entry_index;

            struct ibv_send_wr *send_wr = new ibv_send_wr();
            struct ibv_send_wr *bad_send_wr;
            zqp->wr_entry_[entry_index].time_stamp = time_stamp;
            zqp->wr_entry_[entry_index].wr_addr = (uint64_t)send_wr;
            zqp->wr_entry_[entry_index].finished = 1;
            zqp->wr_entry_[entry_index].reserved = q->wr.atomic.swap;

            send_wr->wr.atomic.swap = *(uint64_t*)entry;

            struct ibv_sge *buffer_sge = new ibv_sge();
            struct ibv_send_wr *buffer_wr = new ibv_send_wr();
            zAtomic_buffer* buffer = new zAtomic_buffer();
            buffer->buffer = q->wr.atomic.swap;
            buffer->finished = 0;
            buffer->target_addr = (uint64_t)(q->wr.atomic.remote_addr);
            buffer->time_stamp = time_stamp;
            uint64_t wr_id = *((uint64_t*)(&zqp->wr_entry_[entry_index])+1);
            buffer_sge->addr = (uint64_t)(buffer);
            buffer_sge->length = sizeof(zAtomic_buffer);
            buffer_sge->lkey = 0;
            buffer_wr->wr_id = wr_id;
            buffer_wr->sg_list = buffer_sge;
            buffer_wr->num_sge = 1;
            buffer_wr->next = NULL;
            buffer_wr->opcode = IBV_WR_RDMA_WRITE;
            buffer_wr->send_flags = IBV_SEND_INLINE;
            buffer_wr->wr.rdma.remote_addr = requestor->server_cmd_msg_ + entry_index * sizeof(zAtomic_buffer);
            buffer_wr->wr.rdma.rkey = requestor->server_cmd_rkey_[zqp->current_device];
            q->next = buffer_wr;
            q = q->next;
            // zqp->entry_end_ = (zqp->entry_end_ + 1)%WR_ENTRY_NUM;
        }
        p = p->next; 
        if(p != NULL) {
            q->next = new ibv_send_wr();
            q = q->next;
        } else {
            q->next = NULL;
        }
    }
    if (retry != non_idempotent) {
        std::cerr << "Warning, inconsistent non_idempotent flag" << std::endl;
    }
    
    struct ibv_sge *log_sge = new ibv_sge();
    struct ibv_send_wr *log_wr = new ibv_send_wr();
    
    zWR_entry* entry = new zWR_entry();
    entry->time_stamp = time_stamp;
    entry->wr_addr = (uint64_t)copy_wr;
    entry->finished = 1;

    int entry_index = zqp->entry_end_.fetch_add(1)%WR_ENTRY_NUM;
    zqp->wr_entry_[entry_index].time_stamp = entry->time_stamp;
    zqp->wr_entry_[entry_index].wr_addr = entry->wr_addr;
    zqp->wr_entry_[entry_index].finished = 0;
    uint64_t wr_id = *((uint64_t*)(&zqp->wr_entry_[entry_index])+1);
    
    if(non_idempotent){
        log_sge->addr = (uint64_t)(entry);
        log_sge->length = sizeof(zWR_entry);
        log_sge->lkey = 0;
        log_wr->wr_id = wr_id;
        log_wr->sg_list = log_sge;
        log_wr->num_sge = 1;
        log_wr->next = NULL;
        log_wr->opcode = IBV_WR_RDMA_WRITE;
        log_wr->send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
        log_wr->wr.rdma.remote_addr = requestor->server_cmd_msg_ + entry_index * sizeof(zWR_entry);
        log_wr->wr.rdma.rkey = requestor->server_cmd_rkey_[zqp->current_device];
    }        

    if(non_idempotent)
        q->next = log_wr;
    if(non_idempotent)
        q->send_flags = 0;
    ibv_qp* qp = requestor->qp_;
    if (ibv_post_send(qp, copy_wr, bad_wr)) {
        zqp->wr_entry_[entry_index].finished = 1;
        perror("Error, ibv_post_send failed");
    }
    wr_ids->push_back(wr_id);
    // zqp->entry_end_ = (zqp->entry_end_ + 1)%WR_ENTRY_NUM;

    delete entry;
    if(non_idempotent){
        delete log_sge;
        delete log_wr;
    }
    q->next = NULL;
    return 0;
}

int zqp_read_async(zqp *zqp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, uint32_t time_stamp, vector<uint64_t> *wr_ids) {
    zqp_requestor *requestor = zqp->m_requestors[zqp->current_device];
    struct ibv_sge *sge = new ibv_sge();
    sge->addr = (uint64_t)local_addr;
    sge->length = length;
    if(zqp->current_device != 0){
        tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
        zqp->m_pd->m_lkey_table.find(a, lkey);
        sge->lkey = a->second[zqp->current_device];
    } else {
        sge->lkey = lkey;
    }
    struct ibv_send_wr *send_wr = new ibv_send_wr();
    struct ibv_send_wr *bad_send_wr;
    
    int entry_index = zqp->entry_end_.fetch_add(1)%WR_ENTRY_NUM;
    zqp->wr_entry_[entry_index].time_stamp = time_stamp;
    zqp->wr_entry_[entry_index].wr_addr = (uint64_t)send_wr;
    zqp->wr_entry_[entry_index].finished = 0;
    uint64_t wr_id = *((uint64_t*)(&zqp->wr_entry_[entry_index])+1);

    send_wr->wr_id = wr_id;
    send_wr->sg_list = sge;
    send_wr->num_sge = 1;
    send_wr->next = NULL;
    send_wr->opcode = IBV_WR_RDMA_READ;
    send_wr->send_flags = IBV_SEND_SIGNALED;
    send_wr->wr.rdma.remote_addr = (uint64_t)remote_addr;
    if(zqp->current_device != 0){
        send_wr->wr.rdma.rkey = zqp->m_rkey_table->at(rkey)[zqp->current_device];
    } else {
        send_wr->wr.rdma.rkey = rkey;
    }

    ibv_qp* qp = requestor->qp_;
    if (ibv_post_send(qp, send_wr, &bad_send_wr)) {
        zqp->wr_entry_[entry_index].finished = 1;
        std::cerr << "Error, ibv_post_send failed" << std::endl;
        return -1;
    }
    wr_ids->push_back(wr_id);

    // zqp->entry_end_ = (zqp->entry_end_ + 1)%WR_ENTRY_NUM;

    send_wr->next = NULL;
    return 0;
}

int zqp_write_async(zqp *zqp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, uint32_t time_stamp, bool use_log, vector<uint64_t> *wr_ids) {
    zqp_requestor *requestor = zqp->m_requestors[zqp->current_device];
    struct ibv_sge *sge = new ibv_sge();
    struct ibv_send_wr *send_wr = new ibv_send_wr();
    struct ibv_send_wr *bad_send_wr;
    struct ibv_sge *log_sge = new ibv_sge();
    struct ibv_send_wr *log_wr = new ibv_send_wr();
    
    zWR_entry* entry = new zWR_entry();
    entry->time_stamp = time_stamp;
    entry->wr_addr = (uint64_t)send_wr;
    entry->finished = 1;

    int entry_index = zqp->entry_end_.fetch_add(1)%WR_ENTRY_NUM;
    zqp->wr_entry_[entry_index].time_stamp = entry->time_stamp;
    zqp->wr_entry_[entry_index].wr_addr = entry->wr_addr;
    zqp->wr_entry_[entry_index].finished = 0;
    uint64_t wr_id = *((uint64_t*)(&zqp->wr_entry_[entry_index])+1);
    
    if(use_log){
        log_sge->addr = (uint64_t)(entry);
        log_sge->length = sizeof(zWR_entry);
        log_sge->lkey = 0;
        log_wr->wr_id = wr_id;
        log_wr->sg_list = log_sge;
        log_wr->num_sge = 1;
        log_wr->next = NULL;
        log_wr->opcode = IBV_WR_RDMA_WRITE;
        log_wr->send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
        log_wr->wr.rdma.remote_addr = requestor->server_cmd_msg_ + entry_index * sizeof(zWR_entry);
        log_wr->wr.rdma.rkey = requestor->server_cmd_rkey_[zqp->current_device];
    }        

    sge->addr = (uint64_t)local_addr;
    sge->length = length;
    if(zqp->current_device != 0){
        tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
        zqp->m_pd->m_lkey_table.find(a, lkey);
        sge->lkey = a->second[zqp->current_device];
    } else {
        sge->lkey = lkey;
    }
    if(use_log)
        send_wr->wr_id = 0;
    else
        send_wr->wr_id = wr_id;
    send_wr->sg_list = sge;
    send_wr->num_sge = 1;
    if(use_log)
        send_wr->next = log_wr;
    else
        send_wr->next = NULL;
    send_wr->opcode = IBV_WR_RDMA_WRITE;
    if(use_log)
        send_wr->send_flags = 0;
    else
        send_wr->send_flags = IBV_SEND_SIGNALED;
    send_wr->wr.rdma.remote_addr = (uint64_t)remote_addr;
    if(zqp->current_device != 0){
        send_wr->wr.rdma.rkey = zqp->m_rkey_table->at(rkey)[zqp->current_device];
    } else {
        send_wr->wr.rdma.rkey = rkey;
    }
    ibv_qp* qp = requestor->qp_;
    if (int return_val = ibv_post_send(qp, send_wr, &bad_send_wr)) {
        // perror("Error, ibv_post_send failed");
        // printf("ibv_post_send return value: %d\n", return_val);
        assert(zqp->current_device == 0);
        zqp->wr_entry_[entry_index].finished = 1;
        return -1;
    }
    wr_ids->push_back(wr_id);

    // zqp->wr_entry_[zqp->entry_end_] = *entry;

    // zqp->entry_end_ = (zqp->entry_end_ + 1)%WR_ENTRY_NUM;

    delete entry;
    if(use_log){
        delete log_sge;
        delete log_wr;
    }
    send_wr->next = NULL;
    return 0;
}

int zqp_CAS_async(zqp *zqp, void *local_addr, uint32_t lkey, uint64_t new_val, void* remote_addr, uint32_t rkey, uint32_t time_stamp, vector<uint64_t> *wr_ids) {
    zqp_requestor *requestor = zqp->m_requestors[zqp->current_device];
    uint64_t expected = *(uint64_t*)local_addr;

    struct ibv_sge *sge = new ibv_sge();
    sge->addr = (uint64_t)local_addr;
    sge->length = sizeof(uint64_t);
    if(zqp->current_device != 0){
        tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
        zqp->m_pd->m_lkey_table.find(a, lkey);
        sge->lkey = a->second[zqp->current_device];
    } else {
        sge->lkey = lkey;
    }
    int entry_index = zqp->entry_end_.fetch_add(1)%WR_ENTRY_NUM;

    // int offset = zqp->entry_end_;
    zAtomic_entry *entry = new zAtomic_entry();
    entry->time_stamp = time_stamp;
    entry->qp_id = zqp->qp_id_;
    entry->offset = entry_index;

    struct ibv_send_wr *send_wr = new ibv_send_wr();
    struct ibv_send_wr *bad_send_wr;
    zqp->wr_entry_[entry_index].time_stamp = time_stamp;
    zqp->wr_entry_[entry_index].wr_addr = (uint64_t)send_wr;
    zqp->wr_entry_[entry_index].finished = 0;
    zqp->wr_entry_[entry_index].reserved = new_val;
    uint64_t wr_id = *((uint64_t*)(&zqp->wr_entry_[entry_index])+1);

    send_wr->wr_id = wr_id;
    send_wr->sg_list = sge;
    send_wr->num_sge = 1;
    send_wr->next = NULL;
    send_wr->opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    send_wr->send_flags = 0;
    send_wr->wr.atomic.remote_addr = (uint64_t)remote_addr;
        if(zqp->current_device != 0){
        send_wr->wr.atomic.rkey = zqp->m_rkey_table->at(rkey)[zqp->current_device];
    } else {
        send_wr->wr.atomic.rkey = rkey;
    }
    send_wr->wr.atomic.compare_add = expected;
    send_wr->wr.atomic.swap = *(uint64_t*)entry;
    // printf("CAS: %lu --> %lu\n", expected, *(uint64_t*)entry);

    struct ibv_sge *buffer_sge = new ibv_sge();
    struct ibv_send_wr *buffer_wr = new ibv_send_wr();
    zAtomic_buffer* buffer = new zAtomic_buffer();
    buffer->buffer = new_val;
    buffer->finished = 0;
    buffer->target_addr = (uint64_t)(remote_addr);
    buffer->time_stamp = time_stamp;

    buffer_sge->addr = (uint64_t)(buffer);
    buffer_sge->length = sizeof(zAtomic_buffer);
    buffer_sge->lkey = 0;
    buffer_wr->wr_id = wr_id;
    buffer_wr->sg_list = buffer_sge;
    buffer_wr->num_sge = 1;
    buffer_wr->next = NULL;
    buffer_wr->opcode = IBV_WR_RDMA_WRITE;
    buffer_wr->send_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED;
    buffer_wr->wr.rdma.remote_addr = requestor->server_cmd_msg_ + entry_index * sizeof(zAtomic_buffer);
    buffer_wr->wr.rdma.rkey = requestor->server_cmd_rkey_[zqp->current_device];
    send_wr->next = buffer_wr;

    ibv_qp* qp = requestor->qp_;
    int result;
    if (result = ibv_post_send(qp, send_wr, &bad_send_wr)) {
        zqp->wr_entry_[entry_index].finished = 1;
        std::cerr << "Error, ibv_post_send failed:" << result << std::endl;
        return -1;
    }
    wr_ids->push_back(wr_id);

    // zqp->entry_end_ = (zqp->entry_end_ + 1)%WR_ENTRY_NUM;

    // auto start = TIME_NOW;
    struct ibv_wc wc;
    ibv_cq* cq = requestor->cq_;

    delete entry;
    delete buffer;
    delete buffer_sge;
    delete buffer_wr;
    send_wr->next = NULL;
    return entry_index;
}

int zqp_CAS_step2_async(zqp *zqp, uint64_t new_val, void* remote_addr, uint32_t rkey, uint32_t time_stamp, uint64_t offset) {
    zqp_requestor *requestor = zqp->m_requestors[zqp->current_device];
    
    zAtomic_entry *entry = new zAtomic_entry();
    entry->time_stamp = time_stamp;
    entry->qp_id = zqp->qp_id_;
    entry->offset = offset;

    struct ibv_sge *sge = new ibv_sge();
    sge->addr = (uint64_t)requestor->cmd_resp_;
    sge->length = sizeof(uint64_t);
    sge->lkey = requestor->resp_mr_->lkey;

    struct ibv_send_wr *send_wr = new ibv_send_wr();
    struct ibv_send_wr *bad_send_wr;
    send_wr->wr_id = 0;
    send_wr->sg_list = sge;
    send_wr->num_sge = 1;
    send_wr->next = NULL;
    send_wr->opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    send_wr->send_flags = 0;
    send_wr->wr.atomic.remote_addr = (uint64_t)remote_addr;
    send_wr->wr.atomic.rkey = rkey;
    send_wr->wr.atomic.compare_add = *(uint64_t*)entry;
    send_wr->wr.atomic.swap = new_val;
    // printf("CAS: %lu --> %lu\n", *(uint64_t*)entry, new_val);

    struct ibv_sge *buffer_sge = new ibv_sge();
    struct ibv_send_wr *buffer_wr = new ibv_send_wr();
    zAtomic_buffer* buffer = new zAtomic_buffer();
    buffer->buffer = new_val;
    buffer->finished = 0;
    buffer->target_addr = (uint64_t)(remote_addr);
    buffer->time_stamp = time_stamp;
    uint64_t old_val = *((uint64_t*)(buffer)+1);
    buffer->finished = 1;
    uint64_t check_val = *((uint64_t*)(buffer)+1);
    // printf("old: %lu, check: %lu\n", old_val, check_val);

    buffer_sge->addr = (uint64_t)((uint64_t*)(requestor->cmd_resp_)+1);
    buffer_sge->length = sizeof(uint64_t);
    buffer_sge->lkey = requestor->resp_mr_->lkey;
    buffer_wr->wr_id = 0;
    buffer_wr->sg_list = buffer_sge;
    buffer_wr->num_sge = 1;
    buffer_wr->next = NULL;
    buffer_wr->opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    buffer_wr->send_flags = 0;
    buffer_wr->wr.atomic.remote_addr = requestor->server_cmd_msg_ + entry->offset * sizeof(zAtomic_buffer) + sizeof(uint64_t);
    buffer_wr->wr.atomic.rkey = requestor->server_cmd_rkey_[zqp->current_device];
    buffer_wr->wr.atomic.compare_add = old_val;
    buffer_wr->wr.atomic.swap = check_val;
    send_wr->next = buffer_wr;

    ibv_qp* qp = requestor->qp_;
    if (ibv_post_send(qp, send_wr, &bad_send_wr)) {
        std::cerr << "Error, ibv_post_send failed" << std::endl;
        return -1;
    }

    return 0;
}

int zqp_send_wr(zqp* zqp, ibv_send_wr* send_wr){
    struct ibv_send_wr *bad_send_wr;
    zqp_requestor *requestor = zqp->m_requestors[zqp->current_device];
    ibv_qp* qp_ = requestor->qp_;
    if (ibv_post_send(qp_, send_wr, &bad_send_wr)) {
        perror("Error, ibv_post_send failed");
    }

    auto start = TIME_NOW;
    struct ibv_wc wc;
    ibv_cq* cq = requestor->cq_;
    while(true) {
        if(TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
            // std::cerr << "Error, write timeout" << std::endl;
            break;
        }
        if(ibv_poll_cq(cq, 1, &wc) > 0) {
            if(wc.status != IBV_WC_SUCCESS) {
                std::cerr << "Error, write failed: " << wc.status << std::endl;
                break;
            }
            int start_ = zqp->entry_start_;
            int end_ = zqp->entry_end_;
            if(start_ > end_)
                end_ += WR_ENTRY_NUM;
            if(start_ != end_){
                for(int i = start_; i < end_; i++){
                    if(zqp->wr_entry_[i%WR_ENTRY_NUM].time_stamp == wc.wr_id){
                        delete (ibv_send_wr*)zqp->wr_entry_[zqp->entry_end_].wr_addr;
                        zqp->wr_entry_[zqp->entry_end_].wr_addr = 0;
                    }
                    if(zqp->wr_entry_[i%WR_ENTRY_NUM].wr_addr == 0 && i%WR_ENTRY_NUM == zqp->entry_start_){
                        zqp->entry_start_ = (zqp->entry_start_ + 1)%WR_ENTRY_NUM;
                    }
                }
            }
            break;
        }
    }

    return 0;
}

void zqp_RPC_Alloc(zqp* qp, uint64_t* addr, uint32_t* rkey, size_t size){
    zqp_requestor* requestor = qp->m_requestors[qp->current_device];
    memset(requestor->cmd_msg_, 0, sizeof(CmdMsgBlock));
    memset(requestor->cmd_resp_, 0, sizeof(CmdMsgRespBlock));
    requestor->cmd_resp_->notify = NOTIFY_IDLE;
    RegisterRequest *request = (RegisterRequest *)requestor->cmd_msg_;
    request->resp_addr = (uint64_t)requestor->cmd_resp_;
    request->resp_rkey = requestor->resp_mr_->rkey;
    request->id = requestor->conn_id_;
    request->type = MSG_REGISTER;
    request->size = size;
    requestor->cmd_msg_->notify = NOTIFY_WORK;
  
    // printf("cmd_resp: %lx, rkey: %u\n", requestor->cmd_resp_, requestor->resp_mr_->rkey);
    // printf("cmd_resp: %lx, rkey: %u\n", request->resp_addr, request->resp_rkey);

    /* send a request to sever */
    qp->time_stamp = (qp->time_stamp+1) % MAX_REQUESTOR_NUM;
#ifdef POLLTHREAD
    zqp_write(qp, (void *)requestor->cmd_msg_, requestor->msg_mr_->lkey, sizeof(CmdMsgBlock), (void*)requestor->server_cmd_msg_, requestor->server_cmd_rkey_[qp->current_device], qp->time_stamp, false);
#else
    z_simple_write(qp, (void *)requestor->cmd_msg_, requestor->msg_mr_->lkey, sizeof(CmdMsgBlock), (void*)requestor->server_cmd_msg_, requestor->server_cmd_rkey_[qp->current_device]);
#endif
    /* wait for response */
    auto start = TIME_NOW;
    while (requestor->cmd_resp_->notify == NOTIFY_IDLE) {
      if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US*1000) {
        printf("wait for request completion timeout\n");
        return;
      }
    }
    RegisterResponse *resp_msg = (RegisterResponse *)requestor->cmd_resp_;
    if (resp_msg->status != RES_OK) {
      printf("register remote memory fail\n");
      return ;
    }
    *addr = resp_msg->addr;
    for(int i = 0; i < MAX_NIC_NUM; i++){
        if(qp->m_rkey_table->find(resp_msg->rkey[0]) == qp->m_rkey_table->end())
            (*qp->m_rkey_table)[resp_msg->rkey[0]] = std::vector<uint32_t>();
        qp->m_rkey_table->at(resp_msg->rkey[0]).push_back(resp_msg->rkey[i]);
    }
    *rkey = resp_msg->rkey[0];
    return;
}

int z_post_send_async(zqp* qp, ibv_send_wr *send_wr, ibv_send_wr **bad_wr, bool non_idempotent, uint32_t time_stamp, vector<uint64_t> *wr_ids){
    while(qp->m_ep->m_devices[qp->current_device]->status.load() == ZSTATUS_ERROR) {
        z_switch(qp);
        // qp->current_device = (qp->current_device + 1) % qp->m_ep->m_devices.size();
        // std::cout << "Warning, switch to device " << qp->current_device << std::endl;
        // int result = z_recovery(qp);
        // if (result != 0) {
        //     std::cout << "Error, recovery failed" << std::endl;
        //     return -1;
        // }
        // return zdcqp_send(qp->m_pd->m_requestors[qp->current_device][0], qp->m_targets[qp->current_device]->ah, send_wr, qp->m_targets[qp->current_device]->lid_, qp->m_targets[qp->current_device]->dct_num_);
    }

    if(  qp->m_requestors.size() > qp->current_device && qp->m_requestors[qp->current_device] != NULL && qp->m_requestors[qp->current_device]->status_ == ZSTATUS_CONNECTED){
        qp->time_stamp = (qp->time_stamp+1) % MAX_REQUESTOR_NUM;
        int result = zqp_post_send(qp, send_wr, bad_wr, non_idempotent, qp->time_stamp, wr_ids);
        if(result == -1){
            z_switch(qp);
            return z_post_send_async(qp, send_wr, bad_wr, non_idempotent, time_stamp, wr_ids);
        }
        return result;
    } else{
        int result = zdcqp_send(qp->m_pd->m_requestors[qp->current_device][0], qp->m_targets[qp->current_device]->ah, send_wr, qp->m_targets[qp->current_device]->lid_, qp->m_targets[qp->current_device]->dct_num_);
        return result;
    }
}

int z_read_async(zqp *qp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, vector<uint64_t> *wr_ids){
    while(qp->m_ep->m_devices[qp->current_device]->status.load() == ZSTATUS_ERROR) {
        z_switch(qp);
        // qp->current_device = (qp->current_device + 1) % qp->m_ep->m_devices.size();
        // std::cout << "Warning, switch to device " << qp->current_device << std::endl;
        // int result = z_recovery(qp);
        // if (result != 0) {
        //     std::cout << "Error, recovery failed" << std::endl;
        //     return -1;
        // }
        // return zdcqp_read(qp->m_pd->m_requestors[qp->current_device][0], qp->m_targets[qp->current_device]->ah, local_addr, lkey, length, remote_addr, rkey, qp->m_targets[qp->current_device]->lid_, qp->m_targets[qp->current_device]->dct_num_);
    }
    uint32_t new_lkey = lkey; uint32_t new_rkey = rkey;
    if(  qp->m_requestors.size() > qp->current_device && qp->m_requestors[qp->current_device] != NULL && qp->m_requestors[qp->current_device]->status_ == ZSTATUS_CONNECTED){
        qp->time_stamp = (qp->time_stamp+1) % MAX_REQUESTOR_NUM;
        int result = zqp_read_async(qp, local_addr, new_lkey, length, remote_addr, new_rkey, qp->time_stamp, wr_ids);
        if(result == -1){
            z_switch(qp);
            return z_read_async(qp, local_addr, lkey, length, remote_addr, rkey, wr_ids);
        }
        return result;
    } else{
        if(qp->current_device != 0){
            tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
            qp->m_pd->m_lkey_table.find(a, lkey);
            new_lkey = a->second[qp->current_device];
            new_rkey = qp->m_rkey_table->at(rkey)[qp->current_device];
        }
        int result = zdcqp_read(qp->m_pd->m_requestors[qp->current_device][0], qp->m_targets[qp->current_device]->ah, local_addr, new_lkey, length, remote_addr, new_rkey, qp->m_targets[qp->current_device]->lid_, qp->m_targets[qp->current_device]->dct_num_);
        qp->size_counter_.fetch_add(length);
        return result;
    }
}

int z_write_async(zqp *qp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, vector<uint64_t> *wr_ids){
    while(qp->m_ep->m_devices[qp->current_device]->status.load() == ZSTATUS_ERROR) {
        z_switch(qp);
        // qp->current_device = (qp->current_device + 1) % qp->m_ep->m_devices.size();
        // std::cout << "Warning, switch to device " << qp->current_device << std::endl;
        // int result = z_recovery(qp);
        // if (result != 0) {
        //     std::cout << "Error, recovery failed" << std::endl;
        //     return -1;
        // }
        // return zdcqp_write(qp->m_pd->m_requestors[qp->current_device][0], qp->m_targets[qp->current_device]->ah, local_addr, lkey, length, remote_addr, rkey, qp->m_targets[qp->current_device]->lid_, qp->m_targets[qp->current_device]->dct_num_);
    }
    uint32_t new_lkey = lkey; uint32_t new_rkey = rkey;
    // if(qp->current_device != 0){
    //     tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
    //     qp->m_pd->m_lkey_table.find(a, lkey);
    //     new_lkey = a->second[qp->current_device];
    //     new_rkey = qp->m_rkey_table->at(rkey)[qp->current_device];
    // }
    if(qp->m_requestors.size() > qp->current_device && qp->m_requestors[qp->current_device] != NULL && qp->m_requestors[qp->current_device]->status_ == ZSTATUS_CONNECTED){
        qp->time_stamp = (qp->time_stamp+1) % MAX_REQUESTOR_NUM;
        int result = zqp_write_async(qp, local_addr, new_lkey, length, remote_addr, new_rkey, qp->time_stamp, true, wr_ids);
        if(result == -1){
            z_switch(qp);
            return z_write_async(qp, local_addr, lkey, length, remote_addr, rkey, wr_ids);
        }
        return result;
    } else{
        if(qp->current_device != 0){
            tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
            qp->m_pd->m_lkey_table.find(a, lkey);
            new_lkey = a->second[qp->current_device];
            new_rkey = qp->m_rkey_table->at(rkey)[qp->current_device];
        }
        int result = zdcqp_write(qp->m_pd->m_requestors[qp->current_device][0], qp->m_targets[qp->current_device]->ah, local_addr, new_lkey, length, remote_addr, new_rkey, qp->m_targets[qp->current_device]->lid_, qp->m_targets[qp->current_device]->dct_num_);
        qp->size_counter_.fetch_add(length);
        return result;
    }
}

int z_CAS_async(zqp *qp, void* local_addr, uint32_t lkey, uint64_t new_val, void* remote_addr, uint32_t rkey, vector<uint64_t> *wr_ids){
    while(qp->m_ep->m_devices[qp->current_device]->status.load() == ZSTATUS_ERROR) {
        z_switch(qp);
        // qp->current_device = (qp->current_device + 1) % qp->m_ep->m_devices.size();
        // std::cout << "Warning, switch to device " << qp->current_device << std::endl;
        // int result = z_recovery(qp);
        // if (result != 0) {
        //     std::cout << "Error, recovery failed" << std::endl;
        //     return -1;
        // }
    }
    uint32_t new_lkey = lkey; uint32_t new_rkey = rkey;
    // if(qp->current_device != 0){
    //     tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
    //     qp->m_pd->m_lkey_table.find(a, lkey);
    //     new_lkey = a->second[qp->current_device];
    //     new_rkey = qp->m_rkey_table->at(rkey)[qp->current_device];
    // }
    if(  qp->m_requestors.size() > qp->current_device && qp->m_requestors[qp->current_device] != NULL && qp->m_requestors[qp->current_device]->status_ == ZSTATUS_CONNECTED){
        qp->time_stamp = (qp->time_stamp+1) % MAX_REQUESTOR_NUM;
        uint64_t old_val = *(uint64_t*)local_addr;
        int result = zqp_CAS_async(qp, local_addr, new_lkey, new_val, remote_addr, new_rkey, qp->time_stamp, wr_ids);
        if(result == -1){
            z_switch(qp);
            return z_CAS_async(qp, local_addr, lkey, new_val, remote_addr, rkey, wr_ids);
        }
        return 0;
    } else{
        if(qp->current_device != 0){
            tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
            qp->m_pd->m_lkey_table.find(a, lkey);
            new_lkey = a->second[qp->current_device];
            new_rkey = qp->m_rkey_table->at(rkey)[qp->current_device];
        }
        int result = zdcqp_CAS(qp->m_pd->m_requestors[qp->current_device][0], qp->m_targets[qp->current_device]->ah, local_addr, new_lkey, new_val, remote_addr, new_rkey, qp->m_targets[qp->current_device]->lid_, qp->m_targets[qp->current_device]->dct_num_);
        qp->size_counter_.fetch_add(sizeof(uint64_t));
        return result;
    }
}

int z_write(zqp *qp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey) {
    while(qp->m_ep->m_devices[qp->current_device]->status.load() == ZSTATUS_ERROR) {
        // qp->current_device = (qp->current_device + 1) % qp->m_ep->m_devices.size();
        z_switch(qp);
        // std::cout << "Warning, switch to device " << qp->current_device << std::endl;
#ifdef RECOVERY
        int result = z_recovery(qp);
        if (result != 0) {
            std::cout << "Error, recovery failed" << std::endl;
            return -1;
        }
#endif
        // return zdcqp_write(qp->m_pd->m_requestors[qp->current_device][0], qp->m_targets[qp->current_device]->ah, local_addr, lkey, length, remote_addr, rkey, qp->m_targets[qp->current_device]->lid_, qp->m_targets[qp->current_device]->dct_num_);
    }
    if(qp->current_device != 0){
        tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
        qp->m_pd->m_lkey_table.find(a, lkey);
        lkey = a->second[qp->current_device];
        rkey = qp->m_rkey_table->at(rkey)[qp->current_device];
    }
    if(  qp->m_requestors.size() > qp->current_device && qp->m_requestors[qp->current_device] != NULL && qp->m_requestors[qp->current_device]->status_ == ZSTATUS_CONNECTED){
        qp->time_stamp = (qp->time_stamp+1) % MAX_REQUESTOR_NUM;
        int result = zqp_write(qp, local_addr, lkey, length, remote_addr, rkey, qp->time_stamp, true);
        return result;
    } else{
        int result = zdcqp_write(qp->m_pd->m_requestors[qp->current_device][0], qp->m_targets[qp->current_device]->ah, local_addr, lkey, length, remote_addr, rkey, qp->m_targets[qp->current_device]->lid_, qp->m_targets[qp->current_device]->dct_num_);
        qp->size_counter_.fetch_add(length);
        return result;
    }
}

int z_read(zqp *qp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey){
    while(qp->m_ep->m_devices[qp->current_device]->status.load() == ZSTATUS_ERROR) {
        // qp->current_device = (qp->current_device + 1) % qp->m_ep->m_devices.size();
        z_switch(qp);
        std::cout << "Warning, switch to device " << qp->current_device << std::endl;
        int result = z_recovery(qp);
        if (result != 0) {
            std::cout << "Error, recovery failed" << std::endl;
            return -1;
        }
        // return zdcqp_read(qp->m_pd->m_requestors[qp->current_device][0], qp->m_targets[qp->current_device]->ah, local_addr, lkey, length, remote_addr, rkey, qp->m_targets[qp->current_device]->lid_, qp->m_targets[qp->current_device]->dct_num_);
    }
    if(qp->current_device != 0){
        tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
        qp->m_pd->m_lkey_table.find(a, lkey);
        lkey = a->second[qp->current_device];
        rkey = qp->m_rkey_table->at(rkey)[qp->current_device];
    }
    if(  qp->m_requestors.size() > qp->current_device && qp->m_requestors[qp->current_device] != NULL && qp->m_requestors[qp->current_device]->status_ == ZSTATUS_CONNECTED){
        qp->time_stamp = (qp->time_stamp+1) % MAX_REQUESTOR_NUM;
        int result = zqp_read(qp, local_addr, lkey, length, remote_addr, rkey, qp->time_stamp);
        return 0;
    } else{
        int result = zdcqp_read(qp->m_pd->m_requestors[qp->current_device][0], qp->m_targets[qp->current_device]->ah, local_addr, lkey, length, remote_addr, rkey, qp->m_targets[qp->current_device]->lid_, qp->m_targets[qp->current_device]->dct_num_);
        qp->size_counter_.fetch_add(length);
        return result;
    }
}

int z_CAS(zqp *qp, void* local_addr, uint32_t lkey, uint64_t new_val, void* remote_addr, uint32_t rkey) {
    while(qp->m_ep->m_devices[qp->current_device]->status.load() == ZSTATUS_ERROR) {
        // qp->current_device = (qp->current_device + 1) % qp->m_ep->m_devices.size();
        z_switch(qp);
        std::cout << "Warning, switch to device " << qp->current_device << std::endl;
        int result = z_recovery(qp);
        if (result != 0) {
            std::cout << "Error, recovery failed" << std::endl;
            return -1;
        }
        // return zdcqp_CAS(qp->m_pd->m_requestors[qp->current_device][0], qp->m_targets[qp->current_device]->ah, local_addr, lkey, new_val, remote_addr, rkey, qp->m_targets[qp->current_device]->lid_, qp->m_targets[qp->current_device]->dct_num_);
    }
    if(qp->current_device != 0){
        tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
        qp->m_pd->m_lkey_table.find(a, lkey);
        lkey = a->second[qp->current_device];
        rkey = qp->m_rkey_table->at(rkey)[qp->current_device];
    }
    if(  qp->m_requestors.size() > qp->current_device && qp->m_requestors[qp->current_device] != NULL && qp->m_requestors[qp->current_device]->status_ == ZSTATUS_CONNECTED){
        qp->time_stamp = (qp->time_stamp+1) % MAX_REQUESTOR_NUM;
        uint64_t old_val = *(uint64_t*)local_addr;
        int entry_index = zqp_CAS(qp, local_addr, lkey, new_val, remote_addr, rkey, qp->time_stamp);
        return 0;
    } else{
        int result = zdcqp_CAS(qp->m_pd->m_requestors[qp->current_device][0], qp->m_targets[qp->current_device]->ah, local_addr, lkey, new_val, remote_addr, rkey, qp->m_targets[qp->current_device]->lid_, qp->m_targets[qp->current_device]->dct_num_);
        qp->size_counter_.fetch_add(sizeof(uint64_t));
        return result;
    }
}

int z_poll_completion(zqp* qp, uint64_t wr_id) {
    int return_val = 0;
    int error_device = -1;
    if (wr_id == 0) {
        return 0;
    }
    auto start = TIME_NOW;
    tbb::concurrent_hash_map<uint64_t, cq_info>::const_accessor a;
    while(qp->m_ep->completed_table.find(a, wr_id) == false){
        if(TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
            // std::cerr << "Error, poll completion timeout" << std::endl;
            z_switch(qp);
            z_recovery(qp);
            return -1;
        }
    }
        cq_info status_info = a->second;
        if(status_info.status != IBV_WC_SUCCESS) {
            // std::cerr << "Error, operation failed: " << status_info.status << " on device " << status_info.device_id << std::endl;
            error_device = status_info.device_id;
            return_val = -1;
            qp->m_ep->completed_table.erase(a);
        } else {
            int start_ = qp->entry_start_;
            int end_ = qp->entry_end_.load()%WR_ENTRY_NUM;
            zWR_header* header = (zWR_header*)&wr_id;
            qp->size_counter_.fetch_add(((ibv_send_wr*)(header->wr_addr))->sg_list->length);
            if(start_ > end_)
                end_ += WR_ENTRY_NUM;
            if(start_ != end_){
                for(int i = start_; i < end_; i++){
                    if(qp->wr_entry_[i%WR_ENTRY_NUM].time_stamp == header->time_stamp &&
                        qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr == header->wr_addr){
                        delete ((ibv_send_wr*)qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr)->sg_list;
                        delete (ibv_send_wr*)qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr;
                        qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr = 0;
                        qp->wr_entry_[i%WR_ENTRY_NUM].finished = 1;
                    }
                    if(qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr == 0 && i%WR_ENTRY_NUM == qp->entry_start_){
                        qp->entry_start_ = (qp->entry_start_ + 1)%WR_ENTRY_NUM;
                    }
                }
            }
            // a->second.valid = 0;
            qp->m_ep->completed_table.erase(a);
        }
    if (return_val < 0){
        if(error_device != qp->current_device){
            z_recovery(qp);
        } else {
            z_switch(qp);
            z_recovery(qp);
        }
    }
    return return_val;
}

int z_poll_completion(zqp* qp, vector<uint64_t> *wr_ids){
    int return_val = 0;
    int error_device = -1;
    while(wr_ids->size() > 0){
        for(auto id : *wr_ids){
            if (id == 0) {
                wr_ids->erase(std::remove(wr_ids->begin(), wr_ids->end(), id), wr_ids->end());
                break;
            }
            tbb::concurrent_hash_map<uint64_t, cq_info>::const_accessor a;
            // printf("Polling size: %lu, table size: %lu\n", wr_ids->size(), qp->completed_table.size());
            if(qp->m_ep->completed_table.find(a, id)){
                cq_info status_info = a->second;
                if(status_info.status != IBV_WC_SUCCESS) {
                    // std::cerr << "Error, operation failed: " << status_info.status << " on device " << status_info.device_id << std::endl;
                    error_device = status_info.device_id;
                    return_val = -1;
                    wr_ids->erase(std::remove(wr_ids->begin(), wr_ids->end(), id), wr_ids->end());
                    qp->m_ep->completed_table.erase(a);
                    break;
                }
                int start_ = qp->entry_start_;
                int end_ = qp->entry_end_.load()%WR_ENTRY_NUM;
                if(start_ != end_){    
                    zWR_header* header = (zWR_header*)&id;
                    qp->size_counter_.fetch_add(((ibv_send_wr*)(header->wr_addr))->sg_list->length);
                    if(start_ > end_)
                        end_ += WR_ENTRY_NUM;
                    for(int i = start_; i < end_; i++){
                            if(qp->wr_entry_[i%WR_ENTRY_NUM].time_stamp == header->time_stamp &&
                                qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr == header->wr_addr){
                                delete ((ibv_send_wr*)qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr)->sg_list;
                                delete (ibv_send_wr*)qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr;
                                qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr = 0;
                                qp->wr_entry_[i%WR_ENTRY_NUM].finished = 1;
                            }
                            if((qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr == 0 || qp->wr_entry_[i%WR_ENTRY_NUM].finished == 1) && i%WR_ENTRY_NUM == qp->entry_start_){
                                qp->entry_start_ = (qp->entry_start_ + 1)%WR_ENTRY_NUM;
                            }
                    }
                }
                wr_ids->erase(std::remove(wr_ids->begin(), wr_ids->end(), id), wr_ids->end());
                qp->m_ep->completed_table.erase(a);
                break;
            }
        }
    }
    if (return_val < 0){
        if(error_device != qp->current_device){
#ifdef RECOVERY
            z_recovery(qp);
#endif
        } else {
            z_switch(qp);
#ifdef RECOVERY
            z_recovery(qp);
#endif
        }
    }
    return return_val;
}

int z_switch(zqp *qp) {
    printf("Start switch on device %d\n", qp->current_device);
#ifndef NO_ERROR_HANDLE
    zStatus expected = ZSTATUS_INIT;
    qp->m_ep->m_devices[qp->current_device]->status.compare_exchange_strong(expected, ZSTATUS_ERROR);
    qp->m_requestors[qp->current_device]->status_ = ZSTATUS_ERROR;
    if(qp->qp_type == ZQP_RPC){
        return 0;
    }
    // ibv_destroy_qp(qp->m_requestors[qp->current_device]->qp_);
    int recovery_device = qp->current_device;
    qp->current_device = (qp->current_device + 1) % qp->m_ep->m_devices.size();
    if(  qp->m_requestors.size() <= qp->current_device || (qp->m_requestors[qp->current_device] == NULL || qp->m_requestors[qp->current_device]->status_ != ZSTATUS_CONNECTED)){
#ifndef ASYNC_CONNECT
        zqp_connect(qp, qp->current_device, qp->m_targets[qp->current_device]->ip, qp->m_targets[qp->current_device]->port);
#else
        new std::thread(&zqp_connect, qp, qp->current_device, qp->m_targets[qp->current_device]->ip, qp->m_targets[qp->current_device]->port);
#endif
    }
#endif
    return 0;
}

int z_recovery(zqp *qp) {
    int recovery_device = (qp->current_device - 1) % qp->m_ep->m_devices.size();
    // printf("Start recovery on device %d\n", qp->current_device);
    // read recovery log
    int start = qp->entry_start_;
    int end = qp->entry_end_.load()%WR_ENTRY_NUM;
    if(start == end){
        return 0;
    }
    memset(qp->cmd_resp_, 0, sizeof(CmdMsgRespBlock));
    z_read(qp, (void*)qp->cmd_resp_, qp->resp_mr_[0]->lkey, sizeof(CmdMsgRespBlock), (void*)qp->m_requestors[recovery_device]->server_cmd_msg_, qp->m_requestors[recovery_device]->server_cmd_rkey_[0]);
   // z_read(qp, (void*)qp->cmd_resp_, qp->resp_mr_[0]->lkey, sizeof(CmdMsgRespBlock), (void*)qp->m_requestors[recovery_device]->server_cmd_msg_, qp->m_requestors[recovery_device]->server_cmd_rkey_[0]);
    zWR_entry *entry = (zWR_entry *)qp->cmd_resp_;

    // for(int i = 0; i < WR_ENTRY_NUM; i++){
    //     printf("Debug: local time_stamp %d, remote time_stamp %d\n", qp->wr_entry_[i%WR_ENTRY_NUM].time_stamp, entry[i].time_stamp);
    // }
    if(start > end)
        end += WR_ENTRY_NUM;
    if(start != end){
        for(int i = start; i < end; i++){
            if(qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr == 0 || qp->wr_entry_[i%WR_ENTRY_NUM].finished == 1){
                if(i%WR_ENTRY_NUM == qp->entry_start_){
                    qp->entry_start_ = (qp->entry_start_ + 1)%WR_ENTRY_NUM;
                }
                continue;
            } 
            struct ibv_send_wr *send_wr = (struct ibv_send_wr *)qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr;
            if (send_wr->opcode == IBV_WR_ATOMIC_CMP_AND_SWP){
                zAtomic_buffer* buffer = (zAtomic_buffer*)(&entry[i%WR_ENTRY_NUM]);
                int local_time = qp->wr_entry_[i%WR_ENTRY_NUM].time_stamp;
                int remote_time = buffer->time_stamp;
                if((local_time > remote_time && local_time - remote_time < 16384) || (local_time < remote_time && remote_time - local_time > 16384)){
                    // resend CAS
                    // printf("qp %d resend local timestamp %d, remote timestamp %d, wr_id %lu, opcode %d, addr %lx, length %u\n", qp->qp_id_, local_time, remote_time, send_wr->wr_id, send_wr->opcode, send_wr->sg_list->addr, send_wr->sg_list->length);
                    z_CAS(qp, (uint64_t*)send_wr->sg_list->addr, send_wr->sg_list->lkey, qp->wr_entry_[i%WR_ENTRY_NUM].reserved, (void*)send_wr->wr.atomic.remote_addr, send_wr->wr.atomic.rkey);
                    int start_ = qp->entry_start_;
                    int end_ = qp->entry_end_.load()%WR_ENTRY_NUM;
                    if(start_ > end_)
                        end_ += WR_ENTRY_NUM;
                    if(start_ != end_){
                        for(int i = start_; i < end_; i++){
                            if(qp->wr_entry_[i%WR_ENTRY_NUM].time_stamp == local_time &&
                                qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr == (uint64_t)send_wr){
                                delete ((ibv_send_wr*)qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr)->sg_list;
                                delete (ibv_send_wr*)qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr;
                                qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr = 0;
                            }
                            if(qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr == 0 && i%WR_ENTRY_NUM == qp->entry_start_){
                                qp->entry_start_ = (qp->entry_start_ + 1)%WR_ENTRY_NUM;
                            }
                        }
                    }
                    continue;
                }
                if(buffer->finished == 1) {
                    continue;
                }
                int new_val = qp->wr_entry_[i%WR_ENTRY_NUM].reserved;
                z_read(qp, (void*)&entry[i%WR_ENTRY_NUM].reserved, qp->resp_mr_[qp->current_device]->lkey, sizeof(uint64_t), (void*)send_wr->wr.atomic.remote_addr, send_wr->wr.atomic.rkey);
                if(entry[i%WR_ENTRY_NUM].reserved == send_wr->wr.atomic.swap){
                    continue;    
                } else {
                    *(uint64_t*)send_wr->sg_list->addr = entry[i%WR_ENTRY_NUM].reserved;
                }
            } else {
                int local_time = qp->wr_entry_[i%WR_ENTRY_NUM].time_stamp;
                int remote_time = entry[i%WR_ENTRY_NUM].time_stamp;
#ifndef SEND_TWICE
                if((local_time > remote_time && local_time - remote_time < 16384) || (local_time < remote_time && remote_time - local_time > 16384)){
#endif
                    // attention: 48bit address to 64bit address
                    while(send_wr != NULL){
                        if(send_wr->opcode == IBV_WR_RDMA_WRITE){
                            if (send_wr->send_flags == (IBV_SEND_SIGNALED | IBV_SEND_INLINE)){
                                // log write
                                send_wr = send_wr->next;
                                continue;
                            }
                            // printf("resend local timestamp %d, remote timestamp %d, wr_id %lu, opcode %d, addr %lx, length %u, value %lu\n", local_time, remote_time, send_wr->wr_id, send_wr->opcode, send_wr->sg_list->addr, send_wr->sg_list->length, *(uint64_t*)send_wr->sg_list->addr);
                            z_write(qp, (void *)send_wr->sg_list->addr, send_wr->sg_list->lkey, send_wr->sg_list->length, (void *)send_wr->wr.rdma.remote_addr, send_wr->wr.rdma.rkey);
                        } else if(send_wr->opcode == IBV_WR_RDMA_READ){
                            // printf("resend local timestamp %d, remote timestamp %d, wr_id %lu, opcode %d, addr %lx, length %u\n", local_time, remote_time, send_wr->wr_id, send_wr->opcode, send_wr->sg_list->addr, send_wr->sg_list->length);
                            z_read(qp, (void *)send_wr->sg_list->addr, send_wr->sg_list->lkey, send_wr->sg_list->length, (void *)send_wr->wr.rdma.remote_addr, send_wr->wr.rdma.rkey);
                        } else {
                            printf("Error, unsupported opcode %d\n", send_wr->opcode);
                            continue;
                        }
                        int start_ = qp->entry_start_;
                        int end_ = qp->entry_end_.load()%WR_ENTRY_NUM;
                        if(start_ > end_)
                            end_ += WR_ENTRY_NUM;
                        ibv_send_wr* next_wr = send_wr->next;
                        if(start_ != end_){
                            for(int i = start_; i < end_; i++){
                                if(qp->wr_entry_[i%WR_ENTRY_NUM].time_stamp == local_time &&
                                    qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr == (uint64_t)send_wr){
                                    delete ((ibv_send_wr*)qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr)->sg_list;
                                    delete (ibv_send_wr*)qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr;
                                    qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr = 0;
                                    qp->wr_entry_[i%WR_ENTRY_NUM].finished = 1;
                                }
                                if((qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr == 0 || qp->wr_entry_[i%WR_ENTRY_NUM].finished == 1) && i%WR_ENTRY_NUM == qp->entry_start_){
                                    qp->entry_start_ = (qp->entry_start_ + 1)%WR_ENTRY_NUM;
                                }
                            }
                        }
                        send_wr = next_wr;
                    }
#ifndef SEND_TWICE
                } else if (local_time == remote_time){
                    // printf("finished timestamp %d, remote timestamp %d, wr_id %lu, opcode %d, addr %lx, length %u\n", local_time, remote_time, send_wr->wr_id, send_wr->opcode, send_wr->sg_list->addr, send_wr->sg_list->length);
                } 
#endif
            }
        }
    }
    // ibv_destroy_cq(qp->m_requestors[recovery_device]->cq_);
    rdma_destroy_id(qp->m_requestors[recovery_device]->cm_id_);
    // rdma_destroy_event_channel(qp->m_requestors[recovery_device]->channel_);
    // qp->m_requestors[recovery_device]->status_ = ZSTATUS_INIT;
    return 0;
}

}
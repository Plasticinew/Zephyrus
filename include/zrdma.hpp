#pragma once

// #include "common.hpp"

#include "zdcqp.hpp"

namespace zrdma {

#define DCQP_POOL_SIZE 1
    
class zdevice {
public:
    zdevice(string mlx_name, string eth_name, string eth_ip, string port, ibv_context *context) : mlx_name(mlx_name), eth_name(eth_name), eth_ip(eth_ip), port(port), context(context) {
        status.store(zStatus::ZSTATUS_INIT);
    }
    ~zdevice() {}
    bool down_status() {
        zStatus expected = zStatus::ZSTATUS_CONNECTED;
        status.compare_exchange_strong(expected, zStatus::ZSTATUS_ERROR);
        return status.load() == zStatus::ZSTATUS_ERROR;
    }
    ibv_context* get_context() {
        return context;
    }
    bool get_address(char* ip_buf, char* port_buf) {
        memcpy(ip_buf, eth_ip.c_str(), eth_ip.size());
        memcpy(port_buf, port.c_str(), port.size());
        return true;
    }
private:
    string mlx_name;
    string eth_name;
    string eth_ip;
    string port;
    ibv_context *context = NULL;
    std::atomic<zStatus> status{zStatus::ZSTATUS_INIT};
};


class zpd {
public:
    zpd(zendpoint* ep, int pool_size) {
        for(int i = 0; i < ep->get_device_num(); i ++) {
            ibv_pd *pd_ = ibv_alloc_pd(ep->get_device_context(i));
            m_pds.push_back(pd_);
        }
        // create DCQP requestor and responder
        for(int i = 0; i < ep->get_device_num(); i ++) {
            vector<zdcqp_requestor*> requestors;
            vector<zdcqp_responder*> responders;
            for(int j = 0; j < pool_size; j ++) {
                zdcqp_requestor *requestor = new zdcqp_requestor(m_pds[i], ep->get_device_context(i));
                zdcqp_responder *responder = new zdcqp_responder(m_pds[i], ep->get_device_context(i));
                requestors.push_back(requestor);
                responders.push_back(responder);
            }
            m_requestors.push_back(requestors);
            m_responders.push_back(responders);
        }
    }
    ~zpd() {}
    ibv_mr* mr_create(int dev, void *addr, size_t length) {
        ibv_mr* mr = ibv_reg_mr(m_pds[dev], addr, length, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE  | IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_MW_BIND);
        return mr;
    }

    ibv_mr* mr_malloc_create(uint64_t &addr, size_t length) {
        addr = (uint64_t)mmap(NULL, length, PROT_READ | PROT_WRITE, MAP_PRIVATE |MAP_ANONYMOUS, -1, 0);
        ibv_mr* primary_mr = mr_create(0, (void*)addr, length);
        if (primary_mr == NULL) {
            munmap((void*)addr, length);
            return NULL;
        }
        tbb::concurrent_hash_map<ibv_mr*, tbb::concurrent_vector<ibv_mr*>>::accessor a;
        if(m_mrs.find(a, primary_mr)) {
            a->second.push_back(primary_mr);
        } else {
            tbb::concurrent_vector<ibv_mr*> mr_list;
            mr_list.push_back(primary_mr);
            m_mrs.insert(tbb::concurrent_hash_map<ibv_mr*, tbb::concurrent_vector<ibv_mr*>>::value_type(primary_mr, mr_list));
            // pd->m_mrs.insert(std::make_pair(primary_mr, mr_list));
        }
        tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor b;
        if(m_lkey_table.find(b, primary_mr->lkey)) {
            b->second.push_back(primary_mr->lkey);
        } else {
            tbb::concurrent_vector<uint32_t> lkey_list;
            lkey_list.push_back(primary_mr->lkey);
            m_lkey_table.insert(tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::value_type(primary_mr->lkey, lkey_list));
            // pd->m_lkey_table.insert(std::make_pair(primary_mr->lkey, lkey_list));
        }
        // m_mrs[primary_mr].push_back(primary_mr);
        // m_lkey_table[primary_mr->lkey].push_back(primary_mr->lkey);
        for (int i = 1; i < m_pds.size(); i++) {
            ibv_mr* mr = mr_create(i, (void*)addr, length);
            tbb::concurrent_hash_map<ibv_mr*, tbb::concurrent_vector<ibv_mr*>>::accessor c;
            if(m_mrs.find(c, primary_mr)) {
                c->second.push_back(mr);
            } else {
                tbb::concurrent_vector<ibv_mr*> mr_list;
                mr_list.push_back(mr);
                m_mrs.insert(tbb::concurrent_hash_map<ibv_mr*, tbb::concurrent_vector<ibv_mr*>>::value_type(mr, mr_list));
            }
            tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor d;
            if(m_lkey_table.find(d, primary_mr->lkey)) {
                d->second.push_back(mr->lkey);
            } else {
                tbb::concurrent_vector<uint32_t> lkey_list;
                lkey_list.push_back(mr->lkey);
                m_lkey_table.insert(tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::value_type(primary_mr->lkey, lkey_list));
            }
        }
        return primary_mr;
    }

    int get_device_num() {
        return m_pds.size();
    }

    ibv_mr* get_mr_by_primary(ibv_mr* primary_mr, int dev) {
        tbb::concurrent_hash_map<ibv_mr*, tbb::concurrent_vector<ibv_mr*>>::accessor a;
        if(m_mrs.find(a, primary_mr)) {
            return a->second[dev];
        } else {
            return NULL;
        }
    }

    uint32_t get_lkey_by_primary(uint32_t primary_lkey, int dev) {
        tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
        if(m_lkey_table.find(a, primary_lkey)) {
            return a->second[dev];
        } else {
            return 0;
        }
    }

    ibv_pd* get_pd(int dev) {
        return m_pds[dev];
    }

    bool fill_rkey_table(uint32_t *rkey_list, ibv_mr *mr){
        tbb::concurrent_hash_map<ibv_mr*, tbb::concurrent_vector<ibv_mr*>>::accessor a;
        if(m_mrs.find(a, mr)) {
            for(int i = 0; i < a->second.size(); i++){
                rkey_list[i] = a->second[i]->rkey;
            }
            return true;
        } else {
            return false;
        }
    }

    bool fill_mr_list(ibv_mr **mr_list, ibv_mr *mr){
        tbb::concurrent_hash_map<ibv_mr*, tbb::concurrent_vector<ibv_mr*>>::accessor a;
        if(m_mrs.find(a, mr)) {
            for(int i = 0; i < a->second.size(); i++){
                mr_list[i] = a->second[i];
            }
            return true;
        } else {
            return false;
        }
    }

    ibv_ah* create_ah(int dev ,uint64_t input_gid1, uint64_t input_gid2, uint64_t input_interface, uint64_t input_subnet, uint32_t input_lid){
        ibv_gid gid;
        *(uint64_t*)gid.raw = input_gid1;
        *((uint64_t*)(gid.raw)+1) = input_gid2;
        gid.global.interface_id = input_interface;
        gid.global.subnet_prefix = input_subnet;

        struct ibv_ah_attr ah_attr;
        ah_attr.dlid = input_lid;
        ah_attr.port_num = 1;
        ah_attr.is_global = 1;
        ah_attr.grh.hop_limit = 1;
        ah_attr.grh.sgid_index = 1;
        ah_attr.grh.dgid = gid;
            
        ibv_ah* ah = ibv_create_ah(m_pds[dev], &ah_attr);
        return ah;
    }

public:
    vector<vector<zdcqp_requestor*>> m_requestors;   
    vector<vector<zdcqp_responder*>> m_responders;
private:
    tbb::concurrent_vector<ibv_pd*> m_pds;
    tbb::concurrent_hash_map<ibv_mr*, tbb::concurrent_vector<ibv_mr*>> m_mrs;
    tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>> m_lkey_table;
    std::mutex pd_mutex;
};

class zendpoint {
public:
    tbb::concurrent_vector<ibv_cq*> cq_list_;
    tbb::concurrent_hash_map<uint64_t, cq_info> completed_table;
    zendpoint(string config_file) {
        struct zdeviceConfig m_config;
        load_config(config_file.c_str(), &m_config);
        m_device_num = m_config.num_devices;
        m_node_id = m_config.node_id;
        struct ibv_context **device_list;
        int device_num;
        device_list = rdma_get_devices(&device_num);
        for (int i = 0; i < m_device_num; i++)
        {
            for(int j = 0; j < device_num; j ++) {
                if(m_config.mlx_names[i] == device_list[j]->device->name){
                    zdevice *device = new zdevice(m_config.mlx_names[i], m_config.eth_names[i], m_config.ips[i], m_config.ports[i], device_list[j]);
                    m_devices.push_back(device);
                    break;
                }        
            }
            if(i >= m_devices.size()) {
                printf("Device %s not found\n", m_config.mlx_names[i].c_str());
                exit(-1);
            }
        }
        m_pd = new zpd(this, DCQP_POOL_SIZE);
        m_rkey_table = new rkeyTable();
    #ifdef POLLTHREAD
        std::thread* poll_thread = new std::thread(zqp_poll_thread, this);
    #endif
    }
    ~zendpoint();
    ibv_context* get_device_context(int device_index) {
        return m_devices[device_index]->get_context();
    }
    int get_device_num() {
        return m_device_num;
    }
    bool get_address(int dev, char* ip_buf, char* port_buf) {
        return m_devices[dev]->get_address(ip_buf, port_buf);
    }
public:
    zpd* m_pd;
    rkeyTable *m_rkey_table;
private:
    vector<zdevice*> m_devices;
    int m_device_num;
    int m_node_id;
};

int zqp_poll_thread(zendpoint *zep) {
    struct ibv_wc wc[1024];
    while(zep != NULL) {
        for(int i = 0; i < zep->get_device_num(); i ++) {
            for(int j = 0; j < zep->cq_list_.size(); j ++) {
                if(zep->cq_list_[j] == NULL) {
                    continue;
                }
                int ne = ibv_poll_cq(zep->cq_list_[j], 1024, wc);
                if(ne < 0) {
                    perror("poll cq failed");
                    return -1;
                }
                for(int i = 0; i < ne; i ++) {
                    uint64_t wr_id = wc[i].wr_id;
                    if(wr_id == 0) {
                        continue;
                    }
                    // zqp->completed_table.[wr_id] = wc[i].status;
                    tbb::concurrent_hash_map<uint64_t, cq_info>::accessor a;
                    bool result = zep->completed_table.insert(a, {wr_id, {wc[i].status, .valid=1, .device_id=i}});
                    if(!result) {
                        // printf("Error, wr_id %lu already exists in completed_table\n", wr_id);
                        a->second.status = wc[i].status;
                        a->second.valid = 1;
                        a->second.device_id = i;
                    }
                    if(wc[i].status != IBV_WC_SUCCESS) {
                        // printf("wc[%lu] error: %s\n", wr_id, ibv_wc_status_str(wc[i].status));
                        continue;
                    }
                }
            }
        }
    }
    return 0;
}

}

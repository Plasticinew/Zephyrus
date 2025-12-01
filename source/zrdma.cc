#include "zrdma.h"

namespace zrdma {

int load_config(const char* fname, struct zDeviceConfig* config) {
    std::fstream config_fs(fname);

    boost::property_tree::ptree pt;
    try {
        boost::property_tree::read_json(config_fs, pt);
    } catch (boost::property_tree::ptree_error & e) {
        return -1;
    }

    try {
        config->node_id = pt.get<uint16_t>("node_id");

        int i = 0;
        BOOST_FOREACH(boost::property_tree::ptree::value_type & v, pt.get_child("eth_names")) {
            config->eth_names[i] = v.second.get<string>("");
            i ++;
        }

        i = 0;
        BOOST_FOREACH(boost::property_tree::ptree::value_type & v, pt.get_child("mlx_names")) {
            config->mlx_names[i] = v.second.get<string>("");
            i ++;
        }

        config->num_devices = i;
        i = 0;
        BOOST_FOREACH(boost::property_tree::ptree::value_type & v, pt.get_child("ports")) {
            config->ports[i] = v.second.get<string>("");
            i ++;
        }

        i = 0;
        BOOST_FOREACH(boost::property_tree::ptree::value_type & v, pt.get_child("ips")) { 
            config->ips[i] = v.second.get<string>("");
            i ++;
        }
        
    } catch (boost::property_tree::ptree_error & e) {
        return -1;
    }
    return 0;
}

int load_config(const char* fname, struct zTargetConfig* config) {
    std::fstream config_fs(fname);

    boost::property_tree::ptree pt;
    try {
        boost::property_tree::read_json(config_fs, pt);
    } catch (boost::property_tree::ptree_error & e) {
        return -1;
    }

    try {
        int i = 0;
        BOOST_FOREACH(boost::property_tree::ptree::value_type & v, pt.get_child("target_ports")) {
            config->target_ports[i] = v.second.get<string>("");
            i ++;
        }
        config->num_nodes = i;
        i = 0;
        BOOST_FOREACH(boost::property_tree::ptree::value_type & v, pt.get_child("target_ips")) {
            config->target_ips[i] = v.second.get<string>("");
            i ++;
        }
        
    } catch (boost::property_tree::ptree_error & e) {
        return -1;
    }
    return 0;
}

zEndpoint* zEP_create(string config_file) {

    zEndpoint *endpoint = new zEndpoint();
    struct zDeviceConfig m_config;
    load_config(config_file.c_str(), &m_config);
    endpoint->m_device_num = m_config.num_devices;
    endpoint->m_node_id = m_config.node_id;
    endpoint->qp_num_.store(1);
    for (int i = 0; i < endpoint->m_device_num; i++)
    {
        zDevice *device = new zDevice();
        device->mlx_name = m_config.mlx_names[i];
        device->eth_name = m_config.eth_names[i];
        device->eth_ip = m_config.ips[i];
        device->port = m_config.ports[i];
        endpoint->m_devices.push_back(device);
        device->status.store(zStatus::ZSTATUS_INIT);
    }

    struct ibv_context **device_list;
    int device_num;
    device_list = rdma_get_devices(&device_num);
    for(int i = 0; i < device_num; i ++) {
        for(int j = 0; j < endpoint->m_device_num; j ++) {
            if(endpoint->m_devices[j]->mlx_name == device_list[i]->device->name) {
                endpoint->m_devices[j]->context = device_list[i];
                break;
            }
        }
    }

    for(int i = 0; i < endpoint->m_device_num; i ++) {
        if(endpoint->m_devices[i]->context == NULL) {
            printf("Device %s not found\n", endpoint->m_devices[i]->mlx_name.c_str());
            exit(-1);
        }
    }

#ifdef POLLTHREAD
    std::thread* poll_thread = new std::thread(zQP_poll_thread, endpoint);
#endif

    return endpoint;
}


int zQP_poll_thread(zEndpoint *zep) {
    struct ibv_wc wc[1024];
    while(zep->stop == false) {
        for(int i = 0; i < zep->m_device_num; i ++) {
            for(int j = 0; j < zep->m_devices[i]->cq_list_.size(); j ++) {
                if(zep->m_devices[i]->cq_list_[j] == NULL) {
                    continue;
                }
                int ne = ibv_poll_cq(zep->m_devices[i]->cq_list_[j], 1024, wc);
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

zPD* zPD_create(zEndpoint *ep, int pool_size)
{
    zPD *pd = new zPD();
    for(int i = 0; i < ep->m_device_num; i ++) {
        ibv_pd *pd_ = ibv_alloc_pd(ep->m_devices[i]->context);
        pd->m_pds.push_back(pd_);
    }
    // create DCQP requestor and responder
    for(int i = 0; i < ep->m_device_num; i ++) {
        vector<zDCQP_requestor*> requestors;
        vector<zDCQP_responder*> responders;
        for(int j = 0; j < pool_size; j ++) {
            zDCQP_requestor *requestor = zDCQP_create_requestor(ep->m_devices[i], pd->m_pds[i]);
            zDCQP_responder *responder = zDCQP_create_responder(ep->m_devices[i], pd->m_pds[i]);
            requestors.push_back(requestor);
            responders.push_back(responder);
        }
        pd->m_requestors.push_back(requestors);
        pd->m_responders.push_back(responders);
    }
    return pd; 
}

void zEP_destroy(zEndpoint *ep){
    ep->stop = true;
    delete ep;
}
void zPD_destroy(zPD *pd) {
    delete pd;
}

ibv_mr* mr_create(ibv_pd *pd, void *addr, size_t length) {
    ibv_mr* mr = ibv_reg_mr(pd, addr, length, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE  | IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_MW_BIND);
    return mr;
}

ibv_mr* mr_malloc_create(zPD* pd, uint64_t &addr, size_t length) {
    addr = (uint64_t)mmap(NULL, length, PROT_READ | PROT_WRITE, MAP_PRIVATE |MAP_ANONYMOUS, -1, 0);
    ibv_mr* primary_mr = mr_create(pd->m_pds[0], (void*)addr, length);
    if (primary_mr == NULL) {
        munmap((void*)addr, length);
        return NULL;
    }
    tbb::concurrent_hash_map<ibv_mr*, tbb::concurrent_vector<ibv_mr*>>::accessor a;
    if(pd->m_mrs.find(a, primary_mr)) {
        a->second.push_back(primary_mr);
    } else {
        tbb::concurrent_vector<ibv_mr*> mr_list;
        mr_list.push_back(primary_mr);
        pd->m_mrs.insert(tbb::concurrent_hash_map<ibv_mr*, tbb::concurrent_vector<ibv_mr*>>::value_type(primary_mr, mr_list));
        // pd->m_mrs.insert(std::make_pair(primary_mr, mr_list));
    }
    tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor b;
    if(pd->m_lkey_table.find(b, primary_mr->lkey)) {
        b->second.push_back(primary_mr->lkey);
    } else {
        tbb::concurrent_vector<uint32_t> lkey_list;
        lkey_list.push_back(primary_mr->lkey);
        pd->m_lkey_table.insert(tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::value_type(primary_mr->lkey, lkey_list));
        // pd->m_lkey_table.insert(std::make_pair(primary_mr->lkey, lkey_list));
    }
    // pd->m_mrs[primary_mr].push_back(primary_mr);
    // pd->m_lkey_table[primary_mr->lkey].push_back(primary_mr->lkey);
    for (int i = 1; i < pd->m_pds.size(); i++) {
        ibv_mr* mr = mr_create(pd->m_pds[i], (void*)addr, length);
        tbb::concurrent_hash_map<ibv_mr*, tbb::concurrent_vector<ibv_mr*>>::accessor c;
        if(pd->m_mrs.find(c, primary_mr)) {
            c->second.push_back(mr);
        } else {
            tbb::concurrent_vector<ibv_mr*> mr_list;
            mr_list.push_back(mr);
            // pd->m_mrs.insert(std::make_pair(primary_mr, mr_list));
            pd->m_mrs.insert(tbb::concurrent_hash_map<ibv_mr*, tbb::concurrent_vector<ibv_mr*>>::value_type(mr, mr_list));
        }
        tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor d;
        if(pd->m_lkey_table.find(d, primary_mr->lkey)) {
            d->second.push_back(mr->lkey);
        } else {
            tbb::concurrent_vector<uint32_t> lkey_list;
            lkey_list.push_back(mr->lkey);
            pd->m_lkey_table.insert(tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::value_type(primary_mr->lkey, lkey_list));
            // pd->m_lkey_table.insert(std::make_pair(primary_mr->lkey, lkey_list));
        }
        // pd->m_mrs[primary_mr].push_back(mr);
        // pd->m_lkey_table[primary_mr->lkey].push_back(mr->lkey);
    }
    return primary_mr;
}

} // namespace zrdma
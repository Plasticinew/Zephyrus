#pragma once

#include "common.h"

#include "zdcqp.h"

namespace zrdma {

struct zPD {
    tbb::concurrent_vector<ibv_pd*> m_pds;
    tbb::concurrent_hash_map<ibv_mr*, tbb::concurrent_vector<ibv_mr*>> m_mrs;
    tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>> m_lkey_table;
    vector<vector<zDCQP_requestor*>> m_requestors;   
    vector<vector<zDCQP_responder*>> m_responders;
    std::mutex pd_mutex;
};

struct zEndpoint
{
    vector<zDevice*> m_devices;
    int m_device_num;
    int m_node_id;
    std::atomic<uint64_t> qp_num_{1};
    tbb::concurrent_hash_map<uint64_t, cq_info> completed_table;
    bool stop = false;
};

zEndpoint* zEP_create(string config_file);
zPD* zPD_create(zEndpoint *ep, int pool_size);
void zEP_destroy(zEndpoint *ep);
void zPD_destroy(zPD *pd);

ibv_mr* mr_create(ibv_pd *pd, void *addr, size_t length);
ibv_mr* mr_malloc_create(zPD* pd, uint64_t &addr, size_t length);

int load_config(const char* fname, struct zDeviceConfig* config);
int load_config(const char* fname, struct zTargetConfig* config);

}
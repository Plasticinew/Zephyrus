#include "zQP.h"

using namespace Zephyrus;

void test_zQP(string config_file, string remote_config_file) {
    zEndpoint *ep = zEP_create(config_file);
    zPD *pd = zPD_create(ep, 1);
    rkeyTable *table = new rkeyTable();
    vector<zQP*> qps;
    vector<zQP*> rpc_qps;
    zTargetConfig config;
    load_config(remote_config_file.c_str(), &config);
    for(int i = 0; i < config.num_nodes; i ++) {
        zQP* qp = zQP_create(pd, ep, table, ZQP_ONESIDED);
        zQP_connect(qp, 0, config.target_ips[i], config.target_ports[i]);
        qps.push_back(qp);
        zQP* rpc_qp = zQP_create(pd, ep, table, ZQP_RPC);
        zQP_connect(rpc_qp, 0, config.target_ips[i], config.target_ports[i]);
        rpc_qps.push_back(rpc_qp);
    }
    size_t alloc_size = 1024;
    for(int i = 0; i < qps.size(); i ++) {
        uint64_t addr;
        uint32_t rkey;
        zQP_RPC_Alloc(rpc_qps[i], &addr, &rkey, alloc_size);
        printf("Allocated memory at addr: %lx, rkey: %u\n", addr, rkey);
        void* local_buf; 
        ibv_mr* mr = mr_malloc_create(pd, (uint64_t&)local_buf, alloc_size);
         if (mr == NULL) {
            printf("Memory registration failed\n");
            return;
        }
         if (table->find(rkey) == table->end()) {
            printf("RKey %u not found in rkey table\n", rkey);
            return;
        }
        printf("Local MR lkey: %u\n", mr->lkey);
        printf("Remote RKey list: ");
        for(auto rk : table->at(rkey)) {
            printf("%u ", rk);
        }
        memset(local_buf, 1, alloc_size);
        z_write(qps[i], local_buf, mr->lkey, alloc_size, (void*)addr, rkey);
        memset(local_buf, 0, alloc_size);
        z_read(qps[i], local_buf, mr->lkey, alloc_size, (void*)addr, rkey);
        for(int j = 0; j < alloc_size; j ++) {
            if(((char*)local_buf)[j] != 1) {
                printf("Data mismatch at byte %d\n", j);
                break;
            }
        }
        for(int j = 0; j < alloc_size / sizeof(uint64_t); j++) {
            zQP_CAS(qps[i], ((uint64_t*)local_buf)+j, mr->lkey, 2, (void*)(addr + j * sizeof(uint64_t)), rkey, 0);
        }
        memset(local_buf, 0, alloc_size);
        z_read(qps[i], local_buf, mr->lkey, alloc_size, (void*)addr, rkey);
        for(int j = 0; j < alloc_size / sizeof(uint64_t); j++) {
            if(((uint64_t*)local_buf)[j] != 2) {
                printf("CAS Data mismatch at uint64 %d, value: %lu\n", j, ((uint64_t*)local_buf)[j]);
                break;
            }
        }
    }
    printf("Test completed successfully\n");
}

int main(int argc, char** argv) {
    if(argc < 3) {
        printf("Usage: %s <local_config_file> <remote_config_file>\n", argv[0]);
        return -1;
    }
    string config_file = argv[1];
    string remote_config_file = argv[2];
    test_zQP(config_file, remote_config_file);
    return 0;
}
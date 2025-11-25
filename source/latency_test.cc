#include "zQP.h"
#include <fstream>
#include <random>
#include <gperftools/profiler.h>

using namespace Zephyrus;

// #define SHARED_EP_NUM 4

pthread_barrier_t barrier_start;

std::atomic<int> global_counter{0};
std::atomic<int> global_latency{0};

void nic_shutdown_handler() {
    // usleep(rand() % 500000 + 500000);
    usleep(500000);
    system("sudo ip link set ens1f0 down");
    printf("ens1f0 down\n");
    // system("sudo ifconfig ens3f0 down");
}

void bandwidth_counting_thread(tbb::concurrent_vector<std::atomic<uint64_t>*>& counter_vec) {
    std::ofstream log_file("bandwidth_log.txt", std::ios::app);
    while (true) {
        uint64_t start_size = 0;
        for(auto counter : counter_vec) {
            start_size += counter->load();
        }
        usleep(100000); // 100 ms
        uint64_t end_size = 0;
        for(auto counter : counter_vec) {
            end_size += counter->load();
        }
        double mbps = (end_size - start_size) * 10 / (1024.0 * 1024.0);
        // print to log file
        log_file << mbps << std::endl;
    }
}
#ifdef SHARED_EP_NUM
void test_zQP_shared_p2p(string config_file, string remote_config_file, zEndpoint* ep, zPD* pd, int thread_id, tbb::concurrent_vector<std::atomic<uint64_t>*>& counter_vec, int thread_count, int write_size) {
#else
void test_zQP_shared_p2p(string config_file, string remote_config_file, int thread_id, tbb::concurrent_vector<std::atomic<uint64_t>*>& counter_vec, int thread_count, int write_size) {
#endif
    rkeyTable *table = new rkeyTable();
    vector<zQP*> qps;
    zTargetConfig config;
#ifndef SHARED_EP_NUM
    zEndpoint *ep = zEP_create(config_file);
    zPD *pd = zPD_create(ep, 1);
#endif
    load_config(remote_config_file.c_str(), &config);
    zQP* rpc_qp = zQP_create(pd, ep, table, ZQP_RPC);
    zQP_connect(rpc_qp, 0, config.target_ips[0], config.target_ports[0]);
    int qp_per_thread = thread_count/thread_count;
    for(int i = 0; i < qp_per_thread; i ++) {
        zQP* qp = zQP_create(pd, ep, table, ZQP_ONESIDED);
        zQP_connect(qp, 0, config.target_ips[0], config.target_ports[0]);
        // zQP_connect(qp, 1, qp->m_targets[1]->ip, qp->m_targets[1]->port);
        qps.push_back(qp);
        counter_vec.push_back(&qp->size_counter_);
    }
        size_t alloc_size = 1024*1024;
        // size_t write_size = 16384;
        uint64_t addr;
        uint32_t rkey;
        zQP_RPC_Alloc(rpc_qp, &addr, &rkey, alloc_size);
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
        // for(int j = 0; j < alloc_size / sizeof(uint64_t); j++){
        //     ((uint64_t*)local_buf)[j] = 1;
        // }
        memset(local_buf, 0, alloc_size);
        std::thread* t;
        if(thread_id == 0) {
            // t = new std::thread(system, "sudo ip link set ens1f0 up");
            // t = new std::thread(system, "sudo ip link set ens3f0 down");
            t = new std::thread(nic_shutdown_handler);
        }
        int nic_index = 0;
        std::vector<uint64_t> wr_ids;
        // using random qps
        std::default_random_engine generator;
        std::uniform_int_distribution<int> distribution(0, qp_per_thread -1);
        uint64_t* counter = (uint64_t*)local_buf;
        *counter = 0;
        // z_simple_write(qps[0], ((char*)local_buf), mr->lkey, sizeof(uint64_t), (void*)(addr), rkey);
        pthread_barrier_wait(&barrier_start);
        for(int k = 0; k < 4000; k++) {
            // int i = distribution(generator);
            int result;
            auto star_time = TIME_NOW;
            for(int j = 0; j < 100; j++){
                // *counter = k*1024 + j;
                int prev = *counter;
                int target = k + j + 1;
                // *counter = target;
                // zDCQP_write(qps[i]->m_pd->m_requestors[nic_index][0], qps[i]->m_targets[nic_index]->ah, local_buf, qps[i]->m_pd->m_lkey_table[mr->lkey][nic_index], alloc_size, (void*)addr, table->at(rkey)[nic_index], qps[i]->m_targets[nic_index]->lid_, qps[i]->m_targets[nic_index]->dct_num_);
                // z_write_async(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey, &wr_ids);
                // z_simple_read_async(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey, &wr_ids);
                // z_read_async(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey, &wr_ids);
                // z_simple_write_async(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey, &wr_ids);
                // int result = z_simple_write(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey);
                // int result = z_write(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey);
                // int result = z_read(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey);
                // int result = z_simple_read(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey);
                result = z_simple_CAS(qps[0], ((char*)local_buf), mr->lkey, target, (void*)(addr), rkey);
                // result = z_CAS(qps[0], ((char*)local_buf)+j*sizeof(uint64_t), mr->lkey, target, (void*)(addr)+j*sizeof(uint64_t), rkey);
                // result = z_CAS_async(qps[0], ((char*)local_buf)+j*sizeof(uint64_t), mr->lkey, target, (void*)(addr)+j*sizeof(uint64_t), rkey, &wr_ids);
                // printf("%d:%d\n", *counter, target);
                // z_simple_read(qps[0], ((char*)local_buf), mr->lkey, sizeof(uint64_t), (void*)(addr), rkey);
                // printf("%d:%d\n", *counter, target);
                if(*(counter) != prev){
                    printf("CAS error!\n");
                } else {
                    *(counter) = target;
                }
                // if(result < 0) {
                //     // printf("%d:%d\n", *counter, target);
                //     z_read(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey);
                //     // z_simple_read(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey);
                //     // z_simple_read(qps[0], ((char*)local_buf), mr->lkey, sizeof(uint64_t), (void*)(addr), rkey);
                //     // printf("%d:%d\n", *counter, target);
                //     if(*counter == target) {
                //         global_counter.fetch_add(1);
                //         // printf("no need to send\n");
                //     } 
                //     // break;
                // }
                // *(counter+j) = target;
                // z_CAS(qps[i], ((uint64_t*)local_buf), mr->lkey, 2, (void*)(addr), rkey);
                // z_write_async(qps[i], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey, &wr_ids);
            }
            // auto send_time = TIME_NOW;
            // z_poll_completion(qps[0], &wr_ids);
            auto end_time = TIME_NOW;
            double total_us = TIME_DURATION_US(star_time, end_time);
            // double write_us = TIME_DURATION_US(star_time, send_time);
            // double poll_us = TIME_DURATION_US(send_time, end_time);
            // printf("send time: %lf us, poll time: %lf us, total time: %lf us\n", write_us, poll_us, total_us);
            // printf("send time: %lf us, poll time: %lf us, total time: %lf us\n", TIME_DURATION_US(star_time, send_time), TIME_DURATION_US(send_time, end_time), total_us);
            global_latency.fetch_add(total_us);
            // usleep(100);
            // printf("Thread %d, QP %d, write iteration %d completed\n", thread_id, i, k);
        }
        // double throughput = (double)(alloc_size * 1000) / (total_us);
        // double average_latency = total_us / (200000);
        // printf("Thread %d, Time spend: %f us, Average Latency: %f us, Throughput: %f MB/s\n", thread_id, total_us, average_latency, throughput);
        // memset(local_buf, 0, alloc_size);
        // for(int j = 0; j < alloc_size / sizeof(uint64_t); j++){
        //     // zDCQP_write(qps[i]->m_pd->m_requestors[nic_index][0], qps[i]->m_targets[nic_index]->ah, local_buf, qps[i]->m_pd->m_lkey_table[mr->lkey][nic_index], alloc_size, (void*)addr, table->at(rkey)[nic_index], qps[i]->m_targets[nic_index]->lid_, qps[i]->m_targets[nic_index]->dct_num_);
        //     // z_read(qps[i], ((char*)local_buf)+j, mr->lkey, sizeof(char), (void*)(addr + j * sizeof(char)), rkey);
        //     z_read_async(qps[i], ((uint64_t*)local_buf)+j, mr->lkey, sizeof(uint64_t), (void*)(addr + j * sizeof(uint64_t)), rkey, &wr_ids);
        // }
        // z_poll_completion(qps[i], &wr_ids);
        // for(int j = 0; j < alloc_size / sizeof(uint64_t); j++) {
        //     if(((uint64_t*)local_buf)[j] != 1) {
        //         printf("Data mismatch at uint64 %d: expected 1, got %lu\n", j, ((uint64_t*)local_buf)[j]);
        //         break;
        //     }
        // }

        // sleep(1);
        // pthread_barrier_wait(&barrier_start);
        // for(int j = 0; j < alloc_size / sizeof(uint64_t); j++) {
        //     // zDCQP_CAS(qps[i]->m_pd->m_requestors[nic_index][0], qps[i]->m_targets[nic_index]->ah, ((uint64_t*)local_buf)+j, qps[i]->m_pd->m_lkey_table[mr->lkey][nic_index], 2, (void*)((uint64_t)addr + j * sizeof(uint64_t)), table->at(rkey)[nic_index], qps[i]->m_targets[nic_index]->lid_, qps[i]->m_targets[nic_index]->dct_num_);
        //     z_CAS(qps[i], ((uint64_t*)local_buf)+j, mr->lkey, 2, (void*)(addr + j * sizeof(uint64_t)), rkey);
        // }
        // memset(local_buf, 0, alloc_size);
        // // sleep(10);
        // z_read(qps[i], local_buf, mr->lkey, alloc_size, (void*)addr, rkey);
        // for(int j = 0; j < alloc_size / sizeof(uint64_t); j++) {
        //     if(((uint64_t*)local_buf)[j] != 2) {
        //         printf("CAS Data mismatch at uint64 %d, value: %lu\n", j, ((uint64_t*)local_buf)[j]);
        //         // break;
        //     }
        // }
        if(thread_id == 0) {
            t->join();
        }
    // printf("Test completed successfully\n");
}

// void test_zQP_shared(string config_file, string remote_config_file, zEndpoint* ep, zPD* pd, int thread_id, tbb::concurrent_vector<zQP*>& listen_qps ) {
void test_zQP_shared(string config_file, string remote_config_file, int thread_id, tbb::concurrent_vector<std::atomic<uint64_t>*>& counter_vec ) {
    rkeyTable *table = new rkeyTable();
    vector<zQP*> qps;
    vector<zQP*> rpc_qps;
    zTargetConfig config;
    zEndpoint *ep = zEP_create(config_file);
    zPD *pd = zPD_create(ep, 1);
    load_config(remote_config_file.c_str(), &config);
    for(int i = 0; i < config.num_nodes; i ++) {
        zQP* qp = zQP_create(pd, ep, table, ZQP_ONESIDED);
        zQP_connect(qp, 0, config.target_ips[i], config.target_ports[i]);
        qps.push_back(qp);
        counter_vec.push_back(&qp->size_counter_);
        zQP* rpc_qp = zQP_create(pd, ep, table, ZQP_RPC);
        zQP_connect(rpc_qp, 0, config.target_ips[i], config.target_ports[i]);
        rpc_qps.push_back(rpc_qp);
    }
    size_t alloc_size = 1024*1024;
    for(int i = 0; i < qps.size(); i ++) {
        uint64_t addr;
        uint32_t rkey;
        zQP_RPC_Alloc(rpc_qps[i], &addr, &rkey, alloc_size);
        // printf("Allocated memory at addr: %lx, rkey: %u\n", addr, rkey);
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
        // printf("Local MR lkey: %u\n", mr->lkey);
        // printf("Remote RKey list: ");
        for(auto rk : table->at(rkey)) {
            printf("%u ", rk);
        }
        printf("\n");
        // for(int j = 0; j < alloc_size / sizeof(uint64_t); j++){
        //     ((uint64_t*)local_buf)[j] = 1;
        // }
        memset(local_buf, 1, alloc_size);
        std::thread* t;
        if(thread_id == 0) {
            // t = new std::thread(system, "sudo ip link set ens1f0 up");
            // t = new std::thread(system, "sudo ip link set ens3f0 down");
            t = new std::thread(nic_shutdown_handler);
        }
        int nic_index = 0;
        std::vector<uint64_t> wr_ids;
        auto star_time = TIME_NOW;
        for(int k = 0; k < 4000; k++) {
            for(int j = 0; j < alloc_size / (4096); j++){
                // zDCQP_write(qps[i]->m_pd->m_requestors[nic_index][0], qps[i]->m_targets[nic_index]->ah, local_buf, qps[i]->m_pd->m_lkey_table[mr->lkey][nic_index], alloc_size, (void*)addr, table->at(rkey)[nic_index], qps[i]->m_targets[nic_index]->lid_, qps[i]->m_targets[nic_index]->dct_num_);
                // z_write(qps[i], ((char*)local_buf)+j, mr->lkey, sizeof(char), (void*)(addr + j), rkey);
                z_write_async(qps[i], ((char*)local_buf)+ j * 4096, mr->lkey, 4096, (void*)(addr), rkey, &wr_ids);
            }
            z_poll_completion(qps[i], &wr_ids);
            // printf("Thread %d, QP %d, write iteration %d completed\n", thread_id, i, k);
        }
        auto end_time = TIME_NOW;
        double total_us = TIME_DURATION_US(star_time, end_time);
        double throughput = (double)(alloc_size * 1000) / (total_us);
        double average_latency = total_us / (alloc_size * 1000);
        printf("Thread %d, QP %d, Time spend: %f us, Average Latency: %f us, Throughput: %f MB/s\n", thread_id, i, total_us, average_latency, throughput);
        // memset(local_buf, 0, alloc_size);
        // for(int j = 0; j < alloc_size / sizeof(uint64_t); j++){
        //     // zDCQP_write(qps[i]->m_pd->m_requestors[nic_index][0], qps[i]->m_targets[nic_index]->ah, local_buf, qps[i]->m_pd->m_lkey_table[mr->lkey][nic_index], alloc_size, (void*)addr, table->at(rkey)[nic_index], qps[i]->m_targets[nic_index]->lid_, qps[i]->m_targets[nic_index]->dct_num_);
        //     // z_read(qps[i], ((char*)local_buf)+j, mr->lkey, sizeof(char), (void*)(addr + j * sizeof(char)), rkey);
        //     z_read_async(qps[i], ((uint64_t*)local_buf)+j, mr->lkey, sizeof(uint64_t), (void*)(addr + j * sizeof(uint64_t)), rkey, &wr_ids);
        // }
        // z_poll_completion(qps[i], &wr_ids);
        // for(int j = 0; j < alloc_size / sizeof(uint64_t); j++) {
        //     if(((uint64_t*)local_buf)[j] != 1) {
        //         printf("Data mismatch at uint64 %d: expected 1, got %lu\n", j, ((uint64_t*)local_buf)[j]);
        //         break;
        //     }
        // }

        // sleep(1);
        // pthread_barrier_wait(&barrier_start);
        // for(int j = 0; j < alloc_size / sizeof(uint64_t); j++) {
        //     // zDCQP_CAS(qps[i]->m_pd->m_requestors[nic_index][0], qps[i]->m_targets[nic_index]->ah, ((uint64_t*)local_buf)+j, qps[i]->m_pd->m_lkey_table[mr->lkey][nic_index], 2, (void*)((uint64_t)addr + j * sizeof(uint64_t)), table->at(rkey)[nic_index], qps[i]->m_targets[nic_index]->lid_, qps[i]->m_targets[nic_index]->dct_num_);
        //     z_CAS(qps[i], ((uint64_t*)local_buf)+j, mr->lkey, 2, (void*)(addr + j * sizeof(uint64_t)), rkey);
        // }
        // memset(local_buf, 0, alloc_size);
        // // sleep(10);
        // z_read(qps[i], local_buf, mr->lkey, alloc_size, (void*)addr, rkey);
        // for(int j = 0; j < alloc_size / sizeof(uint64_t); j++) {
        //     if(((uint64_t*)local_buf)[j] != 2) {
        //         printf("CAS Data mismatch at uint64 %d, value: %lu\n", j, ((uint64_t*)local_buf)[j]);
        //         // break;
        //     }
        // }
        if(thread_id == 0) {
            t->join();
        }
    }
    printf("Test completed successfully\n");
}

int main(int argc, char** argv) {
    std::ofstream log_file("bandwidth_log.txt");
    log_file.close();
    system("sudo ip link set ens1f0 up");
    // system("sudo ifconfig ens3f0 10.10.1.1/24");
    system("sudo ip link set ens1f1 up");
    sleep(2);
    if(argc < 4) {
        printf("Usage: %s <local_config_file> <remote_config_file> <thread_num>\n", argv[0]);
        return -1;
    }
    ProfilerStart("latency_test.prof");
    string config_file = argv[1];
    string remote_config_file = argv[2];
    int thread_num = atoi(argv[3]);
    pthread_barrier_init(&barrier_start, NULL, thread_num);
    std::vector<std::thread*> threads;
    tbb::concurrent_vector<std::atomic<uint64_t>*> counter_vec;
    int test_size[] = {16, 64, 256, 1024, 4096, 16384, 65536};
    for(int j = 0; j < 7; j++) {
#ifdef SHARED_EP_NUM
        zEndpoint *ep[SHARED_EP_NUM];
        for(int i = 0; i < SHARED_EP_NUM; i ++) {
            ep[i] = zEP_create(config_file);
        }
        zPD *pd[SHARED_EP_NUM];
        for(int i = 0; i < SHARED_EP_NUM; i ++) {
            pd[i] = zPD_create(ep[i], 1);
        }
#endif
        for(int i = 0; i < thread_num; i ++) {
            // threads.push_back(new std::thread(test_zQP, config_file, remote_config_file, i));
            // threads.push_back(new std::thread(test_zQP_shared_p2p, config_file, remote_config_file, i, std::ref(counter_vec), thread_num, test_size[j]));
#ifdef SHARED_EP_NUM
            threads.push_back(new std::thread(test_zQP_shared_p2p, config_file, remote_config_file, ep[i % SHARED_EP_NUM], pd[i % SHARED_EP_NUM], i, std::ref(counter_vec), thread_num, test_size[j]));
#else
            threads.push_back(new std::thread(test_zQP_shared_p2p, config_file, remote_config_file, i, std::ref(counter_vec), thread_num, test_size[j]));
#endif
            // threads.push_back(new std::thread(test_zQP_shared, config_file, remote_config_file, i, std::ref(counter_vec)));
            // threads.push_back(new std::thread(test_zQP_shared, config_file, remote_config_file, ep, pd, i, std::ref(listen_qps)));
        }
        std::thread* bw_thread = new std::thread(bandwidth_counting_thread, std::ref(counter_vec));
        for(int i = 0; i < thread_num; i ++) {
            threads[i]->join();
        }
        system("sudo ip link set ens1f0 up");
        // system("sudo ifconfig ens1f0 10.10.1.1/24");
        system("sudo ip link set ens1f1 up");
        threads.clear();
        sleep(2);
        // printf("%lf\n", global_counter.load()/(thread_num*1.0));
        // printf("Size %d Average Latency: %lf us\n", test_size[j], global_latency.load()/(400000.0*thread_num));
        printf("%lf\n", global_latency.load()/(400000.0*thread_num));
        global_latency.store(0);
        global_counter.store(0);
        counter_vec.clear();
#ifdef SHARED_EP_NUM
        for(int i = 0; i < SHARED_EP_NUM; i ++) {
            zEP_destroy(ep[i]);
        }
        for(int i = 0; i < SHARED_EP_NUM; i ++) {
            zPD_destroy(pd[i]);
        }
#endif
    }
    // printf("%lf\n", global_counter.load()/(1000*thread_num*1.0));
    pthread_barrier_destroy(&barrier_start);
    ProfilerStop();
    return 0;
}
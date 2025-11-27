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
#include <tbb/concurrent_vector.h>
#include <atomic>
#include <mutex>
#include <arpa/inet.h>

using std::string;
using std::vector;
using std::unordered_map;
using std::thread;
using std::atomic;

namespace zrdma {

#define RECOVERY

#define POLLTHREAD

// #define SEND_TWICE

#define ASYNC_CONNECT

#define NO_ERROR_HANDLE


#define NOTIFY_WORK 0xFF
#define NOTIFY_IDLE 0x00
#define MAX_MSG_SIZE 16384
#define MAX_SERVER_WORKER 1
#define MAX_SERVER_CLIENT 4096
#define RESOLVE_TIMEOUT_MS 5000
#define RDMA_TIMEOUT_US (uint64_t)100000000
#define RETRY_TIMEOUT 7
#define MAX_REQUESTOR_NUM 32768
#define MAX_REMOTE_SIZE (1UL << 25)
#define WR_ENTRY_NUM 1024
#define MAX_QP_NUM 1024
#define ATOMIC_ENTRY_NUM 512

// Time
#define TIME_NOW (std::chrono::high_resolution_clock::now())
#define TIME_DURATION_US(start, end) (std::chrono::duration_cast<std::chrono::microseconds>((end) - (start)).count())

#define CHECK_RDMA_MSG_SIZE(T) \
    static_assert(sizeof(T) < MAX_MSG_SIZE, #T " msg size is too big!")

#define MAX_NIC_NUM 2

typedef unordered_map<uint32_t, vector<uint32_t>> rkeyTable;

struct QpInfo {
    uint32_t qp_num;
    uint16_t lid;
    uint8_t  port_num;
    uint8_t  gid[16];
    uint8_t  gid_idx;
};

struct qp_info_table {
    uint64_t addr;
    uint32_t rkey[MAX_NIC_NUM];
};

struct WorkerInfo {
    CmdMsgBlock *cmd_msg;
    CmdMsgRespBlock *cmd_resp_msg;
    struct ibv_mr *msg_mr;
    struct ibv_mr *resp_mr;
    rdma_cm_id *cm_id;
    struct ibv_cq *cq;
}; 

struct cq_info {
    ibv_wc_status status;
    int valid;
    int device_id;
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

enum zqpType { ZQP_ONESIDED, ZQP_RPC };

struct CNodeInit{
    uint16_t node_id;
    zqpType qp_type;
};

enum ResStatus { RES_OK, RES_FAIL };

enum MsgType { MSG_REGISTER, MSG_UNREGISTER, MSG_FETCH, MSG_FETCH_FAST, MSG_MW_BIND, RPC_FUSEE_SUBTABLE, MSG_MW_REBIND, MSG_MW_CLASS_BIND, MSG_FREE_FAST, MSG_PRINT_INFO, MSG_MW_BATCH};

// Status Info
enum zStatus {
    ZSTATUS_INIT,
    ZSTATUS_CONNECTED,
    ZSTATUS_ACCEPTED,
    ZSTATUS_ERROR
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

struct zdeviceConfig {
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

int load_config(const char* fname, struct zdeviceConfig* config) {
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

}
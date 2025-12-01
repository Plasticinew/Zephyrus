#pragma once

#include "common.h"

namespace zrdma {

struct zDCQP_requestor { 
    zDevice* device_;
    ibv_pd* pd_;
    ibv_qp* qp_;
    ibv_qp_ex* qp_ex_;
    mlx5dv_qp_ex* qp_mlx_ex_;
    ibv_cq* cq_;
    zStatus status_;
    uint8_t port_num_;
    uint16_t lid_;
    uint32_t dct_num_;
    // atomic<uint64_t> size_counter_{0};
};

struct zDCQP_responder { 
    zDevice* device_;
    ibv_pd* pd_;
    ibv_qp* qp_;
    ibv_qp_ex* qp_ex_;
    mlx5dv_qp_ex* qp_mlx_ex_;
    ibv_cq* cq_;
    ibv_srq* srq_;
    ibv_ah* ah_;
    zStatus status_;
    uint64_t gid1, gid2, interface, subnet;
    uint8_t port_num_;
    uint16_t lid_;
    uint32_t dct_num_;
};

zDCQP_requestor* zDCQP_create_requestor(zDevice *device, ibv_pd *pd);
zDCQP_responder* zDCQP_create_responder(zDevice *device, ibv_pd *pd);
ibv_ah* zDCQP_create_ah(zDCQP_requestor* requestor, uint64_t input_gid1, uint64_t input_gid2, uint64_t input_interface, uint64_t input_subnet, uint32_t input_lid);

int zDCQP_read(zDCQP_requestor* requestor, ibv_ah* ah, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, uint32_t lid, uint32_t dct_num);
int zDCQP_write(zDCQP_requestor* requestor, ibv_ah* ah, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, uint32_t lid, uint32_t dct_num);
int zDCQP_CAS(zDCQP_requestor* requestor, ibv_ah* ah, void* local_addr, uint32_t lkey, uint64_t new_val, void* remote_addr, uint32_t rkey, uint32_t lid, uint32_t dct_num);
int zDCQP_send(zDCQP_requestor* requestor, ibv_ah* ah, ibv_send_wr* send_wr, uint32_t lid, uint32_t dct_num);

}
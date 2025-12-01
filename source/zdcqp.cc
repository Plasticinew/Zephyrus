#include "zdcqp.h"

namespace zrdma {

zDCQP_requestor* zDCQP_create_requestor(zDevice *device, ibv_pd *pd) { 
    zDCQP_requestor *dcqp = new zDCQP_requestor();
    dcqp->device_ = device;
    dcqp->pd_ = pd;
    struct mlx5dv_qp_init_attr dv_init_attr;
    struct ibv_qp_init_attr_ex init_attr;
    
    memset(&dv_init_attr, 0, sizeof(dv_init_attr));
    memset(&init_attr, 0, sizeof(init_attr));
    
    dcqp->cq_ = ibv_create_cq(device->context, 128, NULL, NULL, 0);

    // 和正常QP创建相同，需要设置参数
    init_attr.qp_type = IBV_QPT_DRIVER;
    init_attr.send_cq = dcqp->cq_;
    init_attr.recv_cq = dcqp->cq_;
    init_attr.pd = pd; 
    init_attr.cap.max_send_wr = 512;
    init_attr.cap.max_send_sge = 16;
    init_attr.sq_sig_all = 0;

    // DCQP需要额外设置的内容
    init_attr.comp_mask |= IBV_QP_INIT_ATTR_SEND_OPS_FLAGS | IBV_QP_INIT_ATTR_PD ;
    init_attr.send_ops_flags |= IBV_QP_EX_WITH_SEND | IBV_QP_EX_WITH_RDMA_READ | IBV_QP_EX_WITH_RDMA_WRITE | IBV_QP_EX_WITH_ATOMIC_CMP_AND_SWP;
 
    dv_init_attr.comp_mask |=
                MLX5DV_QP_INIT_ATTR_MASK_DC |
                MLX5DV_QP_INIT_ATTR_MASK_QP_CREATE_FLAGS;
    dv_init_attr.create_flags |=
                MLX5DV_QP_CREATE_DISABLE_SCATTER_TO_CQE;
    
    // 类型为发送端DCI
    dv_init_attr.dc_init_attr.dc_type = MLX5DV_DCTYPE_DCI;

    dcqp->qp_ = mlx5dv_create_qp(device->context, &init_attr, &dv_init_attr);

    if(dcqp->qp_ == NULL) {
        perror("create dcqp failed!");
    }

    struct ibv_qp_attr         qp_attr_to_init;
    struct ibv_qp_attr         qp_attr_to_rtr;
    struct ibv_qp_attr         qp_attr_to_rts;

    memset(&qp_attr_to_init, 0, sizeof(qp_attr_to_init));
    memset(&qp_attr_to_rtr, 0, sizeof(qp_attr_to_rtr));
    memset(&qp_attr_to_rts, 0, sizeof(qp_attr_to_rts));

    // 状态切换为INIT
    qp_attr_to_init.qp_state   = IBV_QPS_INIT;
    qp_attr_to_init.pkey_index = 0;
    qp_attr_to_init.port_num   = 1;
    
    // 状态切换为RTR
    qp_attr_to_rtr.qp_state          = IBV_QPS_RTR;
    qp_attr_to_rtr.path_mtu          = IBV_MTU_4096;
    qp_attr_to_rtr.min_rnr_timer     = 10;
    qp_attr_to_rtr.ah_attr.port_num  = 1;
    qp_attr_to_rtr.ah_attr.is_global = 1;

    // 状态切换为RTS
    qp_attr_to_rts.qp_state      = IBV_QPS_RTS;
    qp_attr_to_rts.timeout       = 0;
    qp_attr_to_rts.retry_cnt     = 7;
    qp_attr_to_rts.rnr_retry     = 7;
    qp_attr_to_rts.sq_psn        = 114;
    qp_attr_to_rts.max_rd_atomic = 1;

    int ret = ibv_modify_qp(dcqp->qp_, &qp_attr_to_init, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT);
    if (ret) {
        printf("%d\n", ret);
        perror("init state failed\n");
        abort();
    }

    ret = ibv_modify_qp(dcqp->qp_, &qp_attr_to_rtr, IBV_QP_STATE | IBV_QP_PATH_MTU | IBV_QP_AV);
    if (ret) {
        printf("%d\n", ret);
        perror("rtr state failed\n");
        abort();
    }
    
    ret = ibv_modify_qp(dcqp->qp_, &qp_attr_to_rts, IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                      IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                      IBV_QP_MAX_QP_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER);
    if (ret) {
        printf("%d\n", ret);
        perror("rts state failed\n");
        abort();
    }

    struct ibv_qp_attr attr;
    struct ibv_qp_init_attr init_attr_;
    
    // 获取qp的信息，主要是dct_num
    // lid和port在IB网络下有效，RoCE网络下不会用到
    ibv_query_qp(dcqp->qp_, &attr,
            IBV_QP_STATE, &init_attr_);

    dcqp->lid_ = attr.ah_attr.dlid;
    dcqp->port_num_ = attr.ah_attr.port_num;
    dcqp->dct_num_ = dcqp->qp_->qp_num;

    // printf("%d, %d, %d, %d\n", attr.qp_state ,dcqp->lid_, dcqp->port_num_, dcqp->dct_num_);

    // 提取ex qp与mlx qp
    dcqp->qp_ex_ = ibv_qp_to_qp_ex(dcqp->qp_);

    dcqp->qp_mlx_ex_ = mlx5dv_qp_ex_from_ibv_qp_ex(dcqp->qp_ex_);
    
    return dcqp;
}

zDCQP_responder* zDCQP_create_responder(zDevice *device, ibv_pd *pd) { 

    zDCQP_responder *dcqp = new zDCQP_responder();
    dcqp->device_ = device;
    dcqp->pd_ = pd;
    struct mlx5dv_qp_init_attr dv_init_attr;
    struct ibv_qp_init_attr_ex init_attr;
    
    memset(&dv_init_attr, 0, sizeof(dv_init_attr));
    memset(&init_attr, 0, sizeof(init_attr));
    
    dcqp->cq_ = ibv_create_cq(device->context, 1024, NULL, NULL, 0);
    struct ibv_srq_init_attr srq_init_attr;
 
    memset(&srq_init_attr, 0, sizeof(srq_init_attr));
 
    srq_init_attr.attr.max_wr  = 1;
    srq_init_attr.attr.max_sge = 1;
 
    dcqp->srq_ = ibv_create_srq(pd, &srq_init_attr);

    init_attr.qp_type = IBV_QPT_DRIVER;
    init_attr.send_cq = dcqp->cq_;
    init_attr.recv_cq = dcqp->cq_;
    init_attr.pd = pd; 
    init_attr.cap.max_send_wr = 1;
    init_attr.cap.max_recv_wr = 1;
    init_attr.cap.max_send_sge = 1;
    init_attr.cap.max_recv_sge = 1;
    init_attr.cap.max_inline_data = 256;
    init_attr.sq_sig_all = 0;

    init_attr.comp_mask |= IBV_QP_INIT_ATTR_PD;
    init_attr.srq = dcqp->srq_;
    dv_init_attr.comp_mask = MLX5DV_QP_INIT_ATTR_MASK_DC;
    // 类型为接收端DCT
    dv_init_attr.dc_init_attr.dc_type = MLX5DV_DCTYPE_DCT;
    // 需要设置访问的key
    dv_init_attr.dc_init_attr.dct_access_key = 114514;

    dcqp->qp_ = mlx5dv_create_qp(device->context, &init_attr, &dv_init_attr);

    if(dcqp->qp_ == NULL) {
        perror("create dcqp failed!");
    }

    // 切换为INIT
    ibv_qp_attr qp_attr{};
    qp_attr.qp_state = IBV_QPS_INIT;
    qp_attr.pkey_index = 0;
    qp_attr.port_num = 1;
    qp_attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE |
                                IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC;

    int attr_mask =
        IBV_QP_STATE | IBV_QP_ACCESS_FLAGS | IBV_QP_PORT |IBV_QP_PKEY_INDEX;

    int ret = ibv_modify_qp(dcqp->qp_, &qp_attr, attr_mask);
    if (ret) {
        printf("%d\n", ret);
        perror("change state failed\n");
        abort();
    }

    // 切换为RTR
    qp_attr.qp_state = IBV_QPS_RTR;
    qp_attr.path_mtu = IBV_MTU_4096;
    qp_attr.min_rnr_timer = 12;
    qp_attr.ah_attr.is_global = 1;
    qp_attr.ah_attr.grh.hop_limit = 1;
    qp_attr.ah_attr.grh.traffic_class = 0;
    qp_attr.ah_attr.grh.sgid_index = 1;
    qp_attr.ah_attr.port_num = 1;

    attr_mask = IBV_QP_STATE | IBV_QP_MIN_RNR_TIMER | IBV_QP_AV | IBV_QP_PATH_MTU;

    ret = ibv_modify_qp(dcqp->qp_, &qp_attr, attr_mask);
    if (ret) {
        printf("%d\n", ret);
        perror("change state failed\n");
        abort();
    }

    struct ibv_port_attr port_attr;

	memset(&port_attr, 0, sizeof(port_attr));

    // 查询dct_num与gid
	ibv_query_port(device->context, 1,
		&port_attr);

    dcqp->lid_ = port_attr.lid;
    dcqp->port_num_ = 1;

    ibv_gid gid;
    ibv_query_gid(device->context, 1, 1, &gid);

    dcqp->dct_num_ = dcqp->qp_->qp_num;

    // printf("%u, %u, %u\n", dcqp->lid_, dcqp->port_num_, dcqp->dct_num_);
    // printf("%lu, %lu, %lu, %lu\n", *(uint64_t*)gid.raw, *((uint64_t*)(gid.raw)+1), gid.global.interface_id, gid.global.subnet_prefix);
    dcqp->gid1 = *(uint64_t*)gid.raw;
    dcqp->gid2 = *((uint64_t*)(gid.raw)+1);
    dcqp->interface = gid.global.interface_id;
    dcqp->subnet = gid.global.subnet_prefix;
 
    return dcqp;

}

ibv_ah* zDCQP_create_ah(zDCQP_requestor* requestor, uint64_t input_gid1, uint64_t input_gid2, uint64_t input_interface, uint64_t input_subnet, uint32_t input_lid){
        
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
        
    ibv_ah* ah = ibv_create_ah(requestor->pd_, &ah_attr);
    return ah;
}

int zDCQP_send(zDCQP_requestor* requestor, ibv_ah* ah, ibv_send_wr* send_wr, uint32_t lid, uint32_t dct_num) {
    ibv_wr_start(requestor->qp_ex_);
    ibv_send_wr* p = send_wr;
    int i = 0;
    while(p != NULL) {
        requestor->qp_ex_->wr_id = i++;
        if(p->next != NULL) {
            requestor->qp_ex_->wr_flags = 0;
        } else {
            requestor->qp_ex_->wr_flags = IBV_SEND_SIGNALED;
        }
        if(p->opcode == IBV_WR_RDMA_WRITE) {
            ibv_wr_rdma_write(requestor->qp_ex_, p->wr.rdma.rkey, p->wr.rdma.remote_addr);
        } else if (p->opcode == IBV_WR_RDMA_READ) {
            ibv_wr_rdma_read(requestor->qp_ex_, p->wr.rdma.rkey, p->wr.rdma.remote_addr);
        } else if (p->opcode == IBV_WR_ATOMIC_CMP_AND_SWP) {
            ibv_wr_atomic_cmp_swp(requestor->qp_ex_, p->wr.atomic.rkey, p->wr.atomic.remote_addr, p->wr.atomic.compare_add, p->wr.atomic.swap);
        } else if (p->opcode == IBV_WR_SEND) {
            ibv_wr_send(requestor->qp_ex_);
        } else {
            std::cerr << "Error, unsupported opcode in zDCQP_send" << std::endl;
            return -1;
        }
        ibv_wr_set_sge_list(requestor->qp_ex_, p->num_sge, p->sg_list);
        mlx5dv_wr_set_dc_addr(requestor->qp_mlx_ex_, ah, dct_num, 114514);
        p->next = NULL;
        p = p->next;
    }
    ibv_wr_complete(requestor->qp_ex_);
    auto start = TIME_NOW;
    struct ibv_wc wc;
    while(true) {
        if(TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
            std::cerr << "Error, dcqp read timeout" << std::endl;
            break;
        }
        if(ibv_poll_cq(requestor->cq_, 1, &wc) > 0) {
            if(wc.status != IBV_WC_SUCCESS) {
                std::cerr << "Error, dcqp read failed: " << wc.status << std::endl;
                break;
            }
            break;        
        }
    }
    return 0;
}

int zDCQP_read(zDCQP_requestor* requestor, ibv_ah* ah, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, uint32_t lid, uint32_t dct_num){
    ibv_wr_start(requestor->qp_ex_);
    requestor->qp_ex_->wr_id = 0;
    requestor->qp_ex_->wr_flags = IBV_SEND_SIGNALED;
    ibv_wr_rdma_read(requestor->qp_ex_, rkey, (uint64_t)remote_addr);
    ibv_wr_set_sge(requestor->qp_ex_, lkey, (uint64_t)local_addr, length);
    mlx5dv_wr_set_dc_addr(requestor->qp_mlx_ex_, ah, dct_num, 114514);
    ibv_wr_complete(requestor->qp_ex_);
    auto start = TIME_NOW;
    struct ibv_wc wc;
    while(true) {
        if(TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
            std::cerr << "Error, dcqp read timeout" << std::endl;
            break;
        }
        if(ibv_poll_cq(requestor->cq_, 1, &wc) > 0) {
            if(wc.status != IBV_WC_SUCCESS) {
                std::cerr << "Error, dcqp read failed: " << wc.status << std::endl;
                break;
            }
            break;        
        }
    }
    return 0;
}

int zDCQP_write(zDCQP_requestor* requestor, ibv_ah* ah, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, uint32_t lid, uint32_t dct_num){
    ibv_wr_start(requestor->qp_ex_);
    requestor->qp_ex_->wr_id = 0;
    requestor->qp_ex_->wr_flags = IBV_SEND_SIGNALED;
    ibv_wr_rdma_write(requestor->qp_ex_, rkey, (uint64_t)remote_addr);
    ibv_wr_set_sge(requestor->qp_ex_, lkey, (uint64_t)local_addr, length);
    mlx5dv_wr_set_dc_addr(requestor->qp_mlx_ex_, ah, dct_num, 114514);
    ibv_wr_complete(requestor->qp_ex_);
    auto start = TIME_NOW;
    struct ibv_wc wc;
    while(true) {
        if(TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
            std::cerr << "Error, dcqp write timeout" << std::endl;
            break;
        }
        if(ibv_poll_cq(requestor->cq_, 1, &wc) > 0) {
            if(wc.status != IBV_WC_SUCCESS) {
                std::cerr << "Error, dcqp write failed: " << wc.status << std::endl;
                break;
            }
            break;        
        }
    }
    return 0;
}

int zDCQP_CAS(zDCQP_requestor* requestor, ibv_ah* ah, void* local_addr, uint32_t lkey, uint64_t new_val, void* remote_addr, uint32_t rkey, uint32_t lid, uint32_t dct_num){
    uint64_t expected = *(uint64_t*)local_addr;
    ibv_wr_start(requestor->qp_ex_);
    requestor->qp_ex_->wr_id = 0;
    requestor->qp_ex_->wr_flags = IBV_SEND_SIGNALED;
    ibv_wr_atomic_cmp_swp(requestor->qp_ex_, rkey, (uint64_t)remote_addr, expected, new_val);
    ibv_wr_set_sge(requestor->qp_ex_, lkey, (uint64_t)local_addr, sizeof(uint64_t));
    mlx5dv_wr_set_dc_addr(requestor->qp_mlx_ex_, ah, dct_num, 114514);
    ibv_wr_complete(requestor->qp_ex_);
    auto start = TIME_NOW;
    struct ibv_wc wc;
    while(true) {
        if(TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
            std::cerr << "Error, dcqp cas timeout" << std::endl;
            break;
        }
        if(ibv_poll_cq(requestor->cq_, 1, &wc) > 0) {
            if(wc.status != IBV_WC_SUCCESS) {
                std::cerr << "Error, dcqp cas failed: " << wc.status << std::endl;
                break;
            }
            break;        
        }
    }
    return 0;
}


}
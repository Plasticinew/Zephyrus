#include <arpa/inet.h>
#include "zQP.h"

namespace Zephyrus {

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

zEndpoint* zEP_create(string config_file)
{
    zEndpoint *endpoint = new zEndpoint();
    struct zDeviceConfig m_config;
    load_config(config_file.c_str(), &m_config);
    endpoint->m_device_num = m_config.num_devices;
    endpoint->m_node_id = m_config.node_id;

    for (int i = 0; i < endpoint->m_device_num; i++)
    {
        zDevice *device = new zDevice();
        device->mlx_name = m_config.mlx_names[i];
        device->eth_name = m_config.eth_names[i];
        device->eth_ip = m_config.ips[i];
        device->port = m_config.ports[i];
        endpoint->m_devices.push_back(device);
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

    return endpoint;
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

zQP* zQP_create(zPD* pd, zEndpoint *ep, rkeyTable* table, zQPType qp_type)
{
    zQP *zqp = new zQP();
    zqp->m_ep = ep;
    zqp->m_pd = pd;
    zqp->m_rkey_table = table;
    zqp->qp_type = qp_type;

    ibv_mr* msg_mr = mr_malloc_create(pd, (uint64_t&)zqp->cmd_msg_, sizeof(CmdMsgBlock));
    memset(zqp->cmd_msg_, 0, sizeof(CmdMsgBlock));
    for (int i = 0; i < pd->m_pds.size(); i ++) {
        if (!pd->m_mrs[msg_mr][i]) {
            perror("ibv_reg_mr m_msg_mr_ fail");
            return NULL;
        }
        zqp->msg_mr_[i] = pd->m_mrs[msg_mr][i];
    }
    
    ibv_mr* resp_mr = mr_malloc_create(pd, (uint64_t&)zqp->cmd_msg_, sizeof(CmdMsgRespBlock));
    memset(zqp->cmd_msg_, 0, sizeof(CmdMsgRespBlock));
    for (int i = 0; i < pd->m_pds.size(); i ++) {
        if (!pd->m_mrs[resp_mr][i]) {
            perror("ibv_reg_mr m_msg_mr_ fail");
            return NULL;
        }
        zqp->resp_mr_[i] = pd->m_mrs[resp_mr][i];
    }

    return zqp; 
}

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
    init_attr.send_ops_flags |= IBV_QP_EX_WITH_SEND | IBV_QP_EX_WITH_RDMA_READ | IBV_QP_EX_WITH_RDMA_WRITE;
 
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
    qp_attr_to_rts.timeout       = 14;
    qp_attr_to_rts.retry_cnt     = 10;
    qp_attr_to_rts.rnr_retry     = 10;
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

    printf("%d, %d, %d, %d\n", attr.qp_state ,dcqp->lid_, dcqp->port_num_, dcqp->dct_num_);

    // 提取ex qp与mlx qp
    dcqp->qp_ex_ = ibv_qp_to_qp_ex(dcqp->qp_);

    dcqp->qp_mlx_ex_ = mlx5dv_qp_ex_from_ibv_qp_ex(dcqp->qp_ex_);
    
    return 0;
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
                                IBV_ACCESS_REMOTE_READ; 

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
    qp_attr.min_rnr_timer = 10;
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

    printf("%u, %u, %u\n", dcqp->lid_, dcqp->port_num_, dcqp->dct_num_);
    printf("%lu, %lu, %lu, %lu\n", *(uint64_t*)gid.raw, *((uint64_t*)(gid.raw)+1), gid.global.interface_id, gid.global.subnet_prefix);
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
            std::cerr << "Error, read timeout" << std::endl;
            break;
        }
        if(ibv_poll_cq(requestor->cq_, 1, &wc) > 0) {
            if(wc.status != IBV_WC_SUCCESS) {
                std::cerr << "Error, read failed: " << wc.status << std::endl;
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
            std::cerr << "Error, read timeout" << std::endl;
            break;
        }
        if(ibv_poll_cq(requestor->cq_, 1, &wc) > 0) {
            if(wc.status != IBV_WC_SUCCESS) {
                std::cerr << "Error, read failed: " << wc.status << std::endl;
                break;
            }
            break;        
        }
    }
    return 0;
}

int zQP_connect(zQP *qp, int nic_index, string ip, string port, int node_id) {
    if (qp->m_requestors.find(nic_index) != qp->m_requestors.end() && qp->m_requestors[nic_index]->status_ == ZSTATUS_CONNECTED){
        return -1;
    }
    zQPType qp_type = qp->qp_type;
    zQP_requestor *qp_instance = new zQP_requestor();
    qp_instance->status_ = ZSTATUS_INIT;
    qp_instance->channel_ = rdma_create_event_channel();
    qp_instance->pd_ = qp->m_pd->m_pds[nic_index];
    int result = rdma_create_id(qp_instance->channel_, &(qp_instance->cm_id_), NULL, RDMA_PS_TCP);
    assert(result == 0);

    rdma_addrinfo *t = NULL, *s = NULL;
    rdma_addrinfo *res, *src;
    int counter = 0;
    while( t == NULL && counter < 100) {
        counter += 1;
        struct sockaddr_in src_addr;   // 设置源地址（指定网卡设备）
        memset(&src_addr, 0, sizeof(src_addr));
        src_addr.sin_family = AF_INET;
        inet_pton(AF_INET, qp->m_ep->m_devices[nic_index]->eth_ip.c_str(), &src_addr.sin_addr); // 本地网卡IP地址
        
        result = rdma_getaddrinfo(ip.c_str(), port.c_str(), NULL, &res);
        assert(result == 0);
        
        for(t = res; t; t = t->ai_next) {
            if(!rdma_resolve_addr(qp_instance->cm_id_, (struct sockaddr *)&src_addr, t->ai_dst_addr, RESOLVE_TIMEOUT_MS)) {
                break;
            }
        }
    }
    assert(t != NULL);

    rdma_cm_event* event;
    result = rdma_get_cm_event(qp_instance->channel_, &event);
    assert(result == 0);
    assert(event->event == RDMA_CM_EVENT_ADDR_RESOLVED);
    rdma_ack_cm_event(event);
    // Addr resolve finished, make route resolve

    result = rdma_resolve_route(qp_instance->cm_id_, RESOLVE_TIMEOUT_MS);
    assert(result == 0);
    result = rdma_get_cm_event(qp_instance->channel_, &event);
    assert(result == 0);
    assert(event->event == RDMA_CM_EVENT_ROUTE_RESOLVED);
    rdma_ack_cm_event(event);
    // Addr route resolve finished

    ibv_comp_channel* comp_channel = ibv_create_comp_channel(qp->m_ep->m_devices[nic_index]->context);
    assert(comp_channel != NULL);
    ibv_cq* cq = ibv_create_cq(qp->m_ep->m_devices[nic_index]->context, 1024, NULL, comp_channel, 0);
    assert(cq != NULL);
    result = ibv_req_notify_cq(cq, 0);
    assert(result == 0);

    ibv_qp_init_attr qp_init_attr;
    memset(&qp_init_attr, 0, sizeof(qp_init_attr));
    qp_init_attr.send_cq = cq;
    qp_init_attr.recv_cq = cq;
    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.cap.max_send_wr = 512;
    qp_init_attr.cap.max_recv_wr = 1;
    qp_init_attr.cap.max_send_sge = 16;
    qp_init_attr.cap.max_recv_sge = 16;
    qp_init_attr.cap.max_inline_data = 256;
    qp_init_attr.sq_sig_all = 0;
    result = rdma_create_qp(qp_instance->cm_id_, qp->m_pd->m_pds[nic_index], &qp_init_attr);
    assert(result == 0);

    CNodeInit init_msg = {node_id, qp_type};
    rdma_conn_param conn_param;
    memset(&conn_param, 0, sizeof(conn_param));
    conn_param.responder_resources = 16;
    conn_param.initiator_depth = 16;
    conn_param.private_data = &init_msg;
    conn_param.private_data_len = sizeof(CNodeInit);
    conn_param.retry_count = RETRY_TIMEOUT;
    conn_param.rnr_retry_count = RETRY_TIMEOUT;
    while(event->event != RDMA_CM_EVENT_ESTABLISHED){
        result = rdma_connect(qp_instance->cm_id_, &conn_param);
        rdma_get_cm_event(qp_instance->channel_, &event);
    }
    // assert(result == 0);
    // assert(event->event == RDMA_CM_EVENT_ESTABLISHED);
    
    struct PData server_pdata;
    memset(&server_pdata, 0, sizeof(server_pdata));
    memcpy(&server_pdata, event->param.conn.private_data, sizeof(server_pdata));

    qp_instance->server_cmd_msg_ = server_pdata.buf_addr;
    qp_instance->server_cmd_rkey_ = server_pdata.buf_rkey;
    qp_instance->conn_id_ = server_pdata.id;
    for(int i = 0; i < server_pdata.nic_num_; i++){
        zTarget* target;
        if(qp->m_targets.find(i) != qp->m_targets.end()){
            target = qp->m_targets[i];
        }
        else{
            target = new zTarget();
            qp->m_targets[i] = target;
        }
        target->ah = zDCQP_create_ah(qp->m_pd->m_requestors[i][0], server_pdata.gid1[i], server_pdata.gid2[i], server_pdata.interface[i], server_pdata.subnet[i], server_pdata.lid_[i]);
        target->lid_ = server_pdata.lid_[i];
        target->dct_num_ = server_pdata.dct_num_[i];
        target->ip = string(server_pdata.ip[i]);
        target->port = server_pdata.port[i];
    }
    
    assert(server_pdata.size == sizeof(CmdMsgBlock));
    qp_instance->cmd_msg_ = qp->cmd_msg_;
    qp_instance->msg_mr_ = qp->msg_mr_[nic_index];

    qp_instance->cmd_resp_ = qp->cmd_resp_;
    qp_instance->resp_mr_ = qp->resp_mr_[nic_index];

    // qp_instance->cmd_msg_ = new CmdMsgBlock();
    // memset(qp_instance->cmd_msg_, 0, sizeof(CmdMsgBlock));
    // qp_instance->msg_mr_ = mr_create(qp->m_pd->m_pds[nic_index], (void *)qp_instance->cmd_msg_, sizeof(CmdMsgBlock));
    // if (!qp_instance->msg_mr_) {
    //     perror("ibv_reg_mr m_msg_mr_ fail");
    //     return -1;
    // }

    // qp_instance->cmd_resp_ = new CmdMsgRespBlock();
    // memset(qp_instance->cmd_resp_, 0, sizeof(CmdMsgRespBlock));
    // qp_instance->resp_mr_ =
    //      mr_create(qp->m_pd->m_pds[nic_index], (void *)qp_instance->cmd_resp_, sizeof(CmdMsgRespBlock));
    // if (!qp_instance->resp_mr_) {
    //     perror("ibv_reg_mr m_resp_mr_ fail");
    //     return -1;
    // }

    rdma_ack_cm_event(event);

    rdma_freeaddrinfo(res);

    // Connect finished
    qp_instance->cq_ = cq;
    qp_instance->qp_ = qp_instance->cm_id_->qp;

    qp->m_requestors[nic_index] = qp_instance;
    qp_instance->status_ = ZSTATUS_CONNECTED;

    return 0;
}

int zQP_read(zQP *zqp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, uint32_t time_stamp) {
    zQP_requestor *requestor = zqp->m_requestors[zqp->current_device];
    struct ibv_sge sge;
    sge.addr = (uint64_t)local_addr;
    sge.length = length;
    sge.lkey = lkey;
    struct ibv_send_wr *send_wr = new ibv_send_wr();
    struct ibv_send_wr *bad_send_wr;
    send_wr->wr_id = time_stamp;
    send_wr->sg_list = &sge;
    send_wr->num_sge = 1;
    send_wr->next = NULL;
    send_wr->opcode = IBV_WR_RDMA_READ;
    send_wr->send_flags = IBV_SEND_SIGNALED;
    send_wr->wr.rdma.remote_addr = (uint64_t)remote_addr;
    send_wr->wr.rdma.rkey = rkey;

    ibv_qp* qp = requestor->qp_;
    if (ibv_post_send(qp, send_wr, &bad_send_wr)) {
        std::cerr << "Error, ibv_post_send failed" << std::endl;
        return -1;
    }

    zqp->wr_entry_[zqp->entry_end_].time_stamp = time_stamp;
    zqp->wr_entry_[zqp->entry_end_].wr_addr = (uint64_t)send_wr;
    zqp->entry_end_ = (zqp->entry_end_ + 1)%WR_ENTRY_NUM;

    auto start = TIME_NOW;
    struct ibv_wc wc;
    ibv_cq* cq = requestor->cq_;
    while(true) {
        if(TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
            std::cerr << "Error, read timeout" << std::endl;
            break;
        }
        if(ibv_poll_cq(cq, 1, &wc) > 0) {
            if(wc.status != IBV_WC_SUCCESS) {
                std::cerr << "Error, read failed: " << wc.status << std::endl;
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



int zQP_write(zQP *zqp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey, uint32_t time_stamp, bool use_log) {
    zQP_requestor *requestor = zqp->m_requestors[zqp->current_device];
    struct ibv_sge sge;
    struct ibv_send_wr *send_wr = new ibv_send_wr();
    struct ibv_send_wr *bad_send_wr;
    struct ibv_sge log_sge;
    struct ibv_send_wr log_wr = {};
    zWR_entry* entry = new zWR_entry();
    entry->time_stamp = time_stamp;
    entry->wr_addr = (uint64_t)send_wr;
    entry->finished = 1;
    if(use_log){
        log_sge.addr = (uint64_t)(entry);
        log_sge.length = sizeof(uint64_t);
        log_sge.lkey = 0;
        log_wr.wr_id = time_stamp;
        log_wr.sg_list = &log_sge;
        log_wr.num_sge = 1;
        log_wr.next = NULL;
        log_wr.opcode = IBV_WR_RDMA_WRITE;
        log_wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
        log_wr.wr.rdma.remote_addr = requestor->server_cmd_msg_ + zqp->entry_end_ * sizeof(uint32_t);
        log_wr.wr.rdma.rkey = requestor->server_cmd_rkey_;
    }        

    sge.addr = (uint64_t)local_addr;
    sge.length = length;
    sge.lkey = lkey;

    send_wr->wr_id = time_stamp;
    send_wr->sg_list = &sge;
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
        perror("Error, ibv_post_send failed");
    }

    zqp->wr_entry_[zqp->entry_end_] = *entry;
    zqp->entry_end_ = (zqp->entry_end_ + 1)%WR_ENTRY_NUM;

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

int zQP_send_wr(zQP* zqp, ibv_send_wr* send_wr){
    struct ibv_send_wr *bad_send_wr;
    zQP_requestor *requestor = zqp->m_requestors[zqp->current_device];
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

void zQP_RPC_Alloc(zQP* qp, uint64_t* addr, uint32_t* rkey, size_t size){
    zQP_requestor* requestor = qp->m_requestors[qp->current_device];
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
  
    printf("cmd_resp: %lx, rkey: %u\n", requestor->cmd_resp_, requestor->resp_mr_->rkey);
    printf("cmd_resp: %lx, rkey: %u\n", request->resp_addr, request->resp_rkey);

    /* send a request to sever */
    qp->time_stamp = (qp->time_stamp+1) % MAX_REQUESTOR_NUM;
    zQP_write(qp, (void *)requestor->cmd_msg_, requestor->msg_mr_->lkey, sizeof(CmdMsgBlock), (void*)requestor->server_cmd_msg_, requestor->server_cmd_rkey_, qp->time_stamp, false);

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
    for(int i = 0; i < 8; i++){
        if(qp->m_rkey_table->find(resp_msg->rkey[0]) == qp->m_rkey_table->end())
            (*qp->m_rkey_table)[resp_msg->rkey[0]] = std::vector<uint32_t>();
        qp->m_rkey_table->at(resp_msg->rkey[0]).push_back(resp_msg->rkey[i]);
    }
    *rkey = resp_msg->rkey[0];
    return;
}

int z_write(zQP *qp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey) {
    if(qp->m_ep->m_devices[qp->current_device]->status == ZSTATUS_ERROR) {
        qp->current_device = (qp->current_device + 1) % qp->m_ep->m_devices.size();
        qp->time_stamp = 0;
        std::cerr << "Warning, switch to device " << qp->current_device << std::endl;
        int result = z_recovery(qp);
        if (result != 0) {
            std::cerr << "Error, recovery failed" << std::endl;
            return -1;
        }
        return zDCQP_write(qp->m_ep->m_requestors[qp->current_device][0], qp->m_targets[qp->current_device]->ah, local_addr, lkey, length, remote_addr, rkey, qp->m_targets[qp->current_device]->lid_, qp->m_targets[qp->current_device]->dct_num_);
    }
    if(qp->m_requestors[qp->current_device]->status_ == ZSTATUS_CONNECTED){
        qp->time_stamp = (qp->time_stamp+1) % MAX_REQUESTOR_NUM;
        int result = zQP_write(qp, local_addr, lkey, length, remote_addr, rkey, qp->time_stamp, true);
        if (result != 0){
            qp->m_requestors[qp->current_device]->status_ = ZSTATUS_ERROR;
            qp->m_ep->m_devices[qp->current_device]->status = ZSTATUS_ERROR;
            std::cerr << "Error, connection lost, start recovery" << std::endl;
            result = z_recovery(qp);
            if (result != 0) {
                std::cerr << "Error, recovery failed" << std::endl;
            }
            return 0;
        }
        return 0;
    } else{
        return zDCQP_write(qp->m_ep->m_requestors[qp->current_device][0], qp->m_targets[qp->current_device]->ah, local_addr, lkey, length, remote_addr, rkey, qp->m_targets[qp->current_device]->lid_, qp->m_targets[qp->current_device]->dct_num_);
    }
}

int z_read(zQP *qp, void* local_addr, uint32_t lkey, uint64_t length, void* remote_addr, uint32_t rkey){
    if(qp->m_ep->m_devices[qp->current_device]->status == ZSTATUS_ERROR) {
        qp->current_device = (qp->current_device + 1) % qp->m_ep->m_devices.size();
        qp->time_stamp = 0;
        std::cerr << "Warning, switch to device " << qp->current_device << std::endl;
        int result = z_recovery(qp);
        if (result != 0) {
            std::cerr << "Error, recovery failed" << std::endl;
            return -1;
        }
        return zDCQP_read(qp->m_ep->m_requestors[qp->current_device][0], qp->m_targets[qp->current_device]->ah, local_addr, lkey, length, remote_addr, rkey, qp->m_targets[qp->current_device]->lid_, qp->m_targets[qp->current_device]->dct_num_);
    }
    if(qp->m_requestors[qp->current_device]->status_ == ZSTATUS_CONNECTED){
        qp->time_stamp = (qp->time_stamp+1) % MAX_REQUESTOR_NUM;
        int result = zQP_read(qp, local_addr, lkey, length, remote_addr, rkey, qp->time_stamp);
        if (result != 0){
            qp->m_requestors[qp->current_device]->status_ = ZSTATUS_ERROR;
            qp->m_ep->m_devices[qp->current_device]->status = ZSTATUS_ERROR;
            std::cerr << "Error, connection lost, start recovery" << std::endl;
            result = z_recovery(qp);
            if (result != 0) {
                std::cerr << "Error, recovery failed" << std::endl;
            }
            return 0;
        }
        return 0;
    } else{
        return zDCQP_read(qp->m_ep->m_requestors[qp->current_device][0], qp->m_targets[qp->current_device]->ah, local_addr, lkey, length, remote_addr, rkey, qp->m_targets[qp->current_device]->lid_, qp->m_targets[qp->current_device]->dct_num_);
    }
}

int z_recovery(zQP *qp) {
    if(qp->qp_type == ZQP_RPC){
        return 0;
    }
    new std::thread(&zQP_connect, qp, qp->current_device, qp->m_ep->m_devices[qp->current_device]->eth_ip, qp->m_ep->m_devices[qp->current_device]->port, qp->m_ep->m_node_id);
    // read recovery log
    z_read(qp, (void*)qp->cmd_resp_, qp->resp_mr_[qp->current_device]->lkey, sizeof(CmdMsgRespBlock), (void*)qp->m_requestors[qp->current_device]->server_cmd_msg_, qp->m_requestors[qp->current_device]->server_cmd_rkey_);
    zWR_entry *entry = (zWR_entry *)qp->cmd_resp_;
    int start = qp->entry_start_;
    int end = qp->entry_end_;
    if(start > end)
        end += WR_ENTRY_NUM;
    if(start != end){
        for(int i = start; i < end; i++){
            if(qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr == 0){
                continue;
            } 
            int local_time = qp->wr_entry_[i%WR_ENTRY_NUM].time_stamp;
            int remote_time = entry[i%WR_ENTRY_NUM].time_stamp;
            if((local_time > remote_time && local_time - remote_time < 16384) || (local_time < remote_time && remote_time - local_time > 16384)){
                // attention: 48bit address to 64bit address
                struct ibv_send_wr *send_wr = (struct ibv_send_wr *)qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr;
                while(send_wr != NULL){
                    if(send_wr->opcode == IBV_WR_RDMA_WRITE){
                        if (send_wr->send_flags == (IBV_SEND_SIGNALED | IBV_SEND_INLINE)){
                            // log write
                            send_wr = send_wr->next;
                            continue;
                        }
                        z_write(qp, (void *)send_wr->sg_list->addr, send_wr->sg_list->lkey, send_wr->sg_list->length, (void *)send_wr->wr.rdma.remote_addr, send_wr->wr.rdma.rkey);
                    } else if(send_wr->opcode == IBV_WR_RDMA_READ){
                        z_read(qp, (void *)send_wr->sg_list->addr, send_wr->sg_list->lkey, send_wr->sg_list->length, (void *)send_wr->wr.rdma.remote_addr, send_wr->wr.rdma.rkey);
                    } else {
                        printf("Error, unsupported opcode %d\n", send_wr->opcode);
                    }
                    send_wr = send_wr->next;
                }
            }
        }
    }
}

ibv_mr* mr_create(ibv_pd *pd, void *addr, size_t length) {
    ibv_mr* mr = ibv_reg_mr(pd, addr, length, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE  | IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_MW_BIND);
    return mr;
}

ibv_mr* mr_malloc_create(zPD* pd, uint64_t &addr, size_t length) {
    addr = (uint64_t)mmap(NULL, length, PROT_READ | PROT_WRITE, MAP_PRIVATE |MAP_ANONYMOUS, -1, 0);
    ibv_mr* primary_mr = mr_create(pd->m_pds[0], (void*)addr, length);
    pd->m_mrs[primary_mr].push_back(primary_mr);
    for (int i = 1; i < pd->m_pds.size(); i++) {
        ibv_mr* mr = mr_create(pd->m_pds[i], (void*)addr, length);
        pd->m_mrs[primary_mr].push_back(mr);
    }
    return primary_mr;
}

int zQP_listen(zQP *qp, int nic_index, string ip, string port) {
    if (qp->m_responders.find(nic_index) != qp->m_responders.end()){
        return -1;
    }

    zQP_responder *qp_instance = new zQP_responder();
    qp_instance->worker_info_ = new WorkerInfo *[MAX_SERVER_WORKER*MAX_SERVER_CLIENT];
    qp_instance->worker_threads_ = new std::thread *[MAX_SERVER_WORKER];
    qp_instance->pd_ = qp->m_pd->m_pds[nic_index];
    for (uint32_t i = 0; i < MAX_SERVER_WORKER; i++) {
      qp_instance->worker_info_[i] = nullptr;
      qp_instance->worker_threads_[i] = nullptr;
    }
    qp_instance->channel_ = rdma_create_event_channel();
    int result = rdma_create_id(qp_instance->channel_, &(qp_instance->cm_id_), NULL, RDMA_PS_TCP);
    qp->m_responders[nic_index] = qp_instance;

    assert(result == 0);
    sockaddr_in sin;
    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_port = htons(atoi(port.c_str()));
    sin.sin_addr.s_addr = inet_addr(ip.c_str());
    result = rdma_bind_addr(qp_instance->cm_id_, (sockaddr*)&sin);
    assert(result == 0);
    result = rdma_listen(qp_instance->cm_id_, 1024);
    assert(result == 0);
    z_debug("device %s listen on %s:%s\n", qp->m_ep->m_devices[nic_index]->mlx_name.c_str(), ip.c_str(), port.c_str());
    while(true) {
        rdma_cm_event* event;
        int result = rdma_get_cm_event(qp_instance->channel_, &event);
        assert(result == 0);
        if(event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
            rdma_cm_id* new_cm_id = event->id;
            CNodeInit msg = *(CNodeInit*)event->param.conn.private_data;
            rdma_ack_cm_event(event);
            zQP_accept(qp, qp_instance, nic_index, new_cm_id, msg.qp_type, msg.node_id);
        } else {
            rdma_ack_cm_event(event);
        }
    }
    return 0;
}

void zQP_worker(zPD *pd, zQP_responder *qp_instance, WorkerInfo *work_info, uint32_t num) {
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
            if ((mr = mr_malloc_create(pd, resp_msg->addr, 
                                            reg_req->size)) == NULL) {
                resp_msg->status = RES_FAIL;
            } else {
                for(int i = 0; i < pd->m_mrs[mr].size(); i++){
                    resp_msg->rkey[i] = pd->m_mrs[mr][i]->rkey;
                }
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


int zQP_accept(zQP *qp, zQP_responder *qp_instance, int nic_index, rdma_cm_id *cm_id, zQPType qp_type, int node_id) {
    
    zQP_responder_connection *conn = new zQP_responder_connection();
    conn->cm_id_ = cm_id;

    ibv_comp_channel* comp_channel = ibv_create_comp_channel(qp->m_ep->m_devices[nic_index]->context);
    assert(comp_channel != NULL);
    ibv_cq* cq = ibv_create_cq(qp->m_ep->m_devices[nic_index]->context, 1, NULL, comp_channel, 0);
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
    result = rdma_create_qp(cm_id, qp->m_pd->m_pds[nic_index], &qp_init_attr);
    assert(result == 0);

    struct PData rep_pdata;
    memset(&rep_pdata, 0, sizeof(rep_pdata));
    CmdMsgBlock *cmd_msg = nullptr;
    CmdMsgRespBlock *cmd_resp = nullptr;
    struct ibv_mr *msg_mr = nullptr;
    struct ibv_mr *resp_mr = nullptr;
    cmd_msg = new CmdMsgBlock();
    memset(cmd_msg, 0, sizeof(CmdMsgBlock));
    msg_mr = mr_create(qp->m_pd->m_pds[nic_index], (void *)cmd_msg, sizeof(CmdMsgBlock));

    cmd_resp = new CmdMsgRespBlock();
    memset(cmd_resp, 0, sizeof(CmdMsgRespBlock));
    resp_mr = mr_create(qp->m_pd->m_pds[nic_index], (void *)cmd_resp, sizeof(CmdMsgRespBlock));

    if(conn->qp_log_list_[node_id] == NULL){
        conn->qp_log_[node_id] = new CmdMsgBlock();    
        memset(conn->qp_log_[node_id], 0, sizeof(CmdMsgBlock));
        conn->qp_log_list_[node_id] = mr_create(qp->m_pd->m_pds[nic_index], (void *)conn->qp_log_[node_id], sizeof(CmdMsgBlock));
    }

    rep_pdata.id = -1;
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
            new std::thread(&zQP_worker, qp->m_pd, qp_instance, qp_instance->worker_info_[num], num);
        } else {
            qp_instance->worker_info_[num] = new WorkerInfo();
            qp_instance->worker_info_[num]->cmd_msg = cmd_msg;
            qp_instance->worker_info_[num]->cmd_resp_msg = cmd_resp;
            qp_instance->worker_info_[num]->msg_mr = msg_mr;
            qp_instance->worker_info_[num]->resp_mr = resp_mr;
            qp_instance->worker_info_[num]->cm_id = cm_id;
            qp_instance->worker_info_[num]->cq = cq;
        } 
        rep_pdata.id = num;
        qp_instance->worker_num_ += 1;
    } 
    if(qp_type == ZQP_RPC){
        rep_pdata.buf_addr = (uintptr_t)cmd_msg;
        rep_pdata.buf_rkey = msg_mr->rkey;
    }
    else{
        rep_pdata.buf_addr = (uintptr_t)conn->qp_log_[node_id];
        rep_pdata.buf_rkey = conn->qp_log_list_[node_id]->rkey;
    }
    rep_pdata.size = sizeof(CmdMsgRespBlock);
    rep_pdata.nic_num_ = qp->m_pd->m_responders.size();
    for(int i = 0; i < rep_pdata.nic_num_; i++){
        rep_pdata.gid1[i] = qp->m_pd->m_responders[i][0]->gid1;
        rep_pdata.gid2[i] = qp->m_pd->m_responders[i][0]->gid2;
        rep_pdata.interface[i] = qp->m_pd->m_responders[i][0]->interface;
        rep_pdata.subnet[i] = qp->m_pd->m_responders[i][0]->subnet;
        rep_pdata.lid_[i] = qp->m_pd->m_responders[i][0]->lid_;
        rep_pdata.dct_num_[i] = qp->m_pd->m_responders[i][0]->dct_num_;
        memcpy(rep_pdata.ip[i], qp->m_ep->m_devices[nic_index]->eth_ip.c_str(), qp->m_ep->m_devices[nic_index]->eth_ip.size());
        memcpy(rep_pdata.port[i], qp->m_ep->m_devices[nic_index]->port.c_str(), qp->m_ep->m_devices[nic_index]->port.size());
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
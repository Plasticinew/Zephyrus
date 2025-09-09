#include "zQP.h"

using namespace Zephyrus;
int main(int argc, char** argv) {
    if(argc < 2) {
        printf("Usage: %s <local_config_file>\n", argv[0]);
        return -1;
    }
    string config_file = argv[1];
    zEndpoint *ep = zEP_create(config_file);
    zPD *pd = zPD_create(ep, 1);
    rkeyTable *table = new rkeyTable();
    zQP *qp = zQP_create(pd, ep, table, ZQP_RPC);
    std::thread listener = std::thread(&zQP_listen, qp, 0, std::ref(ep->m_devices[0]->eth_ip), std::ref(ep->m_devices[0]->port));
    listener.join();
    return 0;
}
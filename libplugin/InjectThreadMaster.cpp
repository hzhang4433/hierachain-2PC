#include "Common.h"
#include "InjectThreadMaster.h"

using namespace std;
using namespace dev::consensus;

//  load workload
void InjectThreadMaster::load_WorkLoad(int intra_shardTxNumber, int inter_shardTxNumber, int cross_layerTxNumber,std::shared_ptr<dev::rpc::Rpc> rpcService, std::shared_ptr<dev::ledger::LedgerManager> ledgerManager, int threadId){
    
    // string intra_workload_filename = "shard"+ to_string(internal_groupId) +"_intrashard_workload_10w_" + to_string(threadId) + ".json";
    // string inter_workload_filename = "shard"+ to_string(internal_groupId) +"_intershard_workload_10w_" + to_string(threadId) + ".json";
    // string cross_workload_filename = "shard"+ to_string(internal_groupId) +"_crosslayer_workload_10w_" + to_string(threadId) + ".json";

    string intra_workload_filename = "./intrashardworkload.json";
    string inter_workload_filename = "./intershardworkload.json";
    string cross_workload_filename = "./crosslayerworkload.json";

    transactionInjectionTest _inject(rpcService, internal_groupId, ledgerManager);
    _inject.injectionTransactions(intra_workload_filename, inter_workload_filename, cross_workload_filename, intra_shardTxNumber, inter_shardTxNumber, cross_layerTxNumber, threadId);
}

void InjectThreadMaster::injectTransactions(int threadId, int threadNum) {

    // 3 shards
    cout << "m_minitest_3shard.get_intra() = " << m_minitest_3shard->get_intra() << endl
         << "m_minitest_3shard.get_inter() = " << m_minitest_3shard->get_inter() << endl;

    if(internal_groupId <= 2) {
        intra_shardTxNumber = m_minitest_3shard->get_intra() / threadNum;
        inter_shardTxNumber = 0;
        cross_layerTxNumber = 0;
        std::this_thread::sleep_for(std::chrono::milliseconds(5000));
        // std::this_thread::sleep_for(std::chrono::seconds(15));
        load_WorkLoad(intra_shardTxNumber, inter_shardTxNumber, cross_layerTxNumber, m_rpc_service, m_ledgerManager, threadId);
    } else if(internal_groupId == 3) {
        intra_shardTxNumber = m_minitest_3shard->get_intra() / threadNum;
        inter_shardTxNumber = m_minitest_3shard->get_inter() / threadNum;
        cross_layerTxNumber = 0;
        load_WorkLoad(intra_shardTxNumber, inter_shardTxNumber, cross_layerTxNumber, m_rpc_service, m_ledgerManager, threadId);
    }

    // // 9 shards
    // cout << "m_minitest_9shard.get_6shard_intra() = " << m_minitest_9shard->get_6shard_intra() << endl
    //      << "m_minitest_9shard.get_2shard_cross() = " << m_minitest_9shard->get_2shard_cross() << endl
    //      << "m_minitest_9shard.get_1shard_cross() = " << m_minitest_9shard->get_1shard_cross() << endl
    //      << "m_minitest_9shard.get_1shard_cross2() = " << m_minitest_9shard->get_1shard_cross2() << endl;

    // if(internal_groupId <= 6){
    //     intra_shardTxNumber = m_minitest_9shard->get_6shard_intra() / threadNum;
    //     inter_shardTxNumber = 0;
    //     cross_layerTxNumber = 0;
    //     std::this_thread::sleep_for(std::chrono::seconds(3)); // 暂停1秒
    //     // std::this_thread::sleep_for(std::chrono::milliseconds(500));

    //     load_WorkLoad(intra_shardTxNumber, inter_shardTxNumber, cross_layerTxNumber, m_rpc_service, m_ledgerManager, threadId);
    // }
    // else if(internal_groupId <= 8){
    //     intra_shardTxNumber = m_minitest_9shard->get_6shard_intra() / threadNum;
    //     inter_shardTxNumber = m_minitest_9shard->get_2shard_cross() / threadNum;
    //     cross_layerTxNumber = 0;
    //     load_WorkLoad(intra_shardTxNumber, inter_shardTxNumber, cross_layerTxNumber, m_rpc_service, m_ledgerManager, threadId);
    // }
    // else if(internal_groupId == 9){
    //     intra_shardTxNumber = m_minitest_9shard->get_6shard_intra() / threadNum;
    //     inter_shardTxNumber = m_minitest_9shard->get_1shard_cross() / threadNum;
    //     cross_layerTxNumber = m_minitest_9shard->get_1shard_cross2() / threadNum;
    //     load_WorkLoad(intra_shardTxNumber, inter_shardTxNumber, cross_layerTxNumber, m_rpc_service, m_ledgerManager, threadId);
    // }

    // // 13 shards
    // cout << "<m_minitest_13shard->get_down_intra() / threadNum = " <<m_minitest_13shard->get_down_intra() / threadNum << endl
    //      << "m_minitest_9shard.get_2shard_cross() = " <<m_minitest_13shard->get_mid_cross() / threadNum << endl
    //      << "m_minitest_9shard.get_1shard_cross() = " <<  m_minitest_13shard->get_top_cross()/threadNum << endl
    //      << "m_minitest_9shard.get_1shard_cross2() = " <<  m_minitest_13shard->get_top_cross2() / threadNum<< endl;


    // if(internal_groupId == 1 || internal_groupId == 2 || internal_groupId == 3 
    //     || internal_groupId == 5 || internal_groupId == 6 || internal_groupId == 7
    //     || internal_groupId == 9 || internal_groupId == 10 || internal_groupId == 11){
    //     intra_shardTxNumber = m_minitest_13shard->get_down_intra() / threadNum;
    //     inter_shardTxNumber = 0;
    //     cross_layerTxNumber = 0;
    //     std::this_thread::sleep_for(std::chrono::seconds(6)); // 暂停1秒
    //     load_WorkLoad(intra_shardTxNumber, inter_shardTxNumber, cross_layerTxNumber, m_rpc_service, m_ledgerManager, threadId);
    // }
    // else if(internal_groupId == 4 || internal_groupId == 8 || internal_groupId == 12){
    //     intra_shardTxNumber = 0;
    //     inter_shardTxNumber = m_minitest_13shard->get_mid_cross() / threadNum;
    //     cross_layerTxNumber = 0;
    //     load_WorkLoad(intra_shardTxNumber, inter_shardTxNumber, cross_layerTxNumber, m_rpc_service, m_ledgerManager, threadId);
    // }
    // else if(internal_groupId == 13){
    //     intra_shardTxNumber = 0;
    //     inter_shardTxNumber = m_minitest_13shard->get_top_cross()/threadNum;
    //     cross_layerTxNumber = m_minitest_13shard->get_top_cross2() / threadNum;
    //     load_WorkLoad(intra_shardTxNumber, inter_shardTxNumber, cross_layerTxNumber, m_rpc_service, m_ledgerManager, threadId);
    // }






















}

void InjectThreadMaster::startInjectThreads(int threadNum) {
    int i = 1;
    for (; i <= threadNum; i++) {
        std::thread{[this, i, threadNum]()  {
            injectTransactions(i, threadNum);
        }}.detach();
    }
}
#include "Common.h"
#include "libdevcore/CommonIO.h"
#include "libdevcore/Log.h"
#include "libprotobasic/shard.pb.h"
#include "libsync/SyncMsgPacket.h"
#include <libplugin/deterministExecute.h>
#include <libethcore/Transaction.h>
#include <string>
#include <thread>
#include <utility>
#include <libethcore/ABI.h>

using namespace dev::plugin;
using namespace dev::consensus;
using namespace dev::p2p;
using namespace dev::eth;
using namespace std;

int deterministExecute::getRand(int a, int b) {
    srand((unsigned)time(NULL));
    return (rand() % (b - a + 1)) + a;
}

void deterministExecute::average_latency() {
    int totalTime = 0;
    int totalTxNum = 0;

    // 计算交易平均延时
    for(auto iter = m_txid_to_endtime->begin(); iter != m_txid_to_endtime->end(); iter++){
        string txid = iter->first;
        int endtime = iter->second;
        int starttime = 0;
        if(m_txid_to_starttime->count(txid) != 0){
            starttime = m_txid_to_starttime->at(txid);
            int interval = endtime - starttime;
            totalTime = totalTime + interval;
            totalTxNum++;
            // PLUGIN_LOG(INFO) << LOG_KV("starttime", starttime)
            //                  << LOG_KV("endtime", endtime);
        }
    }

    double avgLatency;
    if (totalTime != 0) {
        avgLatency = double(totalTime) / double(totalTxNum);
    } else {
        avgLatency = 0;
    }
    PLUGIN_LOG(INFO) << LOG_DESC("latency")
                     << LOG_KV("total_txNum", totalTxNum)
                     << LOG_KV("total_time", totalTime)
                     << LOG_KV("average_latency", avgLatency);
}

void deterministExecute::replyToCoordinator(shared_ptr<dev::plugin::transaction> txInfo, dev::PROTOCOL_ID& m_group_protocolID, std::shared_ptr<dev::p2p::Service> m_group_service) {    
	unsigned long source_shard_id = txInfo->source_shard_id; // 协调者id
    string crossTxHash = txInfo->cross_tx_hash;
    unsigned long destin_shard_id = txInfo->destin_shard_id; // 本分片id
    unsigned long messageID = txInfo->message_id;
    string shardIds = txInfo->shardIds;
    string messageIds = txInfo->messageIds;
    unsigned long txNum = txInfo->txNum;
    // string txIds = txInfo->txIds;


    // 如果该笔交易早已收到了足够的commit包，则直接执行
    // if (m_lateCrossTxMutex.try_lock()) {
        // if (lateCrossTxMessageId->find(messageID) != lateCrossTxMessageId->end()) {
        //     PLUGIN_LOG(INFO) << LOG_DESC("commit包之前就已集齐, 直接执行交易... in deter.reply")
        //                      << LOG_KV("messageId", messageID);
        //     // auto readwriteset = crossTx2StateAddress->at(crossTxHash);
        //     executeCrossTx();
        //     // groupVerifier->executeCrossTx(readwriteset);
        //     // crossTx2StateAddress->unsafe_erase(crossTxHash);
        //     // lateCrossTxMessageId->unsafe_erase(messageID);
        //     // m_lateCrossTxMutex.unlock();
        //     return;
        // }
    // }
    
    
    protos::SubCrossShardTxReply subCrossShardTxReply;
    subCrossShardTxReply.set_crosstxhash(crossTxHash);
    subCrossShardTxReply.set_destinshardid(source_shard_id);
    subCrossShardTxReply.set_sourceshardid(destin_shard_id);
    subCrossShardTxReply.set_messageid(messageID);
    subCrossShardTxReply.set_status(1);
    subCrossShardTxReply.set_shardids(shardIds);
    subCrossShardTxReply.set_messageids(messageIds);
    subCrossShardTxReply.set_txnum(txNum);
    // subCrossShardTxReply.set_txids(txIds);
                        
    std::string serializedSubCrossShardTxReply_str;
    subCrossShardTxReply.SerializeToString(&serializedSubCrossShardTxReply_str);
    auto txByte = asBytes(serializedSubCrossShardTxReply_str);

    dev::sync::SyncCrossTxReplyPacket crossTxReplyPacket; // 类型需要自定义
    crossTxReplyPacket.encode(txByte);
    auto msg = crossTxReplyPacket.toMessage(m_group_protocolID);

    PLUGIN_LOG(INFO) << LOG_DESC("状态锁获得, 开始向协调者分片发送状态包....")
                     << LOG_KV("sourceId", source_shard_id)
                     << LOG_KV("messageId", messageID);
                    //  << LOG_KV("crossTxNum", txNum);

    // std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    for(size_t j = 0; j < 4; j++)  // 给所有协调者分片所有节点发
    {
        PLUGIN_LOG(INFO) << LOG_DESC("发送交易中...")
                         << LOG_KV("正在发送给", shardNodeId.at((source_shard_id-1)*4 + j));
        m_group_service->asyncSendMessageByNodeID(shardNodeId.at((source_shard_id-1)*4 + j), msg, CallbackFuncWithSession(), dev::network::Options());
    }
    PLUGIN_LOG(INFO) << LOG_DESC("子分片向协调者发送状态包完毕...");
}

void deterministExecute::forwardToPrimary(shared_ptr<dev::plugin::transaction> txInfo) {
	  unsigned long source_shard_id = txInfo->source_shard_id; // 协调者id
    string crossTxHash = txInfo->cross_tx_hash;
    unsigned long destin_shard_id = txInfo->destin_shard_id; // 本分片id
    auto messageId = txInfo->message_id;
    string txIds = txInfo->txIds;
    auto txNum = txInfo->txNum;
    string shardIds = txInfo->shardIds;
    string messageIds = txInfo->messageIds;

    protos::SubCrossShardTxCommit subCrossShardTxCommit;
    subCrossShardTxCommit.set_crosstxhash(crossTxHash);
    subCrossShardTxCommit.set_commit(1);
    subCrossShardTxCommit.set_sourceshardid(source_shard_id);
    subCrossShardTxCommit.set_destinshardid(destin_shard_id);
    subCrossShardTxCommit.set_messageid(messageId);
    subCrossShardTxCommit.set_shardids(shardIds);
    subCrossShardTxCommit.set_messageids(messageIds);
    subCrossShardTxCommit.set_txnum(txNum);
    subCrossShardTxCommit.set_txids(txIds);

    std::string serializedSubCrossShardTxCommit_str;
    subCrossShardTxCommit.SerializeToString(&serializedSubCrossShardTxCommit_str);
    auto txByte = asBytes(serializedSubCrossShardTxCommit_str);

    dev::sync::SyncCrossTxCommitPacket crossTxCommitPacket; // 类型需要自定义
    crossTxCommitPacket.encode(txByte);
    auto msg = crossTxCommitPacket.toMessage(group_protocolID);

    PLUGIN_LOG(INFO) << LOG_DESC("跨片交易执行完成, 开始向主节点发送commit消息包....")
                     << LOG_KV("corId", source_shard_id)
                     << LOG_KV("messageId", messageId);

    // std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    // 给分片主节点转发commit消息包
    group_p2p_service->asyncSendMessageByNodeID(forwardNodeId.at(internal_groupId - 1), msg, CallbackFuncWithSession(), dev::network::Options());
    
    PLUGIN_LOG(INFO) << LOG_DESC("commit消息包转发完毕...");
}

void deterministExecute::replyToCoordinatorCommitOK(shared_ptr<dev::plugin::transaction> txInfo) {
	unsigned long source_shard_id = txInfo->source_shard_id; // 协调者id
    string crossTxHash = txInfo->cross_tx_hash;
    unsigned long destin_shard_id = txInfo->destin_shard_id; // 本分片id
    auto messageId = txInfo->message_id;
    string txIds = txInfo->txIds;
    auto txNum = txInfo->txNum;
    // auto txNum = 0;
    
    protos::SubCrossShardTxCommitReply subCrossShardTxCommitReply;
    subCrossShardTxCommitReply.set_crosstxhash(crossTxHash);
    subCrossShardTxCommitReply.set_destinshardid(source_shard_id);
    subCrossShardTxCommitReply.set_sourceshardid(destin_shard_id);
    subCrossShardTxCommitReply.set_messageid(messageId);
    subCrossShardTxCommitReply.set_status(1);
    subCrossShardTxCommitReply.set_txnum(txNum);
    subCrossShardTxCommitReply.set_txids(txIds);
                        
    std::string serializedSubCrossShardTxCommitReply_str;
    subCrossShardTxCommitReply.SerializeToString(&serializedSubCrossShardTxCommitReply_str);
    auto txByte = asBytes(serializedSubCrossShardTxCommitReply_str);

    dev::sync::SyncCrossTxCommitReplyPacket crossTxCommitReplyPacket; // 类型需要自定义
    crossTxCommitReplyPacket.encode(txByte);
    auto msg = crossTxCommitReplyPacket.toMessage(group_protocolID);

    PLUGIN_LOG(INFO) << LOG_DESC("跨片交易执行完成, 开始向协调者分片发送commitOK消息包....");

    // std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    for(size_t j = 0; j < 4; j++)  // 给所有协调者分片所有节点发
    {
        // PLUGIN_LOG(INFO) << LOG_KV("正在发送给", shardNodeId.at((source_shard_id-1)*4 + j));
        group_p2p_service->asyncSendMessageByNodeID(shardNodeId.at((source_shard_id-1)*4 + j), msg, CallbackFuncWithSession(), dev::network::Options());
    }
    PLUGIN_LOG(INFO) << LOG_DESC("子分片向协调者发送commitOK消息包完毕...")
                     << LOG_KV("corId", source_shard_id)
                     << LOG_KV("messageId", messageId);

    // 发送完回执再删除相关变量，假设此时以收到了所有的commitMsg
    // crossTx2CommitMsg->unsafe_erase(txInfo.cross_tx_hash);
}

void deterministExecute::sendCommitPacket(shared_ptr<dev::plugin::transaction> txInfo) {    
    
    auto sourceShardId = internal_groupId;
    auto crossTxHash = txInfo->cross_tx_hash;
    auto txNum = txInfo->txNum;
    auto shardIds = txInfo->shardIds;
    auto messageIds = txInfo->messageIds;

    // std::this_thread::sleep_for(std::chrono::milliseconds(50));

    for (auto destinShardID : crossTx2ShardID->at(crossTxHash)) {
        auto destinshardid = destinShardID;
        auto messageId = messageIDs[destinShardID];

        protos::SubCrossShardTxCommit subCrossShardTxCommit;
        subCrossShardTxCommit.set_crosstxhash(crossTxHash);
        subCrossShardTxCommit.set_commit(1);
        subCrossShardTxCommit.set_sourceshardid(sourceShardId);
        subCrossShardTxCommit.set_destinshardid(destinshardid);
        subCrossShardTxCommit.set_messageid(messageId);
        subCrossShardTxCommit.set_shardids(shardIds);
        subCrossShardTxCommit.set_messageids(messageIds);
        subCrossShardTxCommit.set_txnum(txNum);

        std::string serializedSubCrossShardTxCommit_str;
        subCrossShardTxCommit.SerializeToString(&serializedSubCrossShardTxCommit_str);
        auto txByte = asBytes(serializedSubCrossShardTxCommit_str);

        dev::sync::SyncCrossTxCommitPacket crossTxCommitPacket; // 类型需要自定义
        crossTxCommitPacket.encode(txByte);
        auto msg = crossTxCommitPacket.toMessage(group_protocolID);

        // 向子分片的每个节点发送交易
        for(size_t j = 0; j < 4; j++)  // 给所有节点发
        {
            // PLUGIN_LOG(INFO) << LOG_KV("正在发送给", shardNodeId.at((destinShardID - 1) * 4 + j));
            group_p2p_service->asyncSendMessageByNodeID(shardNodeId.at((destinShardID - 1) * 4 + j), msg, CallbackFuncWithSession(), dev::network::Options());
        }
        
        PLUGIN_LOG(INFO) << LOG_DESC("commit消息发送完毕...")
                         << LOG_KV("destinShardId", destinShardID)
                         << LOG_KV("messageId", messageId);
    }
}

void deterministExecute::deterministExecuteTx() {
    PLUGIN_LOG(INFO) << "deterministExecuteTx 线程开启...";
    
    while (true)
    {   
        // why sleep
        std::this_thread::sleep_for(std::chrono::milliseconds(500));

        count++;
        if (count % 100 == 0) {
            auto txInfo = m_blockingTxQueue->frontTx();
            if (txInfo != 0) {
                auto tx = txInfo->tx;
                auto sourceShardId = txInfo->source_shard_id;
                auto size = m_blockingTxQueue->size();

                PLUGIN_LOG(INFO) << LOG_DESC("continuing...")
                                << LOG_KV("candidate size", size)
                                << LOG_KV("cached size", cached_cs_tx->size())
                                << LOG_KV("source shard id", sourceShardId);
                
                PLUGIN_LOG(INFO) << LOG_DESC("队首元素messageID")
                                 << LOG_KV("messageId", txInfo->message_id);
                                //  << LOG_KV("当前正在执行的messageID", current_candidate_tx_messageids->at(sourceShardId - 1));
            }
            // PLUGIN_LOG(INFO) << LOG_KV("executedTx", executedTx);
        }

        checkForDeterministExecuteTxWookLoop();
    }

}

void deterministExecute::checkDelayCommitPacket(shared_ptr<dev::plugin::transaction> txInfo) {
    auto coorId = txInfo->source_shard_id;
    unsigned long messageID = txInfo->message_id;
    
    // 如果该笔交易早已收到了足够的commit包，则直接执行
    if (lateCrossTxMessageId->find(messageID) != lateCrossTxMessageId->end()) {
        PLUGIN_LOG(INFO) << LOG_DESC("commit包之前就已集齐, 直接执行交易... in deter check")
                         << LOG_KV("messageId", messageID);
        // auto readwriteset = crossTx2StateAddress->at(crossTxHash);
        executeCrossTx(coorId, messageID);
        // groupVerifier->executeCrossTx(readwriteset);
        // crossTx2StateAddress->unsafe_erase(crossTxHash);
        // lateCrossTxMessageId->unsafe_erase(messageID);
    }
}

void deterministExecute::checkAbortedTransaction(shared_ptr<dev::plugin::transaction> txInfo) {
    unsigned long source_shard_id = txInfo->source_shard_id; // 协调者id
    unsigned long messageID = txInfo->message_id;
    string shardIds = txInfo->shardIds;
    string messageIds = txInfo->messageIds;

    string abortKey = toString(source_shard_id) + "_" + shardIds + "_" + messageIds;
    // 该交易已被abort
    if (abortSet->find(abortKey) != abortSet->end()) {
        PLUGIN_LOG(INFO) << LOG_DESC("交易早已abort, 直接释放锁... in deter check")
                         << LOG_KV("abortKey", abortKey);
        if (m_blockingTxQueue->popAbortedTx(to_string(source_shard_id))) {
            if (m_blockingTxQueue->size() > 0) {
                PLUGIN_LOG(INFO) << LOG_DESC("in checkAbortedTransaction... 还有等待的交易");
                executeCandidateTx();
            }
        }
    }
}

// 检查并处理被卡住的跨片交易
void deterministExecute::checkForDeterministExecuteTxWookLoop() {
    // 1. check candidate_corssTx_tx
    // 2. check lateCrossTxMessageId
    // for(auto iter = candidate_tx_queues->cbegin(); iter != candidate_tx_queues->cend(); iter++)
    // {
    //     auto readwriteKey = (*iter).first;
    //     if (candidate_tx_queues->at(readwriteKey).queue.size() > 0) {
    //         auto tx = candidate_tx_queues->at(readwriteKey).queue.front().tx;
    //         // transaction txInfo = crossTx[tx->hash()];
    //         transaction txInfo = crossTx->at(tx->hash().abridged());

    //         checkDelayCommitPacket(txInfo);

    //         // =========有问题？
    //         if (txInfo.message_id != current_candidate_tx_messageids->at(txInfo.source_shard_id - 1) && 
    //             txInfo.message_id == complete_candidate_tx_messageids->at(txInfo.source_shard_id - 1) + 1) {
    //             executeCandidateTx();
    //             // groupVerifier->executeCandidateTx(txInfo.readwrite_key);
    //         }
    //     }
    // }

    // if (m_blockingTxQueue->size() > 0) {
        // std::cout << m_blockingTxQueue->size() << std::endl;
        auto txInfo = m_blockingTxQueue->frontTx();
        if (txInfo != 0) { 
            auto tx = txInfo->tx;
            if (crossTx->find(tx->hash().abridged()) != crossTx->end()) { // 跨片交易
                // 异常1: 该笔跨片交易可能错过了之前已经收到的提交包，导致队列阻塞
                // 方案一：直接执行
                // myflag
                // PLUGIN_LOG(INFO) << LOG_DESC("检查落后交易....")
                //                  << LOG_KV("sourceId", txInfo->source_shard_id)
                //                  << LOG_KV("messageId", txInfo->message_id);
                checkDelayCommitPacket(txInfo);

                // 异常2: abort交易没有被移除交易等待队列
                checkAbortedTransaction(txInfo);
            }
            
        }
    // }
}

std::string deterministExecute::dataToHexString(bytes data) {
    // size_t m_data_size = data.size();
    // std::string hex_m_data_str = "";
    // for(size_t i = 0; i < m_data_size; i++)
    // {
    //     string temp;
    //     stringstream ioss;
    //     ioss << std::hex << data.at(i);
    //     ioss >> temp;
    //     hex_m_data_str += temp;
    // }
    // return hex_m_data_str;

    string res2 = "";
    string temp;
    stringstream ioss;

    int count = 0;
    for(auto const &ele:data)
    {
        count++;
        ioss << std::hex << ele;

        if(count > 30)
        {
            ioss >> temp;
            res2 += temp;
            temp.clear();
            ioss.clear();
            count = 0;
        }
    }
    ioss >> temp;
    res2 += temp;
    
    return res2;
}

int deterministExecute::checkTransactionType(std::string& hex_m_data_str, std::shared_ptr<dev::eth::Transaction> tx) {
    int index = -1;
    if((index = hex_m_data_str.find("0x111222333", 0)) != -1) // 跨片交易
    {
        // hex_m_data_str = hex_m_data_str.substr(index);
        return 1;
    }
    else if((index = hex_m_data_str.find("0x444555666", 0)) != -1) // 片内交易
    {
        // hex_m_data_str = hex_m_data_str.substr(index);
        return 2;
    }
    else if(crossTx->find(tx->hash().abridged()) != crossTx->end())
    {
        return 3;
    }
    else // 部署合约交易/跨片交易子交易
    {
        // 23.1.5 舍去这种可能，默认只可能是上面三种
        PLUGIN_LOG(INFO) << LOG_DESC("loop in checkTransactionType...");
        // std::this_thread::sleep_for(std::chrono::milliseconds(500));
        // checkTransactionType(hex_m_data_str, tx);
        while (crossTx->find(tx->hash().abridged()) == crossTx->end()) {
            continue;
        }
        return 3; 
    }
}

void deterministExecute::processInnerShardTx(std::string data_str, std::shared_ptr<dev::eth::Transaction> tx) {
    // PLUGIN_LOG(INFO) << LOG_DESC("开始解析片内交易data字段");
    
    int m = data_str.find("0x444555666", 0);
    int m2 = data_str.find_last_of('|');
    int len = data_str.length();

    std::string subtxStr = data_str.substr(m, m2 - m);
    // std::string subtxStr = data_str.substr(n, n2-n);
    // PLUGIN_LOG(INFO) << LOG_DESC("发现片内交易！")
    //                      << LOG_KV("m", m)
    //                      << LOG_KV("len", len)
    //                      << LOG_KV("subtxStr", subtxStr);

    std::vector<std::string> subSignedTx;
    try
    {
        boost::split(subSignedTx, subtxStr, boost::is_any_of("|"), boost::token_compress_on);
        
        // PLUGIN_LOG(INFO) << LOG_DESC("片内交易data字段解析完毕");

        // 获得片内交易hash的字符串
        std::string tx_hash_str = tx->hash().abridged();
        // 多个状态利用"_"进行划分
        std::string readwriteset = subSignedTx.at(1);
        std::string txId = subSignedTx.at(2);

        if (m_blockingTxQueue->isBlocked(readwriteset)) { // 阻塞，放入队列
            auto txInfo = std::make_shared<transaction>(0, 
                                            (unsigned long)0, 
                                            (unsigned long)0, 
                                            (unsigned long)0,
                                            (unsigned long)0, 
                                            tx_hash_str, 
                                            tx, 
                                            readwriteset,
                                            "",
                                            "",
                                            txId);

            innerTx->insert(std::make_pair(tx->hash().abridged(), txInfo));
            m_blockingTxQueue->insertTx(txInfo);
            blockedInnerTx++;
            PLUGIN_LOG(INFO) << LOG_DESC("阻塞的片内交易数")
                             << LOG_KV("blockedInnerTx", blockedInnerTx);

        } else { // 没阻塞，直接执行
            // 删除txHash2BlockHeight变量，代表相关交易已被执行
            // txHash2BlockHeight->unsafe_erase(txHash);

            if(executedTx == 0) {
                PLUGIN_LOG(INFO) << LOG_KV("executedTx", 0);
            } 
            else if ((executedTx + 1) % 500 == 0) {
                // exec = dev::plugin::executiveContext->getExecutive();
                // auto vm = dev::plugin::executiveContext->getExecutiveInstance();
                // exec->setVM(vm);
                // dev::plugin::executiveContext->m_vminstance_pool.push(vm);
                PLUGIN_LOG(INFO) << LOG_DESC("片内交易执行成功")
                                 << LOG_KV("executedTx", executedTx + 1)
                                 << LOG_KV("executedTx_real", executedTx + 1)
                                 << LOG_KV("当前分片累计提交交易数", executedTx + 1);
            }
            // 记录交易结束时间
            struct timeval tv;
            gettimeofday(&tv, NULL);
            int time_sec = (int)tv.tv_sec;
            m_txid_to_endtime->insert(make_pair(txId, time_sec));

            dev::plugin::executiveContext->executeTransaction(exec, tx);
            executedTx++;
        }
        
    }
    catch (std::exception& e)
    {
        exit(1);
    }
}

/*
void deterministExecute::processCrossShardTx(std::string data_str, std::shared_ptr<dev::eth::Transaction> tx) {
    // // 只需要转发节点处理，积攒后批量发送至参与者
    // if(dev::plugin::nodeIdStr != toHex(forwardNodeId.at(internal_groupId - 1)))
    // {
    //     return;
    // }
    
    int n = data_str.find("0x111222333", 0);
    int n2 = data_str.find_last_of('|');
    int len = data_str.length();

    // std::string subtxStr = data_str.substr(n, len - n);
    std::string subtxStr = data_str.substr(n, n2-n);
    PLUGIN_LOG(INFO) << LOG_DESC("发现跨片交易！")
                         << LOG_KV("subtxStr", subtxStr);
    
    std::vector<std::string> subSignedTx;
    // EDIT BY ZH
    try
    {
        boost::split(subSignedTx, subtxStr, boost::is_any_of("|"), boost::token_compress_on);

        // 对分片中的所有节点id进行遍历, 加入到列表中
        int subSignedTx_size = subSignedTx.size();
        // 拿到所有参与子分片
        std::string subShardIds = "";
        for (int i = 1; i < subSignedTx_size; i = i + 3) {
            std::string destinShardID = subSignedTx.at(i); // 目标分片ID
            if ( i == 1 ) {
                subShardIds += destinShardID;
            } else {
                // std::string tmep = "_" + destinShardID;
                subShardIds += ("_" + destinShardID);
            }
        }

        for(int i = 1; i < subSignedTx_size; i = i + 3)
        {
            int shardID = atoi(subSignedTx.at(i).c_str()); // 目标分片ID
            std::string str_shardID = subSignedTx.at(i);
            std::string signedData = subSignedTx.at(i + 1); // 发向目标分片的跨片交易子交易
            std::string stateAddress = subSignedTx.at(i + 2); // 向目标分片发送目标状态地址
            
            // 这批交易的目标分片都是subShardIds，具体的分片id是shardID，访问的状态地址是stateAddress

            // 访问各个分片的交易对应的crossTxHash应该一样 为什么不采用重新包装后的交易id？
            std::string hashKey = subShardIds + "|" + stateAddress;
            if (key2CrossTxHash->count(hashKey) == 0) {
                key2CrossTxHash->insert(std::make_pair(hashKey, tx->hash().abridged()));
            }

            std::string key = subShardIds + "|" + str_shardID + "|" + stateAddress;
            if (key2Signdatas->count(key) == 0) {
                key2Signdatas->insert(std::make_pair(key, signedData));
            } else {
                // std::string oriSignData = key2Signdatas->at(key);
                // oriSignData += ("|" + signedData);
                key2Signdatas->at(key) += ("|" + signedData);
                // PLUGIN_LOG(INFO) << LOG_DESC("新子交易集合")
                //                      << LOG_KV("key", key);
                //                      << LOG_KV("signedDatas", key2Signdatas->at(key));
            }
            
        }
    }
    catch (std::exception& e)
    {
        exit(1);
    }

}
*/

void deterministExecute::processCrossShardTx(std::string data_str, std::shared_ptr<dev::eth::Transaction> tx) {
    int n = data_str.find("0x111222333", 0);
    int n2 = data_str.find_last_of('|');
    int len = data_str.length();

    // std::string subtxStr = data_str.substr(n, len - n);
    std::string subtxStr = data_str.substr(n, n2-n);
    // PLUGIN_LOG(INFO) << LOG_DESC("发现跨片交易！")
    //                  << LOG_KV("subtxStr", subtxStr);
    
    std::vector<std::string> subSignedTx;
    
    try
    {
        boost::split(subSignedTx, subtxStr, boost::is_any_of("|"), boost::token_compress_on);

        // 对分片中的所有节点id进行遍历, 加入到列表中
        int subSignedTx_size = subSignedTx.size();
        // 拿到所有参与子分片
        std::string subShardIds = "";
        for (int i = 1; i < subSignedTx_size - 1; i = i + 3) {
            std::string destinShardID = subSignedTx.at(i); // 目标分片ID
            if ( i == 1 ) {
                subShardIds += destinShardID;
            } else {
                // std::string tmep = "_" + destinShardID;
                subShardIds += ("_" + destinShardID);
            }
        }
        string crossTxHash;
        for(int i = 1; i < subSignedTx_size - 1; i = i + 3)
        {
            int shardID = atoi(subSignedTx.at(i).c_str()); // 目标分片ID
            std::string str_shardID = subSignedTx.at(i);
            std::string signedData = subSignedTx.at(i + 1); // 发向目标分片的跨片交易子交易
            std::string stateAddress = subSignedTx.at(i + 2); // 向目标分片发送目标状态地址
            
            if (key2CrossTxHash->count(subShardIds) == 0) {
                crossTxHash = tx->hash().abridged();
                key2CrossTxHash->insert(std::make_pair(subShardIds, crossTxHash));
            } else {
                crossTxHash = key2CrossTxHash->at(subShardIds);
            }
            
            // 这批交易的目标分片都是subShardIds，具体的分片id是str_shardID
            std::string key = subShardIds + "|" + str_shardID;
            if (key2Signdatas->count(key) == 0) {
                key2Signdatas->insert(std::make_pair(key, signedData));
                key2StateAddress->insert(std::make_pair(key, stateAddress));
            } else {
                key2Signdatas->at(key) += ("|" + signedData);
                key2StateAddress->at(key) += ("_" + stateAddress); // 注意判重 判重逻辑加到push和pop中
            }
        }

        // 这批跨片交易的txIds
        string txid = subSignedTx.at(7);
        if (crossTxHash2Txids->count(crossTxHash) == 0) {
            crossTxHash2Txids->insert(std::make_pair(crossTxHash, txid));
        } else {
            crossTxHash2Txids->at(crossTxHash) += ("_" + txid);
        }

    }
    catch (std::exception& e)
    {
        exit(1);
    }

}

std::string deterministExecute::createBatchTransaction(std::string signedDatas, int groupId) {
    // std::string requestLabel = "0x777888999";
    std::string flag = "|";
    // std::string hex_m_data_str = requestLabel + flag + signedDatas + flag;
    std::string hex_m_data_str = flag + signedDatas + flag;

    // 自己构造交易
    std::string str_address;
    if (groupId == 1) {
        // PLUGIN_LOG(INFO) << LOG_DESC("in createBatchTransaction GroupID为1...");
        str_address = innerContact_1;
    } else if (groupId == 2) {
        // PLUGIN_LOG(INFO) << LOG_DESC("in createBatchTransaction GroupID为2...");
        str_address = innerContact_2;
    } else if (groupId == 3) {
        // PLUGIN_LOG(INFO) << LOG_DESC("in createBatchTransaction GroupID为3...");
        str_address = innerContact_3;
    } else {
        // PLUGIN_LOG(INFO) << LOG_DESC("in createBatchTransaction GroupID为other...");
        str_address = innerContact_3;
    }
    dev::Address contactAddress(str_address);
    dev::eth::ContractABI abi;
    bytes data = abi.abiIn("add(string)", hex_m_data_str);  // add

    std::shared_ptr<Transaction> tx = std::make_shared<Transaction>(0, 1000, 0, contactAddress, data);
    tx->setNonce(generateRandomValue());
    tx->setGroupId(groupId);
    
    tx->setBlockLimit(u256(m_blockchainManager->number() + 10));
    
    auto keyPair = KeyPair::create();
    auto sig = dev::crypto::Sign(keyPair, tx->hash(WithoutSignature));
    tx->updateSignature(sig);

    return toHex(tx->rlp());
}

void deterministExecute::pushBlockCrossTx(string subShardIds, vector<string> shardIds,
                                          string crossTxHash, vector<string>& keySet, string txIds) {
    std::string allShardID = subShardIds;
    std::string allStateAddress = "";
    std::string allSignedTx = "";

    for (int i = 0; i < shardIds.size(); i++) {
        auto shardID = atoi(shardIds.at(i).c_str()); // 目标分片
        std::string newKey = subShardIds + "|" + to_string(shardID);
        std::string signedDatas = key2Signdatas->at(newKey);
        std::string stateAddress = key2StateAddress->at(newKey);
        keySet.push_back(newKey);

        // 对批量交易进行二次包装
        std::string signedTxs = createBatchTransaction(signedDatas, shardID);
        
        if (i == 0) {
            allStateAddress = stateAddress;
            allSignedTx = signedTxs;
        } else {
            allStateAddress += ("|" + stateAddress);
            allSignedTx += ("|" + signedTxs);
        }
    }

    // blockedCrossTransaction transaction = blockedCrossTransaction{
    //     0,
    //     (long unsigned)internal_groupId,
    //     allShardID,
    //     crossTxHash,
    //     allStateAddress,
    //     allSignedTx,
    //     txIds
    // };
    PLUGIN_LOG(INFO) << LOG_DESC("当前跨片交易阻塞队列大小 blocked")
                     << LOG_KV("size", m_blockingCrossTxQueue->size());
    m_blockingCrossTxQueue->insertTx(std::make_shared<blockedCrossTransaction>(
        0,
        (long unsigned)internal_groupId,
        allShardID,
        crossTxHash,
        allStateAddress,
        allSignedTx,
        txIds
    ));

    PLUGIN_LOG(INFO) << LOG_DESC("当前跨片交易阻塞队列大小 after")
                     << LOG_KV("size", m_blockingCrossTxQueue->size());
}

void deterministExecute::tryToSendSubTxs() {
    std::vector<std::string> keySet;
    std::vector<std::string> hashKeySet;
    for (auto it = key2CrossTxHash->begin(); it != key2CrossTxHash->end(); it ++) {
        std::string subShardIds = it->first; // 子分片集合
        auto crossTxHash = it->second ; // 跨片交易hash的字符串--标志作用
        string txIds = crossTxHash2Txids->at(crossTxHash);

        hashKeySet.push_back(subShardIds);
        // std::string signedDatas = it->second;
        // std::vector<std::string> subKeys;

        // 存储跨片交易对应的分片ID
        std::vector<int> crossShardsID;
        // std::vector<int> crossShardsMessageID;

        try {
            string messageIds = "";
            std::vector<std::string> shardIds;
            boost::split(shardIds, subShardIds, boost::is_any_of("_"), boost::token_compress_on);
            for (int i = 0; i < shardIds.size(); i++) {
                // PLUGIN_LOG(INFO) << LOG_KV("shardId", shardIds.at(i));
                int shardID = atoi(shardIds.at(i).c_str());
                crossShardsID.push_back(shardID);
                // crossShardsMessageID.push_back(messageIDs[shardID] + 1);
                if (i == 0) {
                    messageIds = to_string(messageIDs[shardID] + 1);
                } else {
                    messageIds += ("_" + to_string(messageIDs[shardID] + 1));
                }
            }

            // 只需要转发节点处理，积攒后批量发送至参与者
            if(dev::plugin::nodeIdStr != toHex(forwardNodeId.at(internal_groupId - 1)))
            {
                crossTx2ShardID->insert(std::make_pair(crossTxHash, crossShardsID));
                // crossTx2ShardMessageID->insert(std::make_pair(crossTxHash, crossShardsMessageID));
                continue;
            }
            
            
            if (!m_blockingCrossTxQueue->isBlocked()) { // 没阻塞
                // PLUGIN_LOG(INFO) << LOG_DESC("当前跨片交易阻塞队列大小 unBlocked")
                //                  << LOG_KV("size", m_blockingCrossTxQueue->size());

                // 不管阻不阻塞，加入队列，防止后面的abort
                std::string allShardID = subShardIds;
                std::string allStateAddress = "";
                std::string allSignedTx = "";
                // 对所有涉及到的分片一次性发送对应交易 ===> 或可避免死锁
                for (int i = 0; i < shardIds.size(); i++) {
                    auto shardID = atoi(shardIds.at(i).c_str()); // 目标分片
                    int messageID = messageIDs[shardID];
                    messageIDs[shardID] = ++messageID;
                    std::string newKey = subShardIds + "|" + to_string(shardID);
                    std::string signedDatas = key2Signdatas->at(newKey);
                    std::string stateAddress = key2StateAddress->at(newKey);
                    keySet.push_back(newKey);

                    // 对批量交易进行二次包装
                    std::string signedTxs = createBatchTransaction(signedDatas, shardID);
                    std::vector<std::string> signedTxCount;
                    boost::split(signedTxCount, signedDatas, boost::is_any_of("|"), boost::token_compress_on);
                    int txNum = signedTxCount.size();

                    PLUGIN_LOG(INFO) << LOG_DESC("批量发送跨片子交易")
                                     << LOG_KV("subShardIds", subShardIds)
                                     << LOG_KV("sourceId", internal_groupId)
                                     << LOG_KV("messageId", messageID)
                                     << LOG_KV("destinShardID", shardID);
                                    //  << LOG_KV("crossTxNum", txNum)
                                    //  << LOG_KV("txIds", txIds)
                                    //  << LOG_KV("signedTxs", signedTxs)
                                     // << LOG_KV("stateAddress", stateAddress)
                                    //  << LOG_KV("crossTxHash", crossTxHash);
                    
                    if (i == 0) {
                        allStateAddress = stateAddress;
                        allSignedTx = signedTxs;
                    } else {
                        allStateAddress += ("|" + stateAddress);
                        allSignedTx += ("|" + signedTxs);
                    }

                    // 将跨片子交易转发给相应的参与者分片
                    // 下面调用 void forwardTx(protos::SubCrossShardTx _subCrossShardTx) 对交易进行转发，转发到相应分片
                    protos::SubCrossShardTx subCrossShardTx;
                    subCrossShardTx.set_signeddata(signedTxs);
                    subCrossShardTx.set_stateaddress(stateAddress);
                    subCrossShardTx.set_sourceshardid(internal_groupId);
                    subCrossShardTx.set_destinshardid(shardID);
                    subCrossShardTx.set_messageid(messageID);
                    subCrossShardTx.set_crosstxhash(crossTxHash); // 跨片交易哈希字符串
                    subCrossShardTx.set_shardids(subShardIds); // 参与跨片交易的子分片集合，通过"_"划分
                    subCrossShardTx.set_messageids(messageIds);
                    subCrossShardTx.set_txnum(txNum);
                    subCrossShardTx.set_txids(txIds);

                    std::string serializedSubCrossShardTx_str;
                    subCrossShardTx.SerializeToString(&serializedSubCrossShardTx_str);
                    auto txByte = asBytes(serializedSubCrossShardTx_str);

                    dev::sync::SyncCrossTxPacket crossTxPacket; // 类型需要自定义
                    crossTxPacket.encode(txByte);
                    auto msg = crossTxPacket.toMessage(group_protocolID);

                    // PLUGIN_LOG(INFO) << LOG_DESC("协调者开始向参与者分片发送跨片交易....");
                    
                    // std::this_thread::sleep_for(std::chrono::milliseconds(50));

                    if (shardID == dev::consensus::internal_groupId) {
                        //若参与者与协调者为同一分片，则直接投入交易池且发送给其它节点
                        for (int j = 3; j >=1; j--) {
                            group_p2p_service->asyncSendMessageByNodeID(shardNodeId.at((shardID - 1) * 4 + j), msg, CallbackFuncWithSession(), dev::network::Options());
                        }

                        Transaction::Ptr tx = std::make_shared<Transaction>(
                                jsToBytes(signedTxs, dev::OnFailed::Throw), CheckTransaction::Everything);

                        crossTx->insert(std::make_pair(tx->hash().abridged(), std::make_shared<transaction>(
                            1, 
                            (unsigned long)internal_groupId, 
                            (unsigned long)internal_groupId, 
                            (unsigned long)messageID,
                            (unsigned long)txNum, 
                            crossTxHash, 
                            tx, 
                            stateAddress,
                            subShardIds,
                            messageIds,
                            txIds)));

                        m_rpc_service->sendRawTransaction(internal_groupId, toHex(tx->rlp()));
                    } else { // 发送交易包给所有节点
                        for(int j = 3; j >= 0; j--) { 
                            // PLUGIN_LOG(INFO) << LOG_KV("正在发送给", shardNodeId.at((shardID - 1) * 4 + j));
                            group_p2p_service->asyncSendMessageByNodeID(shardNodeId.at((shardID - 1) * 4 + j), msg, CallbackFuncWithSession(), dev::network::Options());
                        }
                    }
                    
                    PLUGIN_LOG(INFO) << LOG_DESC("跨片消息转发完毕...");
                }

                // blockedCrossTransaction transaction = blockedCrossTransaction{
                //     1,
                //     (long unsigned)internal_groupId,
                //     allShardID,
                //     crossTxHash,
                //     allStateAddress,
                //     allSignedTx,
                //     txIds
                // };
                m_blockingCrossTxQueue->insertTx(std::make_shared<blockedCrossTransaction>(
                    1,
                    (long unsigned)internal_groupId,
                    allShardID,
                    crossTxHash,
                    allStateAddress,
                    allSignedTx,
                    txIds
                ));
            } else {
                pushBlockCrossTx(subShardIds, shardIds, crossTxHash, keySet, txIds);
            }

            //ADD BY ZH  存储跨片交易哈希对应的分片ID
            crossTx2ShardID->insert(std::make_pair(crossTxHash, crossShardsID));
            // crossTx2ShardMessageID->insert(std::make_pair(crossTxHash, crossShardsMessageID));

        } catch (std::exception& e) {
            exit(1);
        }
    }
    // 删除发送过的交易
    for (int i = 0; i < keySet.size(); i++) {
        std::string key = keySet[i];
        key2Signdatas->unsafe_erase(key);
        key2StateAddress->unsafe_erase(key);
    }
    for (int i = 0; i < hashKeySet.size(); i++) {
        std::string key = hashKeySet[i];
        std::string hash = key2CrossTxHash->at(key);
        key2CrossTxHash->unsafe_erase(key);
        crossTxHash2Txids->unsafe_erase(hash);
    }
}

/*
void deterministExecute::processSubShardTx(std::shared_ptr<dev::eth::Transaction> tx, int height) {
    // 批量处理子交易处理
    auto txInfo = crossTx->at(tx->hash().abridged());
    // m_crossTxMutex.unlock();
    auto crossTxHash = txInfo.cross_tx_hash;
    auto blockHeight = height;

    PLUGIN_LOG(INFO) << LOG_DESC("添加(批量)跨片交易至执行队列 in processSubShardTx")
                     << LOG_KV("messageId", txInfo.message_id)
                     << LOG_KV("stateAddress", txInfo.readwrite_key);

    if (blockHeight2CrossTxHash->count(blockHeight) == 0) {
        std::vector<std::string> temp;
        temp.push_back(crossTxHash);
        blockHeight2CrossTxHash->insert(std::make_pair(blockHeight, temp));
    } else {
        blockHeight2CrossTxHash->at(blockHeight).push_back(crossTxHash);
    }
}
*/

bool deterministExecute::isAborted(string abortKey) {
    return (abortSet->find(abortKey) != abortSet->end());
}

void deterministExecute::sendAbortPacket(shared_ptr<transaction> txInfo) {
    unsigned long message_id = txInfo->message_id;
    unsigned long source_shard_id = txInfo->source_shard_id; // 协调者id
    unsigned long destin_shard_id = txInfo->destin_shard_id; // 本分片id
    string subShardIds = txInfo->shardIds;
    string messageIds = txInfo->messageIds;
    bool flag = false; // 标记协调者是否为参与者，默认为false
    // 构造abort消息包
    protos::AbortMsg abortMsg;
    abortMsg.set_coorshardid(source_shard_id);
    abortMsg.set_subshardsid(subShardIds);
    abortMsg.set_messageid(message_id);
    abortMsg.set_messageids(messageIds);

    std::string serializedAbortPacket_str;
    abortMsg.SerializeToString(&serializedAbortPacket_str);
    auto txByte = asBytes(serializedAbortPacket_str);

    dev::sync::SyncAbortPacket abortPacket; // 类型需要自定义
    abortPacket.encode(txByte);
    auto msg = abortPacket.toMessage(group_protocolID);
    
    // 向其它子分片发
    std::vector<std::string> shardIds;
    std::vector<std::string> message_ids;
    boost::split(shardIds, subShardIds, boost::is_any_of("_"), boost::token_compress_on);
    boost::split(message_ids, messageIds, boost::is_any_of("_"), boost::token_compress_on);
    for (int i = 0; i < shardIds.size(); i++) {
        int shardId = atoi(shardIds.at(i).c_str());
        if (source_shard_id == (signed long)shardId) {
            flag = true;
        }

        if (shardId == dev::consensus::internal_groupId) {
            continue;
        }

        int messageId = atoi(message_ids.at(i).c_str());
        abortMsg.set_messageid((unsigned long)messageId);
        std::string serializedAbortPacket_str;
        abortMsg.SerializeToString(&serializedAbortPacket_str);
        auto txByte = asBytes(serializedAbortPacket_str);

        dev::sync::SyncAbortPacket abortPacket; // 类型需要自定义
        abortPacket.encode(txByte);
        auto msg = abortPacket.toMessage(group_protocolID);

        PLUGIN_LOG(INFO) << LOG_DESC("发送交易中...")
                         << LOG_KV("正在发送给", shardId)
                         << LOG_KV("messageId", messageId);
        for(size_t j = 0; j < 4; j++)  // 给所有其它参与节点发
        {
            group_p2p_service->asyncSendMessageByNodeID(shardNodeId.at((shardId-1)*4 + j), msg, CallbackFuncWithSession(), dev::network::Options());
        }
    }
    PLUGIN_LOG(INFO) << LOG_DESC("向子分片节点发送abort包完毕...");

    if (flag == false) { // 若协调者不是参与者，向协调者主节点发abort包
        for(size_t j = 0; j < 4; j++)  // 给所有其它参与节点发
        {
            PLUGIN_LOG(INFO) << LOG_DESC("发送交易中...")
                             << LOG_KV("正在发送给", shardNodeId.at((source_shard_id-1)*4 + j));
            group_p2p_service->asyncSendMessageByNodeID(shardNodeId.at((source_shard_id-1)*4 + j), msg, CallbackFuncWithSession(), dev::network::Options());
        }
        PLUGIN_LOG(INFO) << LOG_DESC("向协调者分片节点发送abort包完毕...")
                         << LOG_KV("corId", source_shard_id)
                         << LOG_KV("subIds", subShardIds);
    } else if (nodeIdStr == toHex(forwardNodeId.at(source_shard_id - 1))) { 
        //若是协调者分片发起的abort，主节点需要过段时间重新发送交易
        // 1. 停随机一段时间 -- [0,100)ms
        int randomTime = getRand(0, 10) * 10;
        std::this_thread::sleep_for(std::chrono::milliseconds(randomTime));
        // 2. 再次发送跨片交易消息包
        processBlockedCrossTx();
        PLUGIN_LOG(INFO) << LOG_DESC("协调者头节点默认处理abort消息包并重发交易");
    }
}

void deterministExecute::processSubShardTx(std::shared_ptr<dev::eth::Transaction> tx, int height) {
    auto txInfo = crossTx->at(tx->hash().abridged());

    unsigned long message_id = txInfo->message_id;
    unsigned long source_shard_id = txInfo->source_shard_id; // 协调者id
    string crossTxHash = txInfo->cross_tx_hash;
    unsigned long destin_shard_id = txInfo->destin_shard_id; // 本分片id
    auto readwriteset = txInfo->readwrite_key; // 跨片交易读写集
    string shardIds = txInfo->shardIds;
    string messageIds = txInfo->messageIds;

    PLUGIN_LOG(INFO) << LOG_DESC("解析跨片交易成功")
                     << LOG_KV("messageId", message_id)
                     << LOG_KV("sourceShardId", source_shard_id)
                     << LOG_KV("destinShardId", destin_shard_id);
                    //  << LOG_KV("shardIds", shardIds)
                    //  << LOG_KV("messageIds", messageIds)
                    //  << LOG_KV("crossTxHash", crossTxHash)
                    //  << LOG_KV("tx", txInfo->tx->abridged())
                    //  << LOG_KV("stateAddress", readwriteset);

    // EDIT ON 22.12.7 采用单队列结构  尝试获取交易需要的锁，若失败，则返回abort消息
    string abortKey = to_string(source_shard_id) + "_" + shardIds + "_" + messageIds;
    if (!isAborted(abortKey) && !m_blockingTxQueue->isBlocked(readwriteset)) {
        m_blockingTxQueue->insertTx(txInfo);
                         
        // 更新正在处理的跨片交易的messageID
        current_candidate_tx_messageids->at(source_shard_id - 1) = message_id;

        // 该交易协调者为参与者 source_shard_id == internal_groupId
        // 此节点为协调者头节点 nodeIdStr == toHex(forwardNodeId.at(internal_groupId - 1))
        // 该条交易的其它子分片的reply消息包已经处理过 ==> 直接发commit消息包
        if (source_shard_id == internal_groupId &&
            nodeIdStr == toHex(forwardNodeId.at(internal_groupId - 1)) && 
            lateReplyMessageId->find(message_id) != lateReplyMessageId->end()) {
            // 发送commit消息包
            sendCommitPacket(txInfo);
            // 执行交易
            executeCrossTx(internal_groupId, message_id);
            return;
        }

        // 由头节点发送reply包 one2all
        if (nodeIdStr == toHex(forwardNodeId.at(dev::consensus::internal_groupId - 1))) {
            replyToCoordinator(txInfo, group_protocolID, group_p2p_service);
        }

    } else {
        // make key
        // string abortKey = to_string(source_shard_id) + "_" + messageIds;
        if (abortSet->find(abortKey) == abortSet->end()) { // 之前没收到过
            // 消息插入abortSet
            abortSet->insert(abortKey);
            // 发送abort消息包
            PLUGIN_LOG(INFO) << LOG_DESC("发送abort消息包...")
                             << LOG_KV("coorId", source_shard_id)
                             << LOG_KV("shardIds", shardIds)
                             << LOG_KV("messageIds", messageIds)
                             << LOG_KV("abortKey", abortKey);
            if (nodeIdStr == toHex(forwardNodeId.at(dev::consensus::internal_groupId - 1))) {
                sendAbortPacket(txInfo);
            }
        } else { // 之前收到过
            PLUGIN_LOG(INFO) << LOG_DESC("已收到过对应abort消息...");
        }
    }
    
    // 更新已经收到的按序的最大的messageID
    // latest_candidate_tx_messageids->at(source_shard_id - 1) = message_id;    
}

void deterministExecute::processDeployContract(std::shared_ptr<dev::eth::Transaction> tx) {
    PLUGIN_LOG(INFO) << LOG_DESC("发现合约部署交易/协调者的跨片交易/重复交易 in deterministExecuteTx...")
                     << LOG_KV("txhash", tx->hash());

    auto exec = dev::plugin::executiveContext->getExecutive();
    auto vm = dev::plugin::executiveContext->getExecutiveInstance();
    exec->setVM(vm);
    dev::plugin::executiveContext->executeTransaction(exec, tx);
    dev::plugin::executiveContext->m_vminstance_pool.push(vm);

    // 获取交易所在区块高度
    auto blockHeight = txHash2BlockHeight->at(tx->hash().abridged());
    // 删除txHash2BlockHeight变量，代表相关交易已被执行
    // txHash2BlockHeight->unsafe_erase(tx->hash().abridged());

    if (block2UnExecutedTxNum->count(blockHeight) == 0) {
        // 该笔交易所在区块交易已全部完成过了
        return;
    }
    
    // 维护区块未完成交易
    PLUGIN_LOG(INFO) << LOG_DESC("该笔交易对应的区块高度") << LOG_KV("blockHeight", blockHeight);
    auto unExecutedTxNum = block2UnExecutedTxNum->at(blockHeight);
    PLUGIN_LOG(INFO) << LOG_DESC("in deterministExecuteTx otherTx...")
                     << LOG_KV("区块未完成交易before_num", unExecutedTxNum);

    unExecutedTxNum = unExecutedTxNum - 1;
    block2UnExecutedTxNum->at(blockHeight) = unExecutedTxNum;
    if (unExecutedTxNum == 0) {
        PLUGIN_LOG(INFO) << LOG_BADGE("区块中的数据全部执行完")
                         << LOG_KV("block_height", blockHeight);
    }
    if (block2UnExecutedTxNum->count(blockHeight) != 0) {
        PLUGIN_LOG(INFO) << LOG_DESC("in deterministExecuteTx otherTx...")  
                         << LOG_KV("区块未完成交易now_num", block2UnExecutedTxNum->at(blockHeight));
    }
}

void deterministExecute::processConsensusBlock() {
    PLUGIN_LOG(INFO) << "processConsensusBlock 线程开启...";

    int blockId = 0;
    int totalTxs = 0;

    while (true) {
        int currentBlockNum = m_blockchainManager->number(); // 当前块高
        if(currentBlockNum > blockId) {
            // PLUGIN_LOG(INFO) << LOG_DESC("共识完成, 区块增加");
            blockId++;
            std::shared_ptr<dev::eth::Block> currentBlock = m_blockchainManager->getBlockByNumber(blockId);
            size_t transactions_size = currentBlock->getTransactionSize();
            auto transactions = currentBlock->transactions();
            auto height = blockId;
            totalTxs += transactions_size;
            // consensusTx += transactions_size;
            block2UnExecutedTxNum->insert(std::make_pair(height, transactions_size));
            PLUGIN_LOG(INFO) << LOG_DESC("共识完成, 解析新增区块交易")
                             << LOG_KV("height", height)
                             << LOG_KV("num", transactions_size)
                             << LOG_KV("totalTxs", totalTxs);

            // /*
            // PLUGIN_LOG(INFO) << LOG_DESC("开始执行区块内片内交易");
            for(size_t i = 0; i < transactions_size; i++){
                auto tx = transactions->at(i);
                auto data_str = dataToHexString(tx->data());

                // 将区块内的每一笔交易映射到具体区块高度  EDIT BY ZH -- 22.11.2
                txHash2BlockHeight->insert(std::make_pair(tx->hash().abridged(), height));
                // PLUGIN_LOG(INFO) << LOG_DESC("添加新交易")
                //                  << LOG_KV("txHash", tx->hash())
                //                  << LOG_KV("height", txHash2BlockHeight->at(tx->hash().abridged()));
                
                switch (checkTransactionType(data_str, tx))
                {
                    case InnerShard:
                        processInnerShardTx(data_str, tx);
                        break;
                    case CrossShard:
                        processCrossShardTx(data_str, tx);
                        break;
                    case SubShard:
                        processSubShardTx(tx, height);
                        break;
                    case DeployContract:
                        processDeployContract(tx);
                        break;
                    default:
                        break;
                }
                // dev::consensus::toExecute_transactions.push(tx); // 将共识完出块的交易逐个放入队列
            }
            // PLUGIN_LOG(INFO) << LOG_DESC("区块内片内交易执行完毕");
            tryToSendSubTxs(); // 上层每处理完一个区块，检查积攒的跨片交易并将其发送给相应的子分片
            // */

            PLUGIN_LOG(INFO) << LOG_DESC("新增区块交易解析完成")
                             << LOG_KV("height", height);
        }
        else{
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        
    }
}

void deterministExecute::setAttribute(std::shared_ptr<dev::blockchain::BlockChainInterface> _blockchainManager, std::shared_ptr<dev::rpc::Rpc> _service) {
    m_blockchainManager = _blockchainManager;
    m_rpc_service = _service;
}

void deterministExecute::executeCrossTx(unsigned long coorId, unsigned long messageId) {
    // BLOCKVERIFIER_LOG(INFO) << LOG_DESC("in executeCrossTx... before pop")
                            // << LOG_KV("keyReadwriteSet", keyReadwriteSet)
                            // << LOG_KV("queue size", m_blockingTxQueue->size());
                            // << LOG_KV("size1", candidate_tx_queues->at(keyReadwriteSet).queue.size());
    
    /*
    auto executableTx = candidate_tx_queues->at(keyReadwriteSet).queue.front();
    // 删除执行后的交易
    candidate_tx_queues->at(keyReadwriteSet).queue.pop();
    auto tx = executableTx.tx;
    // transaction txInfo = crossTx[tx->hash()];
    transaction txInfo = crossTx->at(tx->hash().abridged());
    */

    // 需要判断size大小？正常流程进入该函数 则size必定大于0，因此此时不必判断

    // 取变量 EDIT ON 22.12.7
    auto txInfo = m_blockingTxQueue->CrossTx(coorId, messageId);
    auto tx = txInfo->tx;
    m_blockingTxQueue->popCrossTx(coorId, messageId);

    // BLOCKVERIFIER_LOG(INFO) << LOG_DESC("in executeCrossTx... after pop")
    //                         << LOG_KV("queue size", m_blockingTxQueue->size());
    
    // ADD BY ZH ON 22.12.3 —— 添加批处理逻辑
    auto data_str = dataToHexString(tx->data());

    if (data_str == "") {
        // PLUGIN_LOG(INFO) << LOG_DESC("in executeCrossTx data_str is null");    

        // auto exec = dev::plugin::executiveContext->getExecutive();
        // auto vm = dev::plugin::executiveContext->getExecutiveInstance();
        // exec->setVM(vm);
        dev::plugin::executiveContext->executeTransaction(exec, tx);
        // dev::plugin::executiveContext->m_vminstance_pool.push(vm);
    } else {
        std::vector<std::string> signedTxs;
        std::string signedTx;

        try {
            boost::split(signedTxs, data_str, boost::is_any_of("|"), boost::token_compress_on);
            int signedTxSize = signedTxs.size() - 2;
            // PLUGIN_LOG(INFO) << LOG_DESC("批量执行跨片交易")
            //                  << LOG_KV("跨片交易数", signedTxSize);

            // auto exec = dev::plugin::executiveContext->getExecutive();
            // auto vm = dev::plugin::executiveContext->getExecutiveInstance();
            // exec->setVM(vm);

            for (int i = 1; i < signedTxSize - 1; i++) {
                signedTx = signedTxs.at(i);
                // PLUGIN_LOG(INFO) << LOG_DESC("解析批量交易")
                //                  << LOG_KV("signedTx", signedTx);

                Transaction::Ptr tx = std::make_shared<Transaction>(
                        jsToBytes(signedTx, dev::OnFailed::Throw), CheckTransaction::Everything);
                
                dev::plugin::executiveContext->executeTransaction(exec, tx);
            }

            // dev::plugin::executiveContext->m_vminstance_pool.push(vm);
            // PLUGIN_LOG(INFO) << LOG_DESC("in executeCrossTx 跨片交易执行完成");
        } catch (std::exception& e) {
            PLUGIN_LOG(INFO) << LOG_DESC("error message:")
                             << boost::diagnostic_information(e)
                             << LOG_KV("signedTx", signedTx);
            exit(1);
        }
    }
    
    // EDIT BY ZH 22.11.2
    // auto exec = dev::plugin::executiveContext->getExecutive();
    // auto vm = dev::plugin::executiveContext->getExecutiveInstance();
    // exec->setVM(vm);
    // dev::plugin::executiveContext->executeTransaction(exec, tx);
    // dev::plugin::executiveContext->m_vminstance_pool.push(vm);

    // 更新已完成交易id
    complete_candidate_tx_messageids->at(txInfo->source_shard_id - 1) = txInfo->message_id;

    // auto blockHeight = txHash2BlockHeight->at(tx->hash().abridged());
    // PLUGIN_LOG(INFO) << LOG_DESC("in executeCrossTx... 该笔交易对应的区块高度")
    //                  << LOG_KV("blockHeight", blockHeight);
    // auto unExecutedTxNum = block2UnExecutedTxNum->at(blockHeight);
    // PLUGIN_LOG(INFO) << LOG_DESC("in executeCrossTx...")
    //                  << LOG_KV("区块未完成交易before_num", unExecutedTxNum);
    // unExecutedTxNum = unExecutedTxNum - 1;
    // block2UnExecutedTxNum->at(blockHeight) = unExecutedTxNum;
    // txHash2BlockHeight->unsafe_erase(tx->hash().abridged());

    // 判断剩余交易数并删除相关变量
    // if (unExecutedTxNum == 0) {
    //     PLUGIN_LOG(INFO) << LOG_BADGE("区块中的数据全部执行完")
    //                      << LOG_KV("block_height", blockHeight);
    //     // 删除相关变量
    //     //     1. block2ExecutedTxNum -- 已完成
    //     //     2. 2PC流程中的变量: doneCrossTx —- 已完成
    //     if (m_block2UnExecMutex.try_lock()) {
    //         block2UnExecutedTxNum->unsafe_erase(blockHeight);
    //         m_block2UnExecMutex.unlock();
    //     }
    //     for (auto i : blockHeight2CrossTxHash->at(blockHeight)) {
    //         PLUGIN_LOG(INFO) << LOG_DESC("正在删除doneCrossTx...该区块高度存在的跨片交易有：")
    //                          << LOG_KV("crossTxHash", i);
    //         // maybe 要加锁
    //         if (m_doneCrossTxMutex.try_lock()) {
    //             doneCrossTx->unsafe_erase(i);
    //             m_doneCrossTxMutex.unlock();
    //         }
    //     }
    //     if (m_height2TxHashMutex.try_lock()) {
    //         blockHeight2CrossTxHash->unsafe_erase(blockHeight);
    //         m_height2TxHashMutex.unlock();
    //     }
    // }

    // if (block2UnExecutedTxNum->count(blockHeight) != 0) {
    //     PLUGIN_LOG(INFO) << LOG_DESC("in executeCrossTx...")  
    //                      << LOG_KV("keyReadwriteSet", keyReadwriteSet)
    //                      << LOG_KV("区块未完成交易now_num", block2UnExecutedTxNum->at(blockHeight));
    // }

    /* 解锁相应变量
       1. crossTx
       2. locking_key
       3. crossTx2CommitMsg
    */
    // m_crossTxMutex.lock();
    // crossTx->unsafe_erase(tx->hash());
    // m_crossTxMutex.unlock();

    // m_lockKeyMutex.lock();
    // locking_key->at(keyReadwriteSet)--;
    // m_lockKeyMutex.unlock();

    // 仅让头节点发送成功回执
    if (nodeIdStr == toHex(forwardNodeId.at(dev::consensus::internal_groupId - 1))) {
        replyToCoordinatorCommitOK(txInfo);
    }

    // // 非头节点转发提交交易包至头节点 one for all
    // if (nodeIdStr != toHex(forwardNodeId.at(dev::consensus::internal_groupId - 1))) {
    //     forwardToPrimary(txInfo);
    // }

    // 将完成的跨片交易加入doneCrossTx
    doneCrossTx->insert(txInfo->cross_tx_hash);

    // 判断是否还有等待交易
    if (m_blockingTxQueue->size() > 0) {
        BLOCKVERIFIER_LOG(INFO) << LOG_DESC("in executeCrossTx... 还有等待的交易");
        executeCandidateTx();
    }
}

void deterministExecute::executeCandidateTx() {

    // auto executableTx = candidate_tx_queues->at(keyReadwriteSet).queue.front();
    // auto tx = executableTx.tx;

    // 取出下一笔交易 EDIT BY ZH ON 22.12.7
    // 确定 size > 0 ?
    auto txInfo = m_blockingTxQueue->frontTx();
    auto tx = txInfo->tx;
    auto txId = txInfo->txIds;

    //判断是否为跨片交易
    // m_crossTxMutex.lock();
    if (crossTx->find(tx->hash().abridged()) == crossTx->end()) { // 非跨片交易 => 直接执行
        // PLUGIN_LOG(INFO) << LOG_DESC("该笔交易为片内交易... in executeCandidateTx");
        
        // 记录交易结束时间
        struct timeval tv;
        gettimeofday(&tv, NULL);
        int time_sec = (int)tv.tv_sec;
        m_txid_to_endtime->insert(make_pair(txId, time_sec));

        if (executedTx == 0) {
            PLUGIN_LOG(INFO) << LOG_KV("executedTx", 0);
        } else if ((executedTx + 1) % 500 == 0) {
            PLUGIN_LOG(INFO) << LOG_DESC("片内交易执行成功")
                             << LOG_KV("executedTx", executedTx + 1)
                             << LOG_KV("executedTx_real", executedTx + 1)
                             << LOG_KV("当前分片累计提交交易数", executedTx + 1);
        }
        executedTx++;

        // EDIT BY ZH 22.12.11
        dev::plugin::executiveContext->executeTransaction(exec, tx);


        // auto blockHeight = txHash2BlockHeight->at(tx->hash().abridged());
        // PLUGIN_LOG(INFO) << LOG_DESC("该笔交易对应的区块高度") << LOG_KV("blockHeight", blockHeight);
        // auto unExecutedTxNum = block2UnExecutedTxNum->at(blockHeight);
        // PLUGIN_LOG(INFO) << LOG_DESC("in executeCandidateTx...")
        //                  << LOG_KV("区块未完成交易before", unExecutedTxNum);
        
        // unExecutedTxNum = unExecutedTxNum - 1;
        // block2UnExecutedTxNum->at(blockHeight) = unExecutedTxNum;

        // 判断剩余交易数并删除相关变量
        // if (unExecutedTxNum == 0) {
        //     PLUGIN_LOG(INFO) << LOG_BADGE("区块中的数据全部执行完")
        //                      << LOG_KV("block_height", blockHeight);
            /*  删除相关变量
                1. block2ExecutedTxNum -- 已完成
                2. 2PC流程中的变量: doneCrossTx —- 已完成
            
            if (m_block2UnExecMutex.try_lock()) {
                block2UnExecutedTxNum->unsafe_erase(blockHeight);
                m_block2UnExecMutex.unlock();
            }
            for (auto i : blockHeight2CrossTxHash->at(blockHeight)) {
                PLUGIN_LOG(INFO) << LOG_DESC("正在删除doneCrossTx...该区块高度存在的跨片交易有：")
                                 << LOG_KV("crossTxHash", i);
                if (m_doneCrossTxMutex.try_lock()) {
                    doneCrossTx->unsafe_erase(i);
                    m_doneCrossTxMutex.unlock();
                }
            }
            if (m_height2TxHashMutex.try_lock()) {
                blockHeight2CrossTxHash->unsafe_erase(blockHeight);
                m_height2TxHashMutex.unlock();
            }
            */ 
        // }

        // if (block2UnExecutedTxNum->count(blockHeight) != 0) {
        //     PLUGIN_LOG(INFO) << LOG_DESC("in executeCandidateTx...")  
        //                      << LOG_KV("区块未完成交易now_num", block2UnExecutedTxNum->at(blockHeight));
        // }
        // // 删除执行过的交易
        // candidate_tx_queues->at(keyReadwriteSet).queue.pop();
        // // 释放锁
        // m_lockKeyMutex.lock();
        // locking_key->at(keyReadwriteSet)--;
        // m_lockKeyMutex.unlock();
        // if (candidate_tx_queues->at(keyReadwriteSet).queue.size() != 0) {
        //     executeCandidateTx();
        // }
        
        // EDIT BY ZH ON 22.12.7
        m_blockingTxQueue->popTx();
        auto size = m_blockingTxQueue->size();
        if (size > 0) {
            // PLUGIN_LOG(INFO) << LOG_DESC("in executeCandidateTx...")
            //                  << LOG_KV("blockingTxQueue剩余交易数", size);
            executeCandidateTx();
        }
    } 
    // 跨片交易不需要处理，因为是其它协调者发起的
    // else { 
    //     // 获取跨片交易相关信息
    //     // transaction txInfo = crossTx[tx->hash()];
    //     transaction txInfo = crossTx->at(tx->hash().abridged());
    //     // m_crossTxMutex.unlock();

    //     BLOCKVERIFIER_LOG(INFO) << LOG_DESC("该笔交易为跨片交易...")
    //                             << LOG_KV("messageId", txInfo.message_id);
        
    //     string abortKey = toString(txInfo.source_shard_id) + "_" + toString(txInfo.message_id);
    //     if (!isAborted(abortKey)) { // 未被abort的消息
    //         // 向coordinator发送成功状态消息
    //         current_candidate_tx_messageids->at(txInfo.source_shard_id - 1) = txInfo.message_id;
    //         replyToCoordinator(txInfo, group_protocolID, group_p2p_service);
    //     } else { // 若是已经被abrot的消息
    //         // 解锁
    //         m_blockingTxQueue->popTx();
    //         // 执行后续交易
    //         if (m_blockingTxQueue->size() > 0) {
    //             executeCandidateTx();
    //         }
    //     }
    // }
}

void deterministExecute::processBlockedCrossTx() {
    // 存在逻辑漏洞 pop后新的跨片交易立马插入导致两笔交易同时
    if (m_blockingCrossTxQueue->isBlocked()) {
        PLUGIN_LOG(INFO) << LOG_DESC("阻塞交易数")
                         << LOG_KV("size", m_blockingCrossTxQueue->size());

        auto txInfo = m_blockingCrossTxQueue->frontTx();
        int type = txInfo->type;
        // if (type == 1) { // 插入即执行的交易，说明晚了一步 直接返回即可
        //     return;
        // }
        // std::this_thread::sleep_for(std::chrono::milliseconds(50));
        // 解析跨片交易
        std::string allShardID = txInfo->destin_shard_id; // 用 "_" 分隔
        std::string allStateAddress = txInfo->stateAddress; // 用 "｜" 分隔
        std::string allSignedTx = txInfo->signedTx; // 用 "｜" 分隔
        std::string txIds = txInfo->txIds; // 用 "_" 分隔
        
        std::vector<std::string> shardIds;
        boost::split(shardIds, allShardID, boost::is_any_of("_"), boost::token_compress_on);
        string messageIds = "";
        // std::vector<int> crossShardsMessageID;

        for (int i = 0; i < shardIds.size(); i++) {
            // PLUGIN_LOG(INFO) << LOG_KV("shardId", shardIds.at(i));
            int shardID = atoi(shardIds.at(i).c_str());
            // crossShardsMessageID.push_back(messageIDs[shardID] + 1);
            if (i == 0) {
                messageIds = to_string(messageIDs[shardID] + 1);
            } else {
                messageIds += ("_" + to_string(messageIDs[shardID] + 1));
            }
        }
        // 修改crossTx对应的messageID
        // crossTx2ShardMessageID->insert(std::make_pair(txInfo.corss_tx_hash, crossShardsMessageID));

        std::vector<std::string> stateAddresses;
        boost::split(stateAddresses, allStateAddress, boost::is_any_of("|"), boost::token_compress_on);
        std::vector<std::string> signedTxs;
        boost::split(signedTxs, allSignedTx, boost::is_any_of("|"), boost::token_compress_on);

        for (int i = 0; i < shardIds.size(); i++) {
            auto shardID = atoi(shardIds.at(i).c_str()); // 目标分片
            auto stateAddress = stateAddresses.at(i);
            auto signedTx = signedTxs.at(i);
            int messageID = messageIDs[shardID];
            messageIDs[shardID] = ++messageID;
            
            Transaction::Ptr tx = std::make_shared<Transaction>(
                    jsToBytes(signedTx, dev::OnFailed::Throw), CheckTransaction::Everything);
            tx->setNonce(generateRandomValue());
            auto keyPair = KeyPair::create();
            auto sig = dev::crypto::Sign(keyPair, tx->hash(WithoutSignature));
            tx->updateSignature(sig);
            string newSignedTx = toHex(tx->rlp());

            
            std::vector<std::string> signedTxCount;
            auto data_str = dataToHexString(tx->data());
            boost::split(signedTxCount, data_str, boost::is_any_of("|"), boost::token_compress_on);
            int txNum = signedTxCount.size() - 2;

            // PLUGIN_LOG(INFO) << LOG_DESC("in processBlockedCrossTx 批量发送跨片子交易")
            PLUGIN_LOG(INFO) << LOG_DESC("批量发送跨片子交易")
                             << LOG_KV("subShardIds", allShardID)
                             << LOG_KV("sourceId", internal_groupId)
                             << LOG_KV("messageId", messageID)
                             << LOG_KV("destinShardID", shardID);

            protos::SubCrossShardTx subCrossShardTx;
            subCrossShardTx.set_signeddata(newSignedTx);
            subCrossShardTx.set_stateaddress(stateAddress);
            subCrossShardTx.set_sourceshardid(txInfo->source_shard_id);
            subCrossShardTx.set_destinshardid(shardID);
            subCrossShardTx.set_messageid(messageID);
            subCrossShardTx.set_crosstxhash(txInfo->cross_tx_hash); // 跨片交易哈希字符串
            subCrossShardTx.set_shardids(allShardID); // 参与跨片交易的子分片集合，通过"_"划分
            subCrossShardTx.set_messageids(messageIds);
            subCrossShardTx.set_txnum(txNum);
            subCrossShardTx.set_txids(txIds);

            std::string serializedSubCrossShardTx_str;
            subCrossShardTx.SerializeToString(&serializedSubCrossShardTx_str);
            auto txByte = asBytes(serializedSubCrossShardTx_str);

            dev::sync::SyncCrossTxPacket crossTxPacket; // 类型需要自定义
            crossTxPacket.encode(txByte);
            auto msg = crossTxPacket.toMessage(group_protocolID);

            // PLUGIN_LOG(INFO) << LOG_DESC("协调者开始向参与者分片发送跨片交易....");

            if (shardID == dev::consensus::internal_groupId) {
                //若参与者与协调者为同一分片，则直接投入交易池且发送给其它节点
                for (int j = 3; j >=1; j--) {
                    group_p2p_service->asyncSendMessageByNodeID(shardNodeId.at((shardID - 1) * 4 + j), msg, CallbackFuncWithSession(), dev::network::Options());
                }

                crossTx->insert(std::make_pair(tx->hash().abridged(), std::make_shared<transaction>(
                    1, 
                    (unsigned long)internal_groupId, 
                    (unsigned long)internal_groupId, 
                    (unsigned long)messageID,
                    (unsigned long)txNum, 
                    txInfo->cross_tx_hash, 
                    tx, 
                    stateAddress,
                    allShardID,
                    messageIds,
                    txIds)));

                m_rpc_service->sendRawTransaction(internal_groupId, toHex(tx->rlp()));
            } else { // 发送交易包给所有节点
                for(int j = 3; j >= 0; j--) { 
                    // PLUGIN_LOG(INFO) << LOG_KV("正在发送给", shardNodeId.at((shardID - 1) * 4 + j));
                    group_p2p_service->asyncSendMessageByNodeID(shardNodeId.at((shardID - 1) * 4 + j), msg, CallbackFuncWithSession(), dev::network::Options());
                }
            }

            PLUGIN_LOG(INFO) << LOG_DESC("跨片消息转发完毕...");
        }

    } else {
        // PLUGIN_LOG(INFO) << LOG_DESC("暂无阻塞交易");
    }
}
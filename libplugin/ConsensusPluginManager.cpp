#include "libconsensus/pbft/Common.h"
#include "libdevcore/CommonIO.h"
#include "libdevcore/Log.h"
#include "libplugin/Common.h"
#include <libplugin/ConsensusPluginManager.h>
#include <libethcore/CommonJS.h>
#include <libconsensus/pbft/PBFTEngine.h>
#include <cmath>

using namespace std;
using namespace dev;
using namespace dev::plugin;
using namespace dev::consensus;

void ConsensusPluginManager::updateNotLatest(std::string const _state)
{
    std::lock_guard<std::mutex> lock(x_latest_Mutex);
    not_latest.insert(_state);
}

void ConsensusPluginManager::removeNotLatest(std::string const _state)
{
    std::lock_guard<std::mutex> lock(x_latest_Mutex);
    not_latest.erase(_state);
}

bool ConsensusPluginManager::isLatest(std::string const _state)
{
    std::lock_guard<std::mutex> lock(x_latest_Mutex);
    if(not_latest.count(_state)>0) {return false;}
    return true;
}

void ConsensusPluginManager::processReceivedTx(protos::Transaction _tx)
{
    txs->push(_tx);
}

void ConsensusPluginManager::setAttribute(std::shared_ptr<dev::blockchain::BlockChainInterface> _blockchainManager, std::shared_ptr<dev::rpc::Rpc> _service) {
    m_deterministExecute->setAttribute(_blockchainManager, _service);
}


// void ConsensusPluginManager::processReceivedDisTx(protos::RLPWithReadSet _txrlp)
// {
//     distxs->push(_txrlp);
// }

void ConsensusPluginManager::processReceivedDisTx(protos::SubCrossShardTx _txrlp)
{
    // receive and storage
    protos::SubCrossShardTx msg_txWithReadset;
    msg_txWithReadset = _txrlp;
    
    std::cout << "接收到协调者发来跨片交易请求..." << std::endl;
    // auto rlp = msg_txWithReadset.subtxrlp();
    // auto readset = msg_txWithReadset.messageid();
    auto sourceShardId = msg_txWithReadset.sourceshardid();
    auto destinshardid = msg_txWithReadset.destinshardid();
    auto signeddata = msg_txWithReadset.signeddata();

    PLUGIN_LOG(INFO) << LOG_DESC("交易解析完毕")
                        << LOG_KV("signeddata", signeddata)
                        // << LOG_KV("readset", readset)
                        << LOG_KV("sourceShardId", sourceShardId)
                        << LOG_KV("destinshardid", destinshardid);

    m_rpc_service->sendRawTransaction(destinshardid, signeddata); // 通过调用本地的RPC接口发起新的共识
    // m_rpc_service->sendSubCsRawTransaction(destinshardid, signeddata, 1); // 通过调用本地的RPC接口发起新的共识
    // distxs->push(_txrlp);
}

// ADD BY ZH
void ConsensusPluginManager::sendCommitPacket(protos::SubCrossShardTxReply _txrlp) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    protos::SubCrossShardTxReply msg_status;
    msg_status = _txrlp;
    
    auto sourceShardId = msg_status.destinshardid();
    auto status = msg_status.status();
    auto crossTxHash = msg_status.crosstxhash();
    auto txNum = msg_status.txnum();

    for (auto destinShardID : crossTx2ShardID->at(crossTxHash)) {
        auto destinshardid = destinShardID;
        auto messageId = messageIDs[destinShardID];

        protos::SubCrossShardTxCommit subCrossShardTxCommit;
        subCrossShardTxCommit.set_crosstxhash(crossTxHash);
        subCrossShardTxCommit.set_commit(1);
        subCrossShardTxCommit.set_sourceshardid(sourceShardId);
        subCrossShardTxCommit.set_destinshardid(destinshardid);
        subCrossShardTxCommit.set_messageid(messageId);
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

void ConsensusPluginManager::sendCommitPacketToShard(protos::SubCrossShardTxReply _txrlp, unsigned long shardID) {
    protos::SubCrossShardTxReply msg_status;
    msg_status = _txrlp;
    
    auto sourceShardId = msg_status.sourceshardid();
    auto destinshardid = msg_status.destinshardid();
    auto status = msg_status.status();
    auto crossTxHash = msg_status.crosstxhash();
    auto messageId = msg_status.messageid();
    auto txNum = msg_status.txnum();


    protos::SubCrossShardTxCommit subCrossShardTxCommit;
    subCrossShardTxCommit.set_crosstxhash(crossTxHash);
    subCrossShardTxCommit.set_commit(1);
    subCrossShardTxCommit.set_sourceshardid(destinshardid);
    subCrossShardTxCommit.set_destinshardid(sourceShardId);
    subCrossShardTxCommit.set_messageid(messageId);
    subCrossShardTxCommit.set_txnum(txNum);

    std::string serializedSubCrossShardTxCommit_str;
    subCrossShardTxCommit.SerializeToString(&serializedSubCrossShardTxCommit_str);
    auto txByte = asBytes(serializedSubCrossShardTxCommit_str);

    dev::sync::SyncCrossTxCommitPacket crossTxCommitPacket; // 类型需要自定义
    crossTxCommitPacket.encode(txByte);
    auto msg = crossTxCommitPacket.toMessage(group_protocolID);

    PLUGIN_LOG(INFO) << LOG_DESC("协调者共识早已完毕, 开始向落后分片发送跨片交易提交消息包....")
                     << LOG_KV("group_protocolID", group_protocolID);

    // 向子分片的每个节点发送交易
    for(size_t j = 0; j < 4; j++)  // 给所有节点发
    {
        // PLUGIN_LOG(INFO) << LOG_KV("正在发送给", shardNodeId.at((shardID - 1) * 4 + j));
        group_p2p_service->asyncSendMessageByNodeID(shardNodeId.at((shardID - 1) * 4 + j), msg, CallbackFuncWithSession(), dev::network::Options());
    }
    
    PLUGIN_LOG(INFO) << LOG_DESC("commit消息发送完毕...")
                     << LOG_KV("目标", shardID);
}

void ConsensusPluginManager::processReceivedCrossTx(protos::SubCrossShardTx _txrlp) {
    lock_guard<std::mutex> lock(m_crossTxMutex);

    // receive and storage
    protos::SubCrossShardTx msg;
    msg = _txrlp;
    
    // PLUGIN_LOG(INFO) << LOG_DESC("接收到协调者发来跨片交易请求...");
    auto stateAddress = msg.stateaddress();
    auto sourceShardId = msg.sourceshardid();
    auto destinshardId = msg.destinshardid();
    auto signedDatas = msg.signeddata();
    auto messageId = msg.messageid();
    auto crossTxHash = msg.crosstxhash();
    auto shardIds = msg.shardids();
    auto messageIds = msg.messageids();
    auto txNum = msg.txnum();

    // 若该跨片交易已被其它分片abort，则直接不处理
    string abortKey = toString(sourceShardId) + "_" + shardIds + "_" + messageIds;
    if (m_deterministExecute->isAborted(abortKey)) {
        return;
    }

    // 存储跨片交易信息 用于发送res给coordinator
    Transaction::Ptr tx = std::make_shared<Transaction>(
            jsToBytes(signedDatas, dev::OnFailed::Throw), CheckTransaction::Everything);
    
    // 制造交易，利用data字段对signedDat进行二次包装 ==> 由于各个节点的结果不一致 放由协调者运行
    // Transaction::Ptr tx = createBatchTransaction(signedDatas);

    PLUGIN_LOG(INFO) << LOG_DESC("交易解析完毕")
                     << LOG_KV("messageId", messageId)
                     << LOG_KV("messageIds", messageIds)
                     << LOG_KV("sourceShardId", sourceShardId)
                     << LOG_KV("destinshardid", destinshardId)
                     << LOG_KV("crossTxNum", txNum)
                     << LOG_KV("shardIds", shardIds)
                     << LOG_KV("crossTxHash", crossTxHash)
                    //  << LOG_KV("signeddata", signedDatas)
                     << LOG_KV("stateAddress", stateAddress); // 多个状态集通过"_"来划分 

    crossTx->insert(std::make_pair(tx->hash().abridged(), std::make_shared<transaction>(
        1, 
        (unsigned long)sourceShardId, 
        (unsigned long)destinshardId, 
        (unsigned long)messageId,
        (unsigned long)txNum, 
        crossTxHash, 
        tx, 
        stateAddress,
        shardIds,
        messageIds)));

    // 判断当前节点是否为头节点
    // PLUGIN_LOG(INFO) << LOG_DESC("当前forwardNodeId为: ") << LOG_DESC(toHex(forwardNodeId.at(i)));
    
    if(nodeIdStr == toHex(forwardNodeId.at(dev::consensus::internal_groupId - 1))){
        PLUGIN_LOG(INFO) << LOG_DESC("匹配成功，当前节点为头节点")
                         << LOG_KV("messageId", messageId)
                         << LOG_KV("sourceId", sourceShardId);
                        //  << LOG_KV("rlp", toHex(tx->rlp()));
        // 通过调用本地的RPC接口发起新的共识
        m_rpc_service->sendRawTransaction(destinshardId, toHex(tx->rlp()));
    }
}

void ConsensusPluginManager::processReceivedCrossTxReply(protos::SubCrossShardTxReply _txrlp) {
    lock_guard<std::mutex> lock(m_crossTx2ReceivedMsgMutex);

    protos::SubCrossShardTxReply msg_status;
    msg_status = _txrlp;
    
    // PLUGIN_LOG(INFO) << "接收到子分片发来的跨片交易状态消息包...";
    auto sourceShardId = msg_status.sourceshardid();
    auto destinShardId = msg_status.destinshardid();
    auto status = msg_status.status();
    auto crossTxHash = msg_status.crosstxhash();
    auto messageId = msg_status.messageid();
    auto shardIds = msg_status.shardids();
    auto messageIds = msg_status.messageids();

    // std::this_thread::sleep_for(std::chrono::milliseconds(12));

    // 更新非主节点的messageID变量
    if (messageIDs[sourceShardId] < messageId) {
        messageIDs[sourceShardId] = messageId;
    }

    // 若该跨片交易已被abort，则其对应reply包消息不处理
    string abortKey = toString(sourceShardId) + "_" + shardIds + "_" + messageIds;
    if (m_deterministExecute->isAborted(abortKey) || messageIDs[sourceShardId] > messageId) {
        return;
    }

    PLUGIN_LOG(INFO) << LOG_DESC("跨片交易回执包解析完毕")
                    //  << LOG_KV("status", status)
                     << LOG_KV("messageId", messageId)
                     << LOG_KV("sourceShardId", sourceShardId)
                     << LOG_KV("destinShardId", destinShardId)
                     << LOG_KV("crossTxHash", crossTxHash);

    if ((int)status != 1) 
        return;

    // 获取涉及线性安全的变量
    int msgCount;
    // int msgNum = 0;

    // m_crossTx2ReceivedMsgMutex.lock(); 
    msgCount = crossTx2ReceivedMsg->count(crossTxHash);
    // if (msgCount != 0) {
    //     msgNum = count(crossTx2ReceivedMsg->at(crossTxHash).begin(), crossTx2ReceivedMsg->at(crossTxHash).end(), (int)sourceShardId);
    // }

    // 已经收到了大多数包就直接收取而不执行后面的逻辑，防止重复执行
    // 待添加逻辑：若是对应交易hash已经完成2PC，则不再收取
    if (msgCount == 0 && doneCrossTx->find(crossTxHash) != doneCrossTx->end()) {
        PLUGIN_LOG(INFO) << LOG_DESC("晚了，对于:") << LOG_KV("id", (int)sourceShardId);
        // 如果是已完成交易 ==> 可能是有的子分片过于落后 ==> 再次发送提交消息包
        sendCommitPacketToShard(_txrlp, sourceShardId);
        return;
    }

    // if (msgCount != 0 && msgNum >= 3) {
    //     PLUGIN_LOG(INFO) << LOG_DESC("够了，对于:") << LOG_KV("id", (int)sourceShardId);
    //     return;
    // }

    // 遍历crossTx2ReceivedMsg->at(crossTxHash)
    // PLUGIN_LOG(INFO) << LOG_DESC("遍历crossTx2ReceivedMsg->at(crossTxHash)");
    // if(crossTx2ReceivedMsg->count(crossTxHash)!=0)
    // {
    //     for (int i = 0; i < crossTx2ReceivedMsg->at(crossTxHash).size(); i++) {
    //         PLUGIN_LOG(INFO) << LOG_DESC("目前收到的reply包的id有")
    //                          << LOG_KV("id", crossTx2ReceivedMsg->at(crossTxHash)[i]);
    //     }
    // }

    // 存消息
    if (msgCount == 0) {
        std::vector<std::string> temp;
        temp.push_back(to_string(sourceShardId) + "_" + to_string(messageId));
        crossTx2ReceivedMsg->insert(std::make_pair(crossTxHash, temp));
        return;
    } else {
        crossTx2ReceivedMsg->at(crossTxHash).push_back(to_string(sourceShardId) + "_" + to_string(messageId));
    }
    
    // 判断是否收集足够的包
    std::vector<std::string> allMessageID;
    boost::split(allMessageID, messageIds, boost::is_any_of("_"), boost::token_compress_on);
    std::vector<std::string> allShardID;
    boost::split(allShardID, shardIds, boost::is_any_of("_"), boost::token_compress_on);
    

    // 根据messageId包的数量来确定是否收齐而不是对应分片包的数量
    // 因为一次交易可能会产生多个messageId从而使得主节点收到同一个shard的不同messageId回执
    for (int i = 0; i < allMessageID.size(); i++) {
        // int sId = atoi(allShardID.at(i).c_str());
        // int mId = atoi(allMessageID.at(i).c_str());
        int threshold = 4;
        if (atoi(allShardID.at(i).c_str()) == internal_groupId) {
            threshold = 3;
        }
        string id = allShardID.at(i) + "_" + allMessageID.at(i);
        if (count(crossTx2ReceivedMsg->at(crossTxHash).begin(), crossTx2ReceivedMsg->at(crossTxHash).end(), id) < threshold) {
            PLUGIN_LOG(INFO) << LOG_DESC("回执包数量不足")
                             << LOG_KV("id", id);
            return;
        }
    }

    // 集齐 -> 发送commit包
    PLUGIN_LOG(INFO) << LOG_DESC("状态包集齐, 开始发送commit消息包...");
    sendCommitPacket(_txrlp);
}

void ConsensusPluginManager::processReceivedCrossTxCommit(protos::SubCrossShardTxCommit _txrlp) {
    lock_guard<std::mutex> lock(m_crossTx2CommitMsgMutex);
    
    // 解析消息包
    protos::SubCrossShardTxCommit msg_status;
    msg_status = _txrlp;
    
    // PLUGIN_LOG(INFO) << "接收到协调者发来的跨片交易提交消息包...";
    auto commit = msg_status.commit();
    auto crossTxHash = msg_status.crosstxhash();
    auto sourceShardId = msg_status.sourceshardid();
    auto destinshardid = msg_status.destinshardid();
    auto messageId = msg_status.messageid();
    auto crossTxNum = msg_status.txnum();


    PLUGIN_LOG(INFO) << LOG_DESC("跨片交易提交包解析完毕")
                    //  << LOG_KV("commit", commit)
                     << LOG_KV("messageId", messageId)
                     << LOG_KV("sourceShardId", sourceShardId)
                     << LOG_KV("crossTxHash", crossTxHash);

    if ((int)commit != 1) 
        return;

    //获取涉及线程安全的变量
    int msgCount;
    int commitMsgNum = 0;

    msgCount = crossTx2CommitMsg->count(crossTxHash);
    
    if (msgCount != 0) {
        commitMsgNum = crossTx2CommitMsg->at(crossTxHash);
    }

    // 如果早已收到了足够的包就停止收取，防止重复执行
    if ((msgCount == 0 && doneCrossTx->find(crossTxHash) != doneCrossTx->end())
        ||
        (msgCount != 0 && commitMsgNum >= 3)) {
        PLUGIN_LOG(INFO) << LOG_DESC("够了") << LOG_KV("crossTx", crossTxHash);
        return;
    }
    
    // 存储消息
    if (crossTx2CommitMsg->count(crossTxHash) == 0) {
        crossTx2CommitMsg->insert(std::make_pair(crossTxHash, 1));
        commitMsgNum = 1;
    } else {
        crossTx2CommitMsg->at(crossTxHash)++;
        commitMsgNum++;
    }
    

    PLUGIN_LOG(INFO) << LOG_DESC("跨片交易提交包添加完毕")
                     << LOG_KV("commitMsgNum", crossTx2CommitMsg->at(crossTxHash))
                     << LOG_KV("messageId", messageId)
                     << LOG_KV("commitMsgNum", commitMsgNum);

    // 判断跨片交易是否满足提交条件
    // 存在两个消息包一起加1后进入该判断语句的可能性，导致executeCrossTx被调用两次
    if (commitMsgNum == 3) {
        auto currentMessageId = current_candidate_tx_messageids->at(sourceShardId - 1);
        // ADD BY ZH -- 22.11.16
        // 考虑commit包集齐但跨片交易还没收到的情况：每个节点收到包的顺序不一致
        // 解决方案：将集齐的相关交易先进行存储，待对应交易收到后立马执行
        PLUGIN_LOG(INFO) << LOG_DESC("跨片交易提交包收齐")
                         << LOG_KV("packet messageId", messageId)
                         << LOG_KV("current messageId", currentMessageId);
        
        //  如果收到的commit包的messageId不是当前待处理的最小messageId包，则存储
        if (messageId > currentMessageId) {
            PLUGIN_LOG(INFO) << LOG_DESC("commit包集齐, 但相关跨片交易尚未收到, 存储")
                             << LOG_KV("messageId", messageId);

            lateCrossTxMessageId->insert(messageId);
            return;
        }

        // 执行交易
        PLUGIN_LOG(INFO) << LOG_DESC("commit包集齐, 子分片开始执行并提交相关跨片交易...")
                         << LOG_KV("messageId", messageId);
                        
        // auto readwriteset = crossTx2StateAddress->at(crossTxHash);
        m_deterministExecute->executeCrossTx(sourceShardId, messageId);
        // groupVerifier->executeCrossTx(readwriteset);
        // crossTx2StateAddress->unsafe_erase(crossTxHash);

        if (sourceShardId == internal_groupId && lateCommitReplyMessageId->find(messageId) != lateCommitReplyMessageId->end()) {
            doneCrossTx->insert(crossTxHash);

            m_deterministExecute->executedTx += crossTxNum;
            PLUGIN_LOG(INFO) << LOG_DESC("跨片交易流程完成... in processReceivedCrossTxCommit")
                             << LOG_KV("messageId", messageId)
                             << LOG_KV("executedTx", m_deterministExecute->executedTx);

            // 非头节点不必转发
            if(dev::plugin::nodeIdStr != toHex(forwardNodeId.at(internal_groupId - 1)))
            {
                return;
            }
            m_deterministExecute->m_blockingCrossTxQueue->popTx();
            m_deterministExecute->processBlockedCrossTx();
        }
    
        return;
    }

    PLUGIN_LOG(INFO) << LOG_DESC("跨片交易暂不满足提交条件")
                     << LOG_KV("commitMsgNum", crossTx2CommitMsg->at(crossTxHash))
                     << LOG_KV("messageId", messageId)
                     << LOG_KV("sourceId", sourceShardId);

}

void ConsensusPluginManager::processReceivedCrossTxCommitReply(protos::SubCrossShardTxCommitReply _txrlp) {
    lock_guard<std::mutex> lock(m_crossTx2ReceivedCommitMsgMutex);
    
    // 解析消息包
    protos::SubCrossShardTxCommitReply msg_status;
    msg_status = _txrlp;
    
    // PLUGIN_LOG(INFO) << "接收到子分片发来的跨片交易提交回执包...";
    auto status = msg_status.status();
    auto crossTxHash = msg_status.crosstxhash();
    auto sourceShardId = msg_status.sourceshardid();
    auto destinshardid = msg_status.destinshardid();
    auto messageId = msg_status.messageid();
    auto txNum = msg_status.txnum();

    PLUGIN_LOG(INFO) << LOG_DESC("交易执行成功消息包解析完毕")
                     << LOG_KV("status", status)
                     << LOG_KV("messageId", messageId)
                     << LOG_KV("sourceShardId", sourceShardId)
                     << LOG_KV("crossTxHash", crossTxHash)
                     << LOG_KV("crossTxNum", txNum);

    if ((int)status != 1) 
        return;

    // 获取涉及多线程的变量
    int msgCount;
    // int receivedMsgNum = 0;

    msgCount = crossTx2ReceivedCommitMsg->count(crossTxHash);
    // if (msgCount != 0) {
    //     receivedMsgNum = count(crossTx2ReceivedCommitMsg->at(crossTxHash).begin(), crossTx2ReceivedCommitMsg->at(crossTxHash).end(), (int)sourceShardId);
    // }

    // 已经收到了大多数包就停止收取，防止重复执行
    // 历史交易包也不会收——22.11.6
    // 修改逻辑：收齐全部四个节点的commitreply包再执行下一笔
    // if ((msgCount == 0 && doneCrossTx->find(crossTxHash) != doneCrossTx->end()) 
    //     || 
    //     (msgCount != 0 && receivedMsgNum >= 3)) {
    //     PLUGIN_LOG(INFO) << LOG_DESC("交易提交回执包晚了/够了，对于:")
    //                      << LOG_KV("id", (int)sourceShardId);
    //     // m_crossTx2ReceivedCommitMsgMutex.unlock();
    //     return;
    // }

    // 存储消息
    if (msgCount == 0) {
        std::vector<int> temp;
        temp.push_back((int)sourceShardId);
        crossTx2ReceivedCommitMsg->insert(std::make_pair(crossTxHash, temp));
        // 必定不可能集齐 直接返回
        return;
    } else {
        crossTx2ReceivedCommitMsg->at(crossTxHash).push_back((int)sourceShardId);
    }

    // 判断是否收集足够（全部）的包
    for (auto i : crossTx2ShardID->at(crossTxHash)) {
        int threshold = 4;
        if (i == internal_groupId) {
            threshold = 3;
        }
        if (count(crossTx2ReceivedCommitMsg->at(crossTxHash).begin(), crossTx2ReceivedCommitMsg->at(crossTxHash).end(), i) < threshold) {
            return;
        }
    }
    // m_crossTx2ReceivedCommitMsgMutex.unlock();

    /*
      集齐 ==> 删除相关变量
        1. crossTx2ShardID
        2. crossTx2ReceivedMsg
        3. crossTx2ReceivedCommitMsg
    */
    // PLUGIN_LOG(INFO) << LOG_DESC("commit reply包集齐, 协调者分片开始删除相关变量...")
    //                  << LOG_KV("messageId", messageId);
    // crossTx2ShardID->unsafe_erase(crossTxHash);
    // crossTx2ReceivedMsg->unsafe_erase(crossTxHash);
    // crossTx2ReceivedCommitMsg->unsafe_erase(crossTxHash);

    // 本节点还未到提交阶段，分片内其它节点都已到提交阶段
    if (sourceShardId == internal_groupId) {
        lock_guard<std::mutex> lock(m_crossTx2CommitMsgMutex);
        int count = crossTx2CommitMsg->count(crossTxHash);
        if (count ==0 || (count != 0 && crossTx2CommitMsg->at(crossTxHash) < 3)) {
            lateCommitReplyMessageId->insert(messageId);
            return;
        }
    }

    // 添加doneCrossTx，防止收到历史交易包——22.11.6
    m_deterministExecute->executedTx += txNum;
    PLUGIN_LOG(INFO) << LOG_DESC("跨片交易流程完成...")
                     << LOG_KV("messageId", messageId)
                     << LOG_KV("executedTx", m_deterministExecute->executedTx);

    // 非头节点不必转发
    if(dev::plugin::nodeIdStr != toHex(forwardNodeId.at(internal_groupId - 1)))
    {
        return;
    }
    m_deterministExecute->m_blockingCrossTxQueue->popTx();
    m_deterministExecute->processBlockedCrossTx();
}


int ConsensusPluginManager::getRand(int a, int b) {
    srand((unsigned)time(NULL));
    return (rand() % (b - a + 1)) + a;
}

void ConsensusPluginManager::processReceivedAbortMessage(protos::AbortMsg _txrlp) {
    lock_guard<std::mutex> lock(m_abortMsgMutex);
    
    // 解析消息包
    protos::AbortMsg msg_status;
    msg_status = _txrlp;
    
    auto coorShardId = msg_status.coorshardid();
    auto subShardsId = msg_status.subshardsid();
    auto messageId = msg_status.messageid();
    auto messageIds = msg_status.messageids();

    PLUGIN_LOG(INFO) << LOG_DESC("abort包解析完毕")
                     << LOG_KV("coorShardId", coorShardId)
                     << LOG_KV("subShardsId", subShardsId)
                     << LOG_KV("messageId", messageId)
                     << LOG_KV("messageIds", messageIds);
    
    // 判断该abort消息包是否之前已经处理过 ==> 查询abortSet
    //                           否则 ==> 插入abortKey
    string abortKey = toString(coorShardId) + "_" + subShardsId + "_" + messageIds;
    if (abortSet->find(abortKey) != abortSet->end()) {
        PLUGIN_LOG(INFO) << LOG_DESC("该abort消息包之前处理过...")
                         << LOG_KV("abortKey", abortKey);
        return;
    } else {
        abortSet->insert(abortKey);
    }
    
    if (coorShardId == dev::consensus::internal_groupId) {
        // 协调者节点处理abort消息包
        // 若协调者也是参与者，当收到其它参与者分片发送的abort包时，同样要处理阻塞队列
        std::vector<std::string> shardIds;
        boost::split(shardIds, subShardsId, boost::is_any_of("_"), boost::token_compress_on);
        for (int i = shardIds.size() - 1; i >= 0; i--) {
            if (atoi(shardIds.at(i).c_str()) == coorShardId) {
                m_deterministExecute->m_blockingTxQueue->popAbortedTx(to_string(coorShardId));
                break;
            }
        }
        if (nodeIdStr == toHex(forwardNodeId.at(coorShardId - 1))) {
            // 1. 停随机一段时间 -- [0,100)ms
            int randomTime = getRand(0, 10) * 10;
            std::this_thread::sleep_for(std::chrono::milliseconds(randomTime));
            // 2. 再次发送跨片交易消息包
            m_deterministExecute->processBlockedCrossTx();
            PLUGIN_LOG(INFO) << LOG_DESC("协调者头节点处理abort消息包完毕");
        }
    } else {
        // 子分片节点处理abort消息包
        // 阻塞队列中是否有交易
        if (m_deterministExecute->m_blockingTxQueue->size() > 0) {
            // 1. 尝试释放相关交易和锁
            if(m_deterministExecute->m_blockingTxQueue->popAbortedTx(to_string(coorShardId))) {
                BLOCKVERIFIER_LOG(INFO) << LOG_DESC("in processAbortMessage... popAbort成功");
                // 2. 执行队列中后续交易
                if (m_deterministExecute->m_blockingTxQueue->size() > 0) {
                    BLOCKVERIFIER_LOG(INFO) << LOG_DESC("in processAbortMessage... 还有等待的交易");
                    m_deterministExecute->executeCandidateTx();
                }
            }
        }
        PLUGIN_LOG(INFO) << LOG_DESC("子分片处理abort消息包完毕");
    }
}

void ConsensusPluginManager::processReceivedPreCommitedTx(protos::SubPreCommitedDisTx _txrlp)
{

    // auto rlp = msg_committedRLPWithReadSet.subtxrlp();
    // auto readset = msg_committedRLPWithReadSet.readset();
    // auto contractAddress = msg_committedRLPWithReadSet.contractaddress();

    // std::cout<< "参与者收到了commit命令, 开始执行被阻塞交易..." << std::endl;
    // // std::cout << "rlp = " << rlp << std::endl;
    // // std::cout << "readset = " << readset << std::endl;
    // // std::cout << "contractAddress = " << contractAddress << std::endl;

    // std::vector<std::string> committed_txrlp;
    // committed_txrlp.push_back(rlp);
    // committed_txrlp.push_back(readset);
    // committed_txrlp.push_back(contractAddress);

    // // //参与者记录所有收到的 committed 交易， 交易rlp、交易地址、交易读集
    // dev::plugin::receCommTxRlps.push(committed_txrlp); // 用队列去接收管道消息

    precommit_txs->push(_txrlp);
}

void ConsensusPluginManager::processReceivedCommitedTx(protos::CommittedRLPWithReadSet _txrlp)
{
    // std::cout << "P2P模块收到comitted交易, 压入缓存堆栈" << std::endl;
    // 对收到的commit交易进行缓存

    commit_txs->push(_txrlp);
}

void ConsensusPluginManager::processReceivedWriteSet(protos::TxWithReadSet _rs)
{
    readSetQueue->push(_rs);
}

int ConsensusPluginManager::numOfNotFinishedDAGs()
{
    return notFinishedDAG;
}

int ConsensusPluginManager::addNotFinishedDAGs(int _num)
{
    notFinishedDAG += _num;
}

u256 ConsensusPluginManager::getLatestState(std::string _addr){

    if(testMap.count(_addr)>0){
        return testMap[_addr];
    }else{
        testMap.insert(std::make_pair(_addr, u256(0)));
        return u256(0);
    }
}

h512 ConsensusPluginManager::getNodeId(int _index)
{
    return exNodeId[_index]; // 
}
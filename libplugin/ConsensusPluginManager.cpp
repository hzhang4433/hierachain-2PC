#include "libconsensus/pbft/Common.h"
#include "libdevcore/CommonIO.h"
#include "libdevcore/Log.h"
#include "libplugin/Common.h"
#include <libplugin/ConsensusPluginManager.h>
#include <libethcore/CommonJS.h>
#include <libconsensus/pbft/PBFTEngine.h>
#include <cmath>
#include <cstdlib>

using namespace std;
using namespace dev;
using namespace dev::plugin;
using namespace dev::consensus;

// void ConsensusPluginManager::updateNotLatest(std::string const _state)
// {
//     std::lock_guard<std::mutex> lock(x_latest_Mutex);
//     not_latest.insert(_state);
// }

// void ConsensusPluginManager::removeNotLatest(std::string const _state)
// {
//     std::lock_guard<std::mutex> lock(x_latest_Mutex);
//     not_latest.erase(_state);
// }

// bool ConsensusPluginManager::isLatest(std::string const _state)
// {
//     std::lock_guard<std::mutex> lock(x_latest_Mutex);
//     if(not_latest.count(_state)>0) {return false;}
//     return true;
// }

void ConsensusPluginManager::setAttribute(std::shared_ptr<dev::blockchain::BlockChainInterface> _blockchainManager, std::shared_ptr<dev::rpc::Rpc> _service) {
    m_deterministExecute->setAttribute(_blockchainManager, _service);
}

void ConsensusPluginManager::sendCommitPacket(protos::SubCrossShardTxReply _txrlp) {
    // std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    protos::SubCrossShardTxReply msg_status;
    msg_status = _txrlp;
    
    auto sourceShardId = msg_status.destinshardid();
    auto status = msg_status.status();
    auto crossTxHash = msg_status.crosstxhash();
    auto txNum = msg_status.txnum();
    auto shardIds = msg_status.shardids();
    auto messageIds = msg_status.messageids();
    auto txIds = msg_status.txids();

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
        subCrossShardTxCommit.set_txids(txIds);

        std::string serializedSubCrossShardTxCommit_str;
        subCrossShardTxCommit.SerializeToString(&serializedSubCrossShardTxCommit_str);
        auto txByte = asBytes(serializedSubCrossShardTxCommit_str);

        dev::sync::SyncCrossTxCommitPacket crossTxCommitPacket; // 类型需要自定义
        crossTxCommitPacket.encode(txByte);
        auto msg = crossTxCommitPacket.toMessage(group_protocolID);

        // 向子分片的每个节点发送交易
        for(size_t j = 0; j < 4; j++)  // 给子分片所有节点发
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
    auto shardIds = msg_status.shardids();
    auto messageIds = msg_status.messageids();
    auto txNum = msg_status.txnum();
    // auto txIds = msg_status.txids();


    protos::SubCrossShardTxCommit subCrossShardTxCommit;
    subCrossShardTxCommit.set_crosstxhash(crossTxHash);
    subCrossShardTxCommit.set_commit(1);
    subCrossShardTxCommit.set_sourceshardid(destinshardid);
    subCrossShardTxCommit.set_destinshardid(sourceShardId);
    subCrossShardTxCommit.set_messageid(messageId);
    subCrossShardTxCommit.set_shardids(shardIds);
    subCrossShardTxCommit.set_messageids(messageIds);
    subCrossShardTxCommit.set_txnum(txNum);
    // subCrossShardTxCommit.set_txids(txIds);

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

void ConsensusPluginManager::replyToCoordinatorCommitOK(protos::SubCrossShardTxCommit _txrlp) {

    // 解析消息包
    protos::SubCrossShardTxCommit msg_status;
    msg_status = _txrlp;
    
    // PLUGIN_LOG(INFO) << "接收到协调者发来的跨片交易提交消息包...";
    auto crossTxHash = msg_status.crosstxhash();
    auto source_shard_id = msg_status.sourceshardid();
    auto destin_shard_id = msg_status.destinshardid();
    auto messageId = msg_status.messageid();
    auto shardIds = msg_status.shardids();
    auto txNum = msg_status.txnum();
    auto txIds = msg_status.txids();
    
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

    PLUGIN_LOG(INFO) << LOG_DESC("跨片交易执行完成, 向协调者分片发送commitOK消息包....")
                     << LOG_KV("destinShardId", source_shard_id)
                     << LOG_KV("messageId", messageId);

    // std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    for(size_t j = 0; j < 4; j++)  // 给所有协调者分片所有节点发
    {
        // PLUGIN_LOG(INFO) << LOG_KV("正在发送给", shardNodeId.at((source_shard_id-1)*4 + j));
        group_p2p_service->asyncSendMessageByNodeID(shardNodeId.at((source_shard_id-1)*4 + j), msg, CallbackFuncWithSession(), dev::network::Options());
    }
    PLUGIN_LOG(INFO) << LOG_DESC("发送commitOK消息包完毕...");

    // 发送完回执再删除相关变量，假设此时以收到了所有的commitMsg
    // crossTx2CommitMsg->unsafe_erase(txInfo.cross_tx_hash);
}

void ConsensusPluginManager::pushReceivedCrossTx(protos::SubCrossShardTx _txrlp) {
  crossTxs->push(_txrlp);
}
void ConsensusPluginManager::pushReceivedCrossTxReply(protos::SubCrossShardTxReply _txrlp) {
  crossTxReplys->push(_txrlp);
}
void ConsensusPluginManager::pushReceivedCrossTxCommit(protos::SubCrossShardTxCommit _txrlp) {
  commitTxs->push(_txrlp);
}
void ConsensusPluginManager::pushReceivedCrossTxCommitReply(protos::SubCrossShardTxCommitReply _txrlp) {
  commitTxReplys->push(_txrlp);
}

void ConsensusPluginManager::pushReceivedAbortMessage(protos::AbortMsg _txrlp) {
  abortTxs->push(_txrlp);
}

void ConsensusPluginManager::receiveRemoteMsgWorker() {
    PLUGIN_LOG(INFO) << "receiveRemoteMsgWorker 线程开启...";

    protos::SubCrossShardTx msg_crossShardTx; 
    protos::SubCrossShardTxReply msg_crossShardTxReply; 
    protos::SubCrossShardTxCommit msg_commitTx;
    protos::SubCrossShardTxCommitReply msg_commitTxReply;
    protos::AbortMsg msg_abort;

    bool got_message=false;
    while(true) {
        // 处理上层分片发来的批量跨片交易
        got_message = crossTxs->try_pop(msg_crossShardTx);
        if(got_message == true) {
            processReceivedCrossTx(msg_crossShardTx);
        }

        // 处理来自参与者的跨片交易回复交易包
        got_message = crossTxReplys->try_pop(msg_crossShardTxReply);
        if(got_message == true){
            processReceivedCrossTxReply(msg_crossShardTxReply);
        }

        // 处理来自上层分片的提交交易包
        got_message = commitTxs->try_pop(msg_commitTx);
        if(got_message == true){
            processReceivedCrossTxCommit(msg_commitTx);
        }
        
        // 处理来自参与者分片的提交状态交易包
        got_message = commitTxReplys->try_pop(msg_commitTxReply);
        if(got_message == true){
            processReceivedCrossTxCommitReply(msg_commitTxReply);
        }

        // 处理abort消息包
        got_message = abortTxs->try_pop(msg_abort);
        if(got_message == true) {
            processReceivedAbortMessage(msg_abort);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(50)); // 模拟跨分片通信延迟
    }
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
    auto txIds = msg.txids();

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

    PLUGIN_LOG(INFO) << LOG_DESC("'跨片交易'消息包解析完毕")
                     << LOG_KV("messageId", messageId)
                    //  << LOG_KV("messageIds", messageIds)
                     << LOG_KV("sourceShardId", sourceShardId)
                     << LOG_KV("destinshardid", destinshardId);
                    //  << LOG_KV("crossTxNum", txNum)
                    //  << LOG_KV("shardIds", shardIds)
                    //  << LOG_KV("crossTxHash", crossTxHash)
                    //  << LOG_KV("txIds", txIds)
                    //  << LOG_KV("stateAddress", stateAddress); // 多个状态集通过"_"来划分 

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
        messageIds,
        txIds)));

    // 判断当前节点是否为头节点
    // PLUGIN_LOG(INFO) << LOG_DESC("当前forwardNodeId为: ") << LOG_DESC(toHex(forwardNodeId.at(i)));
    
    if(nodeIdStr == toHex(forwardNodeId.at(dev::consensus::internal_groupId - 1))){
        PLUGIN_LOG(INFO) << LOG_DESC("匹配成功, 当前节点为头节点, 发起该笔跨片交易共识");
                        //  << LOG_KV("messageId", messageId)
                        //  << LOG_KV("sourceShardId", sourceShardId);
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

    PLUGIN_LOG(INFO) << LOG_DESC("'跨片交易回执'消息包解析完毕")
                     << LOG_KV("messageId", messageId)
                     << LOG_KV("sourceShardId", sourceShardId)
                     << LOG_KV("destinShardId", destinShardId)
                     << LOG_KV("status", status);
                    //  << LOG_KV("crossTxHash", crossTxHash)
                    //  << LOG_KV("shardIds", shardIds)
                    //  << LOG_KV("messageIds", messageIds);

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
    } else {
        string id = to_string(sourceShardId) + "_" + to_string(messageId);
        int threshold = 1;
        // int threshold = 3;
        if (sourceShardId == internal_groupId) {
          threshold = 0;
          // threshold = 2;
        }
        if (count(crossTx2ReceivedMsg->at(crossTxHash).begin(), crossTx2ReceivedMsg->at(crossTxHash).end(), id) < threshold) {
          crossTx2ReceivedMsg->at(crossTxHash).push_back(id);
        }
        // crossTx2ReceivedMsg->at(crossTxHash).push_back(id);
    }
    
    // 判断是否收集足够的包
    std::vector<std::string> allMessageID;
    boost::split(allMessageID, messageIds, boost::is_any_of("_"), boost::token_compress_on);
    std::vector<std::string> allShardID;
    boost::split(allShardID, shardIds, boost::is_any_of("_"), boost::token_compress_on);
    

    // // 根据messageId包的数量来确定是否收齐而不是对应分片包的数量
    // // 因为一次交易可能会产生多个messageId从而使得主节点收到同一个shard的不同messageId回执
    // for (int i = 0; i < allMessageID.size(); i++) {
    //     // int sId = atoi(allShardID.at(i).c_str());
    //     // int mId = atoi(allMessageID.at(i).c_str());
    //     int threshold = 4;
    //     if (atoi(allShardID.at(i).c_str()) == internal_groupId) {
    //         threshold = 3;
    //     }
    //     string id = allShardID.at(i) + "_" + allMessageID.at(i);
    //     if (count(crossTx2ReceivedMsg->at(crossTxHash).begin(), crossTx2ReceivedMsg->at(crossTxHash).end(), id) < threshold) {
    //         PLUGIN_LOG(INFO) << LOG_DESC("回执包数量不足")
    //                          << LOG_KV("id", id);
    //         return;
    //     }
    // }

    // 根据messageId包的数量来确定是否收齐而不是对应分片包的数量
    // 因为一次交易可能会产生多个messageId从而使得主节点收到同一个shard的不同messageId回执
    int flag = false; // 标志协调者是否为参与者
    int corMessageId; // 记录次消息包对应的协调者id
    for (int i = 0; i < allMessageID.size(); i++) {
        int threshold = 1; //one2all
        // int threshold = 3; // normal
        if (atoi(allShardID.at(i).c_str()) == internal_groupId) {
            threshold = 0; //one2all
            // threshold = 2; // normal
            flag = true;
            corMessageId = atoi(allMessageID.at(i).c_str());
        }
        string id = allShardID.at(i) + "_" + allMessageID.at(i);
        int packetNum = count(crossTx2ReceivedMsg->at(crossTxHash).begin(), crossTx2ReceivedMsg->at(crossTxHash).end(), id);

        if (packetNum != threshold) {
            PLUGIN_LOG(INFO) << LOG_DESC("回执包数量不足")
                             << LOG_KV("i", i)
                             << LOG_KV("packetNum", packetNum);
            return;
        }
    }

    int currentMessageId = current_candidate_tx_messageids->at(internal_groupId - 1);
    if (flag && currentMessageId < corMessageId) { // 若协调者尚未共识到该条交易，则先保存，后续共识完成后直接发送commit包
        PLUGIN_LOG(INFO) << LOG_DESC("reply包集齐, 但相关跨片交易尚未收到, 存储等待");
        lateReplyMessageId->insert(corMessageId);
        return;
    }

    // 集齐 -> 发送commit包
    PLUGIN_LOG(INFO) << LOG_DESC("状态包集齐, 开始发送commit消息包...");
    // 只由头节点发送commit包
    if (nodeIdStr == toHex(forwardNodeId.at(dev::consensus::internal_groupId - 1))) {
        sendCommitPacket(_txrlp);
        if (flag) { // 协调者也是参与者，主节点直接进行commit操作
            // auto messageId = messageIDs[internal_groupId];
            // auto currentMessageId = current_candidate_tx_messageids->at(internal_groupId - 1);

            // // 考虑commit包集齐但跨片交易还没收到的情况：每个节点收到包的顺序不一致
            // // 解决方案：将集齐的相关交易先进行存储，待对应交易收到后立马执行
            // //         如果收到的commit包的messageId不是当前待处理的最小messageId包，则存储
            // if (messageId > currentMessageId) {
            //     PLUGIN_LOG(INFO) << LOG_DESC("commit包集齐, 但相关跨片交易尚未收到, 存储, in reply")
            //                      << LOG_KV("messageId", messageId)
            //                      << LOG_KV("currentMessageId", currentMessageId);

            //     lateCrossTxMessageId->insert(messageId);
            //     return;
            // }
            // 执行交易
            m_deterministExecute->executeCrossTx(internal_groupId, corMessageId);
        }
    }
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
    auto shardIds = msg_status.shardids();
    auto messageIds = msg_status.messageids();
    auto crossTxNum = msg_status.txnum();

    // 若该跨片交易已被abort，则其对应reply包消息不处理
    string abortKey = toString(sourceShardId) + "_" + shardIds + "_" + messageIds;
    if (m_deterministExecute->isAborted(abortKey)) {
        return;
    }

    PLUGIN_LOG(INFO) << LOG_DESC("'跨片交易提交'消息包解析完毕")
                     << LOG_KV("messageId", messageId)
                     << LOG_KV("sourceShardId", sourceShardId);
                    //  << LOG_KV("crossTxHash", crossTxHash)
                    //  << LOG_KV("shardIds", shardIds)
                    //  << LOG_KV("messageIds", messageIds);

    if ((int)commit != 1) 
        return;

    //获取涉及线程安全的变量
    int msgCount = crossTx2CommitMsg->count(crossTxHash);
    int commitMsgNum = 0;
    
    if (msgCount != 0) {
        commitMsgNum = crossTx2CommitMsg->at(crossTxHash);
    }

    // 如果早已收到了足够的包就停止收取，防止重复执行
    if ((msgCount == 0 && doneCrossTx->find(crossTxHash) != doneCrossTx->end())
        ||
        (msgCount != 0 && commitMsgNum == 3)) {
        // PLUGIN_LOG(INFO) << LOG_DESC("够了") << LOG_KV("crossTx", crossTxHash);
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
    

    PLUGIN_LOG(INFO) << LOG_DESC("'跨片交易提交'消息包添加完毕")
                    //  << LOG_KV("commitMsgNum", crossTx2CommitMsg->at(crossTxHash))
                     << LOG_KV("messageId", messageId)
                     << LOG_KV("commitMsgNum", commitMsgNum);

    // 判断跨片交易是否满足提交条件
    // 存在两个消息包一起加1后进入该判断语句的可能性，导致executeCrossTx被调用两次
    if (commitMsgNum == 1) { // 若包等于1 则执行跨片交易
        auto currentMessageId = current_candidate_tx_messageids->at(sourceShardId - 1);
        // ADD BY ZH -- 22.11.16
        // 考虑commit包集齐但跨片交易还没收到的情况：每个节点收到包的顺序不一致
        // 解决方案：将集齐的相关交易先进行存储，待对应交易收到后立马执行
        // PLUGIN_LOG(INFO) << LOG_DESC("跨片交易提交包收齐")
        //                  << LOG_KV("messageId", messageId);
                        //  << LOG_KV("packet messageId", messageId)
                        //  << LOG_KV("current messageId", currentMessageId);
        
        //  如果收到的commit包的messageId不是当前待处理的最小messageId包，则存储
        if (messageId > currentMessageId) {
            PLUGIN_LOG(INFO) << LOG_DESC("commit包集齐, 但相关跨片交易尚未收到, 存储, in commit")
                             << LOG_KV("messageId", messageId)
                             << LOG_KV("currentMessageId", currentMessageId);

            lateCrossTxMessageId->insert(messageId);
            return;
        }

        // 执行交易
        // PLUGIN_LOG(INFO) << LOG_DESC("commit包集齐, 子分片开始执行并提交相关跨片交易...")
        //                  << LOG_KV("messageId", messageId);
                        
        // auto readwriteset = crossTx2StateAddress->at(crossTxHash);
        m_deterministExecute->executeCrossTx(sourceShardId, messageId);
        // groupVerifier->executeCrossTx(readwriteset);
        // crossTx2StateAddress->unsafe_erase(crossTxHash);

        // 其它节点已经完成提交并返回消息包的情况
        if (sourceShardId == internal_groupId && lateCommitReplyMessageId->find(messageId) != lateCommitReplyMessageId->end()) {
            doneCrossTx->insert(crossTxHash);
            int executedTxNum = m_deterministExecute->executedTx;
            if (executedTxNum == 0) {
                PLUGIN_LOG(INFO) << LOG_KV("executedTx", 0);
            }
            executedTxNum += crossTxNum;
            m_deterministExecute->executedTx = executedTxNum;
            // PLUGIN_LOG(INFO) << LOG_DESC("跨片交易流程完成... in processReceivedCrossTxCommit")
            //                  << LOG_KV("messageId", messageId)
            //                  << LOG_KV("当笔跨片交易数", crossTxNum)
            //                  << LOG_KV("executedTx", executedTxNum - (executedTxNum % 500))
            //                  << LOG_KV("当前分片累计提交交易数", executedTxNum);

            PLUGIN_LOG(INFO) << LOG_DESC("'跨片交易'流程完成")
                             << LOG_KV("当前跨片交易messageId", messageId)
                             << LOG_KV("跨片交易总数", crossTxNum)
                             << LOG_KV("executedTx", executedTxNum - (executedTxNum % 500))
                             << LOG_KV("executedTx_real", executedTxNum)
                             << LOG_KV("当前分片累计提交交易数", executedTxNum);

            // 非头节点不必转发
            if(dev::plugin::nodeIdStr != toHex(forwardNodeId.at(internal_groupId - 1)))
            {
                return;
            }
            m_deterministExecute->m_blockingCrossTxQueue->popTx();
            m_deterministExecute->processBlockedCrossTx();
        }
    
        return;
    } else if (commitMsgNum == 3 && nodeIdStr == toHex(forwardNodeId.at(internal_groupId - 1))) { // 若包数量大于等于3 则回复commitok消息包
        PLUGIN_LOG(INFO) << LOG_DESC("'跨片交易提交'消息包收齐, 开始执行跨片交易")
                         << LOG_KV("messageId", messageId);
        replyToCoordinatorCommitOK(_txrlp);
    }

    // PLUGIN_LOG(INFO) << LOG_DESC("跨片交易暂不满足提交条件")
    //                  << LOG_KV("commitMsgNum", crossTx2CommitMsg->at(crossTxHash))
    //                  << LOG_KV("messageId", messageId)
    //                  << LOG_KV("sourceId", sourceShardId);

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
    auto txIds = msg_status.txids();

    PLUGIN_LOG(INFO) << LOG_DESC("'跨片交易提交回执'消息包解析完毕")
                     << LOG_KV("messageId", messageId)
                     << LOG_KV("sourceShardId", sourceShardId)
                     << LOG_KV("status", status);
                    //  << LOG_KV("crossTxHash", crossTxHash)
                    //  << LOG_KV("crossTxNum", txNum);

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
    } else {
        crossTx2ReceivedCommitMsg->at(crossTxHash).push_back((int)sourceShardId);
    }

    // 判断是否收集足够（全部）的包
    // for (auto i : crossTx2ShardID->at(crossTxHash)) {
    //     int threshold = 4;
    //     if (i == internal_groupId) {
    //         threshold = 3;
    //     }
    //     if (count(crossTx2ReceivedCommitMsg->at(crossTxHash).begin(), crossTx2ReceivedCommitMsg->at(crossTxHash).end(), i) < threshold) {
    //         return;
    //     }
    // }

    for (auto i : crossTx2ShardID->at(crossTxHash)) {
        int threshold = 1; // one2all
        // int threshold = 3; // normal
        if (i == internal_groupId) {
            threshold = 0; // one2all
            // threshold = 2;
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
        if (count ==0 || (count != 0 && crossTx2CommitMsg->at(crossTxHash) < 1)) {
            lateCommitReplyMessageId->insert(messageId);
            return;
        }
    }

    // 添加doneCrossTx，防止收到历史交易包——22.11.6
    int executedTxNum = m_deterministExecute->executedTx;
    if (executedTxNum == 0) {
        PLUGIN_LOG(INFO) << LOG_KV("executedTx", 0);
    }
    executedTxNum += txNum;
    m_deterministExecute->executedTx = executedTxNum;
    PLUGIN_LOG(INFO) << LOG_DESC("'跨片交易'流程完成")
                     << LOG_KV("当前跨片交易messageId", messageId)
                     << LOG_KV("跨片交易总数", txNum)
                     << LOG_KV("executedTx", executedTxNum - (executedTxNum % 500))
                     << LOG_KV("executedTx_real", executedTxNum)
                     << LOG_KV("当前分片累计提交交易数", executedTxNum);

    // 记录交易结束时间
    vector<string> dataItems;
    boost::split(dataItems, txIds, boost::is_any_of("_"), boost::token_compress_on);
    for (string txid : dataItems) {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        int time_sec = (int)tv.tv_sec;
        m_txid_to_endtime->insert(make_pair(txid, time_sec));
    }

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

h512 ConsensusPluginManager::getNodeId(int _index)
{
    return exNodeId[_index]; // 
}
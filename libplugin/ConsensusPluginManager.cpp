#include "libplugin/Common.h"
#include <libplugin/ConsensusPluginManager.h>
#include <libethcore/CommonJS.h>
#include <libconsensus/pbft/PBFTEngine.h>

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

    // m_rpc_service->sendRawTransaction(destinshardid, signeddata); // 通过调用本地的RPC接口发起新的共识
    m_rpc_service->sendSubCsRawTransaction(destinshardid, signeddata, 1); // 通过调用本地的RPC接口发起新的共识
    // distxs->push(_txrlp);
}

// ADD BY ZH
void ConsensusPluginManager::processReceivedCrossTx(protos::SubCrossShardTx _txrlp)
{
    // receive and storage
    protos::SubCrossShardTx msg_txWithReadset;
    msg_txWithReadset = _txrlp;
    
    PLUGIN_LOG(INFO) << LOG_DESC("接收到协调者发来跨片交易请求...");
    auto stateAddress = msg_txWithReadset.stateaddress();
    auto sourceShardId = msg_txWithReadset.sourceshardid();
    auto destinshardid = msg_txWithReadset.destinshardid();
    auto signeddata = msg_txWithReadset.signeddata();
    auto messageID = msg_txWithReadset.messageid();
    auto crossTxHash = msg_txWithReadset.crosstxhash();

    PLUGIN_LOG(INFO) << LOG_DESC("交易解析完毕")
                     << LOG_KV("signeddata", signeddata)
                     << LOG_KV("stateAddress", stateAddress)
                     << LOG_KV("sourceShardId", sourceShardId)
                     << LOG_KV("destinshardid", destinshardid)
                     << LOG_KV("messageid", messageID);

    // m_rpc_service->sendRawTransaction(destinshardid, signeddata); /hash2用本地的RPC接口发起新的共识
    // 存储跨片交易信息 用于发送res给coordinator
    Transaction::Ptr tx = std::make_shared<Transaction>(
            jsToBytes(signeddata, dev::OnFailed::Throw), CheckTransaction::Everything);
    auto trans = transaction{
                1, 
                (unsigned long)sourceShardId, 
                (unsigned long)destinshardid, 
                (unsigned long)messageID, 
                crossTxHash, 
                tx, 
                signeddata,
                stateAddress};

    crossTx.insert(std::make_pair(tx->hash(), trans));

    crossTx2StateAddress->insert(std::make_pair(crossTxHash, stateAddress));

    // PLUGIN_LOG(INFO) << LOG_DESC(nodeIdStr);
    // 先根据messageID判断跨片交易是否为按序到达
    if (messageID == latest_candidate_tx_messageids->at(sourceShardId - 1) + 1) {
        for(size_t i = 0; i < forwardNodeId.size(); i++) {
            // 判断当前节点是否为头节点
            PLUGIN_LOG(INFO) << LOG_DESC("当前forwardNodeId为: ") << LOG_DESC(toHex(forwardNodeId.at(i)));
            if(nodeIdStr == toHex(forwardNodeId.at(i))){
                PLUGIN_LOG(INFO) << LOG_DESC("匹配成功，当前节点为头节点")
                                 << LOG_KV("发送交易messageID", messageID);
                m_rpc_service->sendRawTransaction(destinshardid, signeddata); // 通过调用本地的RPC接口发起新的共识
                
                // 更新已经收到的按序的最大的messageID
                latest_candidate_tx_messageids->at(sourceShardId - 1) = messageID;

                // 检查cached_cs_tx 中后继 _message_id + 1 的交易是否已经到达, 若已经到达，也插入到 candidate_cs_tx 中，更新上锁的读写集
                PLUGIN_LOG(INFO) << LOG_DESC("检查cached_cs_tx中后继message_id + 1的交易是否已经到达");
                messageID = messageID + 1;
                std::string attempt_key = std::to_string(sourceShardId) + std::to_string(messageID);
                while(cached_cs_tx->count(attempt_key) != 0) {// 若后继 key 的跨片交易也在，也放入 candidate_cs_tx
                    PLUGIN_LOG(INFO) << LOG_DESC("存在之前乱序到达的满足条件的交易")
                                     << LOG_KV("messageId", messageID);

                    auto _subtx = cached_cs_tx->at(attempt_key);
                    latest_candidate_tx_messageids->at(sourceShardId - 1) = messageID;
                    m_rpc_service->sendRawTransaction(_subtx.destin_shard_id, _subtx.signeddata); // 通过调用本地的RPC接口发起新的共识

                    // 从 cached_cs_tx 中将交易删除
                    if (m_cachedTx.try_lock()) {
                        cached_cs_tx->unsafe_erase(attempt_key);
                        m_cachedTx.unlock();
                    }

                    messageID = messageID + 1;
                    attempt_key = std::to_string(sourceShardId) + std::to_string(messageID);
                    // PLUGIN_LOG(INFO) << LOG_DESC("插入成功后")
                    //                  << LOG_KV("candidate_tx_queues.size", candidate_tx_queues->at(stateAddress).queue.size());
                }
                break;
            }
        }
    } else {
        PLUGIN_LOG(INFO) << LOG_DESC("插入乱序到达的跨片交易")
                         << LOG_KV("messageId", messageID)
                         << LOG_KV("sourceShardId", sourceShardId)
                         << LOG_KV("destinShardId", destinshardid)
                         << LOG_KV("crossTxHash", crossTxHash);
        // std::cout << "insert_cached_cs_tx" << std::endl;
        std::string _key = std::to_string(sourceShardId) + std::to_string(messageID);
        cached_cs_tx->insert(std::make_pair(_key, trans));
    }

    
    // distxs->push(_txrlp);
}

void ConsensusPluginManager::processReceivedCrossTxReply(protos::SubCrossShardTxReply _txrlp) {
    protos::SubCrossShardTxReply msg_status;
    msg_status = _txrlp;
    
    PLUGIN_LOG(INFO) << "接收到子分片发来的跨片交易状态消息包...";
    auto sourceShardId = msg_status.sourceshardid();
    auto destinshardid = msg_status.destinshardid();
    auto status = msg_status.status();
    auto crossTxHash = msg_status.crosstxhash();
    auto messageId = msg_status.messageid();

    PLUGIN_LOG(INFO) << LOG_DESC("跨片交易回执包解析完毕")
                     << LOG_KV("status", status)
                     << LOG_KV("messageId", messageId)
                     << LOG_KV("sourceShardId", sourceShardId)
                     << LOG_KV("destinshardid", destinshardid)
                     << LOG_KV("crossTxHash", crossTxHash);

    if ((int)status != 1) 
        return;

    // 已经收到了大多数包就直接收取而不执行后面的逻辑，防止重复执行
    // 待添加逻辑：若是对应交易hash已经完成2PC，则不再收取 ===> 是这么用吧
    if ((crossTx2ReceivedMsg->count(crossTxHash) == 0 &&
        (doneCrossTx->find(crossTxHash)) != doneCrossTx->end())
        ||(crossTx2ReceivedMsg->count(crossTxHash)!=0 && 
        count(crossTx2ReceivedMsg->at(crossTxHash).begin(), crossTx2ReceivedMsg->at(crossTxHash).end(), (int)sourceShardId) >= 3)) {
        PLUGIN_LOG(INFO) << LOG_DESC("够了/晚了，对于:") << LOG_KV("id", (int)sourceShardId);
        // crossTx2ReceivedMsg->at(crossTxHash).push_back((int)sourceShardId);
        return;
    }

    // 遍历crossTx2ReceivedMsg->at(crossTxHash)
    PLUGIN_LOG(INFO) << LOG_DESC("遍历crossTx2ReceivedMsg->at(crossTxHash)");
    if(crossTx2ReceivedMsg->count(crossTxHash)!=0)
    {
        for (int i = 0; i < crossTx2ReceivedMsg->at(crossTxHash).size(); i++) {
            PLUGIN_LOG(INFO) << LOG_DESC("目前收到的reply包的id有")
                             << LOG_KV("id", crossTx2ReceivedMsg->at(crossTxHash)[i]);
        }
    }

    // 存消息
    if (crossTx2ReceivedMsg->count(crossTxHash) == 0) {
        std::vector<int> temp;
        temp.push_back((int)sourceShardId);
        crossTx2ReceivedMsg->insert(std::make_pair(crossTxHash, temp));
    } else {
        crossTx2ReceivedMsg->at(crossTxHash).push_back((int)sourceShardId);
    }
    // 判断是否收集足够的包
    bool flag = true;
    for (auto i : crossTx2ShardID->at(crossTxHash)) {
        // PLUGIN_LOG(INFO) << LOG_KV("shard" + i, count(crossTx2ReceivedMsg->at(crossTxHash).begin(), crossTx2ReceivedMsg->at(crossTxHash).end(), i));
        if (count(crossTx2ReceivedMsg->at(crossTxHash).begin(), crossTx2ReceivedMsg->at(crossTxHash).end(), i) < 3) {
            flag = false;
            break;
        }
    }
    if (flag) { // 集齐 -> 发送commit包
        PLUGIN_LOG(INFO) << LOG_DESC("状态包集齐, 开始发送commit消息包...");

        protos::SubCrossShardTxCommit subCrossShardTxCommit;
        subCrossShardTxCommit.set_crosstxhash(crossTxHash);
        subCrossShardTxCommit.set_commit(1);
        subCrossShardTxCommit.set_sourceshardid(destinshardid);
        subCrossShardTxCommit.set_destinshardid(sourceShardId);
        subCrossShardTxCommit.set_messageid(messageId);

        std::string serializedSubCrossShardTxCommit_str;
        subCrossShardTxCommit.SerializeToString(&serializedSubCrossShardTxCommit_str);
        auto txByte = asBytes(serializedSubCrossShardTxCommit_str);

        dev::sync::SyncCrossTxCommitPacket crossTxCommitPacket; // 类型需要自定义
        crossTxCommitPacket.encode(txByte);
        auto msg = crossTxCommitPacket.toMessage(group_protocolID);

        PLUGIN_LOG(INFO) << LOG_DESC("协调者共识完毕, 开始向参与者分片发送跨片交易提交消息包....")
                         << LOG_KV("group_protocolID", group_protocolID);

        for (auto destinShardID : crossTx2ShardID->at(crossTxHash)){
            // 向子分片的每个节点发送交易
            for(size_t j = 0; j < 4; j++)  // 给所有节点发
            {
                PLUGIN_LOG(INFO) << LOG_KV("正在发送给", shardNodeId.at((destinShardID - 1) * 4 + j));
                group_p2p_service->asyncSendMessageByNodeID(shardNodeId.at((destinShardID - 1) * 4 + j), msg, CallbackFuncWithSession(), dev::network::Options());
            }
        }
        PLUGIN_LOG(INFO) << LOG_DESC("commit消息发送完毕...");
    }
}

void ConsensusPluginManager::processReceivedCrossTxCommit(protos::SubCrossShardTxCommit _txrlp) {
    // 解析消息包
    protos::SubCrossShardTxCommit msg_status;
    msg_status = _txrlp;
    
    PLUGIN_LOG(INFO) << "接收到协调者发来的跨片交易提交消息包...";
    auto commit = msg_status.commit();
    auto crossTxHash = msg_status.crosstxhash();
    auto sourceShardId = msg_status.sourceshardid();
    auto destinshardid = msg_status.destinshardid();
    auto messageId = msg_status.messageid();

    PLUGIN_LOG(INFO) << LOG_DESC("跨片交易提交包解析完毕")
                     << LOG_KV("commit", commit)
                     << LOG_KV("messageId", messageId)
                     << LOG_KV("sourceShardId", sourceShardId)
                     << LOG_KV("crossTxHash", crossTxHash);

    if ((int)commit != 1) 
        return;

    // 如果早已收到了足够的包就停止收取，防止重复执行
    if ((crossTx2CommitMsg->count(crossTxHash) == 0 && doneCrossTx->find(crossTxHash) != doneCrossTx->end())
        ||
        (crossTx2CommitMsg->count(crossTxHash) != 0 && crossTx2CommitMsg->at(crossTxHash) >= 3)) {
        PLUGIN_LOG(INFO) << LOG_DESC("够了") << LOG_KV("crossTx", crossTxHash);
        return;
    }

    // 存储消息
    if (crossTx2CommitMsg->count(crossTxHash) == 0) {
        crossTx2CommitMsg->insert(std::make_pair(crossTxHash, 1));
    } else {
        crossTx2CommitMsg->at(crossTxHash) = crossTx2CommitMsg->at(crossTxHash) + 1;
    }
    // 判断跨片交易是否满足提交条件
    if (crossTx2CommitMsg->at(crossTxHash) >= 3) {
        // ADD BY ZH -- 22.11.16
        // 考虑commit包集齐但跨片交易还没收到的情况：每个节点收到包的顺序不一致
        // 解决方案：将集齐的相关交易先进行存储，待对应交易收到后立马执行
        PLUGIN_LOG(INFO) << LOG_DESC("跨片交易提交包收齐")
                         << LOG_KV("packet messageId", messageId)
                         << LOG_KV("current messageId", current_candidate_tx_messageids->at(sourceShardId - 1));
        
        //  如果收到的commit包的messageId不是当前正在处理的messageId包，则存储
        if (messageId > current_candidate_tx_messageids->at(sourceShardId - 1)) {
            PLUGIN_LOG(INFO) << LOG_DESC("commit包集齐, 但相关跨片交易尚未收到, 存储");
            lateCrossTxMessageId->insert(messageId);
            return;
        }

        // 执行交易
        PLUGIN_LOG(INFO) << LOG_DESC("commit包集齐, 子分片开始执行并提交相关跨片交易...");
        // crossTx2StateAddress? maybe later
        auto readwriteset = crossTx2StateAddress->at(crossTxHash);
        groupVerifier->executeCrossTx(readwriteset);
        // groupVerifier->executeCrossTx("state");
        crossTx2StateAddress->unsafe_erase(crossTxHash);
    }
}

void ConsensusPluginManager::processReceivedCrossTxCommitReply(protos::SubCrossShardTxCommitReply _txrlp) {
    // 解析消息包
    protos::SubCrossShardTxCommitReply msg_status;
    msg_status = _txrlp;
    
    PLUGIN_LOG(INFO) << "接收到协调者发来的跨片交易提交消息包...";
    auto status = msg_status.status();
    auto crossTxHash = msg_status.crosstxhash();
    auto sourceShardId = msg_status.sourceshardid();
    auto destinshardid = msg_status.destinshardid();
    auto messageId = msg_status.messageid();

    PLUGIN_LOG(INFO) << LOG_DESC("交易执行成功消息包解析完毕")
                     << LOG_KV("status", status)
                     << LOG_KV("messageId", messageId)
                     << LOG_KV("crossTxHash", crossTxHash);

    if ((int)status != 1) 
        return;

    // 已经收到了大多数包就停止收取，防止重复执行
    // 历史交易包也不会收——22.11.6
    if ((crossTx2ReceivedCommitMsg->count(crossTxHash) == 0 &&
        doneCrossTx->find(crossTxHash) != doneCrossTx->end())
        ||
        (crossTx2ReceivedCommitMsg->count(crossTxHash) != 0 && 
        count(crossTx2ReceivedCommitMsg->at(crossTxHash).begin(), crossTx2ReceivedCommitMsg->at(crossTxHash).end(), (int)sourceShardId) >= 3)) {
        return;
    }

    // 存储消息
    if (crossTx2ReceivedCommitMsg->count(crossTxHash) == 0) {
        std::vector<int> temp;
        temp.push_back((int)sourceShardId);
        crossTx2ReceivedCommitMsg->insert(std::make_pair(crossTxHash, temp));
    } else {
        crossTx2ReceivedCommitMsg->at(crossTxHash).push_back((int)sourceShardId);
    }

    // 判断是否收集足够的包
    bool flag = true;
    for (auto i : crossTx2ShardID->at(crossTxHash)) {
        if (count(crossTx2ReceivedCommitMsg->at(crossTxHash).begin(), crossTx2ReceivedCommitMsg->at(crossTxHash).end(), i) < 3) {
            flag = false;
            break;
        }
    }

    /*
      集齐 ==> 删除相关变量
        1. crossTx2ShardID
        2. crossTx2ReceivedMsg
        3. crossTx2ReceivedCommitMsg
    */
    if (flag) {
        PLUGIN_LOG(INFO) << LOG_DESC("commit reply包集齐, 协调者分片开始删除相关变量...");
        crossTx2ShardID->unsafe_erase(crossTxHash);
        crossTx2ReceivedMsg->unsafe_erase(crossTxHash);
        crossTx2ReceivedCommitMsg->unsafe_erase(crossTxHash);
        // 添加doneCrossTx，防止收到历史交易包——22.11.6
        doneCrossTx->insert(crossTxHash);
        PLUGIN_LOG(INFO) << LOG_DESC("跨片交易流程完成...");
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
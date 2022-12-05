#include "Common.h"
#include <libplugin/deterministExecute.h>
#include <libplugin/executeVM.h>
#include <libethcore/Transaction.h>
#include <thread>
#include <utility>
#include <libethcore/ABI.h>

using namespace dev::plugin;
using namespace dev::consensus;
using namespace dev::p2p;
using namespace dev::eth;

void deterministExecute::replyToCoordinator(dev::plugin::transaction txInfo, 
            dev::PROTOCOL_ID& m_group_protocolID, std::shared_ptr<dev::p2p::Service> m_group_service) {    
    // unsigned long message_id = txInfo.message_id;
	unsigned long source_shard_id = txInfo.source_shard_id; // 协调者id
    string crossTxHash = txInfo.cross_tx_hash;
    unsigned long destin_shard_id = txInfo.destin_shard_id; // 本分片id
    unsigned long messageID = txInfo.message_id;

    // 如果该笔交易早已收到了足够的commit包，则直接执行
    // if (m_lateCrossTxMutex.try_lock()) {
        if (lateCrossTxMessageId->find(messageID) != lateCrossTxMessageId->end()) {
            PLUGIN_LOG(INFO) << LOG_DESC("commit包之前就已集齐, 直接执行交易... in deter.reply")
                             << LOG_KV("messageId", messageID);
            auto readwriteset = crossTx2StateAddress->at(crossTxHash);
            groupVerifier->executeCrossTx(readwriteset);
            // crossTx2StateAddress->unsafe_erase(crossTxHash);
            // lateCrossTxMessageId->unsafe_erase(messageID);
            // m_lateCrossTxMutex.unlock();
            return;
        }
    // }
    
    
    protos::SubCrossShardTxReply subCrossShardTxReply;
    subCrossShardTxReply.set_crosstxhash(crossTxHash);
    subCrossShardTxReply.set_destinshardid(source_shard_id);
    subCrossShardTxReply.set_sourceshardid(destin_shard_id);
    subCrossShardTxReply.set_messageid(messageID);
    subCrossShardTxReply.set_status(1);
                        
    std::string serializedSubCrossShardTxReply_str;
    subCrossShardTxReply.SerializeToString(&serializedSubCrossShardTxReply_str);
    auto txByte = asBytes(serializedSubCrossShardTxReply_str);

    dev::sync::SyncCrossTxReplyPacket crossTxReplyPacket; // 类型需要自定义
    crossTxReplyPacket.encode(txByte);
    auto msg = crossTxReplyPacket.toMessage(m_group_protocolID);

    PLUGIN_LOG(INFO) << LOG_DESC("状态锁获得, 开始向协调者分片发送状态包....")
                     << LOG_KV("m_group_protocolID", m_group_protocolID)
                     << LOG_KV("messageId", messageID);
    for(size_t j = 0; j < 4; j++)  // 给所有协调者分片所有节点发
    {
        PLUGIN_LOG(INFO) << LOG_KV("正在发送给", shardNodeId.at((source_shard_id-1)*4 + j));
        m_group_service->asyncSendMessageByNodeID(shardNodeId.at((source_shard_id-1)*4 + j), msg, CallbackFuncWithSession(), dev::network::Options());
    }
    PLUGIN_LOG(INFO) << LOG_DESC("子分片向协调者发送状态包完毕...");
    // 更新正在处理的跨片交易的messageID
    current_candidate_tx_messageids->at(source_shard_id - 1) = messageID;
}

void deterministExecute::deterministExecuteTx() {
    PLUGIN_LOG(INFO) << "deterministExecuteTx 线程开启...";

    std::shared_ptr<dev::eth::Transaction> tx;
    
    while (true)
    {
        bool gettx = dev::consensus::toExecute_transactions.try_pop(tx);
        
        if(gettx == true)
        {
            popedTxNum++;
            PLUGIN_LOG(INFO) << LOG_DESC("已经取出的交易数") << LOG_KV("popedTxNum", popedTxNum);
            auto tx_hash = tx->hash();
            PLUGIN_LOG(INFO) << LOG_DESC("缓存交易的hash") << LOG_KV("tx_hash", tx_hash);
            
            // /* 2PC流程逻辑
            // 检查交易hash, 根据crossTx判断是否为跨片子交易
            if (innerTx->find(tx->hash().abridged()) != innerTx->end()) { // 片内交易
                // m_crossTxMutex.unlock();

                // auto readwriteset = "state";
                // auto txInfo = innerTx[tx->hash()];
                auto txInfo = innerTx->at(tx->hash().abridged());
                // m_innerTxMutex.unlock(); // 取了变量再释放

                auto readwriteset = txInfo.readwrite_key;
                // 获得交易hash
                auto txHash = tx->hash().abridged();
                // 获取交易所在区块高度
                auto blockHeight = txHash2BlockHeight->at(txHash);

                PLUGIN_LOG(INFO) << LOG_DESC("发现片内交易 in deterministExecuteTx...")
                                 << LOG_KV("txhash", tx->hash())
                                 << LOG_KV("stateAddress", readwriteset);
                
                m_lockKeyMutex.lock();
                if (locking_key->count(readwriteset) == 0 || locking_key->at(readwriteset) == 0) { // 执行片内交易
                    // 删除txHash2BlockHeight变量，代表相关交易已被执行
                    // txHash2BlockHeight->unsafe_erase(txHash);

                    if (block2UnExecutedTxNum->count(blockHeight) == 0) {
                        // 该笔交易所在区块交易已全部完成过了
                        m_lockKeyMutex.unlock();
                        return;
                    }

                    if (locking_key->count(readwriteset) == 0) {
                        locking_key->insert(std::make_pair(readwriteset, 1));
                    } else if (locking_key->at(readwriteset) == 0) {
                        locking_key->at(readwriteset) = 1;
                    }
                    // m_lockKeyMutex.unlock();

                    auto exec = dev::plugin::executiveContext->getExecutive();
                    auto vm = dev::plugin::executiveContext->getExecutiveInstance();
                    exec->setVM(vm);
                    dev::plugin::executiveContext->executeTransaction(exec, tx);
                    dev::plugin::executiveContext->m_vminstance_pool.push(vm);
                    
                    // m_lockKeyMutex.lock();
                    locking_key->at(readwriteset)--;
                    m_lockKeyMutex.unlock();
                    

                    // 维护区块未完成交易
                    PLUGIN_LOG(INFO) << LOG_DESC("该笔交易对应的区块高度") << LOG_KV("blockHeight", blockHeight);
                    auto unExecutedTxNum = block2UnExecutedTxNum->at(blockHeight);
                    PLUGIN_LOG(INFO) << LOG_DESC("in deterministExecuteTx innerTx...")
                                     << LOG_KV("区块未完成交易before_num", unExecutedTxNum);
                    unExecutedTxNum = unExecutedTxNum - 1;
                    block2UnExecutedTxNum->at(blockHeight) = unExecutedTxNum;
                    if (unExecutedTxNum == 0) {
                        PLUGIN_LOG(INFO) << LOG_BADGE("区块中的数据全部执行完")
                                         << LOG_KV("block_height", blockHeight);
                        /* 删除相关变量
                        //     1. block2ExecutedTxNum
                        //     2. 2PC流程中的变量: doneCrossTx
                        if (m_block2UnExecMutex.try_lock()) {
                            // PLUGIN_LOG(INFO) << LOG_DESC("测试锁, 进来了");
                            block2UnExecutedTxNum->unsafe_erase(blockHeight);
                            m_block2UnExecMutex.unlock();
                            // PLUGIN_LOG(INFO) << LOG_DESC("测试锁，就要出去");
                        }
                        if (blockHeight2CrossTxHash->count(blockHeight) != 0) {
                            for (auto i : blockHeight2CrossTxHash->at(blockHeight)) {
                                PLUGIN_LOG(INFO) << LOG_DESC("正在删除doneCrossTx...该区块高度存在的跨片交易有：")
                                                << LOG_KV("crossTxHash", i);
                                // maybe 要加锁
                                if (m_doneCrossTxMutex.try_lock()) {
                                    doneCrossTx->unsafe_erase(i);
                                    m_doneCrossTxMutex.unlock();
                                }
                            }
                            if (m_height2TxHashMutex.try_lock()) {
                                blockHeight2CrossTxHash->unsafe_erase(blockHeight);
                                m_height2TxHashMutex.unlock();
                            }
                        }
                        */ 
                    }
                    if (block2UnExecutedTxNum->count(blockHeight) != 0) {
                        PLUGIN_LOG(INFO) << LOG_DESC("in deterministExecuteTx innerTx...")  
                                         << LOG_KV("区块未完成交易now_num", block2UnExecutedTxNum->at(blockHeight));
                    }
                } else { // 放入等待队列
                    // m_waitTxs.push_back(tx);
                    if(candidate_tx_queues->count(readwriteset) == 0) {
                        std::queue<executableTransaction> queue = std::queue<executableTransaction>();
                        candidate_tx_queue _candidate_tx_queue { readwriteset, queue };
                        _candidate_tx_queue.queue.push(executableTransaction{tx});
                        candidate_tx_queues->insert(std::make_pair(readwriteset, _candidate_tx_queue));
                        locking_key->insert(std::make_pair(readwriteset, 1));
                    } else {
                        candidate_tx_queues->at(readwriteset).queue.push(executableTransaction{tx});
                        locking_key->at(readwriteset)++;
                    }
                }
                
                // 释放变量
                // m_innerTxMutex.lock();
                // innerTx->unsafe_erase(tx->hash().abridged());
                // m_innerTxMutex.unlock();

            } else if (crossTx->find(tx->hash().abridged()) != crossTx->end()) { // 跨片交易

                PLUGIN_LOG(INFO) << LOG_DESC("发现跨片交易 in deterministExecuteTx...")
                                 << LOG_KV("txhash", tx->hash());
                // transaction txInfo = crossTx[tx->hash()];
                transaction txInfo = crossTx->at(tx->hash().abridged());

                unsigned long message_id = txInfo.message_id;
                unsigned long source_shard_id = txInfo.source_shard_id; // 协调者id
                string crossTxHash = txInfo.cross_tx_hash;
                unsigned long destin_shard_id = txInfo.destin_shard_id; // 本分片id
                auto readwriteset = txInfo.readwrite_key; // 跨片交易读写集
                // auto readwriteset = "state"; // 跨片交易读写集
                PLUGIN_LOG(INFO) << LOG_DESC("解析跨片交易成功")
                                 << LOG_KV("messageId", message_id)
                                 << LOG_KV("source_shard_id", source_shard_id)
                                 << LOG_KV("destin_shard_id", destin_shard_id)
                                 << LOG_KV("crossTxHash", crossTxHash)
                                 << LOG_KV("stateAddress", readwriteset);

                // 按序到达的交易未必能直接执行==>添加条件判断
                if(message_id == latest_candidate_tx_messageids->at(source_shard_id - 1) + 1) { //按序到达
                    // 将交易放入队列
                    PLUGIN_LOG(INFO) << LOG_DESC("insert candidate_tx_queues...")
                                     << LOG_KV("insert messageId", message_id);
                    
                    if(candidate_tx_queues->count(readwriteset) == 0) {
                        PLUGIN_LOG(INFO) << LOG_DESC("candidate_tx_queues->count == 0")
                                         << LOG_KV("readwriteset", readwriteset);
                        std::queue<executableTransaction> queue = std::queue<executableTransaction>();
                        candidate_tx_queue _candidate_tx_queue { readwriteset, queue };
                        _candidate_tx_queue.queue.push(executableTransaction{tx});
                        candidate_tx_queues->insert(std::make_pair(readwriteset, _candidate_tx_queue));
                        PLUGIN_LOG(INFO) << LOG_KV("candidate_tx_queue.size() first", candidate_tx_queues->at(readwriteset).queue.size());
                    } else {
                        PLUGIN_LOG(INFO) << LOG_DESC("candidate_tx_queues->count != 0")
                                         << LOG_KV("readwriteset", readwriteset)
                                         << LOG_KV("candidate_tx_queue.size() before", candidate_tx_queues->at(readwriteset).queue.size());
                        // 当前片内交易的读写集（假设跨片交易的第一个读写集是当前片的读写集）, 定位读写集 readwrite_key 的交易缓存队列
                        // auto candidate_tx_queue = candidate_tx_queues->at(readwriteset);
                        // _subtx 插入到 candidate_cs_tx中，更新上锁的读写集
                        // candidate_tx_queue.queue.push(executableTransaction{tx});
                        candidate_tx_queues->at(readwriteset).queue.push(executableTransaction{tx});
                        PLUGIN_LOG(INFO) << LOG_KV("candidate_tx_queue.size() after", candidate_tx_queues->at(readwriteset).queue.size());
                    }

                    // insert_candidate_cs_tx(_tx);
                    // 更新 locking_key
                    m_lockKeyMutex.lock();
                    if(locking_key->count(readwriteset) == 0) {
                        PLUGIN_LOG(INFO) << LOG_DESC("locking_key->count == 0");
                        // 向coordinator发送成功消息
                        locking_key->insert(std::make_pair(readwriteset, 1)); 
                        m_lockKeyMutex.unlock();
                        
                        replyToCoordinator(txInfo, group_protocolID, group_p2p_service);
                    } else {
                        PLUGIN_LOG(INFO) << LOG_DESC("locking_key->count != 0");
                        
                        if (locking_key->at(readwriteset) == 0) { // 抢占锁成功
                            // 向coordinator发送成功消息
                            // PLUGIN_LOG(INFO) << LOG_DESC("holding_tx_num == 0")
                            PLUGIN_LOG(INFO) << LOG_DESC("该笔交易为跨片交易...非队列")
                                                << LOG_KV("messageId", message_id);
                            locking_key->at(readwriteset) = 1;
                            m_lockKeyMutex.unlock();

                            replyToCoordinator(txInfo, group_protocolID, group_p2p_service);
                        } else {
                            // 添加变量, 等待之前的交易完成
                            locking_key->at(readwriteset)++;
                            m_lockKeyMutex.unlock();
                        }
                    }
                    
                    // 更新已经收到的按序的最大的messageID
                    latest_candidate_tx_messageids->at(source_shard_id - 1) = message_id;

                    // 检查cached_cs_tx 中后继 _message_id + 1 的交易是否已经到达, 若已经到达，也插入到 candidate_cs_tx 中，更新上锁的读写集
                    PLUGIN_LOG(INFO) << LOG_DESC("检查cached_cs_tx中后继message_id + 1的交易是否已经到达");
                    message_id = message_id + 1;
                    std::string attempt_key = std::to_string(source_shard_id) + std::to_string(message_id);
                    while(cached_cs_tx->count(attempt_key) != 0) {// 若后继 key 的跨片交易也在，也放入 candidate_cs_tx
                        PLUGIN_LOG(INFO) << LOG_DESC("存在之前乱序到达的满足条件的交易")
                                         << LOG_KV("insert messageId", message_id);
                        dev::plugin::transaction _subtx;
                        m_cachedTx.lock();
                        _subtx = cached_cs_tx->at(attempt_key);
                        m_cachedTx.unlock();

                        // 定位读写集 readwrite_key 的交易缓存队列，先判断是否存在
                        // 判断candidate_tx_queues中是否有readwrite_key的队列，因为之前可能没有
                        if(candidate_tx_queues->count(readwriteset) == 0)
                        {
                            std::queue<executableTransaction> queue = std::queue<executableTransaction>();
                            candidate_tx_queue _candidate_tx_queue { readwriteset, queue };
                            _candidate_tx_queue.queue.push(executableTransaction{_subtx.tx});
                            candidate_tx_queues->insert(std::make_pair(readwriteset, _candidate_tx_queue));
                        }
                        else
                        {
                            PLUGIN_LOG(INFO) << LOG_DESC("交易插入前")
                                             << LOG_KV("candidate_tx_queues.size", candidate_tx_queues->at(readwriteset).queue.size());

                            // auto candidate_tx_queue = candidate_tx_queues->at(readwriteset);
                            // _subtx 插入到candidate_cs_tx中，更新上锁的读写集
                            // candidate_tx_queue.queue.push(executableTransaction{_subtx.tx});
                            candidate_tx_queues->at(readwriteset).queue.push(executableTransaction{_subtx.tx});
                        }

                        latest_candidate_tx_messageids->at(source_shard_id - 1) = message_id;
                        
                        m_lockKeyMutex.lock();
                        if(locking_key->count(readwriteset) == 0) { 
                            locking_key->insert(std::make_pair(readwriteset, 1));
                        } else {
                            locking_key->at(readwriteset)++;
                        }
                        m_lockKeyMutex.unlock();
                        
                        // 从 cached_cs_tx 中将交易删除
                        m_cachedTx.lock();
                        cached_cs_tx->unsafe_erase(attempt_key);
                        m_cachedTx.unlock();

                        message_id = message_id + 1;
                        attempt_key = std::to_string(source_shard_id) + std::to_string(message_id);
                        PLUGIN_LOG(INFO) << LOG_DESC("插入成功后")
                                         << LOG_KV("candidate_tx_queues.size", candidate_tx_queues->at(readwriteset).queue.size());
                    }
                    PLUGIN_LOG(INFO) << LOG_DESC("跨片交易-按序到达逻辑执行完成...");
                } else { // 乱序到达
                    PLUGIN_LOG(INFO) << LOG_DESC("插入乱序到达的跨片交易")
                                     << LOG_KV("messageId", message_id)
                                     << LOG_KV("source_shard_id", source_shard_id)
                                     << LOG_KV("destin_shard_id", destin_shard_id)
                                     << LOG_KV("crossTxHash", crossTxHash);
                    // std::cout << "insert_cached_cs_tx" << std::endl;
                    std::string _key = std::to_string(source_shard_id) + std::to_string(message_id);
                    m_cachedTx.lock();
                    cached_cs_tx->insert(std::make_pair(_key, txInfo));
                    m_cachedTx.unlock();
                }
            } else { // 部署交易/协调者的跨片交易/重复交易
                // m_innerTxMutex.unlock();
                // m_crossTxMutex.unlock();

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
                    /*删除相关变量
                        1. block2ExecutedTxNum
                        2. 2PC流程中的变量: doneCrossTx

                    if (m_block2UnExecMutex.try_lock()) {
                        // PLUGIN_LOG(INFO) << LOG_DESC("测试锁, 进来了");
                        block2UnExecutedTxNum->unsafe_erase(blockHeight);
                        m_block2UnExecMutex.unlock();
                        // PLUGIN_LOG(INFO) << LOG_DESC("测试锁，就要出去");
                    }
                    if (blockHeight2CrossTxHash->count(blockHeight) != 0) {
                        for (auto i : blockHeight2CrossTxHash->at(blockHeight)) {
                            PLUGIN_LOG(INFO) << LOG_DESC("正在删除doneCrossTx...该区块高度存在的跨片交易有：")
                                             << LOG_KV("crossTxHash", i);
                            // maybe 要加锁
                            if (m_doneCrossTxMutex.try_lock()) {
                                doneCrossTx->unsafe_erase(i);
                                m_doneCrossTxMutex.unlock();
                            }
                        }
                        if (m_height2TxHashMutex.try_lock()) {
                            blockHeight2CrossTxHash->unsafe_erase(blockHeight);
                            m_height2TxHashMutex.unlock();
                        }
                    }
                    */ 
                }
                if (block2UnExecutedTxNum->count(blockHeight) != 0) {
                    PLUGIN_LOG(INFO) << LOG_DESC("in deterministExecuteTx otherTx...")  
                                     << LOG_KV("区块未完成交易now_num", block2UnExecutedTxNum->at(blockHeight));
                }
            }
            // */

            // auto exec = dev::plugin::executiveContext->getExecutive();
            // auto vm = dev::plugin::executiveContext->getExecutiveInstance();
            // exec->setVM(vm);
            // dev::plugin::executiveContext->executeTransaction(exec, tx);
            // dev::plugin::executiveContext->m_vminstance_pool.push(vm);
        }
        // why sleep
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        count++;
        if (count % 100 == 0 && candidate_tx_queues->count("state1") != 0) {
            PLUGIN_LOG(INFO) << LOG_DESC("continuing...")
                             << LOG_KV("candidate size", candidate_tx_queues->at("state1").queue.size())
                             << LOG_KV("cached size", cached_cs_tx->size());
            if (candidate_tx_queues->at("state1").queue.size() > 0) {
                auto tx = candidate_tx_queues->at("state1").queue.front().tx;
                // transaction txInfo = crossTx[tx->hash()];
                transaction txInfo = crossTx->at(tx->hash().abridged());
                PLUGIN_LOG(INFO) << LOG_DESC("队首元素messageID")
                                 << LOG_KV("messageId", txInfo.message_id)
                                 << LOG_KV("当前正在执行的messageID", current_candidate_tx_messageids->at(txInfo.source_shard_id - 1));
            }
        }

        checkForDeterministExecuteTxWookLoop();
    }
}

void deterministExecute::checkDelayCommitPacket(dev::plugin::transaction txInfo) {
    unsigned long source_shard_id = txInfo.source_shard_id; // 协调者id
    string crossTxHash = txInfo.cross_tx_hash;
    unsigned long destin_shard_id = txInfo.destin_shard_id; // 本分片id
    unsigned long messageID = txInfo.message_id;
    
    // 如果该笔交易早已收到了足够的commit包，则直接执行
    if (lateCrossTxMessageId->find(messageID) != lateCrossTxMessageId->end()) {
        PLUGIN_LOG(INFO) << LOG_DESC("commit包之前就已集齐, 直接执行交易... in deter check")
                         << LOG_KV("messageId", messageID);
        auto readwriteset = crossTx2StateAddress->at(crossTxHash);
        groupVerifier->executeCrossTx(readwriteset);
        // crossTx2StateAddress->unsafe_erase(crossTxHash);
        // lateCrossTxMessageId->unsafe_erase(messageID);
    }
}

void deterministExecute::checkForDeterministExecuteTxWookLoop() {
    // 1. check candidate_corssTx_tx
    // 2. check lateCrossTxMessageId
    for(auto iter = candidate_tx_queues->cbegin(); iter != candidate_tx_queues->cend(); iter++)
    {
        auto readwriteKey = (*iter).first;
        if (candidate_tx_queues->at(readwriteKey).queue.size() > 0) {
            auto tx = candidate_tx_queues->at(readwriteKey).queue.front().tx;
            // transaction txInfo = crossTx[tx->hash()];
            transaction txInfo = crossTx->at(tx->hash().abridged());

            checkDelayCommitPacket(txInfo);

            // 有问题？
            if (txInfo.message_id != current_candidate_tx_messageids->at(txInfo.source_shard_id - 1) && 
                txInfo.message_id == complete_candidate_tx_messageids->at(txInfo.source_shard_id - 1) + 1) {
                groupVerifier->executeCandidateTx(txInfo.readwrite_key);
            }
        }
    }
}

std::string deterministExecute::dataToHexString(bytes data) {
    size_t m_data_size = data.size();
    std::string hex_m_data_str = "";
    for(size_t i = 0; i < m_data_size; i++)
    {
        string temp;
        stringstream ioss;
        ioss << std::hex << data.at(i);
        ioss >> temp;
        hex_m_data_str += temp;
    }
    return hex_m_data_str;
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
    // else if((index = hex_m_data_str.find("0x777888999", 0)) != -1) // batch交易
    // {
    //     return 4;
    // }
    else // 部署合约交易/跨片交易子交易
    {
        return 0; 
    }
}

void deterministExecute::processInnerShardTx(std::string data_str, std::shared_ptr<dev::eth::Transaction> tx) {
    int m = data_str.find("0x444555666", 0);
    int m2 = data_str.find_last_of('|');
    int len = data_str.length();

    std::string subtxStr = data_str.substr(m, m2 - m);
    // std::string subtxStr = data_str.substr(n, n2-n);
    PBFTENGINE_LOG(INFO) << LOG_DESC("发现片内交易！")
                            << LOG_KV("m", m)
                            << LOG_KV("len", len)
                            << LOG_KV("subtxStr", subtxStr);

    std::vector<std::string> subSignedTx;
    try
    {
        boost::split(subSignedTx, subtxStr, boost::is_any_of("|"), boost::token_compress_on);

        // 获得片内交易hash的字符串
        std::string tx_hash_str = tx->hash().abridged();
        std::string stateAddress = subSignedTx.at(1);
        // m_innerTxMutex.lock();
        innerTx->insert(std::make_pair(tx->hash().abridged(), transaction{
                        0, 
                        (unsigned long)0, 
                        (unsigned long)0, 
                        (unsigned long)0, 
                        tx_hash_str, 
                        tx, 
                        stateAddress}));
        // m_innerTxMutex.unlock();
        
    }
    catch (std::exception& e)
    {
        exit(1);
    }
}

/*
void deterministExecute::processCrossShardTx(std::string data_str, std::shared_ptr<dev::eth::Transaction> tx) {
    int n = data_str.find("0x111222333", 0);
    int n2 = data_str.find_last_of('|');
    int len = data_str.length();

    // std::string subtxStr = data_str.substr(n, len - n);
    std::string subtxStr = data_str.substr(n, n2-n);
    PBFTENGINE_LOG(INFO) << LOG_DESC("发现跨片交易！")
                         << LOG_KV("n", n)
                         << LOG_KV("len", len)
                         << LOG_KV("subtxStr", subtxStr);
    
    std::vector<std::string> subSignedTx;
    // EDIT BY ZH
    try
    {
        boost::split(subSignedTx, subtxStr, boost::is_any_of("|"), boost::token_compress_on);

        // 对分片中的所有节点id进行遍历, 加入到列表中
        int subSignedTx_size = subSignedTx.size();
        // 存储跨片交易对应的分片ID
        std::vector<int> crossShardsID;
        // 获得跨片交易hash的字符串
        std::string cross_tx_hash_str = tx->hash().abridged();

        for(int i = 1; i < subSignedTx_size; i = i + 3)
        {
            int j = i;

            int destinShardID = atoi(subSignedTx.at(j).c_str()); // 目标分片ID
            std::string subSignedData = subSignedTx.at(j+1); // 发向目标分片的跨片交易子交易
            std::string stateAddress = subSignedTx.at(j+2); // 向目标分片发送目标状态地址
            int messageID = messageIDs[destinShardID];
            messageID++;
            messageIDs[destinShardID] = messageID;
            crossShardsID.push_back(destinShardID);

            PBFTENGINE_LOG(INFO) << LOG_DESC("准备转发跨片子交易")
                                 << LOG_KV("messageId", messageID)
                                 << LOG_KV("destinShardID", destinShardID)
                                 << LOG_KV("subSignedData", subSignedData)
                                 << LOG_KV("stateAddress", stateAddress)
                                 << LOG_KV("internal_groupId", internal_groupId)
                                 << LOG_KV("crossTxHash", cross_tx_hash_str);
            
            // 将跨片子交易转发给相应的参与者分片
            // 下面调用 void forwardTx(protos::SubCrossShardTx _subCrossShardTx) 对交易进行转发，转发到相应分片
            protos::SubCrossShardTx subCrossShardTx;
            subCrossShardTx.set_signeddata(subSignedData);
            subCrossShardTx.set_stateaddress(stateAddress);
            subCrossShardTx.set_sourceshardid(internal_groupId);
            subCrossShardTx.set_destinshardid(destinShardID);
            subCrossShardTx.set_messageid(messageID);
            subCrossShardTx.set_crosstxhash(cross_tx_hash_str); //跨片交易哈希字符串

            std::string serializedSubCrossShardTx_str;
            subCrossShardTx.SerializeToString(&serializedSubCrossShardTx_str);
            auto txByte = asBytes(serializedSubCrossShardTx_str);

            dev::sync::SyncCrossTxPacket crossTxPacket; // 类型需要自定义
            crossTxPacket.encode(txByte);
            auto msg = crossTxPacket.toMessage(group_protocolID);

            PBFTENGINE_LOG(INFO) << LOG_DESC("协调者共识完毕, 开始向参与者分片发送跨片交易....")
                                 << LOG_KV("group_protocolID", group_protocolID);

            // std::string nodeIdStr = toHex(m_keyPair.pub());
            for(size_t i = 0; i < forwardNodeId.size(); i++)
            {
                // 判断当前节点是否为转发人
                if(dev::plugin::nodeIdStr == toHex(forwardNodeId.at(i)))
                {
                    // PBFTENGINE_LOG(INFO) << LOG_DESC("匹配成功，当前节点为头节点");
                    for(int j = 3; j >= 0; j--)  // 给所有节点发
                    {
                        // PBFTENGINE_LOG(INFO) << LOG_KV("正在发送给", shardNodeId.at((destinShardID-1) * 4 + j));
                        group_p2p_service->asyncSendMessageByNodeID(shardNodeId.at((destinShardID-1) * 4 + j), msg, CallbackFuncWithSession(), dev::network::Options());
                    }

                    // // 仅发送给分片的主节点
                    // PBFTENGINE_LOG(INFO) << LOG_KV("正在发送给", forwardNodeId.at(destinShardID - 1));
                    // group_p2p_service->asyncSendMessageByNodeID(forwardNodeId.at(destinShardID - 1), msg, CallbackFuncWithSession(), dev::network::Options());
                }
            }
            PBFTENGINE_LOG(INFO) << LOG_DESC("跨片消息转发完毕...");
        }
        //ADD BY ZH  存储跨片交易哈希对应的分片ID
        crossTx2ShardID->insert(std::make_pair(cross_tx_hash_str, crossShardsID));
    }
    catch (std::exception& e)
    {
        exit(1);
    }

}
*/

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
    // PBFTENGINE_LOG(INFO) << LOG_DESC("发现跨片交易！")
    //                      << LOG_KV("subtxStr", subtxStr);
    
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
    
            std::string key = subShardIds + "|" + str_shardID + "|" + stateAddress;
            if (key2Signdatas->count(key) == 0) {
                key2Signdatas->insert(std::make_pair(key, signedData));
                key2CrossTxHash->insert(std::make_pair(key, tx->hash().abridged()));
            } else {
                // std::string oriSignData = key2Signdatas->at(key);
                // oriSignData += ("|" + signedData);
                key2Signdatas->at(key) += ("|" + signedData);
                // PBFTENGINE_LOG(INFO) << LOG_DESC("新子交易集合")
                //                      << LOG_KV("key", key);
                                    //  << LOG_KV("signedDatas", key2Signdatas->at(key));
            }
            
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
        PLUGIN_LOG(INFO) << LOG_DESC("in createBatchTransaction GroupID为1...");
        str_address = innerContact_1;
    } else if (groupId == 2) {
        PLUGIN_LOG(INFO) << LOG_DESC("in createBatchTransaction GroupID为2...");
        str_address = innerContact_2;
    } else if (groupId == 3) {
        PLUGIN_LOG(INFO) << LOG_DESC("in createBatchTransaction GroupID为3...");
        str_address = innerContact_3;
    }
    dev::Address contactAddress(str_address);
    dev::eth::ContractABI abi;
    bytes data = abi.abiIn("add(string)", hex_m_data_str);  // add

    std::shared_ptr<Transaction> tx = std::make_shared<Transaction>(0, 1000, 0, contactAddress, data);
    tx->setNonce(tx->nonce() + u256(utcTime()));
    tx->setGroupId(groupId);
    
    tx->setBlockLimit(u256(m_blockchainManager->number() + 10));
    
    auto keyPair = KeyPair::create();
    auto sig = dev::crypto::Sign(keyPair, tx->hash(WithoutSignature));
    tx->updateSignature(sig);

    return toHex(tx->rlp());
}

void deterministExecute::tryToSendSubTxs() {
    std::vector<std::string> keySet;
    for (auto it = key2Signdatas->begin(); it != key2Signdatas->end(); it ++) {
        std::string key = it->first;
        keySet.push_back(key);
        std::string signedDatas = it->second;
        std::vector<std::string> subKeys;
        // 存储跨片交易对应的分片ID
        std::vector<int> crossShardsID;

        try {
            boost::split(subKeys, key, boost::is_any_of("|"), boost::token_compress_on);
            auto subShardIds = subKeys.at(0); // 子分片集合
            auto shardID = atoi(subKeys.at(1).c_str()); // 目标分片
            auto stateAddress = subKeys.at(2); // 状态集
            // 需要删除？ 会被将来的交易集覆盖
            auto crossTxHash = key2CrossTxHash->at(key) ; // 跨片交易hash的字符串--标志作用
            
            int messageID = messageIDs[shardID];
            // messageID++;
            messageIDs[shardID] = ++messageID;

            std::vector<std::string> shardIds;
            boost::split(shardIds, subShardIds, boost::is_any_of("_"), boost::token_compress_on);
            for (int i = 0; i < shardIds.size(); i++) {
                // PBFTENGINE_LOG(INFO) << LOG_KV("shardId", shardIds.at(i));
                crossShardsID.push_back(atoi(shardIds.at(i).c_str()));
            }

            // 只需要转发节点处理，积攒后批量发送至参与者
            if(dev::plugin::nodeIdStr != toHex(forwardNodeId.at(internal_groupId - 1)))
            {
                crossTx2ShardID->insert(std::make_pair(crossTxHash, crossShardsID));
                for (int i = 0; i < keySet.size(); i++) {
                    std::string key = keySet[i];
                    key2Signdatas->unsafe_erase(key);
                    key2CrossTxHash->unsafe_erase(key);
                }
                return;
            }

            // 对批量交易进行二次包装
            std::string signedTxs = createBatchTransaction(signedDatas, shardID);
            
            PBFTENGINE_LOG(INFO) << LOG_DESC("批量发送跨片子交易")
                                 << LOG_KV("subShardIds", subShardIds)
                                 << LOG_KV("messageId", messageID)
                                 << LOG_KV("destinShardID", shardID)
                                //  << LOG_KV("signedTxs", signedTxs)
                                 << LOG_KV("stateAddress", stateAddress)
                                 << LOG_KV("internal_groupId", internal_groupId)
                                 << LOG_KV("crossTxHash", crossTxHash);
            
            // 将跨片子交易转发给相应的参与者分片
            // 下面调用 void forwardTx(protos::SubCrossShardTx _subCrossShardTx) 对交易进行转发，转发到相应分片
            protos::SubCrossShardTx subCrossShardTx;
            subCrossShardTx.set_signeddata(signedTxs);
            subCrossShardTx.set_stateaddress(stateAddress);
            subCrossShardTx.set_sourceshardid(internal_groupId);
            subCrossShardTx.set_destinshardid(shardID);
            subCrossShardTx.set_messageid(messageID);
            subCrossShardTx.set_crosstxhash(crossTxHash); //跨片交易哈希字符串

            std::string serializedSubCrossShardTx_str;
            subCrossShardTx.SerializeToString(&serializedSubCrossShardTx_str);
            auto txByte = asBytes(serializedSubCrossShardTx_str);

            dev::sync::SyncCrossTxPacket crossTxPacket; // 类型需要自定义
            crossTxPacket.encode(txByte);
            auto msg = crossTxPacket.toMessage(group_protocolID);

            PBFTENGINE_LOG(INFO) << LOG_DESC("协调者共识完毕, 开始向参与者分片发送跨片交易....");

            // 发送交易包给所有节点
            for(int j = 3; j >= 0; j--) { 
                // PBFTENGINE_LOG(INFO) << LOG_KV("正在发送给", shardNodeId.at((shardID - 1) * 4 + j));
                group_p2p_service->asyncSendMessageByNodeID(shardNodeId.at((shardID - 1) * 4 + j), msg, CallbackFuncWithSession(), dev::network::Options());
            }
            PBFTENGINE_LOG(INFO) << LOG_DESC("跨片消息转发完毕...");

            //ADD BY ZH  存储跨片交易哈希对应的分片ID
            crossTx2ShardID->insert(std::make_pair(crossTxHash, crossShardsID));

        } catch (std::exception& e) {
            exit(1);
        }
    }
    // 删除发送过的交易
    for (int i = 0; i < keySet.size(); i++) {
        std::string key = keySet[i];
        key2Signdatas->unsafe_erase(key);
        key2CrossTxHash->unsafe_erase(key);
    }
}

void deterministExecute::processSubShardTx(std::shared_ptr<dev::eth::Transaction> tx, int height) {
    // 批量处理子交易处理
    auto txInfo = crossTx->at(tx->hash().abridged());
    // m_crossTxMutex.unlock();
    auto crossTxHash = txInfo.cross_tx_hash;
    auto blockHeight = height;

    PLUGIN_LOG(INFO) << LOG_DESC("添加(批量)跨片交易至执行队列 in processSubShardTx")
                     << LOG_KV("messageId", txInfo.message_id);

    if (blockHeight2CrossTxHash->count(blockHeight) == 0) {
        std::vector<std::string> temp;
        temp.push_back(crossTxHash);
        blockHeight2CrossTxHash->insert(std::make_pair(blockHeight, temp));
    } else {
        blockHeight2CrossTxHash->at(blockHeight).push_back(crossTxHash);
    }
}

void deterministExecute::processConsensusBlock() {
    PLUGIN_LOG(INFO) << "processConsensusBlock 线程开启...";

    int blockId = 0;
    int totalTxs = 0;

    while (true) {
        int currentBlockNum = m_blockchainManager->number(); // 当前块高
        if(currentBlockNum > blockId) {
            PLUGIN_LOG(INFO) << LOG_DESC("区块增加，放交易入队列...");
            blockId++;
            std::shared_ptr<dev::eth::Block> currentBlock = m_blockchainManager->getBlockByNumber(blockId);
            size_t transactions_size = currentBlock->getTransactionSize();
            auto transactions = currentBlock->transactions();
            auto height = blockId;
            totalTxs += transactions_size;
            block2UnExecutedTxNum->insert(std::make_pair(height, transactions_size));
            PLUGIN_LOG(INFO) << LOG_DESC("添加区块交易数")
                             << LOG_KV("height", height)
                             << LOG_KV("num", transactions_size)
                             << LOG_KV("totalTxs", totalTxs);

            for(size_t i = 0; i < transactions_size; i++){
                auto tx = transactions->at(i);
                auto data_str = dataToHexString(tx->data());

                // 将区块内的每一笔交易映射到具体区块高度  EDIT BY ZH -- 22.11.2
                // //重复插入——待进一步解决？—— 22.11.16
                // if (txHash2BlockHeight->count(tx->hash().abridged()) != 0) { 
                //     continue;
                // }
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
                    // case BatchSubTxs:
                    //     processBatchSubTxs(tx);
                    default:
                        break;
                }
                dev::consensus::toExecute_transactions.push(tx); // 将共识完出块的交易逐个放入队列
            }

            tryToSendSubTxs(); // 上层每处理完一个区块，检查积攒的跨片交易并将其发送给相应的子分片
        }
        else{
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        
    }
}

void deterministExecute::setAttribute(std::shared_ptr<dev::blockchain::BlockChainInterface> _blockchainManager)
{
    m_blockchainManager = _blockchainManager;
}
#include "Common.h"
#include <libplugin/deterministExecute.h>
#include <libplugin/executeVM.h>
#include <libethcore/Transaction.h>
#include <thread>
#include <utility>

using namespace dev::plugin;
using namespace dev::consensus;
using namespace dev::p2p;

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
            PLUGIN_LOG(INFO) << LOG_DESC("commit包之前就已集齐, 直接执行交易...");
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
        // PLUGIN_LOG(INFO) << LOG_KV("正在发送给", shardNodeId.at((source_shard_id-1)*4 + j));
        m_group_service->asyncSendMessageByNodeID(shardNodeId.at((source_shard_id-1)*4 + j), msg, CallbackFuncWithSession(), dev::network::Options());
    }
    PLUGIN_LOG(INFO) << LOG_DESC("子分片向协调者发送状态包完毕...");
    // 更新正在处理的跨片交易的messageID
    current_candidate_tx_messageids->at(source_shard_id - 1) = messageID;
}


void deterministExecute::deterministExecuteTx()
{
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
            if (innerTx.find(tx->hash()) != innerTx.end()) { // 片内交易
                // auto readwriteset = "state";
                auto txInfo = innerTx[tx->hash()];
                auto readwriteset = txInfo.readwrite_key;
                // 获得交易hash
                auto txHash = tx->hash().abridged();
                // 获取交易所在区块高度
                auto blockHeight = txHash2BlockHeight->at(txHash);

                PLUGIN_LOG(INFO) << LOG_DESC("发现片内交易 in deterministExecuteTx...")
                                 << LOG_KV("txhash", tx->hash())
                                 << LOG_KV("stateAddress", readwriteset);
                
                if (m_lockKeyMutex.try_lock()) {
                    if (locking_key->count(readwriteset) == 0 || locking_key->at(readwriteset) == 0) { // 执行片内交易
                        // 删除txHash2BlockHeight变量，代表相关交易已被执行
                        // txHash2BlockHeight->unsafe_erase(txHash);

                        if (block2UnExecutedTxNum->count(blockHeight) == 0) {
                            // 该笔交易所在区块交易已全部完成过了
                            return;
                        }

                        if (locking_key->count(readwriteset) == 0) {
                            locking_key->insert(std::make_pair(readwriteset, 1));
                            m_lockKeyMutex.unlock();
                        } else if (locking_key->at(readwriteset) == 0) {
                            locking_key->at(readwriteset) = 1;
                            m_lockKeyMutex.unlock();
                        }

                        auto exec = dev::plugin::executiveContext->getExecutive();
                        auto vm = dev::plugin::executiveContext->getExecutiveInstance();
                        exec->setVM(vm);
                        dev::plugin::executiveContext->executeTransaction(exec, tx);
                        dev::plugin::executiveContext->m_vminstance_pool.push(vm);
                        
                        if(m_lockKeyMutex.try_lock()) {
                            locking_key->at(readwriteset)--;
                            m_lockKeyMutex.unlock();
                        }

                        // 维护区块未完成交易
                        PLUGIN_LOG(INFO) << LOG_DESC("该笔交易对应的区块高度") << LOG_KV("blockHeight", blockHeight);
                        auto unExecutedTxNum = block2UnExecutedTxNum->at(blockHeight);
                        PLUGIN_LOG(INFO) << LOG_DESC("in deterministExecuteTx..")
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
                            PLUGIN_LOG(INFO) << LOG_DESC("in deterministExecuteTx...")  
                                             << LOG_KV("区块未完成交易now_num", block2UnExecutedTxNum->at(blockHeight));
                        }
                    } else { // 放入等待队列
                        // m_waitTxs.push_back(tx);
                        if(candidate_tx_queues->count(readwriteset) == 0) {
                            std::queue<executableTransaction> queue = std::queue<executableTransaction>();
                            candidate_tx_queue _candidate_tx_queue { readwriteset, queue };
                            _candidate_tx_queue.queue.push(executableTransaction{tx});
                            candidate_tx_queues->insert(std::make_pair(readwriteset, _candidate_tx_queue));
                            if (m_lockKeyMutex.try_lock()) {
                                locking_key->insert(std::make_pair(readwriteset, 1));
                                m_lockKeyMutex.unlock();
                            }
                        } else {
                            candidate_tx_queues->at(readwriteset).queue.push(executableTransaction{tx});
                            if (m_lockKeyMutex.try_lock()) {
                                locking_key->at(readwriteset)++;
                                m_lockKeyMutex.unlock();
                            }
                        }
                    }
                }
                // 释放变量
                innerTx.erase(tx->hash());

            } else if (crossTx.find(tx->hash()) != crossTx.end()) { // 跨片交易
                PLUGIN_LOG(INFO) << LOG_DESC("发现跨片交易 in deterministExecuteTx...")
                                 << LOG_KV("txhash", tx->hash());
                transaction txInfo = crossTx[tx->hash()];
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
                    PLUGIN_LOG(INFO) << LOG_DESC("insert candidate_tx_queues...");
                    
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
                    if (m_lockKeyMutex.try_lock()) {
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
                    }
                    
                    // 更新已经收到的按序的最大的messageID
                    latest_candidate_tx_messageids->at(source_shard_id - 1) = message_id;

                    // 检查cached_cs_tx 中后继 _message_id + 1 的交易是否已经到达, 若已经到达，也插入到 candidate_cs_tx 中，更新上锁的读写集
                    PLUGIN_LOG(INFO) << LOG_DESC("检查cached_cs_tx中后继message_id + 1的交易是否已经到达");
                    message_id = message_id + 1;
                    std::string attempt_key = std::to_string(source_shard_id) + std::to_string(message_id);
                    while(cached_cs_tx->count(attempt_key) != 0) {// 若后继 key 的跨片交易也在，也放入 candidate_cs_tx
                        PLUGIN_LOG(INFO) << LOG_DESC("存在之前乱序到达的满足条件的交易")
                                         << LOG_KV("messageId", message_id);
                        dev::plugin::transaction _subtx;
                        if (m_cachedTx.try_lock()) {
                            _subtx = cached_cs_tx->at(attempt_key);
                            m_cachedTx.unlock();
                        }

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
                        
                        if (m_lockKeyMutex.try_lock()) {
                            if(locking_key->count(readwriteset) == 0) { 
                                locking_key->insert(std::make_pair(readwriteset, 1));
                                m_lockKeyMutex.unlock();
                            } else {
                                locking_key->at(readwriteset)++;
                                m_lockKeyMutex.unlock();
                            }
                        }
                        
                        // 从 cached_cs_tx 中将交易删除
                        if (m_cachedTx.try_lock()) {
                            cached_cs_tx->unsafe_erase(attempt_key);
                            m_cachedTx.unlock();
                        }

                        message_id = message_id + 1;
                        attempt_key = std::to_string(source_shard_id) + std::to_string(message_id);
                        PLUGIN_LOG(INFO) << LOG_DESC("插入成功后")
                                         << LOG_KV("candidate_tx_queues.size", candidate_tx_queues->at(readwriteset).queue.size());
                    }
                    PLUGIN_LOG(INFO) << LOG_DESC("跨片交易-按序到达逻辑执行完成...");
                }
                else { // 乱序到达
                    PLUGIN_LOG(INFO) << LOG_DESC("插入乱序到达的跨片交易")
                                     << LOG_KV("messageId", message_id)
                                     << LOG_KV("source_shard_id", source_shard_id)
                                     << LOG_KV("destin_shard_id", destin_shard_id)
                                     << LOG_KV("crossTxHash", crossTxHash);
                    // std::cout << "insert_cached_cs_tx" << std::endl;
                    std::string _key = std::to_string(source_shard_id) + std::to_string(message_id);
                    if(m_cachedTx.try_lock()) {
                        cached_cs_tx->insert(std::make_pair(_key, txInfo));
                        m_cachedTx.unlock();
                    }
                }
            } else { // 部署交易/协调者的跨片交易/重复交易
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
                PLUGIN_LOG(INFO) << LOG_DESC("in deterministExecuteTx..")
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
                    PLUGIN_LOG(INFO) << LOG_DESC("in deterministExecuteTx...")  
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
                             << LOG_KV("queue.size()", candidate_tx_queues->at("state1").queue.size());
            if (candidate_tx_queues->at("state1").queue.size() > 0) {
                auto tx = candidate_tx_queues->at("state1").queue.front().tx;
                transaction txInfo = crossTx[tx->hash()];
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
        PLUGIN_LOG(INFO) << LOG_DESC("commit包之前就已集齐, 直接执行交易...");
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
            transaction txInfo = crossTx[tx->hash()];

            checkDelayCommitPacket(txInfo);

            // 有问题？
            if (txInfo.message_id != current_candidate_tx_messageids->at(txInfo.source_shard_id - 1) && 
                txInfo.message_id == complete_candidate_tx_messageids->at(txInfo.source_shard_id - 1) + 1) {
                groupVerifier->executeCandidateTx(txInfo.readwrite_key);
            }
        }
    }
}

// void deterministExecute::start()
// {
//     PLUGIN_LOG(INFO) << LOG_DESC("Start DeterministExecute...");
//     std::thread executetxsThread(deterministExecute::deterministExecuteTx);
//     executetxsThread.detach();
// }

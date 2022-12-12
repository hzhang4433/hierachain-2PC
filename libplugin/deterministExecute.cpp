#include "Common.h"
#include <libplugin/deterministExecute.h>
#include <libethcore/Transaction.h>
#include <thread>
#include <utility>
#include <libethcore/ABI.h>

using namespace dev::plugin;
using namespace dev::consensus;
using namespace dev::p2p;
using namespace dev::eth;

void deterministExecute::replyToCoordinator(dev::plugin::transaction txInfo, dev::PROTOCOL_ID& m_group_protocolID, std::shared_ptr<dev::p2p::Service> m_group_service) {    
    // unsigned long message_id = txInfo.message_id;
	unsigned long source_shard_id = txInfo.source_shard_id; // 协调者id
    string crossTxHash = txInfo.cross_tx_hash;
    unsigned long destin_shard_id = txInfo.destin_shard_id; // 本分片id
    unsigned long messageID = txInfo.message_id;

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
                        
    std::string serializedSubCrossShardTxReply_str;
    subCrossShardTxReply.SerializeToString(&serializedSubCrossShardTxReply_str);
    auto txByte = asBytes(serializedSubCrossShardTxReply_str);

    dev::sync::SyncCrossTxReplyPacket crossTxReplyPacket; // 类型需要自定义
    crossTxReplyPacket.encode(txByte);
    auto msg = crossTxReplyPacket.toMessage(m_group_protocolID);

    PLUGIN_LOG(INFO) << LOG_DESC("状态锁获得, 开始向协调者分片发送状态包....")
                     << LOG_KV("m_group_protocolID", m_group_protocolID)
                     << LOG_KV("messageId", messageID);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    for(size_t j = 0; j < 4; j++)  // 给所有协调者分片所有节点发
    {
        PLUGIN_LOG(INFO) << LOG_KV("正在发送给", shardNodeId.at((source_shard_id-1)*4 + j));
        m_group_service->asyncSendMessageByNodeID(shardNodeId.at((source_shard_id-1)*4 + j), msg, CallbackFuncWithSession(), dev::network::Options());
    }
    PLUGIN_LOG(INFO) << LOG_DESC("子分片向协调者发送状态包完毕...");
    // 更新正在处理的跨片交易的messageID
    current_candidate_tx_messageids->at(source_shard_id - 1) = messageID;
}

void deterministExecute::replyToCoordinatorCommitOK(dev::plugin::transaction txInfo) {
	unsigned long source_shard_id = txInfo.source_shard_id; // 协调者id
    string crossTxHash = txInfo.cross_tx_hash;
    unsigned long destin_shard_id = txInfo.destin_shard_id; // 本分片id
    auto messageId = txInfo.message_id;
    
    protos::SubCrossShardTxCommitReply subCrossShardTxCommitReply;
    subCrossShardTxCommitReply.set_crosstxhash(crossTxHash);
    subCrossShardTxCommitReply.set_destinshardid(source_shard_id);
    subCrossShardTxCommitReply.set_sourceshardid(destin_shard_id);
    subCrossShardTxCommitReply.set_messageid(messageId);
    subCrossShardTxCommitReply.set_status(1);
                        
    std::string serializedSubCrossShardTxCommitReply_str;
    subCrossShardTxCommitReply.SerializeToString(&serializedSubCrossShardTxCommitReply_str);
    auto txByte = asBytes(serializedSubCrossShardTxCommitReply_str);

    dev::sync::SyncCrossTxCommitReplyPacket crossTxCommitReplyPacket; // 类型需要自定义
    crossTxCommitReplyPacket.encode(txByte);
    auto msg = crossTxCommitReplyPacket.toMessage(group_protocolID);

    BLOCKVERIFIER_LOG(INFO) << LOG_DESC("跨片交易执行完成, 开始向协调者分片发送commitOK消息包....")
                            << LOG_KV("group_protocolID", group_protocolID);

    // std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    for(size_t j = 0; j < 4; j++)  // 给所有协调者分片所有节点发
    {
        // BLOCKVERIFIER_LOG(INFO) << LOG_KV("正在发送给", shardNodeId.at((source_shard_id-1)*4 + j));
        group_p2p_service->asyncSendMessageByNodeID(shardNodeId.at((source_shard_id-1)*4 + j), msg, CallbackFuncWithSession(), dev::network::Options());
    }
    BLOCKVERIFIER_LOG(INFO) << LOG_DESC("子分片向协调者发送commitOK消息包完毕...");

    // 发送完回执再删除相关变量，假设此时以收到了所有的commitMsg
    // crossTx2CommitMsg->unsafe_erase(txInfo.cross_tx_hash);

    // 将完成的跨片交易加入doneCrossTx
    doneCrossTx->insert(crossTxHash);
}

void deterministExecute::deterministExecuteTx() {
    PLUGIN_LOG(INFO) << "deterministExecuteTx 线程开启...";

     
    // std::shared_ptr<dev::eth::Transaction> tx;
    
    while (true)
    {   
        /*
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
                
                // 
                // m_lockKeyMutex.lock();
                // if (locking_key->count(readwriteset) == 0 || locking_key->at(readwriteset) == 0) { // 执行片内交易
                //     // 删除txHash2BlockHeight变量，代表相关交易已被执行
                //     // txHash2BlockHeight->unsafe_erase(txHash);

                //     if (block2UnExecutedTxNum->count(blockHeight) == 0) {
                //         // 该笔交易所在区块交易已全部完成过了
                //         m_lockKeyMutex.unlock();
                //         return;
                //     }

                //     if (locking_key->count(readwriteset) == 0) {
                //         locking_key->insert(std::make_pair(readwriteset, 1));
                //     } else if (locking_key->at(readwriteset) == 0) {
                //         locking_key->at(readwriteset) = 1;
                //     }
                //     // m_lockKeyMutex.unlock();

                //     auto exec = dev::plugin::executiveContext->getExecutive();
                //     auto vm = dev::plugin::executiveContext->getExecutiveInstance();
                //     exec->setVM(vm);
                //     dev::plugin::executiveContext->executeTransaction(exec, tx);
                //     dev::plugin::executiveContext->m_vminstance_pool.push(vm);
                    
                //     // m_lockKeyMutex.lock();
                //     locking_key->at(readwriteset)--;
                //     m_lockKeyMutex.unlock();
                    

                //     // 维护区块未完成交易
                //     PLUGIN_LOG(INFO) << LOG_DESC("该笔交易对应的区块高度") << LOG_KV("blockHeight", blockHeight);
                //     auto unExecutedTxNum = block2UnExecutedTxNum->at(blockHeight);
                //     PLUGIN_LOG(INFO) << LOG_DESC("in deterministExecuteTx innerTx...")
                //                      << LOG_KV("区块未完成交易before_num", unExecutedTxNum);
                //     unExecutedTxNum = unExecutedTxNum - 1;
                //     block2UnExecutedTxNum->at(blockHeight) = unExecutedTxNum;
                //     if (unExecutedTxNum == 0) {
                //         PLUGIN_LOG(INFO) << LOG_BADGE("区块中的数据全部执行完")
                //                          << LOG_KV("block_height", blockHeight);
                //         /* 删除相关变量
                //         //     1. block2ExecutedTxNum
                //         //     2. 2PC流程中的变量: doneCrossTx
                //         if (m_block2UnExecMutex.try_lock()) {
                //             // PLUGIN_LOG(INFO) << LOG_DESC("测试锁, 进来了");
                //             block2UnExecutedTxNum->unsafe_erase(blockHeight);
                //             m_block2UnExecMutex.unlock();
                //             // PLUGIN_LOG(INFO) << LOG_DESC("测试锁，就要出去");
                //         }
                //         if (blockHeight2CrossTxHash->count(blockHeight) != 0) {
                //             for (auto i : blockHeight2CrossTxHash->at(blockHeight)) {
                //                 PLUGIN_LOG(INFO) << LOG_DESC("正在删除doneCrossTx...该区块高度存在的跨片交易有：")
                //                                 << LOG_KV("crossTxHash", i);
                //                 // maybe 要加锁
                //                 if (m_doneCrossTxMutex.try_lock()) {
                //                     doneCrossTx->unsafe_erase(i);
                //                     m_doneCrossTxMutex.unlock();
                //                 }
                //             }
                //             if (m_height2TxHashMutex.try_lock()) {
                //                 blockHeight2CrossTxHash->unsafe_erase(blockHeight);
                //                 m_height2TxHashMutex.unlock();
                //             }
                //         }
                //         *
                //     }
                //     if (block2UnExecutedTxNum->count(blockHeight) != 0) {
                //         PLUGIN_LOG(INFO) << LOG_DESC("in deterministExecuteTx innerTx...")  
                //                          << LOG_KV("区块未完成交易now_num", block2UnExecutedTxNum->at(blockHeight));
                //     }
                // } else { // 放入等待队列
                //     // m_waitTxs.push_back(tx);
                //     if(candidate_tx_queues->count(readwriteset) == 0) {
                //         std::queue<executableTransaction> queue = std::queue<executableTransaction>();
                //         candidate_tx_queue _candidate_tx_queue { readwriteset, queue };
                //         _candidate_tx_queue.queue.push(executableTransaction{tx});
                //         candidate_tx_queues->insert(std::make_pair(readwriteset, _candidate_tx_queue));
                //         locking_key->insert(std::make_pair(readwriteset, 1));
                //     } else {
                //         candidate_tx_queues->at(readwriteset).queue.push(executableTransaction{tx});
                //         locking_key->at(readwriteset)++;
                //     }
                // }

                if (m_blockingTxQueue->isBlocked(readwriteset)) { // 阻塞，放入队列
                    m_blockingTxQueue->insertTx(txInfo);
                } else { // 没阻塞，直接执行
                    // 删除txHash2BlockHeight变量，代表相关交易已被执行
                    // txHash2BlockHeight->unsafe_erase(txHash);

                    auto exec = dev::plugin::executiveContext->getExecutive();
                    auto vm = dev::plugin::executiveContext->getExecutiveInstance();
                    exec->setVM(vm);
                    dev::plugin::executiveContext->executeTransaction(exec, tx);
                    dev::plugin::executiveContext->m_vminstance_pool.push(vm);

                    // executedInnerTx++;
                    // if (executedInnerTx % 100 == 0) {
                    //     PLUGIN_LOG(INFO) << LOG_KV("executedInnerTx", executedInnerTx);
                    // }

                    executedTx++;
                    if (executedTx % 100 == 0) {
                        PLUGIN_LOG(INFO) << LOG_KV("executedTx", executedTx);
                    }

                    // EDIT ON 22-12-10
                    // // 维护区块未完成交易
                    // PLUGIN_LOG(INFO) << LOG_DESC("该笔交易对应的区块高度") << LOG_KV("blockHeight", blockHeight);
                    // auto unExecutedTxNum = block2UnExecutedTxNum->at(blockHeight);
                    // PLUGIN_LOG(INFO) << LOG_DESC("in deterministExecuteTx innerTx...")
                    //                  << LOG_KV("区块未完成交易before_num", unExecutedTxNum);
                    // unExecutedTxNum = unExecutedTxNum - 1;
                    // block2UnExecutedTxNum->at(blockHeight) = unExecutedTxNum;
                    // if (unExecutedTxNum == 0) {
                    //     PLUGIN_LOG(INFO) << LOG_BADGE("区块中的数据全部执行完")
                    //                      << LOG_KV("block_height", blockHeight);
                    //     删除相关变量
                    //     //     1. block2ExecutedTxNum
                    //     //     2. 2PC流程中的变量: doneCrossTx
                    //     if (m_block2UnExecMutex.try_lock()) {
                    //         // PLUGIN_LOG(INFO) << LOG_DESC("测试锁, 进来了");
                    //         block2UnExecutedTxNum->unsafe_erase(blockHeight);
                    //         m_block2UnExecMutex.unlock();
                    //         // PLUGIN_LOG(INFO) << LOG_DESC("测试锁，就要出去");
                    //     }
                    //     if (blockHeight2CrossTxHash->count(blockHeight) != 0) {
                    //         for (auto i : blockHeight2CrossTxHash->at(blockHeight)) {
                    //             PLUGIN_LOG(INFO) << LOG_DESC("正在删除doneCrossTx...该区块高度存在的跨片交易有：")
                    //                             << LOG_KV("crossTxHash", i);
                    //             // maybe 要加锁
                    //             if (m_doneCrossTxMutex.try_lock()) {
                    //                 doneCrossTx->unsafe_erase(i);
                    //                 m_doneCrossTxMutex.unlock();
                    //             }
                    //         }
                    //         if (m_height2TxHashMutex.try_lock()) {
                    //             blockHeight2CrossTxHash->unsafe_erase(blockHeight);
                    //             m_height2TxHashMutex.unlock();
                    //         }
                    //     }
                    // }

                    // if (block2UnExecutedTxNum->count(blockHeight) != 0) {
                    //     PLUGIN_LOG(INFO) << LOG_DESC("in deterministExecuteTx innerTx...")  
                    //                      << LOG_KV("区块未完成交易now_num", block2UnExecutedTxNum->at(blockHeight));
                    // }

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
                    // PLUGIN_LOG(INFO) << LOG_DESC("insert candidate_tx_queues...")
                    //                  << LOG_KV("insert messageId", message_id);
                    // if(candidate_tx_queues->count(readwriteset) == 0) {
                    //     PLUGIN_LOG(INFO) << LOG_DESC("candidate_tx_queues->count == 0")
                    //                      << LOG_KV("readwriteset", readwriteset);
                    //     std::queue<executableTransaction> queue = std::queue<executableTransaction>();
                    //     candidate_tx_queue _candidate_tx_queue { readwriteset, queue };
                    //     _candidate_tx_queue.queue.push(executableTransaction{tx});
                    //     candidate_tx_queues->insert(std::make_pair(readwriteset, _candidate_tx_queue));
                    //     PLUGIN_LOG(INFO) << LOG_KV("candidate_tx_queue.size() first", candidate_tx_queues->at(readwriteset).queue.size());
                    // } else {
                    //     PLUGIN_LOG(INFO) << LOG_DESC("candidate_tx_queues->count != 0")
                    //                      << LOG_KV("readwriteset", readwriteset)
                    //                      << LOG_KV("candidate_tx_queue.size() before", candidate_tx_queues->at(readwriteset).queue.size());
                    //     // 当前片内交易的读写集（假设跨片交易的第一个读写集是当前片的读写集）, 定位读写集 readwrite_key 的交易缓存队列
                    //     // auto candidate_tx_queue = candidate_tx_queues->at(readwriteset);
                    //     // _subtx 插入到 candidate_cs_tx中，更新上锁的读写集
                    //     // candidate_tx_queue.queue.push(executableTransaction{tx});
                    //     candidate_tx_queues->at(readwriteset).queue.push(executableTransaction{tx});
                    //     PLUGIN_LOG(INFO) << LOG_KV("candidate_tx_queue.size() after", candidate_tx_queues->at(readwriteset).queue.size());
                    // }

                    // // insert_candidate_cs_tx(_tx);
                    // // 更新 locking_key
                    // m_lockKeyMutex.lock();
                    // if(locking_key->count(readwriteset) == 0) {
                    //     PLUGIN_LOG(INFO) << LOG_DESC("locking_key->count == 0");
                    //     // 向coordinator发送成功消息
                    //     locking_key->insert(std::make_pair(readwriteset, 1)); 
                    //     m_lockKeyMutex.unlock();
                        
                    //     replyToCoordinator(txInfo, group_protocolID, group_p2p_service);
                    // } else {
                    //     PLUGIN_LOG(INFO) << LOG_DESC("locking_key->count != 0");
                        
                    //     if (locking_key->at(readwriteset) == 0) { // 抢占锁成功
                    //         // 向coordinator发送成功消息
                    //         // PLUGIN_LOG(INFO) << LOG_DESC("holding_tx_num == 0")
                    //         PLUGIN_LOG(INFO) << LOG_DESC("该笔交易为跨片交易...非队列")
                    //                             << LOG_KV("messageId", message_id);
                    //         locking_key->at(readwriteset) = 1;
                    //         m_lockKeyMutex.unlock();

                    //         replyToCoordinator(txInfo, group_protocolID, group_p2p_service);
                    //     } else {
                    //         // 添加变量, 等待之前的交易完成
                    //         locking_key->at(readwriteset)++;
                    //         m_lockKeyMutex.unlock();
                    //     }
                    // }
                    
                    // EDIT ON 22.12.7 采用单队列结构
                    if (!m_blockingTxQueue->isBlocked(readwriteset)) {
                        replyToCoordinator(txInfo, group_protocolID, group_p2p_service);
                    }
                    m_blockingTxQueue->insertTx(txInfo);
                    
                    // 更新已经收到的按序的最大的messageID
                    latest_candidate_tx_messageids->at(source_shard_id - 1) = message_id;

                    // 检查cached_cs_tx 中后继 _message_id + 1 的交易是否已经到达, 若已经到达，也插入到 candidate_cs_tx 中，更新上锁的读写集
                    PLUGIN_LOG(INFO) << LOG_DESC("检查cached_cs_tx中后继message_id + 1的交易是否已经到达");
                    message_id = message_id + 1;
                    std::string attempt_key = std::to_string(source_shard_id) + std::to_string(message_id);
                    while(cached_cs_tx->count(attempt_key) != 0) { // 若后继 key 的跨片交易也在，也放入 candidate_cs_tx
                        PLUGIN_LOG(INFO) << LOG_DESC("存在之前乱序到达的满足条件的交易")
                                         << LOG_KV("insert messageId", message_id);
                        dev::plugin::transaction _subtx;
                        m_cachedTx.lock();
                        _subtx = cached_cs_tx->at(attempt_key);
                        m_cachedTx.unlock();

                        // EDIT ON 22.12.7 采用单队列结构
                        m_blockingTxQueue->insertTx(_subtx);

                        latest_candidate_tx_messageids->at(source_shard_id - 1) = message_id;

                        // // 定位读写集 readwrite_key 的交易缓存队列，先判断是否存在
                        // // 判断candidate_tx_queues中是否有readwrite_key的队列，因为之前可能没有
                        // if(candidate_tx_queues->count(readwriteset) == 0)
                        // {
                        //     std::queue<executableTransaction> queue = std::queue<executableTransaction>();
                        //     candidate_tx_queue _candidate_tx_queue { readwriteset, queue };
                        //     _candidate_tx_queue.queue.push(executableTransaction{_subtx.tx});
                        //     candidate_tx_queues->insert(std::make_pair(readwriteset, _candidate_tx_queue));
                        // }
                        // else
                        // {
                        //     PLUGIN_LOG(INFO) << LOG_DESC("交易插入前")
                        //                      << LOG_KV("candidate_tx_queues.size", candidate_tx_queues->at(readwriteset).queue.size());

                        //     // auto candidate_tx_queue = candidate_tx_queues->at(readwriteset);
                        //     // _subtx 插入到candidate_cs_tx中，更新上锁的读写集
                        //     // candidate_tx_queue.queue.push(executableTransaction{_subtx.tx});
                        //     candidate_tx_queues->at(readwriteset).queue.push(executableTransaction{_subtx.tx});
                        // }

                        
                        // m_lockKeyMutex.lock();
                        // if(locking_key->count(readwriteset) == 0) { 
                        //     locking_key->insert(std::make_pair(readwriteset, 1));
                        // } else {
                        //     locking_key->at(readwriteset)++;
                        // }
                        // m_lockKeyMutex.unlock();

                        // 从 cached_cs_tx 中将交易删除
                        m_cachedTx.lock();
                        cached_cs_tx->unsafe_erase(attempt_key);
                        m_cachedTx.unlock();

                        message_id = message_id + 1;
                        attempt_key = std::to_string(source_shard_id) + std::to_string(message_id);
                        // PLUGIN_LOG(INFO) << LOG_DESC("插入成功后")
                        //                  << LOG_KV("candidate_tx_queues.size", candidate_tx_queues->at(readwriteset).queue.size());
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
                    // 删除相关变量
                    //     1. block2ExecutedTxNum
                    //     2. 2PC流程中的变量: doneCrossTx

                    // if (m_block2UnExecMutex.try_lock()) {
                    //     // PLUGIN_LOG(INFO) << LOG_DESC("测试锁, 进来了");
                    //     block2UnExecutedTxNum->unsafe_erase(blockHeight);
                    //     m_block2UnExecMutex.unlock();
                    //     // PLUGIN_LOG(INFO) << LOG_DESC("测试锁，就要出去");
                    // }
                    // if (blockHeight2CrossTxHash->count(blockHeight) != 0) {
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
                }
                if (block2UnExecutedTxNum->count(blockHeight) != 0) {
                    PLUGIN_LOG(INFO) << LOG_DESC("in deterministExecuteTx otherTx...")  
                                     << LOG_KV("区块未完成交易now_num", block2UnExecutedTxNum->at(blockHeight));
                }
            }

        }
        */
        
        // why sleep
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        count++;
        if (count % 100 == 0 && m_blockingTxQueue->size() > 0) {
            transaction txInfo = m_blockingTxQueue->frontTx();
            auto tx = txInfo.tx;
            auto sourceShardId = txInfo.source_shard_id;

            PLUGIN_LOG(INFO) << LOG_DESC("continuing...")
                             << LOG_KV("candidate size", m_blockingTxQueue->size())
                             << LOG_KV("cached size", cached_cs_tx->size())
                             << LOG_KV("source shard id", sourceShardId);
            
            
            PLUGIN_LOG(INFO) << LOG_DESC("队首元素messageID")
                             << LOG_KV("messageId", txInfo.message_id)
                             << LOG_KV("当前正在执行的messageID", current_candidate_tx_messageids->at(sourceShardId - 1));
            
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
        // auto readwriteset = crossTx2StateAddress->at(crossTxHash);
        executeCrossTx();
        // groupVerifier->executeCrossTx(readwriteset);
        // crossTx2StateAddress->unsafe_erase(crossTxHash);
        // lateCrossTxMessageId->unsafe_erase(messageID);
    }
}

// 检查并处理被卡住的跨片交易
void deterministExecute::checkForDeterministExecuteTxWookLoop() {
    // 1. check candidate_corssTx_tx
    // 2. check lateCrossTxMessageId
    /*
    for(auto iter = candidate_tx_queues->cbegin(); iter != candidate_tx_queues->cend(); iter++)
    {
        auto readwriteKey = (*iter).first;
        if (candidate_tx_queues->at(readwriteKey).queue.size() > 0) {
            auto tx = candidate_tx_queues->at(readwriteKey).queue.front().tx;
            // transaction txInfo = crossTx[tx->hash()];
            transaction txInfo = crossTx->at(tx->hash().abridged());

            checkDelayCommitPacket(txInfo);

            // =========有问题？
            if (txInfo.message_id != current_candidate_tx_messageids->at(txInfo.source_shard_id - 1) && 
                txInfo.message_id == complete_candidate_tx_messageids->at(txInfo.source_shard_id - 1) + 1) {
                executeCandidateTx();
                // groupVerifier->executeCandidateTx(txInfo.readwrite_key);
            }
        }
    }*/

    if (m_blockingTxQueue->size() > 0) {
        transaction txInfo = m_blockingTxQueue->frontTx();
        auto tx = txInfo.tx;
        if (crossTx->find(tx->hash().abridged()) != crossTx->end()) { // 跨片交易
            // 异常1: 该笔跨片交易可能错过了之前已经收到的提交包，导致队列阻塞
            // 方案一：直接执行
            PLUGIN_LOG(INFO) << LOG_DESC("检查落后交易....")
                             << LOG_KV("messageId", txInfo.message_id);
            checkDelayCommitPacket(txInfo);
            // 方案二：先判断后执行
            // if (txInfo.message_id == current_candidate_tx_messageids->at(txInfo.source_shard_id - 1) && 
            //     txInfo.message_id == complete_candidate_tx_messageids->at(txInfo.source_shard_id - 1) + 1) {
            //     checkDelayCommitPacket(txInfo);
            // }
            
            /* 异常2: 上一笔跨片交易2PC流程已完成，跨片交易已收到，但却没有开始2PC流程 ==> 可能？
            if (txInfo.message_id != current_candidate_tx_messageids->at(txInfo.source_shard_id - 1) && 
                txInfo.message_id == complete_candidate_tx_messageids->at(txInfo.source_shard_id - 1) + 1) {
                executeCandidateTx();
                // groupVerifier->executeCandidateTx(txInfo.readwrite_key);
            }*/
        }
    }
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

        if (m_blockingTxQueue->isBlocked(readwriteset)) { // 阻塞，放入队列
            transaction txInfo = transaction{0, 
                                            (unsigned long)0, 
                                            (unsigned long)0, 
                                            (unsigned long)0, 
                                            tx_hash_str, 
                                            tx, 
                                            readwriteset};

            innerTx->insert(std::make_pair(tx->hash().abridged(), txInfo));
            m_blockingTxQueue->insertTx(txInfo);
        } else { // 没阻塞，直接执行
            // 删除txHash2BlockHeight变量，代表相关交易已被执行
            // txHash2BlockHeight->unsafe_erase(txHash);

            if (executedTx % 500 == 0) {
                exec = dev::plugin::executiveContext->getExecutive();
                auto vm = dev::plugin::executiveContext->getExecutiveInstance();
                exec->setVM(vm);
                dev::plugin::executiveContext->m_vminstance_pool.push(vm);

                PLUGIN_LOG(INFO) << LOG_KV("executedTx", executedTx);
            }
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
    PBFTENGINE_LOG(INFO) << LOG_DESC("发现跨片交易！")
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
                // PBFTENGINE_LOG(INFO) << LOG_DESC("新子交易集合")
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
    PBFTENGINE_LOG(INFO) << LOG_DESC("发现跨片交易！")
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
            
            if (key2CrossTxHash->count(subShardIds) == 0) {
                key2CrossTxHash->insert(std::make_pair(subShardIds, tx->hash().abridged()));
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

/*
void deterministExecute::tryToSendSubTxs() {
    std::vector<std::string> keySet;
    std::vector<std::string> hashKeySet;
    for (auto it = key2CrossTxHash->begin(); it != key2CrossTxHash->end(); it ++) {
        std::string hashKey = it->first;
        auto crossTxHash = it->second ; // 跨片交易hash的字符串--标志作用

        hashKeySet.push_back(hashKey);
        // std::string signedDatas = it->second;
        std::vector<std::string> subKeys;
        // 存储跨片交易对应的分片ID
        std::vector<int> crossShardsID;

        try {
            boost::split(subKeys, hashKey, boost::is_any_of("|"), boost::token_compress_on);
            auto subShardIds = subKeys.at(0); // 子分片集合
            auto stateAddress = subKeys.at(1); // 状态集

            // int messageID = messageIDs[shardID];
            // // messageID++;
            // messageIDs[shardID] = ++messageID;

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
                continue;
            }

            // 对所有涉及到的分片一次性发送对应交易 ===> 或可避免死锁
            for (int i = 0; i < shardIds.size(); i++) {
                auto shardID = atoi(shardIds.at(i).c_str()); // 目标分片
                int messageID = messageIDs[shardID];
                messageIDs[shardID] = ++messageID;
                std::string newKey = subShardIds + "|" + to_string(shardID) + "|" + stateAddress;
                std::string signedDatas = key2Signdatas->at(newKey);
                keySet.push_back(newKey);


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
            }

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
    }
    for (int i = 0; i < hashKeySet.size(); i++) {
        std::string key = hashKeySet[i];
        key2CrossTxHash->unsafe_erase(key);
    }
}
*/

void deterministExecute::tryToSendSubTxs() {
    std::vector<std::string> keySet;
    std::vector<std::string> hashKeySet;
    for (auto it = key2CrossTxHash->begin(); it != key2CrossTxHash->end(); it ++) {
        std::string subShardIds = it->first; // 子分片集合
        auto crossTxHash = it->second ; // 跨片交易hash的字符串--标志作用

        hashKeySet.push_back(subShardIds);
        // std::string signedDatas = it->second;
        // std::vector<std::string> subKeys;

        // 存储跨片交易对应的分片ID
        std::vector<int> crossShardsID;

        try {
            // int messageID = messageIDs[shardID];
            // // messageID++;
            // messageIDs[shardID] = ++messageID;

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
                continue;
            }

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
                
                PBFTENGINE_LOG(INFO) << LOG_DESC("批量发送跨片子交易")
                                    << LOG_KV("subShardIds", subShardIds)
                                    << LOG_KV("messageId", messageID)
                                    << LOG_KV("destinShardID", shardID)
                                    << LOG_KV("internal_groupId", internal_groupId)
                                    //  << LOG_KV("signedTxs", signedTxs)
                                    << LOG_KV("stateAddress", stateAddress)
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
                
                std::this_thread::sleep_for(std::chrono::milliseconds(50)); // 50+150 (*0.2 *0.8)

                // 发送交易包给所有节点
                for(int j = 3; j >= 0; j--) { 
                    // PBFTENGINE_LOG(INFO) << LOG_KV("正在发送给", shardNodeId.at((shardID - 1) * 4 + j));
                    group_p2p_service->asyncSendMessageByNodeID(shardNodeId.at((shardID - 1) * 4 + j), msg, CallbackFuncWithSession(), dev::network::Options());
                }
                PBFTENGINE_LOG(INFO) << LOG_DESC("跨片消息转发完毕...");
            }

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
        key2StateAddress->unsafe_erase(key);
    }
    for (int i = 0; i < hashKeySet.size(); i++) {
        std::string key = hashKeySet[i];
        key2CrossTxHash->unsafe_erase(key);
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

void deterministExecute::processSubShardTx(std::shared_ptr<dev::eth::Transaction> tx, int height) {
    // 批量跨片子交易处理
    // auto txInfo = crossTx->at(tx->hash().abridged());
    // // m_crossTxMutex.unlock();
    // auto crossTxHash = txInfo.cross_tx_hash;
    // auto blockHeight = height;

    // PLUGIN_LOG(INFO) << LOG_DESC("添加(批量)跨片交易至执行队列 in processSubShardTx")
    //                  << LOG_KV("messageId", txInfo.message_id)
    //                  << LOG_KV("stateAddress", txInfo.readwrite_key);

    // if (blockHeight2CrossTxHash->count(blockHeight) == 0) {
    //     std::vector<std::string> temp;
    //     temp.push_back(crossTxHash);
    //     blockHeight2CrossTxHash->insert(std::make_pair(blockHeight, temp));
    // } else {
    //     blockHeight2CrossTxHash->at(blockHeight).push_back(crossTxHash);
    // }
    
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
        // EDIT ON 22.12.7 采用单队列结构
        if (!m_blockingTxQueue->isBlocked(readwriteset)) {
            replyToCoordinator(txInfo, group_protocolID, group_p2p_service);
        }
        m_blockingTxQueue->insertTx(txInfo);
        
        // 更新已经收到的按序的最大的messageID
        latest_candidate_tx_messageids->at(source_shard_id - 1) = message_id;

        // 检查cached_cs_tx 中后继 _message_id + 1 的交易是否已经到达, 若已经到达，也插入到 candidate_cs_tx 中，更新上锁的读写集
        PLUGIN_LOG(INFO) << LOG_DESC("检查cached_cs_tx中后继message_id + 1的交易是否已经到达");
        message_id = message_id + 1;
        std::string attempt_key = std::to_string(source_shard_id) + std::to_string(message_id);
        while(cached_cs_tx->count(attempt_key) != 0) { // 若后继 key 的跨片交易也在，也放入 candidate_cs_tx
            PLUGIN_LOG(INFO) << LOG_DESC("存在之前乱序到达的满足条件的交易")
                             << LOG_KV("insert messageId", message_id);
            dev::plugin::transaction _subtx;
            m_cachedTx.lock();
            _subtx = cached_cs_tx->at(attempt_key);
            m_cachedTx.unlock();

            // EDIT ON 22.12.7 采用单队列结构
            m_blockingTxQueue->insertTx(_subtx);

            latest_candidate_tx_messageids->at(source_shard_id - 1) = message_id;

            // 从 cached_cs_tx 中将交易删除
            m_cachedTx.lock();
            cached_cs_tx->unsafe_erase(attempt_key);
            m_cachedTx.unlock();

            message_id = message_id + 1;
            attempt_key = std::to_string(source_shard_id) + std::to_string(message_id);
        
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
            PLUGIN_LOG(INFO) << LOG_DESC("区块增加，放交易入队列...");
            blockId++;
            std::shared_ptr<dev::eth::Block> currentBlock = m_blockchainManager->getBlockByNumber(blockId);
            size_t transactions_size = currentBlock->getTransactionSize();
            auto transactions = currentBlock->transactions();
            auto height = blockId;
            totalTxs += transactions_size;
            // consensusTx += transactions_size;
            block2UnExecutedTxNum->insert(std::make_pair(height, transactions_size));
            PLUGIN_LOG(INFO) << LOG_DESC("添加区块交易数")
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
        }
        else{
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        
    }
}

void deterministExecute::setAttribute(std::shared_ptr<dev::blockchain::BlockChainInterface> _blockchainManager) {
    m_blockchainManager = _blockchainManager;
}

void deterministExecute::executeCrossTx() {
    BLOCKVERIFIER_LOG(INFO) << LOG_DESC("in executeCrossTx... before pop")
                            // << LOG_KV("keyReadwriteSet", keyReadwriteSet)
                            << LOG_KV("queue size", m_blockingTxQueue->size());
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
    transaction txInfo = m_blockingTxQueue->frontTx();
    auto tx = txInfo.tx;
    m_blockingTxQueue->popTx();

    BLOCKVERIFIER_LOG(INFO) << LOG_DESC("in executeCrossTx... after pop")
                            << LOG_KV("queue size", m_blockingTxQueue->size());
    
    // ADD BY ZH ON 22.12.3 —— 添加批处理逻辑
    auto data_str = dataToHexString(tx->data());

    if (data_str == "") {
        PLUGIN_LOG(INFO) << LOG_DESC("in executeCrossTx data_str is null");
        
        
        if (executedTx % 500 == 0) {
            exec = dev::plugin::executiveContext->getExecutive();
            auto vm = dev::plugin::executiveContext->getExecutiveInstance();
            exec->setVM(vm);
            dev::plugin::executiveContext->m_vminstance_pool.push(vm);

            PLUGIN_LOG(INFO) << LOG_KV("executedTx", executedTx);
        }
        executedTx++;

        // auto exec = dev::plugin::executiveContext->getExecutive();
        // auto vm = dev::plugin::executiveContext->getExecutiveInstance();
        // exec->setVM(vm);
        dev::plugin::executiveContext->executeTransaction(exec, tx);
        // dev::plugin::executiveContext->m_vminstance_pool.push(vm);
    } else {
        std::vector<std::string> signedTxs;
        try {
            boost::split(signedTxs, data_str, boost::is_any_of("|"), boost::token_compress_on);
            int signedTxSize = signedTxs.size();
            PLUGIN_LOG(INFO) << LOG_DESC("in executeCrossTx data_str is not null")
                             << LOG_KV("signedTx num", signedTxSize);

            auto exec = dev::plugin::executiveContext->getExecutive();
            auto vm = dev::plugin::executiveContext->getExecutiveInstance();
            exec->setVM(vm);

            for (int i = 1; i < signedTxSize - 1; i++) {
                std::string signedTx = signedTxs.at(i);
                // PLUGIN_LOG(INFO) << LOG_DESC("解析批量交易")
                //                  << LOG_KV("signedTx", signedTx);

                Transaction::Ptr tx = std::make_shared<Transaction>(
                        jsToBytes(signedTx, dev::OnFailed::Throw), CheckTransaction::Everything);
                
                // PLUGIN_LOG(INFO) << LOG_DESC("222")
                //                  << LOG_KV("tx", toHex(tx->rlp()));
                
                // auto exec = dev::plugin::executiveContext->getExecutive();
                // auto vm = dev::plugin::executiveContext->getExecutiveInstance();
                // exec->setVM(vm);
                dev::plugin::executiveContext->executeTransaction(exec, tx);
                // dev::plugin::executiveContext->m_vminstance_pool.push(vm);
                
                if (executedTx % 500 == 0) {
                    PLUGIN_LOG(INFO) << LOG_KV("executedTx", executedTx);
                }
                executedTx++;
            }

            dev::plugin::executiveContext->m_vminstance_pool.push(vm);
        } catch (std::exception& e) {
            PLUGIN_LOG(INFO) << LOG_DESC("error!");
            exit(1);
        }
        
        // executedCrossTx++;
        // if (executedCrossTx % 100 == 0) {
        //     PLUGIN_LOG(INFO) << LOG_KV("executedCrossTx", executedCrossTx);
        // }
    }
    

    // BLOCKVERIFIER_LOG(INFO) << LOG_DESC("in executeCrossTx...")
    //                         << LOG_KV("keyReadwriteSet", keyReadwriteSet)
    //                         << LOG_KV("size2", candidate_tx_queues->at(keyReadwriteSet).queue.size())
    //                         << LOG_KV("messageId", txInfo.message_id);

    // EDIT BY ZH 22.11.2
    // auto exec = dev::plugin::executiveContext->getExecutive();
    // auto vm = dev::plugin::executiveContext->getExecutiveInstance();
    // exec->setVM(vm);
    // dev::plugin::executiveContext->executeTransaction(exec, tx);
    // dev::plugin::executiveContext->m_vminstance_pool.push(vm);

    // 更新已完成交易id
    complete_candidate_tx_messageids->at(txInfo.source_shard_id - 1) = txInfo.message_id;

    auto blockHeight = txHash2BlockHeight->at(tx->hash().abridged());
    PLUGIN_LOG(INFO) << LOG_DESC("in executeCrossTx... 该笔交易对应的区块高度")
                     << LOG_KV("blockHeight", blockHeight);
    auto unExecutedTxNum = block2UnExecutedTxNum->at(blockHeight);
    PLUGIN_LOG(INFO) << LOG_DESC("in executeCrossTx...")
                     << LOG_KV("区块未完成交易before_num", unExecutedTxNum);
    unExecutedTxNum = unExecutedTxNum - 1;
    block2UnExecutedTxNum->at(blockHeight) = unExecutedTxNum;
    // txHash2BlockHeight->unsafe_erase(tx->hash().abridged());
    // 判断剩余交易数并删除相关变量
    if (unExecutedTxNum == 0) {
        PLUGIN_LOG(INFO) << LOG_BADGE("区块中的数据全部执行完")
                         << LOG_KV("block_height", blockHeight);
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
        */ 
    }

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

    // 判断是否还有等待交易
    if (m_blockingTxQueue->size() > 0) {
        BLOCKVERIFIER_LOG(INFO) << LOG_DESC("in executeCrossTx... 还有等待的交易");
        executeCandidateTx();
    }
    

    // 发送成功回执
    replyToCoordinatorCommitOK(txInfo);
}

void deterministExecute::executeCandidateTx() {

    // auto executableTx = candidate_tx_queues->at(keyReadwriteSet).queue.front();
    // auto tx = executableTx.tx;

    // 取出下一笔交易 EDIT BY ZH ON 22.12.7
    // 确定 size > 0 ?
    transaction txInfo = m_blockingTxQueue->frontTx();
    auto tx = txInfo.tx;

    //判断是否为跨片交易
    // m_crossTxMutex.lock();
    if (crossTx->find(tx->hash().abridged()) == crossTx->end()) { // 非跨片交易 => 直接执行
        // m_crossTxMutex.unlock();

        PLUGIN_LOG(INFO) << LOG_DESC("该笔交易为片内交易...");
        
        // executedInnerTx++;
        // if (executedInnerTx % 100 == 0) {
        //     PLUGIN_LOG(INFO) << LOG_KV("executedInnerTx", executedInnerTx);
        // }
        if (executedTx % 500 == 0) {
            exec = dev::plugin::executiveContext->getExecutive();
            auto vm = dev::plugin::executiveContext->getExecutiveInstance();
            exec->setVM(vm);
            dev::plugin::executiveContext->m_vminstance_pool.push(vm);

            PLUGIN_LOG(INFO) << LOG_KV("executedTx", executedTx);
        }
        executedTx++;

        // EDIT BY ZH 22.12.11
        // auto exec = dev::plugin::executiveContext->getExecutive();
        // auto vm = dev::plugin::executiveContext->getExecutiveInstance();
        // exec->setVM(vm);
        dev::plugin::executiveContext->executeTransaction(exec, tx);
        // dev::plugin::executiveContext->m_vminstance_pool.push(vm);


        auto blockHeight = txHash2BlockHeight->at(tx->hash().abridged());
        PLUGIN_LOG(INFO) << LOG_DESC("该笔交易对应的区块高度") << LOG_KV("blockHeight", blockHeight);
        auto unExecutedTxNum = block2UnExecutedTxNum->at(blockHeight);
        PLUGIN_LOG(INFO) << LOG_DESC("in executeCandidateTx...")
                         << LOG_KV("区块未完成交易before", unExecutedTxNum);
        
        unExecutedTxNum = unExecutedTxNum - 1;
        block2UnExecutedTxNum->at(blockHeight) = unExecutedTxNum;

        // 判断剩余交易数并删除相关变量
        if (unExecutedTxNum == 0) {
            PLUGIN_LOG(INFO) << LOG_BADGE("区块中的数据全部执行完")
                             << LOG_KV("block_height", blockHeight);
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
        }

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

        if (m_blockingTxQueue->size() > 0) {
            executeCandidateTx();
        }
    } else { // 跨片交易
        // 获取跨片交易相关信息
        // transaction txInfo = crossTx[tx->hash()];
        transaction txInfo = crossTx->at(tx->hash().abridged());
        // m_crossTxMutex.unlock();

        BLOCKVERIFIER_LOG(INFO) << LOG_DESC("该笔交易为跨片交易...")
                                << LOG_KV("messageId", txInfo.message_id);
        
        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        // 向coordinator发送成功状态消息
        replyToCoordinator(txInfo, group_protocolID, group_p2p_service);
    }
}

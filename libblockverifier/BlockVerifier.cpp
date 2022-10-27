/*
 * @CopyRight:
 * FISCO-BCOS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * FISCO-BCOS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with FISCO-BCOS.  If not, see <http://www.gnu.org/licenses/>
 * (c) 2016-2018 fisco-dev contributors.
 */
/** @file BlockVerifier.cpp
 *  @author mingzhenliu
 *  @date 20180921
 */
#include "BlockVerifier.h"
#include "ExecutiveContext.h"
#include "TxDAG.h"
#include "libstorage/StorageException.h"
#include "libstoragestate/StorageState.h"
#include <libethcore/Exceptions.h>
#include <libethcore/PrecompiledContract.h>
#include <libethcore/TransactionReceipt.h>
#include <libstorage/Table.h>
#include <tbb/parallel_for.h>
#include <exception>
#include <thread>
#include <librpc/Common.h>
#include <libsync/SyncMsgPacket.h>


using namespace dev;
using namespace std;
using namespace dev::eth;
using namespace dev::blockverifier;
using namespace dev::executive;
using namespace dev::storage;
using namespace dev::rpc;
using namespace dev::plugin;
using namespace dev::consensus;
using namespace dev::p2p;

ExecutiveContext::Ptr BlockVerifier::executeBlock(Block& block, BlockInfo const& parentBlockInfo)
{
    // return nullptr prepare to exit when g_BCOSConfig.shouldExit is true
    if (g_BCOSConfig.shouldExit)
    {
        return nullptr;
    }
    if (block.blockHeader().number() < m_executingNumber)
    {
        return nullptr;
    }
    std::lock_guard<std::mutex> l(m_executingMutex);
    if (block.blockHeader().number() < m_executingNumber)
    {
        return nullptr;
    }
    ExecutiveContext::Ptr context = nullptr;
    try
    {
        context = serialExecuteBlock(block, parentBlockInfo); // 交易全部串行执行
        // if (g_BCOSConfig.version() >= RC2_VERSION && m_enableParallel)
        // {
        //     context = parallelExecuteBlock(block, parentBlockInfo);
        // }
        // else
        // {
        //     context = serialExecuteBlock(block, parentBlockInfo);
        // }
    }
    catch (exception& e)
    {
        BLOCKVERIFIER_LOG(ERROR) << LOG_BADGE("executeBlock") << LOG_DESC("executeBlock exception")
                                 << LOG_KV("blockNumber", block.blockHeader().number());
        return nullptr;
    }
    m_executingNumber = block.blockHeader().number();
    return context;
}

/*
ExecutiveContext::Ptr BlockVerifier::executeBlock(Block& block, BlockInfo const& parentBlockInfo, 
            dev::PROTOCOL_ID& m_group_protocolID, std::shared_ptr<dev::p2p::Service> m_group_service)
{
    // return nullptr prepare to exit when g_BCOSConfig.shouldExit is true
    if (g_BCOSConfig.shouldExit)
    {
        return nullptr;
    }
    if (block.blockHeader().number() < m_executingNumber)
    {
        return nullptr;
    }
    std::lock_guard<std::mutex> l(m_executingMutex);
    if (block.blockHeader().number() < m_executingNumber)
    {
        return nullptr;
    }
    ExecutiveContext::Ptr context = nullptr;
    try
    {
        context = serialExecuteBlock(block, parentBlockInfo, m_group_protocolID, m_group_service); // 交易全部串行执行
        // if (g_BCOSConfig.version() >= RC2_VERSION && m_enableParallel)
        // {
        //     context = parallelExecuteBlock(block, parentBlockInfo);
        // }
        // else
        // {
        //     context = serialExecuteBlock(block, parentBlockInfo);
        // }
    }
    catch (exception& e)
    {
        BLOCKVERIFIER_LOG(ERROR) << LOG_BADGE("executeBlock") << LOG_DESC("executeBlock exception")
                                 << LOG_KV("blockNumber", block.blockHeader().number());
        return nullptr;
    }
    m_executingNumber = block.blockHeader().number();
    return context;
}

ExecutiveContext::Ptr BlockVerifier::serialExecuteBlock(
    Block& block, BlockInfo const& parentBlockInfo)
{
    BLOCKVERIFIER_LOG(INFO) << LOG_DESC("executeBlock]Executing block")
                            << LOG_KV("txNum", block.transactions()->size())
                            << LOG_KV("num", block.blockHeader().number())
                            << LOG_KV("hash", block.header().hash().abridged())
                            << LOG_KV("height", block.header().number())
                            << LOG_KV("receiptRoot", block.header().receiptsRoot())
                            << LOG_KV("stateRoot", block.header().stateRoot())
                            << LOG_KV("dbHash", block.header().dbHash())
                            << LOG_KV("parentHash", parentBlockInfo.hash.abridged())
                            << LOG_KV("parentNum", parentBlockInfo.number)
                            << LOG_KV("parentStateRoot", parentBlockInfo.stateRoot);

    uint64_t startTime = utcTime();

    ExecutiveContext::Ptr executiveContext = std::make_shared<ExecutiveContext>();
    try
    {
        m_executiveContextFactory->initExecutiveContext(
            parentBlockInfo, parentBlockInfo.stateRoot, executiveContext);
    }
    catch (exception& e)
    {
        BLOCKVERIFIER_LOG(ERROR) << LOG_DESC("[executeBlock] Error during initExecutiveContext")
                                 << LOG_KV("blkNum", block.blockHeader().number())
                                 << LOG_KV("EINFO", boost::diagnostic_information(e));

        BOOST_THROW_EXCEPTION(InvalidBlockWithBadStateOrReceipt()
                              << errinfo_comment("Error during initExecutiveContext"));
    }

    BlockHeader tmpHeader = block.blockHeader();
    block.clearAllReceipts();
    block.resizeTransactionReceipt(block.transactions()->size());


    BLOCKVERIFIER_LOG(DEBUG) << LOG_BADGE("executeBlock") << LOG_DESC("Init env takes")
                             << LOG_KV("time(ms)", utcTime() - startTime)
                             << LOG_KV("txNum", block.transactions()->size())
                             << LOG_KV("num", block.blockHeader().number());
    uint64_t pastTime = utcTime();

    try
    {
        EnvInfo envInfo(block.blockHeader(), m_pNumberHash, 0);
        envInfo.setPrecompiledEngine(executiveContext);
        auto executive = createAndInitExecutive(executiveContext->getState(), envInfo);
        for (size_t i = 0; i < block.transactions()->size(); i++)
        {
            auto& tx = (*block.transactions())[i];

            // 检查交易hash, 根据std::vector<h256> subCrossTxsHash判断是否为跨片子交易
            // dev::

            TransactionReceipt::Ptr resultReceipt = execute(tx, executiveContext, executive);
            block.setTransactionReceipt(i, resultReceipt);
            executiveContext->getState()->commit();
        }
    }
    catch (exception& e)
    {
        BLOCKVERIFIER_LOG(ERROR) << LOG_BADGE("executeBlock")
                                 << LOG_DESC("Error during serial block execution")
                                 << LOG_KV("blkNum", block.blockHeader().number())
                                 << LOG_KV("EINFO", boost::diagnostic_information(e));

        BOOST_THROW_EXCEPTION(
            BlockExecutionFailed() << errinfo_comment("Error during serial block execution"));
    }


    BLOCKVERIFIER_LOG(DEBUG) << LOG_BADGE("executeBlock") << LOG_DESC("Run serial tx takes")
                             << LOG_KV("time(ms)", utcTime() - pastTime)
                             << LOG_KV("txNum", block.transactions()->size())
                             << LOG_KV("num", block.blockHeader().number());

    h256 stateRoot = executiveContext->getState()->rootHash();
    // set stateRoot in receipts
    if (g_BCOSConfig.version() >= V2_2_0)
    {
        // when support_version is lower than v2.2.0, doesn't setStateRootToAllReceipt
        // enable_parallel=true can't be run with enable_parallel=false
        block.setStateRootToAllReceipt(stateRoot);
    }
    block.updateSequenceReceiptGas();
    block.calReceiptRoot();
    block.header().setStateRoot(stateRoot);
    if (dynamic_pointer_cast<storagestate::StorageState>(executiveContext->getState()))
    {
        block.header().setDBhash(stateRoot);
    }
    else
    {
        block.header().setDBhash(executiveContext->getMemoryTableFactory()->hash());
    }

    // if executeBlock is called by consensus module, no need to compare receiptRoot and stateRoot
    // since origin value is empty if executeBlock is called by sync module, need to compare
    // receiptRoot, stateRoot and dbHash
    // Consensus module execute block, receiptRoot is empty, skip this judgment
    // The sync module execute block, receiptRoot is not empty, need to compare BlockHeader
    if (tmpHeader.receiptsRoot() != h256())
    {
        if (tmpHeader != block.blockHeader())
        {
            BLOCKVERIFIER_LOG(ERROR)
                << "Invalid Block with bad stateRoot or receiptRoot or dbHash"
                << LOG_KV("blkNum", block.blockHeader().number())
                << LOG_KV("originHash", tmpHeader.hash().abridged())
                << LOG_KV("curHash", block.header().hash().abridged())
                << LOG_KV("orgReceipt", tmpHeader.receiptsRoot().abridged())
                << LOG_KV("curRecepit", block.header().receiptsRoot().abridged())
                << LOG_KV("orgTxRoot", tmpHeader.transactionsRoot().abridged())
                << LOG_KV("curTxRoot", block.header().transactionsRoot().abridged())
                << LOG_KV("orgState", tmpHeader.stateRoot().abridged())
                << LOG_KV("curState", block.header().stateRoot().abridged())
                << LOG_KV("orgDBHash", tmpHeader.dbHash().abridged())
                << LOG_KV("curDBHash", block.header().dbHash().abridged());
            BOOST_THROW_EXCEPTION(
                InvalidBlockWithBadStateOrReceipt() << errinfo_comment(
                    "Invalid Block with bad stateRoot or ReceiptRoot, orgBlockHash " +
                    block.header().hash().abridged()));
        }
    }
    BLOCKVERIFIER_LOG(DEBUG) << LOG_BADGE("executeBlock") << LOG_DESC("Execute block takes")
                             << LOG_KV("time(ms)", utcTime() - startTime)
                             << LOG_KV("txNum", block.transactions()->size())
                             << LOG_KV("num", block.blockHeader().number())
                             << LOG_KV("blockHash", block.headerHash())
                             << LOG_KV("stateRoot", block.header().stateRoot())
                             << LOG_KV("dbHash", block.header().dbHash())
                             << LOG_KV("transactionRoot", block.transactionRoot())
                             << LOG_KV("receiptRoot", block.receiptRoot());
    return executiveContext;
}
*/
void BlockVerifier::replyToCoordinator(dev::plugin::transaction txInfo, 
            dev::PROTOCOL_ID& m_group_protocolID, std::shared_ptr<dev::p2p::Service> m_group_service) {
    // unsigned long message_id = txInfo.message_id;
	unsigned long source_shard_id = txInfo.source_shard_id; // 协调者id
    string crossTxHash = txInfo.cross_tx_hash;
    unsigned long destin_shard_id = txInfo.destin_shard_id; // 本分片id
    
    protos::SubCrossShardTxReply subCrossShardTxReply;
    subCrossShardTxReply.set_crosstxhash(crossTxHash);
    subCrossShardTxReply.set_destinshardid(source_shard_id);
    subCrossShardTxReply.set_sourceshardid(destin_shard_id);
    subCrossShardTxReply.set_status(1);
                        
    std::string serializedSubCrossShardTxReply_str;
    subCrossShardTxReply.SerializeToString(&serializedSubCrossShardTxReply_str);
    auto txByte = asBytes(serializedSubCrossShardTxReply_str);

    dev::sync::SyncCrossTxReplyPacket crossTxReplyPacket; // 类型需要自定义
    crossTxReplyPacket.encode(txByte);
    auto msg = crossTxReplyPacket.toMessage(m_group_protocolID);

    BLOCKVERIFIER_LOG(INFO) << LOG_DESC("状态锁获得, 开始向协调者分片发送状态包....")
                                                << LOG_KV("m_group_protocolID", m_group_protocolID);
    for(size_t j = 0; j < 4; j++)  // 给所有协调者分片所有节点发
    {
        BLOCKVERIFIER_LOG(INFO) << LOG_KV("正在发送给", shardNodeId.at((source_shard_id-1)*4 + j));
        m_group_service->asyncSendMessageByNodeID(shardNodeId.at((source_shard_id-1)*4 + j), msg, CallbackFuncWithSession(), dev::network::Options());
    }
    BLOCKVERIFIER_LOG(INFO) << LOG_DESC("子分片向协调者发送状态包完毕...");
}

void BlockVerifier::replyToCoordinatorCommitOK(dev::plugin::transaction txInfo) {
	unsigned long source_shard_id = txInfo.source_shard_id; // 协调者id
    string crossTxHash = txInfo.cross_tx_hash;
    unsigned long destin_shard_id = txInfo.destin_shard_id; // 本分片id
    
    protos::SubCrossShardTxCommitReply subCrossShardTxCommitReply;
    subCrossShardTxCommitReply.set_crosstxhash(crossTxHash);
    subCrossShardTxCommitReply.set_destinshardid(source_shard_id);
    subCrossShardTxCommitReply.set_sourceshardid(destin_shard_id);
    subCrossShardTxCommitReply.set_status(1);
                        
    std::string serializedSubCrossShardTxCommitReply_str;
    subCrossShardTxCommitReply.SerializeToString(&serializedSubCrossShardTxCommitReply_str);
    auto txByte = asBytes(serializedSubCrossShardTxCommitReply_str);

    dev::sync::SyncCrossTxCommitReplyPacket crossTxCommitReplyPacket; // 类型需要自定义
    crossTxCommitReplyPacket.encode(txByte);
    auto msg = crossTxCommitReplyPacket.toMessage(group_protocolID);

    BLOCKVERIFIER_LOG(INFO) << LOG_DESC("跨片交易执行完成, 开始向协调者分片发送commitOK消息包....")
                                                << LOG_KV("group_protocolID", group_protocolID);
    for(size_t j = 0; j < 4; j++)  // 给所有协调者分片所有节点发
    {
        BLOCKVERIFIER_LOG(INFO) << LOG_KV("正在发送给", shardNodeId.at((source_shard_id-1)*4 + j));
        group_p2p_service->asyncSendMessageByNodeID(shardNodeId.at((source_shard_id-1)*4 + j), msg, CallbackFuncWithSession(), dev::network::Options());
    }
    BLOCKVERIFIER_LOG(INFO) << LOG_DESC("子分片向协调者发送commitOK消息包完毕...");

    // 发送完回执再删除相关变量，假设此时以收到了所有的commitMsg
    crossTx2CommitMsg->unsafe_erase(txInfo.cross_tx_hash);
    // 将完成的跨片交易加入doneCrossTx
    
}


ExecutiveContext::Ptr BlockVerifier::serialExecuteBlock(Block& block, BlockInfo const& parentBlockInfo)
{
    BLOCKVERIFIER_LOG(INFO) << LOG_DESC("executeBlock]Executing block")
                            << LOG_KV("txNum", block.transactions()->size())
                            << LOG_KV("num", block.blockHeader().number())
                            << LOG_KV("hash", block.header().hash().abridged())
                            << LOG_KV("height", block.header().number())
                            << LOG_KV("receiptRoot", block.header().receiptsRoot())
                            << LOG_KV("stateRoot", block.header().stateRoot())
                            << LOG_KV("dbHash", block.header().dbHash())
                            << LOG_KV("parentHash", parentBlockInfo.hash.abridged())
                            << LOG_KV("parentNum", parentBlockInfo.number)
                            << LOG_KV("parentStateRoot", parentBlockInfo.stateRoot);

    uint64_t startTime = utcTime();
    // 标记是否有交易立即被执行 -- ADD BY ZH
    bool flag = false;

    ExecutiveContext::Ptr executiveContext = std::make_shared<ExecutiveContext>();
    try
    {
        m_executiveContextFactory->initExecutiveContext(
            parentBlockInfo, parentBlockInfo.stateRoot, executiveContext);
    }
    catch (exception& e)
    {
        BLOCKVERIFIER_LOG(ERROR) << LOG_DESC("[executeBlock] Error during initExecutiveContext")
                                 << LOG_KV("blkNum", block.blockHeader().number())
                                 << LOG_KV("EINFO", boost::diagnostic_information(e));

        BOOST_THROW_EXCEPTION(InvalidBlockWithBadStateOrReceipt()
                              << errinfo_comment("Error during initExecutiveContext"));
    }

    BlockHeader tmpHeader = block.blockHeader();
    block.clearAllReceipts();
    block.resizeTransactionReceipt(block.transactions()->size());


    BLOCKVERIFIER_LOG(DEBUG) << LOG_BADGE("executeBlock") << LOG_DESC("Init env takes")
                             << LOG_KV("time(ms)", utcTime() - startTime)
                             << LOG_KV("txNum", block.transactions()->size())
                             << LOG_KV("num", block.blockHeader().number());
    uint64_t pastTime = utcTime();

    try
    {
        EnvInfo envInfo(block.blockHeader(), m_pNumberHash, 0);
        envInfo.setPrecompiledEngine(executiveContext);
        auto executive = createAndInitExecutive(executiveContext->getState(), envInfo);
        // BLOCKVERIFIER_LOG(INFO) << LOG_BADGE("sleeping...");
        // sleep(10000);
        // BLOCKVERIFIER_LOG(INFO) << LOG_BADGE("sleeping over...");

        /*
        // 获取指向当前block的智能指针
        std::shared_ptr<dev::eth::Block> block_ptr = std::make_shared<dev::eth::Block>(block);
        for (size_t i = 0; i < block.transactions()->size(); i++)
        {
            auto& tx = (*block.transactions())[i];
            // 检查交易hash, 根据std::vector<h256> subCrossTxsHash判断是否为跨片子交易
            // std::vector<dev::h256>::iterator
            // auto it = find(subCrossTxsHash.begin(), subCrossTxsHash.end(), tx->hash());
            //
            if (crossTx.find(tx->hash()) == crossTx.end()) { // 非跨片交易
                BLOCKVERIFIER_LOG(INFO) << LOG_DESC("commiting state for nomarlTx")
                                        << LOG_KV("txhash", tx->hash());

                if (locking_key->count(readwriteset) == 0) { // 执行片内交易
                    // m_stateMutex = 1; 需要锁吗? 不需要，因为是顺序执行
                    TransactionReceipt::Ptr resultReceipt = execute(tx, executiveContext, executive);
                    block.setTransactionReceipt(i, resultReceipt);
                    executiveContext->getState()->commit();
                    flag = true;
                    block.unExecutedTxNum = block.unExecutedTxNum - 1;
                } else { // 放入等待队列
                    // m_waitTxs.push_back(tx);
                    if(candidate_tx_queues->count(readwriteset) == 0) {
                        std::queue<executableTransaction> queue = std::queue<executableTransaction>();
                        candidate_tx_queue _candidate_tx_queue { readwriteset, queue };
                        
                        _candidate_tx_queue.queue.push(executableTransaction{i, tx, executiveContext, executive, block_ptr});
                        candidate_tx_queues->insert(std::make_pair(readwriteset, _candidate_tx_queue));
                    } else {
                        candidate_tx_queues->at(readwriteset).queue.push(executableTransaction{i, tx, executiveContext, executive, block_ptr});
                    }
                }
            } else { // 跨片交易
                BLOCKVERIFIER_LOG(INFO) << LOG_DESC("commiting state for crossTx")
                                        << LOG_KV("txhash", tx->hash());
                transaction txInfo = crossTx[tx->hash()];
                unsigned long message_id = txInfo.message_id;
				unsigned long source_shard_id = txInfo.source_shard_id; // 协调者id
                string crossTxHash = txInfo.cross_tx_hash;
                unsigned long destin_shard_id = txInfo.destin_shard_id; // 本分片id
                BLOCKVERIFIER_LOG(INFO) << LOG_DESC("解析跨片交易成功")
                                        << LOG_KV("message_id", message_id)
                                        << LOG_KV("source_shard_id", source_shard_id)
                                        << LOG_KV("destin_shard_id", destin_shard_id)
                                        << LOG_KV("crossTxHash", crossTxHash);

                if(message_id == latest_candidate_tx_messageids->at(source_shard_id - 1) + 1) { //按序到达
					BLOCKVERIFIER_LOG(INFO) << LOG_DESC("insert_candidate_cs_tx...");
					// insert_candidate_cs_tx(_tx);
                    // 更新 locking_key
                    if(locking_key->count(readwriteset) == 0) {
                        BLOCKVERIFIER_LOG(INFO) << LOG_DESC("locking_key->count == 0");
                        // 向coordinator发送成功消息
                        replyToCoordinator(txInfo, group_protocolID, group_p2p_service);
                        locking_key->insert(std::make_pair(readwriteset, 1)); 
                    } else {
                        BLOCKVERIFIER_LOG(INFO) << LOG_DESC("locking_key->count != 0");
                        int holding_tx_num = locking_key->at(readwriteset);
                        if (holding_tx_num == 0) { // 抢占锁
                            // 向coordinator发送成功消息
                            replyToCoordinator(txInfo, group_protocolID, group_p2p_service);
                        }
                        locking_key->at(readwriteset) = holding_tx_num + 1;
                    }
                    // 更新messageId
                    latest_candidate_tx_messageids->at(source_shard_id - 1) = message_id;
                    // // 将交易放入队列
                    if(candidate_tx_queues->count(readwriteset) == 0) {
                        BLOCKVERIFIER_LOG(INFO) << LOG_DESC("candidate_tx_queues->count == 0");
                        std::queue<executableTransaction> queue = std::queue<executableTransaction>();
                        candidate_tx_queue _candidate_tx_queue { readwriteset, queue };
                        _candidate_tx_queue.queue.push(executableTransaction{i, tx, executiveContext, executive, block_ptr});
                        candidate_tx_queues->insert(std::make_pair(readwriteset, _candidate_tx_queue));
                    } else {
                        BLOCKVERIFIER_LOG(INFO) << LOG_DESC("candidate_tx_queues->count != 0");
                        // 当前片内交易的读写集（假设跨片交易的第一个读写集是当前片的读写集）, 定位读写集 readwrite_key 的交易缓存队列
                        auto candidate_tx_queue = candidate_tx_queues->at(readwriteset);
                        // _subtx 插入到 candidate_cs_tx中，更新上锁的读写集
                        candidate_tx_queue.queue.push(executableTransaction{i, tx, executiveContext, executive, block_ptr});
                    }

                    // 检查cached_cs_tx 中后继 _message_id + 1 的交易是否已经到达, 若已经到达，也插入到 candidate_cs_tx 中，更新上锁的读写集
                    BLOCKVERIFIER_LOG(INFO) << LOG_DESC("检查cached_cs_tx中后继message_id + 1的交易是否已经到达");
                    message_id = message_id + 1;
                    std::string attempt_key = std::to_string(source_shard_id) + std::to_string(message_id);
                    while(cached_cs_tx->count(attempt_key) != 0) {// 若后继 key 的跨片交易也在，也放入 candidate_cs_tx
                        BLOCKVERIFIER_LOG(INFO) << LOG_DESC("乱序到达的交易之前的交易可以到达");
                        auto _subtx = cached_cs_tx->at(attempt_key);
                        // 定位读写集 readwrite_key 的交易缓存队列，先判断是否存在
                        // 判断candidate_tx_queues中是否有readwrite_key的队列，因为之前可能没有
                        if(candidate_tx_queues->count(readwriteset) == 0)
                        {
                            std::queue<executableTransaction> queue = std::queue<executableTransaction>();
                            candidate_tx_queue _candidate_tx_queue { readwriteset, queue };
                            _candidate_tx_queue.queue.push(
                                executableTransaction{_subtx.begin()->first, _subtx.begin()->second.tx, executiveContext, executive, block_ptr});
                            candidate_tx_queues->insert(std::make_pair(readwriteset, _candidate_tx_queue));
                        }
                        else
                        {
                            auto candidate_tx_queue = candidate_tx_queues->at(readwriteset);
                            // _subtx 插入到candidate_cs_tx中，更新上锁的读写集
                            candidate_tx_queue.queue.push(
                                executableTransaction{_subtx.begin()->first, _subtx.begin()->second.tx, executiveContext, executive, block_ptr});
                        }

                        latest_candidate_tx_messageids->at(source_shard_id - 1) = message_id;

                        if(locking_key->count(readwriteset) == 0) { 
                            locking_key->insert(std::make_pair(readwriteset, 1)); 
                        } else {
                            int holding_tx_num = locking_key->at(readwriteset);
                            locking_key->at(readwriteset) = holding_tx_num + 1;
                        }

                        // 从 cached_cs_tx 中将交易删除
                        for(auto iter = cached_cs_tx->cbegin(); iter != cached_cs_tx->cend();)
                        {
                            if((*iter).first == attempt_key)
                            {
                                iter = cached_cs_tx->unsafe_erase(iter);
                            }
                            else
                            {
                                iter++;
                            }
                        }
                        message_id = message_id + 1;
                        std::string attempt_key = std::to_string(source_shard_id) + std::to_string(message_id);
                    }
                    BLOCKVERIFIER_LOG(INFO) << LOG_DESC("跨片交易-按需到达逻辑执行完成...");
				}
				else { // 乱序到达
					std::cout << "insert_cached_cs_tx" << std::endl;
                    std::string _key = std::to_string(source_shard_id) + std::to_string(message_id);
                    cached_cs_tx->insert(std::make_pair(_key, std::map<unsigned long, transaction>{{i, txInfo}}));
				}
                
            }
            
            
            // TransactionReceipt::Ptr resultReceipt = execute(tx, executiveContext, executive);
            // block.setTransactionReceipt(i, resultReceipt);
            // executiveContext->getState()->commit();
        }
        */
        
        blockExecuteContent _blockExecuteContent{executiveContext, executive};
        cached_executeContents.insert(std::make_pair(block.blockHeader().number(), _blockExecuteContent)); // 缓存区块执行变量
        ENGINE_LOG(INFO) << LOG_KV("BlockVerifer.block->blockHeader().number()", block.blockHeader().number()); 
    }
    catch (exception& e)
    {
        BLOCKVERIFIER_LOG(ERROR) << LOG_BADGE("executeBlock")
                                 << LOG_DESC("Error during serial block execution")
                                 << LOG_KV("blkNum", block.blockHeader().number())
                                 << LOG_KV("EINFO", boost::diagnostic_information(e));

        BOOST_THROW_EXCEPTION(
            BlockExecutionFailed() << errinfo_comment("Error during serial block execution"));
    }


    BLOCKVERIFIER_LOG(DEBUG) << LOG_BADGE("executeBlock") << LOG_DESC("Run serial tx takes")
                             << LOG_KV("time(ms)", utcTime() - pastTime)
                             << LOG_KV("txNum", block.transactions()->size())
                             << LOG_KV("num", block.blockHeader().number());

    /*
    if (flag) { // 若有交易被执行，则设置回执与状态根 ==> 后续改为只有等所有交易都执行并提交再设置？
        h256 stateRoot = executiveContext->getState()->rootHash();

        BLOCKVERIFIER_LOG(INFO) << LOG_KV("stateRoot", stateRoot)
                                << LOG_KV("getState", executiveContext->getState());

        // set stateRoot in receipts
        if (g_BCOSConfig.version() >= V2_2_0)
        {
            // BLOCKVERIFIER_LOG(INFO) << LOG_DESC("testing 1...");
            // when support_version is lower than v2.2.0, doesn't setStateRootToAllReceipt
            // enable_parallel=true can't be run with enable_parallel=false
            block.setStateRootToAllReceipt(stateRoot);
        }
        // BLOCKVERIFIER_LOG(INFO) << LOG_DESC("testing 2...");
        block.updateSequenceReceiptGas();
        // BLOCKVERIFIER_LOG(INFO) << LOG_DESC("testing 3...");
        block.calReceiptRoot();
        // BLOCKVERIFIER_LOG(INFO) << LOG_DESC("testing 4...");
        block.header().setStateRoot(stateRoot);
        // BLOCKVERIFIER_LOG(INFO) << LOG_DESC("testing 5...");
        if (dynamic_pointer_cast<storagestate::StorageState>(executiveContext->getState()))
        {
            BLOCKVERIFIER_LOG(INFO) << LOG_DESC("testing 6...");
            block.header().setDBhash(stateRoot);
        }
        else
        {
            BLOCKVERIFIER_LOG(INFO) << LOG_DESC("testing 7...");
            block.header().setDBhash(executiveContext->getMemoryTableFactory()->hash());
        }
        // BLOCKVERIFIER_LOG(INFO) << LOG_DESC("testing 8...");

        // if executeBlock is called by consensus module, no need to compare receiptRoot and stateRoot
        // since origin value is empty if executeBlock is called by sync module, need to compare
        // receiptRoot, stateRoot and dbHash
        // Consensus module execute block, receiptRoot is empty, skip this judgment
        // The sync module execute block, receiptRoot is not empty, need to compare BlockHeader
        if (tmpHeader.receiptsRoot() != h256())
        {
            if (tmpHeader != block.blockHeader())
            {
                BLOCKVERIFIER_LOG(ERROR)
                    << "Invalid Block with bad stateRoot or receiptRoot or dbHash"
                    << LOG_KV("blkNum", block.blockHeader().number())
                    << LOG_KV("originHash", tmpHeader.hash().abridged())
                    << LOG_KV("curHash", block.header().hash().abridged())
                    << LOG_KV("orgReceipt", tmpHeader.receiptsRoot().abridged())
                    << LOG_KV("curRecepit", block.header().receiptsRoot().abridged())
                    << LOG_KV("orgTxRoot", tmpHeader.transactionsRoot().abridged())
                    << LOG_KV("curTxRoot", block.header().transactionsRoot().abridged())
                    << LOG_KV("orgState", tmpHeader.stateRoot().abridged())
                    << LOG_KV("curState", block.header().stateRoot().abridged())
                    << LOG_KV("orgDBHash", tmpHeader.dbHash().abridged())
                    << LOG_KV("curDBHash", block.header().dbHash().abridged());
                BOOST_THROW_EXCEPTION(
                    InvalidBlockWithBadStateOrReceipt() << errinfo_comment(
                        "Invalid Block with bad stateRoot or ReceiptRoot, orgBlockHash " +
                        block.header().hash().abridged()));
            }
        }
    }
    
    BLOCKVERIFIER_LOG(INFO) << LOG_BADGE("executeBlock") << LOG_DESC("Execute block takes")
                             << LOG_KV("time(ms)", utcTime() - startTime)
                             << LOG_KV("txNum", block.transactions()->size())
                             << LOG_KV("num", block.blockHeader().number())
                             << LOG_KV("blockHash", block.headerHash())
                             << LOG_KV("stateRoot", block.header().stateRoot())
                             << LOG_KV("dbHash", block.header().dbHash())
                             << LOG_KV("transactionRoot", block.transactionRoot())
                             << LOG_KV("receiptRoot", block.receiptRoot());
    */
    return executiveContext;
}

ExecutiveContext::Ptr BlockVerifier::parallelExecuteBlock(
    Block& block, BlockInfo const& parentBlockInfo)

{
    BLOCKVERIFIER_LOG(INFO) << LOG_DESC("[executeBlock]Executing block")
                            << LOG_KV("txNum", block.transactions()->size())
                            << LOG_KV("num", block.blockHeader().number())
                            << LOG_KV("parentHash", parentBlockInfo.hash)
                            << LOG_KV("parentNum", parentBlockInfo.number)
                            << LOG_KV("parentStateRoot", parentBlockInfo.stateRoot);

    auto start_time = utcTime();
    auto record_time = utcTime();
    ExecutiveContext::Ptr executiveContext = std::make_shared<ExecutiveContext>();
    try
    {
        m_executiveContextFactory->initExecutiveContext(
            parentBlockInfo, parentBlockInfo.stateRoot, executiveContext);
    }
    catch (exception& e)
    {
        BLOCKVERIFIER_LOG(ERROR) << LOG_DESC("[executeBlock] Error during initExecutiveContext")
                                 << LOG_KV("EINFO", boost::diagnostic_information(e));

        BOOST_THROW_EXCEPTION(InvalidBlockWithBadStateOrReceipt()
                              << errinfo_comment("Error during initExecutiveContext"));
    }

    auto memoryTableFactory = executiveContext->getMemoryTableFactory();

    auto initExeCtx_time_cost = utcTime() - record_time;
    record_time = utcTime();

    BlockHeader tmpHeader = block.blockHeader();
    block.clearAllReceipts();
    block.resizeTransactionReceipt(block.transactions()->size());
    auto perpareBlock_time_cost = utcTime() - record_time;
    record_time = utcTime();

    shared_ptr<TxDAG> txDag = make_shared<TxDAG>();
    txDag->init(executiveContext, block.transactions(), block.blockHeader().number());


    txDag->setTxExecuteFunc([&](Transaction::Ptr _tr, ID _txId, Executive::Ptr _executive) {
        auto resultReceipt = execute(_tr, executiveContext, _executive);

        block.setTransactionReceipt(_txId, resultReceipt);
        executiveContext->getState()->commit();
        return true;
    });
    auto initDag_time_cost = utcTime() - record_time;
    record_time = utcTime();

    auto parallelTimeOut = utcSteadyTime() + 30000;  // 30 timeout

    try
    {
        tbb::atomic<bool> isWarnedTimeout(false);
        tbb::parallel_for(tbb::blocked_range<unsigned int>(0, m_threadNum),
            [&](const tbb::blocked_range<unsigned int>& _r) {
                (void)_r;
                EnvInfo envInfo(block.blockHeader(), m_pNumberHash, 0);
                envInfo.setPrecompiledEngine(executiveContext);
                auto executive = createAndInitExecutive(executiveContext->getState(), envInfo);

                while (!txDag->hasFinished())
                {
                    if (!isWarnedTimeout.load() && utcSteadyTime() >= parallelTimeOut)
                    {
                        isWarnedTimeout.store(true);
                        BLOCKVERIFIER_LOG(WARNING)
                            << LOG_BADGE("executeBlock") << LOG_DESC("Para execute block timeout")
                            << LOG_KV("txNum", block.transactions()->size())
                            << LOG_KV("blockNumber", block.blockHeader().number());
                    }

                    txDag->executeUnit(executive);
                }
            });
    }
    catch (exception& e)
    {
        BLOCKVERIFIER_LOG(ERROR) << LOG_BADGE("executeBlock")
                                 << LOG_DESC("Error during parallel block execution")
                                 << LOG_KV("EINFO", boost::diagnostic_information(e));

        BOOST_THROW_EXCEPTION(
            BlockExecutionFailed() << errinfo_comment("Error during parallel block execution"));
    }
    // if the program is going to exit, return nullptr directly
    if (g_BCOSConfig.shouldExit)
    {
        return nullptr;
    }
    auto exe_time_cost = utcTime() - record_time;
    record_time = utcTime();

    h256 stateRoot = executiveContext->getState()->rootHash();
    auto getRootHash_time_cost = utcTime() - record_time;
    record_time = utcTime();

    // set stateRoot in receipts
    block.setStateRootToAllReceipt(stateRoot);
    block.updateSequenceReceiptGas();
    auto setAllReceipt_time_cost = utcTime() - record_time;
    record_time = utcTime();

    block.calReceiptRoot();
    auto getReceiptRoot_time_cost = utcTime() - record_time;
    record_time = utcTime();

    block.header().setStateRoot(stateRoot);
    if (dynamic_pointer_cast<storagestate::StorageState>(executiveContext->getState()))
    {
        block.header().setDBhash(stateRoot);
    }
    else
    {
        block.header().setDBhash(executiveContext->getMemoryTableFactory()->hash());
    }
    auto setStateRoot_time_cost = utcTime() - record_time;
    record_time = utcTime();
    // Consensus module execute block, receiptRoot is empty, skip this judgment
    // The sync module execute block, receiptRoot is not empty, need to compare BlockHeader
    if (tmpHeader.receiptsRoot() != h256())
    {
        if (tmpHeader != block.blockHeader())
        {
            BLOCKVERIFIER_LOG(ERROR)
                << "Invalid Block with bad stateRoot or receiptRoot or dbHash"
                << LOG_KV("blkNum", block.blockHeader().number())
                << LOG_KV("originHash", tmpHeader.hash().abridged())
                << LOG_KV("curHash", block.header().hash().abridged())
                << LOG_KV("orgReceipt", tmpHeader.receiptsRoot().abridged())
                << LOG_KV("curRecepit", block.header().receiptsRoot().abridged())
                << LOG_KV("orgTxRoot", tmpHeader.transactionsRoot().abridged())
                << LOG_KV("curTxRoot", block.header().transactionsRoot().abridged())
                << LOG_KV("orgState", tmpHeader.stateRoot().abridged())
                << LOG_KV("curState", block.header().stateRoot().abridged())
                << LOG_KV("orgDBHash", tmpHeader.dbHash().abridged())
                << LOG_KV("curDBHash", block.header().dbHash().abridged());
#ifdef FISCO_DEBUG
            auto receipts = block.transactionReceipts();
            for (size_t i = 0; i < receipts->size(); ++i)
            {
                BLOCKVERIFIER_LOG(ERROR) << LOG_BADGE("FISCO_DEBUG") << LOG_KV("index", i)
                                         << LOG_KV("hash", block.transaction(i)->hash())
                                         << ",receipt=" << *receipts->at(i);
            }
#endif
            BOOST_THROW_EXCEPTION(InvalidBlockWithBadStateOrReceipt() << errinfo_comment(
                                      "Invalid Block with bad stateRoot or ReciptRoot"));
        }
    }
    BLOCKVERIFIER_LOG(DEBUG) << LOG_BADGE("executeBlock") << LOG_DESC("Para execute block takes")
                             << LOG_KV("time(ms)", utcTime() - start_time)
                             << LOG_KV("txNum", block.transactions()->size())
                             << LOG_KV("blockNumber", block.blockHeader().number())
                             << LOG_KV("blockHash", block.headerHash())
                             << LOG_KV("stateRoot", block.header().stateRoot())
                             << LOG_KV("dbHash", block.header().dbHash())
                             << LOG_KV("transactionRoot", block.transactionRoot())
                             << LOG_KV("receiptRoot", block.receiptRoot())
                             << LOG_KV("initExeCtxTimeCost", initExeCtx_time_cost)
                             << LOG_KV("perpareBlockTimeCost", perpareBlock_time_cost)
                             << LOG_KV("initDagTimeCost", initDag_time_cost)
                             << LOG_KV("exeTimeCost", exe_time_cost)
                             << LOG_KV("getRootHashTimeCost", getRootHash_time_cost)
                             << LOG_KV("setAllReceiptTimeCost", setAllReceipt_time_cost)
                             << LOG_KV("getReceiptRootTimeCost", getReceiptRoot_time_cost)
                             << LOG_KV("setStateRootTimeCost", setStateRoot_time_cost);
    return executiveContext;
}

void BlockVerifier::executeCrossTx(std::string keyReadwriteSet) {
    // 取出待执行的交易
    // BLOCKVERIFIER_LOG(INFO) << LOG_KV("size1", candidate_tx_queues->at(keyReadwriteSet).queue.size());
    auto executableTx = candidate_tx_queues->at(keyReadwriteSet).queue.front();
    // 删除执行后的交易
    candidate_tx_queues->at(keyReadwriteSet).queue.pop();
    // BLOCKVERIFIER_LOG(INFO) << LOG_KV("size2", candidate_tx_queues->at(keyReadwriteSet).queue.size());

    // 取变量
    auto index = executableTx.index;
    auto tx = executableTx.tx;
    auto executiveContext = executableTx.executiveContext;
    auto executive = executableTx.executive;
    auto block = executableTx.block;

    // 执行交易
    TransactionReceipt::Ptr resultReceipt = execute(tx, executiveContext, executive);
    block->setTransactionReceipt(index, resultReceipt);
    executiveContext->getState()->commit();
    block->unExecutedTxNum = block->unExecutedTxNum - 1;
    if (block->unExecutedTxNum == 0) {
        auto block_height = block->blockHeader().number();
        BLOCKVERIFIER_LOG(INFO) << LOG_BADGE("区块中的数据全部执行完")
                                << LOG_KV("block_height", block_height);
        // 清除缓存的执行环境变量
        cached_executeContents.erase(block_height);

    }

    /* 解锁相应变量
       1. crossTx
       2. locking_key
       3. crossTx2CommitMsg
    */
    transaction txInfo = crossTx[tx->hash()];
    crossTx.erase(tx->hash());
    int holding_tx_num = locking_key->at(keyReadwriteSet);
    locking_key->at(keyReadwriteSet) = holding_tx_num - 1;

    // 发送成功回执
    replyToCoordinatorCommitOK(txInfo);

    // 判断是否还有等待交易
    // BLOCKVERIFIER_LOG(INFO) << LOG_KV("size3", candidate_tx_queues->at(keyReadwriteSet).queue.size());
    if (candidate_tx_queues->at(keyReadwriteSet).queue.size() != 0) {
        BLOCKVERIFIER_LOG(INFO) << LOG_BADGE("还有等待的交易");
        executeCandidateTx(keyReadwriteSet);
    }
}

void BlockVerifier::executeCandidateTx(std::string keyReadwriteSet) {
    // 取出下一笔交易
    auto executableTx = candidate_tx_queues->at(keyReadwriteSet).queue.front();
    auto tx = executableTx.tx;
    auto index = executableTx.index;
    auto executiveContext = executableTx.executiveContext;
    auto executive = executableTx.executive;
    auto block = executableTx.block;
    
    //判断是否为跨片交易
    if (crossTx.find(tx->hash()) == crossTx.end()) { // 非跨片交易 => 直接执行
        TransactionReceipt::Ptr resultReceipt = execute(tx, executiveContext, executive);
        block->setTransactionReceipt(index, resultReceipt);
        executiveContext->getState()->commit();
        block->unExecutedTxNum = block->unExecutedTxNum - 1;
        if (block->unExecutedTxNum == 0) {
            auto block_height = block->blockHeader().number();
            
        }
        // 删除执行过的交易
        candidate_tx_queues->at(keyReadwriteSet).queue.pop();
        if (candidate_tx_queues->at(keyReadwriteSet).queue.size() != 0) {
            executeCandidateTx(keyReadwriteSet);
        }
    } else { // 跨片交易
        // 获取跨片交易相关信息
        transaction txInfo = crossTx[tx->hash()];
        // 向coordinator发送成功状态消息
        replyToCoordinator(txInfo, group_protocolID, group_p2p_service);
    }
}

TransactionReceipt::Ptr BlockVerifier::executeTransaction(
    const BlockHeader& blockHeader, dev::eth::Transaction::Ptr _t)
{
    ExecutiveContext::Ptr executiveContext = std::make_shared<ExecutiveContext>();
    BlockInfo blockInfo{blockHeader.hash(), blockHeader.number(), blockHeader.stateRoot()};
    try
    {
        m_executiveContextFactory->initExecutiveContext(
            blockInfo, blockHeader.stateRoot(), executiveContext);
    }
    catch (exception& e)
    {
        BLOCKVERIFIER_LOG(ERROR)
            << LOG_DESC("[executeTransaction] Error during execute initExecutiveContext")
            << LOG_KV("errorMsg", boost::diagnostic_information(e));
    }
    EnvInfo envInfo(blockHeader, m_pNumberHash, 0);
    envInfo.setPrecompiledEngine(executiveContext);
    auto executive = createAndInitExecutive(executiveContext->getState(), envInfo);
    // only Rpc::call will use executeTransaction, RPC do catch exception
    return execute(_t, executiveContext, executive);
}

dev::eth::TransactionReceipt::Ptr BlockVerifier::execute(dev::eth::Transaction::Ptr _t,
    dev::blockverifier::ExecutiveContext::Ptr executiveContext, Executive::Ptr executive)
{
    // Create and initialize the executive. This will throw fairly cheaply and quickly if the
    // transaction is bad in any way.
    executive->reset();

    // OK - transaction looks valid - execute.
    try
    {
        executive->initialize(_t);
        if (!executive->execute())
            executive->go();
        executive->finalize();
    }
    catch (StorageException const& e)
    {
        BLOCKVERIFIER_LOG(ERROR) << LOG_DESC("get StorageException") << LOG_KV("what", e.what());
        BOOST_THROW_EXCEPTION(e);
    }
    catch (Exception const& _e)
    {
        // only OutOfGasBase ExecutorNotFound exception will throw
        BLOCKVERIFIER_LOG(ERROR) << diagnostic_information(_e);
    }
    catch (std::exception const& _e)
    {
        BLOCKVERIFIER_LOG(ERROR) << _e.what();
    }

    executive->loggingException();
    return std::make_shared<TransactionReceipt>(executiveContext->getState()->rootHash(false),
        executive->gasUsed(), executive->logs(), executive->status(),
        executive->takeOutput().takeBytes(), executive->newAddress());
}

dev::executive::Executive::Ptr BlockVerifier::createAndInitExecutive(
    std::shared_ptr<StateFace> _s, dev::executive::EnvInfo const& _envInfo)
{
    return std::make_shared<Executive>(_s, _envInfo, m_evmFlags & EVMFlags::FreeStorageGas);
}

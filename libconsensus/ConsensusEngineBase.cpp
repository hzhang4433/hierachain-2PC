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

/**
 * @brief : implementation of PBFT consensus
 * @file: ConsensusEngineBase.cpp
 * @author: yujiechen
 * @date: 2018-09-28
 */
#include "ConsensusEngineBase.h"
using namespace dev::eth;
using namespace dev::db;
using namespace dev::blockverifier;
using namespace dev::blockchain;
using namespace dev::p2p;
using namespace dev::plugin;
using namespace std;

namespace dev
{
namespace consensus
{
void ConsensusEngineBase::start()
{
    if (m_startConsensusEngine)
    {
        ENGINE_LOG(WARNING) << "[ConsensusEngineBase has already been started]";
        return;
    }
    ENGINE_LOG(INFO) << "[Start ConsensusEngineBase]";
    /// start  a thread to execute doWork()&&workLoop()
    startWorking();
    m_startConsensusEngine = true;
}

void ConsensusEngineBase::stop()
{
    if (m_startConsensusEngine == false)
    {
        return;
    }
    ENGINE_LOG(INFO) << "[Stop ConsensusEngineBase]";
    m_startConsensusEngine = false;
    doneWorking();
    if (isWorking())
    {
        stopWorking();
        // will not restart worker, so terminate it
        terminate();
    }
    ENGINE_LOG(INFO) << "ConsensusEngineBase stopped";
}

/// update m_sealing and receiptRoot
dev::blockverifier::ExecutiveContext::Ptr ConsensusEngineBase::executeBlock(Block& block)
{
    auto parentBlock = m_blockChain->getBlockByNumber(m_blockChain->number());
    BlockInfo parentBlockInfo{parentBlock->header().hash(), parentBlock->header().number(),
        parentBlock->header().stateRoot()};
    /// reset execute context
    // std::cout << "准备执行区块" << std::endl;
    // return m_blockVerifier->executeBlock(block, parentBlockInfo, m_group_protocolID, m_group_service);
    return m_blockVerifier->executeBlock(block, parentBlockInfo);
}

void ConsensusEngineBase::replyToCoordinator(dev::plugin::transaction txInfo, 
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

int ConsensusEngineBase::executeBlockTransactions(std::shared_ptr<dev::eth::Block> block)
{
    ENGINE_LOG(INFO) << "放交易入队列...";
    // 将区块高度映射到该区块内未执行的交易数  EDIT BY ZH -- 22.11.2
    auto height = block->blockHeader().number();
    block2UnExecutedTxNum->insert(std::make_pair(height, block->transactions()->size()));
    ENGINE_LOG(INFO) << LOG_DESC("添加区块交易数")
                     << LOG_KV("height", height)
                     << LOG_KV("num", block2UnExecutedTxNum->at(height));

    for (size_t i = 0; i < block->transactions()->size(); i++)
    {
        auto& tx = (*block->transactions())[i];
        // 将区块内的每一笔交易映射到具体区块高度  EDIT BY ZH -- 22.11.2
        //重复插入——待进一步解决？—— 22.11.16
        if (txHash2BlockHeight->count(tx->hash().abridged()) != 0) { 
            continue;
        }
        txHash2BlockHeight->insert(std::make_pair(tx->hash().abridged(), height));
        PLUGIN_LOG(INFO) << LOG_DESC("添加新交易")
                         << LOG_KV("txHash", tx->hash())
                         << LOG_KV("height", txHash2BlockHeight->at(tx->hash().abridged()));

        // auto transactionReceipt = m_blockVerifier->executeTransaction(block->blockHeader(), tx);

        /* 2PC流程逻辑
        ENGINE_LOG(INFO) << LOG_KV("block->blockHeader().number()", block->blockHeader().number()); 
        auto cached_executeContent = dev::blockverifier::cached_executeContents.at(block->blockHeader().number());
        auto executiveContext = cached_executeContent.executiveContext;
        auto executive = cached_executeContent.executive;

        // 检查交易hash, 根据crossTx判断是否为跨片子交易
        if (crossTx.find(tx->hash()) == crossTx.end()) { // 非跨片交易
            ENGINE_LOG(INFO) << LOG_DESC("commiting state for nomarlTx")
                                    << LOG_KV("txhash", tx->hash());
            if (locking_key->count(readwriteset) == 0) { // 执行片内交易
                // m_stateMutex = 1; 需要锁吗? 不需要，因为是顺序执行
                TransactionReceipt::Ptr resultReceipt = m_blockVerifier->execute(tx, executiveContext, executive);
                block->setTransactionReceipt(i, resultReceipt);
                executiveContext->getState()->commit();
                
                // // setStateRoot
                // h256 stateRoot = executiveContext->getState()->rootHash();
                // block->header().setStateRoot(stateRoot);

                ENGINE_LOG(INFO) << LOG_DESC("the value of unExecutedTxNum is changing...")
                                    << LOG_KV("unExecutedTxNum", block->unExecutedTxNum);
                block->unExecutedTxNum = block->unExecutedTxNum - 1;
                ENGINE_LOG(INFO) << LOG_KV("transactionReceipt", resultReceipt->status());
            } else { // 放入等待队列
                // m_waitTxs.push_back(tx);
                if(candidate_tx_queues->count(readwriteset) == 0) {
                    std::queue<executableTransaction> queue = std::queue<executableTransaction>();
                    candidate_tx_queue _candidate_tx_queue { readwriteset, queue };
                    
                    _candidate_tx_queue.queue.push(executableTransaction{i, tx, executiveContext, executive, block});
                    candidate_tx_queues->insert(std::make_pair(readwriteset, _candidate_tx_queue));
                } else {
                    candidate_tx_queues->at(readwriteset).queue.push(executableTransaction{i, tx, executiveContext, executive, block});
                }
            }
        } else { // 跨片交易
            ENGINE_LOG(INFO) << LOG_DESC("commiting state for crossTx")
                                    << LOG_KV("txhash", tx->hash());
            transaction txInfo = crossTx[tx->hash()];
            unsigned long message_id = txInfo.message_id;
            unsigned long source_shard_id = txInfo.source_shard_id; // 协调者id
            string crossTxHash = txInfo.cross_tx_hash;
            unsigned long destin_shard_id = txInfo.destin_shard_id; // 本分片id
            ENGINE_LOG(INFO) << LOG_DESC("解析跨片交易成功")
                                    << LOG_KV("message_id", message_id)
                                    << LOG_KV("source_shard_id", source_shard_id)
                                    << LOG_KV("destin_shard_id", destin_shard_id)
                                    << LOG_KV("crossTxHash", crossTxHash);

            if(message_id == latest_candidate_tx_messageids->at(source_shard_id - 1) + 1) { //按序到达
                ENGINE_LOG(INFO) << LOG_DESC("insert_candidate_cs_tx...");
                // insert_candidate_cs_tx(_tx);
                // 更新 locking_key
                if(locking_key->count(readwriteset) == 0) {
                    ENGINE_LOG(INFO) << LOG_DESC("locking_key->count == 0");
                    // 向coordinator发送成功消息
                    replyToCoordinator(txInfo, group_protocolID, group_p2p_service);
                    locking_key->insert(std::make_pair(readwriteset, 1)); 
                } else {
                    ENGINE_LOG(INFO) << LOG_DESC("locking_key->count != 0");
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
                    ENGINE_LOG(INFO) << LOG_DESC("candidate_tx_queues->count == 0");
                    std::queue<executableTransaction> queue = std::queue<executableTransaction>();
                    candidate_tx_queue _candidate_tx_queue { readwriteset, queue };
                    _candidate_tx_queue.queue.push(executableTransaction{i, tx, executiveContext, executive, block});
                    candidate_tx_queues->insert(std::make_pair(readwriteset, _candidate_tx_queue));
                } else {
                    ENGINE_LOG(INFO) << LOG_DESC("candidate_tx_queues->count != 0");
                    // 当前片内交易的读写集（假设跨片交易的第一个读写集是当前片的读写集）, 定位读写集 readwrite_key 的交易缓存队列
                    auto candidate_tx_queue = candidate_tx_queues->at(readwriteset);
                    // _subtx 插入到 candidate_cs_tx中，更新上锁的读写集
                    candidate_tx_queue.queue.push(executableTransaction{i, tx, executiveContext, executive, block});
                }

                // 检查cached_cs_tx 中后继 _message_id + 1 的交易是否已经到达, 若已经到达，也插入到 candidate_cs_tx 中，更新上锁的读写集
                ENGINE_LOG(INFO) << LOG_DESC("检查cached_cs_tx中后继message_id + 1的交易是否已经到达");
                message_id = message_id + 1;
                std::string attempt_key = std::to_string(source_shard_id) + std::to_string(message_id);
                while(cached_cs_tx->count(attempt_key) != 0) {// 若后继 key 的跨片交易也在，也放入 candidate_cs_tx
                    ENGINE_LOG(INFO) << LOG_DESC("乱序到达的交易之前的交易可以到达");
                    auto _subtx = cached_cs_tx->at(attempt_key);
                    // 定位读写集 readwrite_key 的交易缓存队列，先判断是否存在
                    // 判断candidate_tx_queues中是否有readwrite_key的队列，因为之前可能没有
                    if(candidate_tx_queues->count(readwriteset) == 0)
                    {
                        std::queue<executableTransaction> queue = std::queue<executableTransaction>();
                        candidate_tx_queue _candidate_tx_queue { readwriteset, queue };
                        _candidate_tx_queue.queue.push(
                            executableTransaction{_subtx.begin()->first, _subtx.begin()->second.tx, executiveContext, executive, block});
                        candidate_tx_queues->insert(std::make_pair(readwriteset, _candidate_tx_queue));
                    }
                    else
                    {
                        auto candidate_tx_queue = candidate_tx_queues->at(readwriteset);
                        // _subtx 插入到candidate_cs_tx中，更新上锁的读写集
                        candidate_tx_queue.queue.push(
                            executableTransaction{_subtx.begin()->first, _subtx.begin()->second.tx, executiveContext, executive, block});
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
                ENGINE_LOG(INFO) << LOG_DESC("跨片交易-按序到达逻辑执行完成...");
            }
            else { // 乱序到达
                std::cout << "insert_cached_cs_tx" << std::endl;
                std::string _key = std::to_string(source_shard_id) + std::to_string(message_id);
                cached_cs_tx->insert(std::make_pair(_key, std::map<unsigned long, transaction>{{i, txInfo}}));
            }
            
        }
        */

        if (crossTx.find(tx->hash()) == crossTx.end()) { // 非跨片交易
        } else { // 跨片交易
            // 存储跨片交易对应的区块高度
            auto txInfo = crossTx[tx->hash()];
            auto crossTxHash = txInfo.cross_tx_hash;
            auto blockHeight = height;
            if (blockHeight2CrossTxHash->count(blockHeight) == 0) {
                std::vector<std::string> temp;
                temp.push_back(crossTxHash);
                blockHeight2CrossTxHash->insert(std::make_pair(blockHeight, temp));
            } else {
                blockHeight2CrossTxHash->at(blockHeight).push_back(crossTxHash);
            }
        }
        
        dev::consensus::toExecute_transactions.push(tx); // 将共识完出块的交易逐个放入队列
    }
}

void ConsensusEngineBase::checkBlockValid(Block const& block)
{
    h256 block_hash = block.blockHeader().hash();
    /// check transaction num
    if (block.getTransactionSize() > maxBlockTransactions())
    {
        ENGINE_LOG(DEBUG) << LOG_DESC("checkBlockValid: overthreshold transaction num")
                          << LOG_KV("blockTransactionLimit", maxBlockTransactions())
                          << LOG_KV("blockTransNum", block.getTransactionSize());
        BOOST_THROW_EXCEPTION(
            OverThresTransNum() << errinfo_comment("overthreshold transaction num"));
    }

    /// check the timestamp
    if (block.blockHeader().timestamp() > utcTime() && !m_allowFutureBlocks)
    {
        ENGINE_LOG(DEBUG) << LOG_DESC("checkBlockValid: future timestamp")
                          << LOG_KV("timestamp", block.blockHeader().timestamp())
                          << LOG_KV("utcTime", utcTime()) << LOG_KV("hash", block_hash.abridged());
        BOOST_THROW_EXCEPTION(DisabledFutureTime() << errinfo_comment("Future time Disabled"));
    }
    // check block timestamp: only enabled after v2.6.0
    // don't check timestamp of the genesis block
    if (block.blockHeader().number() >= 1)
    {
        checkBlockTimeStamp(block);
    }

    /// check the block number
    if (block.blockHeader().number() <= m_blockChain->number())
    {
        ENGINE_LOG(DEBUG) << LOG_DESC("checkBlockValid: old height")
                          << LOG_KV("highNumber", m_blockChain->number())
                          << LOG_KV("blockNumber", block.blockHeader().number())
                          << LOG_KV("hash", block_hash.abridged());
        BOOST_THROW_EXCEPTION(InvalidBlockHeight() << errinfo_comment("Invalid block height"));
    }

    /// check the existence of the parent block (Must exist)
    if (!m_blockChain->getBlockByHash(
            block.blockHeader().parentHash(), block.blockHeader().number() - 1))
    {
        ENGINE_LOG(ERROR) << LOG_DESC("checkBlockValid: Parent doesn't exist")
                          << LOG_KV("hash", block_hash.abridged())
                          << LOG_KV("number", block.blockHeader().number());
        BOOST_THROW_EXCEPTION(ParentNoneExist() << errinfo_comment("Parent Block Doesn't Exist"));
    }
    if (block.blockHeader().number() > 1)
    {
        if (m_blockChain->numberHash(block.blockHeader().number() - 1) !=
            block.blockHeader().parentHash())
        {
            ENGINE_LOG(DEBUG)
                << LOG_DESC("checkBlockValid: Invalid block for unconsistent parentHash")
                << LOG_KV("block.parentHash", block.blockHeader().parentHash().abridged())
                << LOG_KV("parentHash",
                       m_blockChain->numberHash(block.blockHeader().number() - 1).abridged());
            BOOST_THROW_EXCEPTION(
                WrongParentHash() << errinfo_comment("Invalid block for unconsistent parentHash"));
        }
    }
}

void ConsensusEngineBase::checkBlockTimeStamp(dev::eth::Block const& _block)
{
    if (!m_nodeTimeMaintenance)
    {
        return;
    }
    int64_t blockTimeStamp = _block.blockHeader().timestamp();
    auto alignedTime = getAlignedTime();
    // the blockTime must be within 30min of the current time
    if (std::abs(blockTimeStamp - alignedTime) > m_maxBlockTimeOffset)
    {
        // The block time is too different from the current time
        ENGINE_LOG(WARNING)
            << LOG_DESC("checkBlockTimeStamp: the block time is too different from the local time")
            << LOG_KV("blockTime", blockTimeStamp) << LOG_KV("alignedTime", alignedTime)
            << LOG_KV("utcTime", utcTime()) << LOG_KV("blkNum", _block.blockHeader().number())
            << LOG_KV("hash", _block.blockHeader().hash().abridged());
    }
}

void ConsensusEngineBase::updateConsensusNodeList()
{
    try
    {
        std::stringstream s2;
        s2 << "[updateConsensusNodeList] Sealers:";
        /// to make sure the index of all sealers are consistent
        auto sealerList = m_blockChain->sealerList();
        std::sort(sealerList.begin(), sealerList.end());
        {
            UpgradableGuard l(m_sealerListMutex);
            if (sealerList != m_sealerList)
            {
                UpgradeGuard ul(l);
                m_sealerList = sealerList;
                m_sealerListUpdated = true;
                m_lastSealerListUpdateNumber = m_blockChain->number();
            }
            else if (m_blockChain->number() != m_lastSealerListUpdateNumber)
            {
                m_sealerListUpdated = false;
            }
            for (dev::h512 node : m_sealerList)
                s2 << node.abridged() << ",";
        }
        s2 << "Observers:";
        dev::h512s observerList = m_blockChain->observerList();
        for (dev::h512 node : observerList)
            s2 << node.abridged() << ",";

        if (m_lastNodeList != s2.str())
        {
            ENGINE_LOG(DEBUG) << LOG_DESC(
                                     "updateConsensusNodeList: nodeList updated, updated nodeList:")
                              << s2.str();

            // get all nodes
            dev::h512s nodeList = sealerList + observerList;
            std::sort(nodeList.begin(), nodeList.end());
            if (m_blockSync->syncTreeRouterEnabled())
            {
                if (m_sealerListUpdated)
                {
                    m_blockSync->updateConsensusNodeInfo(sealerList, nodeList);
                }
                else
                {
                    // update the nodeList
                    m_blockSync->updateNodeListInfo(nodeList);
                }
            }
            m_service->setNodeListByGroupID(m_groupId, nodeList);

            m_lastNodeList = s2.str();
        }
    }
    catch (std::exception& e)
    {
        ENGINE_LOG(ERROR)
            << "[updateConsensusNodeList] update consensus node list failed [EINFO]:  "
            << boost::diagnostic_information(e);
    }
}

void ConsensusEngineBase::resetConfig()
{
    updateMaxBlockTransactions();
    auto node_idx = MAXIDX;
    m_accountType = NodeAccountType::ObserverAccount;
    size_t nodeNum = 0;
    updateConsensusNodeList();
    {
        ReadGuard l(m_sealerListMutex);
        for (size_t i = 0; i < m_sealerList.size(); i++)
        {
            if (m_sealerList[i] == m_keyPair.pub())
            {
                m_accountType = NodeAccountType::SealerAccount;
                node_idx = i;
                break;
            }
        }
        nodeNum = m_sealerList.size();
    }
    if (nodeNum < 1)
    {
        ENGINE_LOG(ERROR) << LOG_DESC(
            "Must set at least one pbft sealer, current number of sealers is zero");
        raise(SIGTERM);
        BOOST_THROW_EXCEPTION(
            EmptySealers() << errinfo_comment("Must set at least one pbft sealer!"));
    }
    // update m_nodeNum
    if (m_nodeNum != nodeNum)
    {
        m_nodeNum = nodeNum;
    }
    m_f = (m_nodeNum - 1) / 3;
    m_cfgErr = (node_idx == MAXIDX);
    m_idx = node_idx;
}

void ConsensusEngineBase::reportBlock(dev::eth::Block const& _block)
{
    if (!g_BCOSConfig.enableStat())
    {
        return;
    }
    // print the block gasUsed
    auto txsNum = _block.transactions()->size();
    if (txsNum == 0)
    {
        return;
    }
    auto blockGasUsed = (*_block.transactionReceipts())[txsNum - 1]->gasUsed();
    STAT_LOG(INFO) << LOG_TYPE("BlockGasUsed") << LOG_KV("g", m_groupId) << LOG_KV("txNum", txsNum)
                   << LOG_KV("gasUsed", blockGasUsed)
                   << LOG_KV("blockNumber", _block.blockHeader().number())
                   << LOG_KV("sealerIdx", _block.blockHeader().sealer())
                   << LOG_KV("blockHash", toHex(_block.blockHeader().hash()))
                   << LOG_KV("nodeID", toHex(m_keyPair.pub()));
    // print the gasUsed for each transaction
    u256 prevGasUsed = 0;
    uint64_t receiptIndex = 0;
    auto receipts = _block.transactionReceipts();
    for (auto const& tx : *_block.transactions())
    {
        auto receipt = (*receipts)[receiptIndex];
        auto gasUsed = receipt->gasUsed() - prevGasUsed;
        STAT_LOG(INFO) << LOG_TYPE("TxsGasUsed") << LOG_KV("g", m_groupId)
                       << LOG_KV("txHash", toHex(tx->hash())) << LOG_KV("gasUsed", gasUsed);
        prevGasUsed = receipt->gasUsed();
        receiptIndex++;
    }
}

}  // namespace consensus
}  // namespace dev

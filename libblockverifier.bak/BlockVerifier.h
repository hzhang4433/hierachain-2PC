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
/** @file BlockVerifier.h
 *  @author mingzhenliu
 *  @date 20180921
 */
#pragma once

#include "BlockVerifierInterface.h"
#include "ExecutiveContext.h"
#include "ExecutiveContextFactory.h"
#include "libprecompiled/Precompiled.h"
#include <libdevcore/FixedHash.h>
#include <libdevcore/ThreadPool.h>
#include <libdevcrypto/Common.h>
#include <libethcore/Block.h>
#include <libethcore/Protocol.h>
#include <libethcore/Transaction.h>
#include <libethcore/TransactionReceipt.h>
#include <libexecutive/Executive.h>
#include <libmptstate/State.h>
#include <boost/function.hpp>
#include <algorithm>
#include <memory>
#include <thread>
#include <libplugin/transform.h>

namespace dev
{
namespace eth
{

// struct BlockTx // 阻塞交易
// {
//     std::string txrlp;
// 	std::shared_ptr<dev::eth::Transaction> transaction;
//     std::shared_ptr<dev::blockverifier::ExecutiveContext> executiveContext;
//     std::shared_ptr<dev::executive::Executive> executive;
// };

// class BlockTxQueuesManager
// {
//     public:

//         bool isBlock(std::string _readWriteSet) // 检查读集是否已经被阻塞
//         {
//             if(std::find(m_readWriteSets.begin(), m_readWriteSets.end(), _readWriteSet) != m_readWriteSets.end())
//             {
//                 return true;
//             }
//             else
//             {
//                 return false;
//             }
//         }

//         bool addQueue(std::string _readWriteSet, BlockTx _tx)
//         {
//             std::queue<BlockTx> m_BlocktxQueue;
//             m_BlocktxQueue.push(_tx);
//             m_BlocktxQueues.push_back(m_BlocktxQueue); // 增加队列
//             m_readWriteSets.push_back(_readWriteSet); // 增加读集
//             return true;
//         }

//         bool addTx(std::string _readWriteSet, BlockTx _tx)
//         {
//             std::lock_guard<std::mutex> lock(mutex_BlocktxQueues);

//             auto iter = m_readWriteSets.begin();
//             size_t index = 0;

//             while (iter!= m_readWriteSets.end())
// 			{
// 				if(*iter == _readWriteSet)
// 				{
// 					m_BlocktxQueues.at(index).push(_tx);
// 					return true;
// 				}
// 				else
// 				{
// 					iter++;
// 					index++;
// 				}
// 			}

//             bool ret = addQueue(_readWriteSet, _tx);
//                 return ret;
//             }

//         bool pop(std::string _readWriteSet)
//         {
//             std::lock_guard<std::mutex> lock(mutex_BlocktxQueues);

// 			auto iter = m_readWriteSets.begin();
// 			size_t index = 0;

// 			// 获取队列索引
// 			while (iter!= m_readWriteSets.end())
// 			{
// 				if(*iter == _readWriteSet)
// 				{
// 					break;
// 				}
// 				else
// 				{
// 					iter++;
// 					index++;
// 				}
// 			}
            
//             if(iter == m_readWriteSets.end())
// 			{
//                 std::cout << "不存在该读集的队列" << std::endl;
// 				return false;
// 			}

//             m_BlocktxQueues.at(index).pop();
// 			if(m_BlocktxQueues.at(index).size()==0)
// 			{
// 				m_BlocktxQueues.erase(m_BlocktxQueues.begin() + index);
// 				m_readWriteSets.erase(m_readWriteSets.begin() + index);
// 			}
// 			return true;
//         }

//         BlockTx front(std::string _readWriteSet)
//         {
//             std::lock_guard<std::mutex> lock(mutex_BlocktxQueues);

// 			auto iter = m_readWriteSets.begin();
// 			size_t index = 0;

//             // 获取队列索引
// 			while (iter!= m_readWriteSets.end())
// 			{
// 				if(*iter == _readWriteSet)
// 				{
// 					break;
// 				}
// 				else
// 				{
// 					iter++;
// 					index++;
// 				}
// 			}
//             auto tx = m_BlocktxQueues.at(index).front();
//             return tx;
//         }

// 		std::vector<std::queue<BlockTx>> m_BlocktxQueues; // 阻塞队列池
// 		std::vector<std::string> m_readWriteSets; // 每个队列对应的读集
//         std::mutex mutex_BlocktxQueues; // 队列池锁
// };

class TransactionReceipt;

}  // namespace eth

namespace blockverifier
{
class BlockVerifier : public BlockVerifierInterface,
                      public std::enable_shared_from_this<BlockVerifier>
{
public:
    typedef std::shared_ptr<BlockVerifier> Ptr;
    typedef boost::function<dev::h256(int64_t x)> NumberHashCallBackFunction;
    BlockVerifier(bool _enableParallel = false) : m_enableParallel(_enableParallel)
    {
        if (_enableParallel)
        {
            m_threadNum = std::max(std::thread::hardware_concurrency(), (unsigned int)1);
        }

        // listenCommittedDisTx();

        // 同时启动线程监听 receCommTxRlps，当 receCommTxRlps 中存在交易时，blockTxQueuesManager尝试将队列的头部交易推出，并执行

    }

    virtual ~BlockVerifier() {}

    ExecutiveContext::Ptr executeBlock(dev::eth::Block& block, BlockInfo const& parentBlockInfo);
    ExecutiveContext::Ptr serialExecuteBlock(dev::eth::Block& block, BlockInfo const& parentBlockInfo);
    ExecutiveContext::Ptr parallelExecuteBlock(dev::eth::Block& block, BlockInfo const& parentBlockInfo);

    dev::eth::TransactionReceipt::Ptr executeTransaction(const dev::eth::BlockHeader& blockHeader, dev::eth::Transaction::Ptr _t);
    dev::eth::TransactionReceipt::Ptr execute(dev::eth::Transaction::Ptr _t,dev::blockverifier::ExecutiveContext::Ptr executiveContext, dev::executive::Executive::Ptr executive);

    void setExecutiveContextFactory(ExecutiveContextFactory::Ptr executiveContextFactory)
    {
        m_executiveContextFactory = executiveContextFactory;
    }
    ExecutiveContextFactory::Ptr getExecutiveContextFactory() { return m_executiveContextFactory; }
    void setNumberHash(const NumberHashCallBackFunction& _pNumberHash)
    {
        m_pNumberHash = _pNumberHash;
    }

    dev::executive::Executive::Ptr createAndInitExecutive( std::shared_ptr<executive::StateFace> _s, dev::executive::EnvInfo const& _envInfo);
    void setEvmFlags(VMFlagType const& _evmFlags) { m_evmFlags = _evmFlags; }

    void ProcessCommittedDisTx();

    bool listenCommittedDisTx();

    // dev::eth::BlockTxQueuesManager blockTxQueuesManager;
    // std::vector<std::vector<std::string>> committed_txs;

    // 因前序交易未完成而被阻塞的区块, std::map<SHARDID_MESSAGEID, std::make_shared<Block>> blocked_blocks
    std::map<std::string, std::shared_ptr<dev::eth::Block>> blocked_blocks;

    // 记录下被阻塞的交易, std::map<SHARDID_MESSAGEID, std::make_shared<Transaction>> blocked_txs
    std::map<std::string, std::shared_ptr<dev::eth::Transaction>> blocked_txs;

private:
    ExecutiveContextFactory::Ptr m_executiveContextFactory;
    NumberHashCallBackFunction m_pNumberHash;
    bool m_enableParallel;
    unsigned int m_threadNum = -1;

    std::mutex m_executingMutex;
    std::atomic<int64_t> m_executingNumber = {0};

    VMFlagType m_evmFlags = 0;

    pthread_t processCommittedDisTx_thread;
};

}  // namespace blockverifier

}  // namespace dev

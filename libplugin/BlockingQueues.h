#pragma once

#include "Common.h"
#include <libethcore/Transaction.h>
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_queue.h>
#include <libdevcore/FixedHash.h>
#include <boost/algorithm/string.hpp>
#include <deque>

using namespace dev;
using namespace std;

namespace dev {
    namespace plugin {

        class BlockingTxQueue:public std::enable_shared_from_this<BlockingTxQueue>
        {

            public:
                BlockingTxQueue()
                {
                    txs = make_shared<deque<transaction>>();
                    lockingkeys = make_shared<map<string, int>>();
                }

                int size();

                bool isBlocked(string& keys); // 判断当前交易所访问的key是否有被其他交易阻塞

                void insertTx(transaction tx);

                void popTx();

                void popCrossTx(unsigned long coorId, unsigned long messageId);

                bool popAbortedTx(string abortKey);

                shared_ptr<transaction> frontTx();

                shared_ptr<transaction> CrossTx(unsigned long coorId, unsigned long messageId);

                ~BlockingTxQueue() { }

            public:
                shared_ptr<deque<transaction>> txs; // 所有缓存的交易
                shared_ptr<map<string, int>> lockingkeys; // 当前被阻塞的key集合
                mutex queueLock; // 保证对lockingkeys和txs操作的并发安全
        };

        class BlockingCrossTxQueue:public std::enable_shared_from_this<BlockingCrossTxQueue>
        {

            public:
                BlockingCrossTxQueue()
                {
                    txs = make_shared<queue<blockedCrossTransaction>>();
                }
                
                int size();

                bool isBlocked(); // 判断当前队列是否有交易

                void insertTx(blockedCrossTransaction tx);

                void popTx();

                blockedCrossTransaction frontTx();

                ~BlockingCrossTxQueue() { }

            public:
                shared_ptr<queue<blockedCrossTransaction>> txs; // 所有阻塞的跨片交易
                mutex queueLock; // 保证对lockingkeys和txs操作的并发安全
        };
    }
}
#pragma once

#include "Common.h"
#include <libethcore/Transaction.h>
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_queue.h>
#include <libdevcore/FixedHash.h>
#include <boost/algorithm/string.hpp>

using namespace dev;
using namespace std;

namespace dev {
    namespace plugin {

        class BlockingTxQueue:public std::enable_shared_from_this<BlockingTxQueue>
        {

            public:
                BlockingTxQueue()
                {
                    txs = make_shared<queue<transaction>>();
                    lockingkeys = make_shared<map<string, int>>();
                }

                int size();

                bool isBlocked(string& keys); // 判断当前交易所访问的key是否有被其他交易阻塞

                void insertTx(transaction tx);

                void popTx();

                transaction frontTx();

                ~BlockingTxQueue() { }

            public:
                shared_ptr<queue<transaction>> txs; // 所有缓存的交易
                shared_ptr<map<string, int>> lockingkeys; // 当前被阻塞的key集合
                mutex queueLock; // 保证对lockingkeys和txs操作的并发安全
        };
    }
}
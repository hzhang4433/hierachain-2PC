#include <libplugin/BlockingQueues.h>

using namespace std;
using namespace dev::plugin;


bool BlockingTxQueue::isBlocked(string& keys)
{
    lock_guard<std::mutex> lock(queueLock);

    vector<string> keyItems;
    boost::split(keyItems, keys, boost::is_any_of("_"), boost::token_compress_on);
    size_t key_size = keyItems.size();

    bool isBlocked = false;
    for(size_t i = 0; i < key_size; i++)
    {
        string key = keyItems.at(i);
        if(lockingkeys->count(key) != 0)
        {
            if(lockingkeys->at(key) != 0) // 非空且不等于0
            {
                isBlocked = true;
                break;
            }
        }
    }

    return isBlocked;
}


void BlockingTxQueue::insertTx(transaction tx) // 后面建议做batch优化
{
    // 将交易访问的所有的本地读写集插入到lockingkeys中
    lock_guard<std::mutex> lock(queueLock);

    string localrwkeys = tx.readwrite_key; // 片内交易也可能访问多个状态，用_分开
    vector<string> localrwkeyItems;
    boost::split(localrwkeyItems, localrwkeys, boost::is_any_of("_"), boost::token_compress_on);
    size_t key_size = localrwkeyItems.size();

    set<string> stateSet;
    for(size_t i = 0; i < key_size; i++)
    {
        stateSet.insert(localrwkeyItems.at(i));
    }

    PLUGIN_LOG(INFO) << LOG_DESC("插入交易");
    for (set<string>::iterator it = stateSet.begin(); it != stateSet.end(); it++) {
        string key = *it;
        PLUGIN_LOG(INFO) << LOG_KV("key", key);
        
        if(lockingkeys->count(key) == 0)
        {
            lockingkeys->insert(make_pair(key, 1));
        }
        else
        {
            int lockNum = lockingkeys->at(key);
            lockingkeys->at(key) = lockNum + 1;
        }
    }

    txs->push(tx); // 将交易压入缓存队列
}

// 交易执行完，将交易和相应的锁清除
void BlockingTxQueue::popTx()
{
    lock_guard<std::mutex> lock(queueLock);

    auto tx = txs->front(); // 即将被pop的交易

    string localrwkeys = tx.readwrite_key;
    vector<string> localrwkeyItems;
    boost::split(localrwkeyItems, localrwkeys, boost::is_any_of("_"), boost::token_compress_on);
    size_t key_size = localrwkeyItems.size();

    set<string> stateSet;
    for(size_t i = 0; i < key_size; i++)
    {
        stateSet.insert(localrwkeyItems.at(i));
    }

    for(set<string>::iterator it = stateSet.begin(); it != stateSet.end(); it++)
    {
        string key = *it;
        int lockNum = lockingkeys->at(key);
        lockingkeys->at(key) = lockNum - 1;
    }

    // 锁删除完毕，交易出队列
    txs->pop();
}

int BlockingTxQueue::size()
{
    lock_guard<std::mutex> lock(queueLock);
    return txs->size();
}

shared_ptr<transaction> BlockingTxQueue::frontTx()
{
    lock_guard<std::mutex> lock(queueLock);
    if (txs->size() > 0) {
        return make_shared<transaction>(txs->front());
    }
    return 0;
}

int BlockingCrossTxQueue::size()
{
    lock_guard<std::mutex> lock(queueLock);
    return txs->size();
}

bool BlockingCrossTxQueue::isBlocked()
{
    lock_guard<std::mutex> lock(queueLock);

    auto size = txs->size();
    bool isBlocked = false;

    if (size != 0) {
        isBlocked = true;
    }

    return isBlocked;
}

void BlockingCrossTxQueue::insertTx(blockedCrossTransaction tx) // 后面建议做batch优化
{
    // 将交易访问的所有的本地读写集插入到lockingkeys中
    lock_guard<std::mutex> lock(queueLock);
    txs->push(tx); // 将交易压入缓存队列
}

// 交易执行完，将交易和相应的锁清除
void BlockingCrossTxQueue::popTx()
{
    lock_guard<std::mutex> lock(queueLock);
    txs->pop(); // 锁删除完毕，交易出队列
}

blockedCrossTransaction BlockingCrossTxQueue::frontTx()
{
    lock_guard<std::mutex> lock(queueLock);
    return txs->front();
}
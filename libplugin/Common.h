#pragma once

#include <libdevcore/CommonData.h>
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_queue.h>
#include <tbb/concurrent_vector.h>
#include <libethcore/Transaction.h>
#include <libethcore/Protocol.h>
#include <libp2p/Service.h>
#include <libblockverifier/BlockVerifierInterface.h>
#include <libblockverifier/ExecutiveContext.h>
#include <libexecutive/Executive.h>

namespace dev {
namespace plugin {

// #define PLUGIN_LOG(LEVEL) LOG(LEVEL) << LOG_BADGE("PLGIN")
#define PLUGIN_LOG(LEVEL) LOG(LEVEL) << LOG_BADGE("PLUGIN") << LOG_BADGE("PLUGIN")

    struct transaction
    {
        int type; // 交易类型, 0 为片内交易, 1 为跨片子交易
        unsigned long source_shard_id;
        unsigned long destin_shard_id;
        unsigned long message_id;
        std::string cross_tx_hash;
        dev::eth::Transaction::Ptr tx;
        std::string readwrite_key; // 多个读写集时候中间用'|'号分隔开，为了便于实验，先假设所有的片内交易只访问片内的一个读写集key，跨片交易的读写集可能有多个
        // 子交易句柄，这里先假设有了，拿到了就可以执行
    };

    struct executableTransaction
    {
        unsigned long index;
        dev::eth::Transaction::Ptr tx;
        dev::blockverifier::ExecutiveContext::Ptr executiveContext;
        dev::executive::Executive::Ptr executive; 
        dev::eth::Block::Ptr block;
    };

    struct candidate_tx_queue
    {
        std::string blocked_readwriteset; // 队列所阻塞的读写集
        std::queue<executableTransaction> queue; // 缓存的交易, 格式：{txIdx, tx}
    };

    /// peer nodeid info just hardcode
    /// delete later
    const dev::h512 exNodeId[1] = {
            dev::h512("a1218f83771287ec2638faf3abea073a64e37bede0ce5dc670e261dc98de08fd2865d2966c3d24c37d636f4b7822f792ec50797c434db5c69016d0d9b904c142")
    };

    // h512 getforwardNodeId(int _index)
    // {
    //     return forwardNodeId[_index]; //
    // }
        
/* 
    全局共享变量（做消息队列使用），介绍：
    1. std::map<std::string, std::string> txRWSet, [交易rlp，交易读写集]，节点已知的交易读写集
    3. tbb::concurrent_queue<std::string> preCommTxRlps, [交易rlp...] ; // 本地已经共识通过的交易
    4. std::map<std::string, std::vector<std::string>> processingTxD; // 协调者记录所有正在处理的跨片交易
    5. std::map<std::string, std::vector<std::string>> recePreCommTxRlps; // 协调者记录已经收到的precommit消息
    6. tbb::concurrent_queue<std::vector<std::string>> receCommTxRlps; // 参与者记录已经收到的committed消息
    7. std::map<std::string, std::vector<std::string>> conAddress2txrlps; // 跨片[合约地址, <跨片子交易rlp1，rlp2...>，(rlp最前面两位为目标分片ID)...
    8. std::vector<std::string> disTxDepositAddrs; // 当前分片作为协调者，可以发起的跨片交易地址
    9. std::map<std::string, int> recVotes; // 记录协调者每个跨片交易收到的投票数目
    10.extern std::vector<std::string> committedDisTxRlp; // 记录所有committed交易的RLP编码
*/
    extern std::map<h256, transaction> crossTx; // 分片待处理的跨片子交易详细信息
    // 缓冲队列跨片交易集合(用以应对网络传输下，收到的交易乱序)，(shardid_messageid-->subtx)，由执行模块代码触发
    // extern std::shared_ptr<tbb::concurrent_unordered_map<std::string, transaction>> cached_cs_tx;
    extern std::shared_ptr<tbb::concurrent_unordered_map<std::string, std::map<unsigned long, transaction>>> cached_cs_tx;
    // 执行队列池 readwriteset --> candidate_tx_queue
	extern std::shared_ptr<tbb::concurrent_unordered_map<std::string, candidate_tx_queue>> candidate_tx_queues;
    // 已经提交candidate_cs_tx的来自不同分片的最大 messageid[3,4]
    extern std::shared_ptr<tbb::concurrent_vector<unsigned long>> latest_candidate_tx_messageids;
    // 交易池交易因等待收齐状态而正在锁定的状态key
    extern std::shared_ptr<tbb::concurrent_unordered_map<std::string, int>> locking_key;

    // ADD BY ZH
    extern std::shared_ptr<tbb::concurrent_unordered_map<std::string, std::vector<int>>> crossTx2ShardID;
    extern std::shared_ptr<tbb::concurrent_unordered_map<std::string, std::vector<int>>> crossTx2ReceivedMsg;
    extern std::shared_ptr<tbb::concurrent_unordered_map<std::string, int>> crossTx2CommitMsg;
    extern std::shared_ptr<tbb::concurrent_unordered_map<std::string, std::vector<int>>> crossTx2ReceivedCommitMsg;
    extern std::shared_ptr<tbb::concurrent_unordered_map<std::string, std::string>> crossTx2StateAddress;
    extern dev::PROTOCOL_ID group_protocolID;
    extern std::shared_ptr<dev::p2p::Service> group_p2p_service;
    extern dev::blockverifier::BlockVerifierInterface::Ptr groupVerifier;
    extern std::string nodeIdStr;
		

    extern std::map<std::string, std::string> txRWSet;
    extern std::map<int, std::vector<std::string>> processingTxD;
    extern std::map<std::string, int> subTxRlp2ID;
    extern std::map<std::string, std::vector<std::string>> resendTxs;
    extern tbb::concurrent_queue<std::vector<std::string>> receCommTxRlps;
    extern std::map<std::string, std::vector<std::string>> conAddress2txrlps;
    extern std::vector<std::string> disTxDepositAddrs;
    extern std::map<std::string, int> subTxNum;
    extern std::vector<std::string> committedDisTxRlp;
    extern std::vector<std::string> preCommittedDisTxRlp;
    extern std::map<std::string, std::string> txRlp2ConAddress;
    extern std::vector<std::string> coordinatorRlp;

    extern int global_internal_groupId;

    // extern tbb::concurrent_queue<std::string> preCommTxRlps;
    // extern std::map<std::string, std::vector<std::string>> recePreCommTxRlps;
    // extern std::vector<std::string> readyDisTxIds;
    // extern std::map<std::string, int> recVotes;
    }  // namespace plugin
}  // namespace dev
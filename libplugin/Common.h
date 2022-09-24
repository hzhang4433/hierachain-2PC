#pragma once

#include <libdevcore/CommonData.h>

namespace dev {
namespace plugin {

// #define PLUGIN_LOG(LEVEL) LOG(LEVEL) << LOG_BADGE("PLGIN")
#define PLUGIN_LOG(LEVEL) LOG(LEVEL) << LOG_BADGE("PLUGIN") << LOG_BADGE("PLUGIN")

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
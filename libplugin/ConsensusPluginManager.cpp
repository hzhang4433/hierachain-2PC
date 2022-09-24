#include <libplugin/ConsensusPluginManager.h>

using namespace std;
using namespace dev;
using namespace dev::plugin;

void ConsensusPluginManager::updateNotLatest(std::string const _state)
{
    std::lock_guard<std::mutex> lock(x_latest_Mutex);
    not_latest.insert(_state);
}

void ConsensusPluginManager::removeNotLatest(std::string const _state)
{
    std::lock_guard<std::mutex> lock(x_latest_Mutex);
    not_latest.erase(_state);
}

bool ConsensusPluginManager::isLatest(std::string const _state)
{
    std::lock_guard<std::mutex> lock(x_latest_Mutex);
    if(not_latest.count(_state)>0) {return false;}
    return true;
}

void ConsensusPluginManager::processReceivedTx(protos::Transaction _tx)
{
    txs->push(_tx);
}

// void ConsensusPluginManager::processReceivedDisTx(protos::RLPWithReadSet _txrlp)
// {
//     distxs->push(_txrlp);
// }

void ConsensusPluginManager::processReceivedDisTx(protos::SubCrossShardTx _txrlp)
{
    // receive and storage
    protos::SubCrossShardTx msg_txWithReadset;
    msg_txWithReadset = _txrlp;
    
    std::cout << "接收到协调者发来跨片交易请求..." << std::endl;
    auto rlp = msg_txWithReadset.subtxrlp();
    auto readset = msg_txWithReadset.messageid();
    auto sourceShardId = msg_txWithReadset.sourceshardid();
    auto destinshardid = msg_txWithReadset.destinshardid();
    auto signeddata = msg_txWithReadset.signeddata();

    PLUGIN_LOG(INFO) << LOG_DESC("交易解析完毕")
                        << LOG_KV("signeddata", signeddata)
                        << LOG_KV("readset", readset)
                        << LOG_KV("sourceShardId", sourceShardId)
                        << LOG_KV("destinshardid", destinshardid);

    // m_rpc_service->sendRawTransaction(destinshardid, signeddata); // 通过调用本地的RPC接口发起新的共识
    m_rpc_service->sendSubCsRawTransaction(destinshardid, signeddata, 1); // 通过调用本地的RPC接口发起新的共识
    // distxs->push(_txrlp);
}

void ConsensusPluginManager::processReceivedPreCommitedTx(protos::SubPreCommitedDisTx _txrlp)
{

    // auto rlp = msg_committedRLPWithReadSet.subtxrlp();
    // auto readset = msg_committedRLPWithReadSet.readset();
    // auto contractAddress = msg_committedRLPWithReadSet.contractaddress();

    // std::cout<< "参与者收到了commit命令, 开始执行被阻塞交易..." << std::endl;
    // // std::cout << "rlp = " << rlp << std::endl;
    // // std::cout << "readset = " << readset << std::endl;
    // // std::cout << "contractAddress = " << contractAddress << std::endl;

    // std::vector<std::string> committed_txrlp;
    // committed_txrlp.push_back(rlp);
    // committed_txrlp.push_back(readset);
    // committed_txrlp.push_back(contractAddress);

    // // //参与者记录所有收到的 committed 交易， 交易rlp、交易地址、交易读集
    // dev::plugin::receCommTxRlps.push(committed_txrlp); // 用队列去接收管道消息

    precommit_txs->push(_txrlp);
}

void ConsensusPluginManager::processReceivedCommitedTx(protos::CommittedRLPWithReadSet _txrlp)
{
    // std::cout << "P2P模块收到comitted交易, 压入缓存堆栈" << std::endl;
    // 对收到的commit交易进行缓存

    commit_txs->push(_txrlp);
}

void ConsensusPluginManager::processReceivedWriteSet(protos::TxWithReadSet _rs)
{
    readSetQueue->push(_rs);
}

int ConsensusPluginManager::numOfNotFinishedDAGs()
{
    return notFinishedDAG;
}

int ConsensusPluginManager::addNotFinishedDAGs(int _num)
{
    notFinishedDAG += _num;
}

u256 ConsensusPluginManager::getLatestState(std::string _addr){

    if(testMap.count(_addr)>0){
        return testMap[_addr];
    }else{
        testMap.insert(std::make_pair(_addr, u256(0)));
        return u256(0);
    }
}

h512 ConsensusPluginManager::getNodeId(int _index)
{
    return exNodeId[_index]; // 
}
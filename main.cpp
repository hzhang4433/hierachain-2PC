//#include <grpcpp/grpcpp.h>
#include "libdevcore/Log.h"
#include <json/json.h>
#include <leveldb/db.h>
#include <libblockchain/BlockChainImp.h>
#include <libblockverifier/BlockVerifier.h>
#include <libblockverifier/Common.h>
#include <libblockverifier/ExecutiveContextFactory.h>
#include <libdevcore/BasicLevelDB.h>
#include <libdevcore/CommonData.h>
#include <libdevcore/CommonJS.h>
#include <libdevcore/TopicInfo.h>
#include <libdevcrypto/Common.h>
#include <libethcore/ABI.h>
#include <libethcore/Block.h>
#include <libethcore/PrecompiledContract.h>
#include <libethcore/Protocol.h>
#include <libethcore/TransactionReceipt.h>
#include <libinitializer/Initializer.h>
#include <libinitializer/P2PInitializer.h>
#include <libmptstate/MPTStateFactory.h>
#include <librpc/Rpc.h>
#include <libstorage/LevelDBStorage.h>
#include <libstorage/MemoryTableFactory.h>
#include <libstorage/Storage.h>
#include <libstoragestate/StorageStateFactory.h>

#include <libplugin/ConsensusPluginManager.h>
#include <libplugin/SyncThreadMaster.h>
#include <libplugin/ex_SyncMsgEngine.h>
#include <libplugin/Common.h>

#include <stdlib.h>
#include <sys/time.h>
#include <tbb/concurrent_queue.h>
#include <cassert>
#include <ctime>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <unistd.h>
#include <thread>
#include <libplugin/benchmark.h>

using namespace std;
using namespace dev;
using namespace dev::crypto;
using namespace dev::eth;
using namespace dev::rpc;
using namespace dev::ledger;
using namespace dev::initializer;
using namespace dev::txpool;
using namespace dev::blockverifier;
using namespace dev::blockchain;
using namespace dev::storage;
using namespace dev::mptstate;
using namespace dev::executive;
using namespace dev::plugin;

namespace dev {
    namespace plugin {
        // 分片待处理的跨片子交易详细信息
        std::shared_ptr<tbb::concurrent_unordered_map<string, shared_ptr<transaction>>> crossTx = 
                                std::make_shared<tbb::concurrent_unordered_map<string, shared_ptr<transaction>>>(); 
        std::mutex m_crossTxMutex;

        // 缓冲队列跨片交易集合(用以应对网络传输下，收到的交易乱序)，(shardid_messageid-->subtx)，由执行模块代码触发
        std::shared_ptr<tbb::concurrent_unordered_map<std::string, transaction>> cached_cs_tx = 
                                std::make_shared<tbb::concurrent_unordered_map<std::string, transaction>>();
        // 执行队列池 readwriteset --> candidate_tx_queue
		std::shared_ptr<tbb::concurrent_unordered_map<std::string, candidate_tx_queue>> candidate_tx_queues = 
                                std::make_shared<tbb::concurrent_unordered_map<std::string, candidate_tx_queue>>();
        // 已经收到的来自不同协调者分片的最大messageid
        std::shared_ptr<tbb::concurrent_vector<unsigned long>> latest_candidate_tx_messageids;
        
        // 已经收到的来自不同协调者分片的当前正在处理的交易messageid ——— 22.11.16
        std::shared_ptr<tbb::concurrent_vector<unsigned long>> current_candidate_tx_messageids;

        // 已经收到的来自不同协调者分片的已完成处理的交易messageid ——— 22.11.20
        std::shared_ptr<tbb::concurrent_vector<unsigned long>> complete_candidate_tx_messageids;

        // 交易池交易因等待收齐状态而正在锁定的状态key
        std::shared_ptr<tbb::concurrent_unordered_map<std::string, int>> locking_key = 
                                std::make_shared<tbb::concurrent_unordered_map<std::string, int>>();
        std::mutex m_lockKeyMutex;
        
        // ADD BY ZH
        // 协调者分片存储跨片交易对应的分片ID
        std::shared_ptr<tbb::concurrent_unordered_map<std::string, std::vector<int>>> crossTx2ShardID = 
                                std::make_shared<tbb::concurrent_unordered_map<std::string, std::vector<int>>>();
        std::mutex m_crossTx2ShardIDMutex;
        
        // std::shared_ptr<tbb::concurrent_unordered_map<std::string, std::vector<int>>> crossTx2ShardMessageID = 
        //                         std::make_shared<tbb::concurrent_unordered_map<std::string, std::vector<int>>>();
        

        // 协调者分片存储对应跨片交易收集到的状态消息包
        std::shared_ptr<tbb::concurrent_unordered_map<std::string, std::vector<std::string>>> crossTx2ReceivedMsg = 
                                std::make_shared<tbb::concurrent_unordered_map<std::string, std::vector<std::string>>>();
        std::mutex m_crossTx2ReceivedMsgMutex;
            
        // 子分片存储对应跨片交易收集到的commit消息包数量
        std::shared_ptr<tbb::concurrent_unordered_map<std::string, int>> crossTx2CommitMsg = 
                                std::make_shared<tbb::concurrent_unordered_map<std::string, int>>();
        std::mutex m_crossTx2CommitMsgMutex;

        // 协调者分片存储对应子分片执行跨片交易成功收集的消息包数量
        std::shared_ptr<tbb::concurrent_unordered_map<std::string, std::vector<int>>> crossTx2ReceivedCommitMsg = 
                                std::make_shared<tbb::concurrent_unordered_map<std::string, std::vector<int>>>();
        std::mutex m_crossTx2ReceivedCommitMsgMutex;
        
        std::shared_ptr<tbb::concurrent_unordered_set<std::string>> abortSet =
                                std::make_shared<tbb::concurrent_unordered_set<std::string>>();;
        std::mutex m_abortMsgMutex;

        // 存放已经完成的跨片交易hash
        std::shared_ptr<tbb::concurrent_unordered_set<std::string>> doneCrossTx = 
                                std::make_shared<tbb::concurrent_unordered_set<std::string>>();
        std::mutex m_doneCrossTxMutex;
        
        // 包含所有节点的protocol ID
		dev::PROTOCOL_ID group_protocolID;
        // 所有节点的p2p通信服务
        std::shared_ptr<dev::p2p::Service> group_p2p_service;
        // 保存分片的blockerverifier变量
        dev::blockverifier::BlockVerifierInterface::Ptr groupVerifier;
        // 节点nodeID
        std::string nodeIdStr;

        // EDIT BY ZH 22.11.2
        // 映射交易hash到其所在区块高度
        std::shared_ptr<tbb::concurrent_unordered_map<std::string, int>> txHash2BlockHeight = 
                                std::make_shared<tbb::concurrent_unordered_map<std::string, int>>();
        std::mutex m_txHash2HeightMutex;

        // 映射区块高度至未执行交易数
        std::shared_ptr<tbb::concurrent_unordered_map<int, int>> block2UnExecutedTxNum = 
                                std::make_shared<tbb::concurrent_unordered_map<int, int>>();
        std::mutex m_block2UnExecMutex;
        
        // ADD ON 22.11.7
        std::shared_ptr<tbb::concurrent_unordered_map<int, std::vector<std::string>>> blockHeight2CrossTxHash = 
                                std::make_shared<tbb::concurrent_unordered_map<int, std::vector<std::string>>>();
        std::mutex m_height2TxHashMutex;

        // ADD ON 22.11.8
        // std::map<h256, transaction> innerTx;
        // 片内交易映射至交易状态集
        std::shared_ptr<tbb::concurrent_unordered_map<string, shared_ptr<transaction>>> innerTx = 
                                std::make_shared<tbb::concurrent_unordered_map<string, shared_ptr<transaction>>>();
        std::mutex m_innerTxMutex;

        // 跨片交易映射至对应状态集
        // std::shared_ptr<tbb::concurrent_unordered_map<std::string, std::string>> crossTx2StateAddress = 
        //                         std::make_shared<tbb::concurrent_unordered_map<std::string, std::string>>();
        // std::mutex m_crossTx2AddressMutex;
        
        // ADD ON 22.11.16
        // 落后的跨片交易
        std::shared_ptr<tbb::concurrent_unordered_set<int>> lateCrossTxMessageId = 
                                std::make_shared<tbb::concurrent_unordered_set<int>>();
        std::shared_ptr<tbb::concurrent_unordered_set<int>> lateCommitReplyMessageId = 
                                std::make_shared<tbb::concurrent_unordered_set<int>>();
        // 协调者共识落后，记录处理过的reply消息
        std::shared_ptr<tbb::concurrent_unordered_set<int>> lateReplyMessageId = 
                                std::make_shared<tbb::concurrent_unordered_set<int>>();
        std::mutex m_lateCrossTxMutex;

        std::vector<bool> flags;


        std::map<std::string, std::string> txRWSet;
        std::map<int, std::vector<std::string>> processingTxD;
        std::map<std::string, int> subTxRlp2ID;
        tbb::concurrent_queue<std::vector<std::string>> receCommTxRlps;
        std::map<std::string, std::vector<std::string>> conAddress2txrlps;
        std::vector<std::string> disTxDepositAddrs;
        std::map<std::string, int> subTxNum;
        std::map<std::string, std::vector<std::string>> resendTxs;
        std::vector<std::string> committedDisTxRlp;
        std::vector<std::string> preCommittedDisTxRlp;
        std::map<std::string, std::string>txRlp2ConAddress;
        std::vector<std::string> coordinatorRlp;
        std::shared_ptr<ExecuteVMTestFixture> executiveContext;
    }
}

namespace dev{
    namespace consensus{
        int internal_groupId; // 当前分片所在的groupID
        int SHARDNUM; // 分片总数
        int NODENUM; // 所有节点数目
        std::vector<dev::h512>forwardNodeId; // 记录各分片主节点id
        std::vector<dev::h512>shardNodeId; // 记录所有节点id
        std::map<unsigned long, unsigned long> messageIDs; // 记录各分片已发送的最大messageID
        tbb::concurrent_queue<dev::eth::Transaction::Ptr> toExecute_transactions; // 缓存共识完的交易，按顺序存放在队列中，等待执行
    }
}

namespace dev{
    namespace blockverifier{
        std::vector<int>latest_commit_cs_tx;
        std::map<std::string, std::shared_ptr<dev::eth::Transaction>> blocked_txs;
        std::map<std::string, std::shared_ptr<dev::eth::Block>> blocked_blocks;
        std::map<int, blockExecuteContent> cached_executeContents; // 缓存区块的执行变量
    }
}

namespace dev{
    namespace rpc{
        std::vector<dev::h256> subCrossTxsHash; // 子分片记录所有待处理的跨片子交易hash
    }
}

void putGroupPubKeyIntoshardNodeId(boost::property_tree::ptree const& _pt)
{
    size_t index = 0;
    for (auto it : _pt.get_child("group"))
    {
        if (it.first.find("groups.") == 0)
        {
            std::vector<std::string> s;
            try
            {
                boost::split(s, it.second.data(), boost::is_any_of(":"), boost::token_compress_on);

                // 对分片中的所有节点id进行遍历，加入到列表中
                size_t s_size = s.size();
                for(size_t i = 0; i < s_size - 1; i++)
                {
                    h512 node;
                    node = h512(s[i]);
                    dev::consensus::shardNodeId.push_back(node);

                    if(index % 4 == 0)
                    {
                        dev::consensus::forwardNodeId.push_back(node);
                    }
                    index++;
                }
            }
            catch (std::exception& e)
            {
                exit(1);
            }
        }
    }
}

void putGroupPubKeyIntoService(std::shared_ptr<Service> service, boost::property_tree::ptree const& _pt)
{
    std::map<GROUP_ID, h512s> groupID2NodeList;
    h512s nodelist;
    int groupid;
    for (auto it : _pt.get_child("group"))
    {
        if (it.first.find("groups.") == 0)
        {
            std::vector<std::string> s;
            try
            {
                boost::split(s, it.second.data(), boost::is_any_of(":"), boost::token_compress_on);

                // 对分片中的所有节点id进行遍历，加入到列表中
                int s_size = s.size();
                for(int i = 0; i < s_size - 1; i++)
                {
                    h512 node;
                    node = h512(s[i]);
                    nodelist.push_back(node);
                }
                groupid = (int)((s[s_size - 1])[0] - '0');  // 放到同一个总的groupid中
            }
            catch (std::exception& e)
            {
                exit(1);
            }
        }
    }
    groupID2NodeList.insert(std::make_pair(groupid, nodelist)); // 都是同一个groupid，所以插入一次就好了
    std::cout << groupID2NodeList[groupid] << std::endl;
    service->setGroupID2NodeList(groupID2NodeList);
}

class GroupP2PService
{
public:
    GroupP2PService(std::string const& _path)
    {
        boost::property_tree::ptree pt;
        boost::property_tree::read_ini(_path, pt);
        m_secureInitializer = std::make_shared<SecureInitializer>();
        m_secureInitializer->initConfig(pt);
        m_p2pInitializer = std::make_shared<P2PInitializer>();
        m_p2pInitializer->setSSLContext(m_secureInitializer->SSLContext(SecureInitializer::Usage::ForP2P));
        m_p2pInitializer->setKeyPair(m_secureInitializer->keyPair());
        m_p2pInitializer->initConfig(pt);
    }
    P2PInitializer::Ptr p2pInitializer() { return m_p2pInitializer; }
    ~GroupP2PService()
    {
        if (m_p2pInitializer)
        {
            m_p2pInitializer->stop();
        }
    }

private:
    P2PInitializer::Ptr m_p2pInitializer;
    SecureInitializer::Ptr m_secureInitializer;
};


class CrossShardTest
{
    private:
        std::vector<std::string> m_transactionsRLP;
        std::string m_contractStr;

        std::shared_ptr<dev::rpc::Rpc> m_rpcService;
        shared_ptr<SyncThreadMaster> m_syncs;
        int32_t m_groupId;

    public:
        CrossShardTest(std::shared_ptr<dev::rpc::Rpc> _rpcService, int32_t _groupId, shared_ptr<SyncThreadMaster> _syncs)
        {
            m_rpcService = _rpcService;
            m_groupId = _groupId;
            m_syncs = _syncs;
        }

        void sendTx()
        {
            for(int i = 0; i < m_transactionsRLP.size(); i++)
            {
                std::string txrlpstr = m_transactionsRLP.at(i);

                dev::plugin::coordinatorRlp.push_back(txrlpstr); // 协调者记录自己发送的跨片交易请求
                std::string response = m_rpcService->sendRawTransaction(1, txrlpstr); // 通过一个节点将交易发送给协调者中的所有节点，共识、出块
                std::cout << "用户向协调者分片发送了一笔交易请求..." << std::endl;
            }
        }

        void start()
        {
            if(m_groupId == 4)
            {
                m_syncs->startThread();
            }
        }

        void deployContract()
        {
            ifstream infile("./deploy.json", ios::binary);
            assert(infile.is_open());
            Json::Reader reader;
            Json::Value root;

            if(reader.parse(infile, root))
            {
                m_contractStr = root[0].asString();
            }
            infile.close();
            //std::string response = m_rpcService->sendRawTransaction(1, m_contractStr);
            std::cout<<" 合约部署结束 " <<std::endl;
        }

        void InjectTxRLP()
        {
            ifstream infile("./signedtxs.json", ios::binary);
            assert(infile.is_open());
            Json::Reader reader;
            Json::Value root;
            int64_t number = 0;
            std::string _transactionRLP = "";

            if(reader.parse(infile, root))
            {
                number = root.size();
                for(int i = 0; i < number; i++)
                {
                    _transactionRLP = root[i].asString();
                    m_transactionsRLP.push_back(_transactionRLP);
                }
            }
            infile.close();
            ifstream infile2("./resendTx.json", ios::binary);
            assert(infile2.is_open());
            number = 0;
            std::string address = "";

            if(reader.parse(infile2, root))
            {
                number = root.size();
                for(int i = 0; i < number; i++)
                {
                    address = root[i][0].asString();
                    std::vector<std::string> resendTx;
                    for(int j = 1; j < root[i].size(); j++)
                    {
                        resendTx.push_back(root[i][j].asString());
                    }
                    dev::plugin::resendTxs.insert(pair<std::string, std::vector<std::string>>(address, resendTx));
                }
            }
            infile2.close();
            // std::cout<<"测试交易导入成功..." <<std::endl;
        }

        void InjextConAddress2txrlps()
        {
            ifstream infile("./conAddress2txrlps.json", ios::binary);
            assert(infile.is_open());
            Json::Reader reader;
            Json::Value root;
            int64_t number = 0;

            if(reader.parse(infile, root))
            {
                number = root.size();
                if(number != 0)
                {
                    for(int i = 0; i < number; i++)
                    {
                        std::string conAddress = root[i][0].asString();
                        std::vector<std::string> subtxrlps;

                        int txNum = root[i].size() - 1;
                        dev::plugin::subTxNum.insert(pair<std::string, int>(conAddress, txNum));
                        for(int j = 1; j < root[i].size(); j++)
                        {
                            std::string item = root[i][j].asString();
                            subtxrlps.push_back(item);
                        }
                        dev::plugin::conAddress2txrlps.insert(pair<std::string, std::vector<std::string>>(conAddress, subtxrlps));
                    }
                }
            }
            infile.close();
            // std::cout<<"跨片子交易信息，以及子交易数目导入成功！" <<std::endl;
        }

        void InjectTxRead_Write_Sets()
        {
            ifstream infile("./read_write_sets.json");
            // assert(infile.is_open());

            Json::Reader reader;
            Json::Value root;
            int64_t number = 0;
            std::string _txRlp = "";
            std::string _readWriteSet = "";
            std::map <std::string, std::string> txRead_Write_Set_Map;

            if(reader.parse(infile, root))
            {
                number = root.size();
                for(int i = 0; i < number; i++)
                {
                    _txRlp = root[i][0].asString();
                    _readWriteSet = root[i][1].asString();
                    dev::plugin::txRWSet.insert(pair<std::string, std::string>(_txRlp, _readWriteSet));               
                }
            }
            infile.close();
            // std::cout<<"交易读写集导入成功..." <<std::endl;
        }

        void InjectTxDAddr()
        {
            ifstream infile("./txDAddress.json", ios::binary);
            assert(infile.is_open());
            Json::Reader reader;
            Json::Value root;
            int64_t number = 0;

            if(reader.parse(infile, root))
            {
                number = root.size();
                for(int i = 0; i < number; i++)
                {
                    std::string conAddress = root[i].asString();
                    dev::plugin::disTxDepositAddrs.push_back(conAddress);                    
                }
            }
            infile.close();
            // std::cout<< "跨片交易存证地址导入成功！" <<std::endl;
        }
};

/*
status:
    0 ==> 一次生成全部交易
    1 ==> 生成部分交易，这次为第一次
    2 ==> 生成部分交易，这次为中间一次
    3 ==> 生成部分交易，这次为最后一次
*/ 
void createInnerTransaction(int groupId, shared_ptr<dev::ledger::LedgerManager> ledgerManager, int txNum, 
                            string fileName, int status, transactionInjectionTest _injectionTest) 
{
    // 批量生产片内交易
    cout << "createInnerTransaction, txNum = " << txNum << ", status = " << status << endl;
    std::string res;
    for (int i =0 ; i < txNum; i++) {
        auto tx = _injectionTest.createInnerTransactions(groupId, ledgerManager);
        if (i % 100 == 0) { // 100笔写一次文件
            if ((status == 0 || status == 1) && i == 0) {
                res = "[\"" + tx + "\"";
            } else {
                ofstream out;
                out.open(fileName, ios::in|ios::out|ios::app);
                if (out.is_open()) {
                    out << res;
                    out.close();
                    res = "";
                }
                res = ",\"" + tx + "\"";
            }
        } else {
            res = res + ",\"" + tx + "\"";
        }
        std::this_thread::sleep_for(std::chrono::milliseconds((1)));
    }
    
    ofstream out;
    out.open(fileName, ios::in|ios::out|ios::app);
    if (out.is_open()) {
        if (status == 0 || status == 3) {
            res += "]";
        }
        out << res;
        out.close();
    }
    
}

void createCrossTransaction(int cor, int sub1, int sub2, shared_ptr<dev::ledger::LedgerManager> ledgerManager, int txNum, string fileName, int status, transactionInjectionTest _injectionTest) 
{    
    // 批量生产跨片交易
    cout << "createCrossTransaction, txNum = " << txNum << ", status = " << status << endl;

    std::string res;
    for (int i =0 ; i < txNum; i++) {
        auto tx = _injectionTest.createCrossTransactions(cor, sub1, sub2, ledgerManager);
        if (i % 100 == 0) { // 100笔写一次文件
            if ((status == 0 || status == 1) && i == 0) {
                res = "[\"" + tx + "\"";
            } else {
                ofstream out;
                out.open(fileName, ios::in|ios::out|ios::app);
                if (out.is_open()) {
                    out << res;
                    out.close();
                    res = "";
                }
                res = ",\"" + tx + "\"";
            }
        } else {
            res = res + ",\"" + tx + "\"";
        }
        std::this_thread::sleep_for(std::chrono::milliseconds((1)));
    }

    ofstream out;
    out.open(fileName, ios::in|ios::out|ios::app);
    if (out.is_open()) {
        if (status == 0 || status == 3) {
            res += "]";
        }
        out << res;
        out.close();
    }
    
}

// 两个及两个以上子分片
void createCrossTransaction(int cor, vector<int>& shardIds, shared_ptr<dev::ledger::LedgerManager> ledgerManager, 
                            int txNum, string fileName, int status, transactionInjectionTest _injectionTest) 
{    
    // 批量生产跨片交易
    cout << "createCrossTransaction, txNum = " << txNum << ", status = " << status << endl;

    std::string res;
    for (int i =0 ; i < txNum; i++) {
        auto tx = _injectionTest.createCrossTransactions(cor, shardIds, ledgerManager);
        if (i % 100 == 0) { // 100笔写一次文件
            if ((status == 0 || status == 1) && i == 0) {
                res = "[\"" + tx + "\"";
            } else {
                ofstream out;
                out.open(fileName, ios::in|ios::out|ios::app);
                if (out.is_open()) {
                    out << res;
                    out.close();
                    res = "";
                }
                res = ",\"" + tx + "\"";
            }
        } else {
            res = res + ",\"" + tx + "\"";
        }
        std::this_thread::sleep_for(std::chrono::milliseconds((1)));
    }

    ofstream out;
    out.open(fileName, ios::in|ios::out|ios::app);
    if (out.is_open()) {
        if (status == 0 || status == 3) {
            res += "]";
        }
        out << res;
        out.close();
    }
    
}

// 无status参数
void createCrossTransaction(int cor, int sub1, int sub2, shared_ptr<dev::ledger::LedgerManager> ledgerManager, int txNum, string fileName, transactionInjectionTest _injectionTest) 
{    
    // std::cout << "in new createCrossTransaction txNum = " << txNum << std::endl;
    // 批量生产跨片交易
    std::string res;
    for (int i =0; i < txNum; i++) {
        auto tx = _injectionTest.createCrossTransactions(cor, sub1, sub2, ledgerManager);
        if (i % 100 == 0) { // 100笔写一次文件
            if (i == 0) { // 根据文件之前写入情况设置第一个字符
                if (flags[cor - 1]) {
                    res = "[\"" + tx + "\"";
                } else {
                    res = ",\"" + tx + "\"";
                }
            } else if (i == 100) { // 第一次写入，或需覆盖
                // std::cout << "flags" << cor - 1 << ":" << flags[cor - 1] << std::endl;
                if (flags[cor - 1]) { // 文件之前未被写过
                    ofstream out;
                    out.open(fileName, ios::in|ios::out|ios::app);
                    if (out.is_open()) {
                        out << res;
                        out.close();
                    }
                    flags[cor - 1] = false;
                } else { // 文件之前被写过
                    // std::cout << "开始覆盖写" << std::endl;
                    ofstream out;
                    out.open(fileName, ios::binary | ios::out | ios::in);
                    if (out.is_open()) {
                        out.seekp(-1, ios::end);
                        out << res;
                        out.close();
                    }
                }
                res = ",\"" + tx + "\"";
            } else { // 不是第一次写，直接续写无需覆盖
                ofstream out;
                out.open(fileName, ios::in|ios::out|ios::app);
                if (out.is_open()) {
                    out << res;
                    out.close();
                }
                res = ",\"" + tx + "\"";
            }
        } else {
            res = res + ",\"" + tx + "\"";
        }
        std::this_thread::sleep_for(std::chrono::milliseconds((1)));
    }

    // 收尾工作
    ofstream out;
    res += "]";
    if (txNum == 100) {
        out.open(fileName, ios::binary | ios::out | ios::in);
        if (out.is_open()) {
            out.seekp(-1, ios::end);
            out << res;
            out.close();
        }
    } else {
        out.open(fileName, ios::in|ios::out|ios::app);
        if (out.is_open()) {
            out << res;
            out.close();
        }
    }
    
}

void createDataSet(int cor, int sub1, int sub2, std::shared_ptr<dev::ledger::LedgerManager> ledgerManager,
                   int txNum, int percent, std::shared_ptr<dev::rpc::Rpc> rpcService) {
    // 计算跨片交易及各分片的片内交易数
    string fileName;
    int crossTxNum = (txNum * percent) / 100;
    int innerTxNum = (txNum - crossTxNum) / 3;
    int remain = (txNum - crossTxNum) % 3;
    transactionInjectionTest _injectionTest(rpcService, 1);

    if (percent == 20) {
        fileName = "workload1.json";
    } else if (percent == 50) {
        fileName = "workload2.json";
    } else if (percent == 80) {
        fileName = "workload3.json";
    } else if (percent == 100) {
        fileName = "workload4.json";
    }

    // 生成子分片1的片内交易
    if(innerTxNum != 0 && dev::consensus::internal_groupId == sub1 && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(sub1 - 1)))
    {
        createInnerTransaction(sub1, ledgerManager, innerTxNum, fileName, 0, _injectionTest);
    }

    // 生成子分片2的片内交易
    if(innerTxNum != 0 && dev::consensus::internal_groupId == sub2 && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(sub2 - 1)))
    {
        createInnerTransaction(sub2, ledgerManager, innerTxNum, fileName, 0, _injectionTest);
    }

    // 生成协调者分片的片内交易和跨片交易
    cout << "fileName:" << fileName << endl;
    if(dev::consensus::internal_groupId == cor && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(cor - 1)))
    {
        int nowInnerNum = innerTxNum + remain;
        int nowCrossNum = crossTxNum;
        if (nowInnerNum == 0) { //纯跨片交易 直接生成返回
            createCrossTransaction(cor, sub1, sub2, ledgerManager, nowCrossNum, fileName, 0, _injectionTest);
            return;
        }
        bool firstTime = true;
        while(nowInnerNum != 0 || nowCrossNum != 0) {
            if (nowInnerNum == 0) {
                createCrossTransaction(cor, sub1, sub2, ledgerManager, nowCrossNum, fileName, 3, _injectionTest);
                break;
            } else if (nowCrossNum == 0) {
                createInnerTransaction(cor, ledgerManager, nowInnerNum, fileName, 3, _injectionTest);
                break;
            }
            // srand((unsigned)time(NULL));
            int flag = rand() % 2;
            if (flag) { // 为1 生成跨片交易
                cout << "createDataSet 111" << endl;
                if (nowCrossNum < 100) {
                    // cout << "createDataSet 222" << endl;
                    if (firstTime == true) {
                        // cout << "进来了 firstTime = true" << endl; 
                        createCrossTransaction(cor, sub1, sub2, ledgerManager, nowCrossNum, fileName, 1, _injectionTest);
                        firstTime = false;
                    } else {
                        createCrossTransaction(cor, sub1, sub2, ledgerManager, nowCrossNum, fileName, 2, _injectionTest);
                    }
                    nowCrossNum = 0;
                } else {
                    // cout << "createDataSet 333" << endl;
                    cout << "firstTime:" << firstTime << endl;
                    if (firstTime == true) { // 第一次
                        // cout << "进来了 firstTime = true" << endl; 
                        createCrossTransaction(cor, sub1, sub2, ledgerManager, 100, fileName, 1, _injectionTest);
                        firstTime = false;
                    } else {
                        createCrossTransaction(cor, sub1, sub2, ledgerManager, 100, fileName, 2, _injectionTest);
                    }
                    nowCrossNum -= 100;
                }
            } else { // 为0 生成片内交易
                cout << "createDataSet 444" << endl;
                if (nowInnerNum < 100) {
                    // cout << "createDataSet 555" << endl;
                    if (firstTime == true) {
                        // cout << "进来了 firstTime = true" << endl; 
                        createInnerTransaction(cor, ledgerManager, nowInnerNum, fileName, 1, _injectionTest);
                        firstTime = false;
                    } else {
                        createInnerTransaction(cor, ledgerManager, nowInnerNum, fileName, 2, _injectionTest);
                    }
                    nowInnerNum = 0;
                } else {
                    // cout << "createDataSet 666" << endl;
                    // cout << "firstTime:" << firstTime << endl;
                    if (firstTime == true) {
                        // cout << "进来了 firstTime = true" << endl; 
                        createInnerTransaction(cor, ledgerManager, 100, fileName, 1, _injectionTest);
                        firstTime = false;
                    } else {
                        createInnerTransaction(cor, ledgerManager, 100, fileName, 2, _injectionTest);
                    }
                    nowInnerNum -= 100;
                }
            }
        }
    }
}

void createHBDataSet(int cor, int sub1, int sub2, std::shared_ptr<dev::ledger::LedgerManager> ledgerManager,
                     std::shared_ptr<dev::rpc::Rpc> rpcService) {
    transactionInjectionTest _injectionTest(rpcService, 1);
    std::string res = "";
    std::string fileName = "workload.json";
    std::string tx = "";
    // 分片1
    if(dev::consensus::internal_groupId == sub1 && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(sub1 - 1)))
    {
        for (int i = 0 ; i < 1000; i++) {
            if (i < 500) {
                tx = _injectionTest.createSpecialInnerTransactions(sub1, ledgerManager, "stateA", "stateC");
            } else {
                tx = _injectionTest.createSpecialInnerTransactions(sub1, ledgerManager, "stateA", "stateD");
            }
            if (i == 0) {
                res = "[\"" + tx + "\"";
            } else {
                res += ",\"" + tx + "\"";
            }
            std::this_thread::sleep_for(std::chrono::milliseconds((1)));
        }
        
        ofstream out;
        out.open(fileName, ios::in|ios::out|ios::app);
        if (out.is_open()) {
            res += "]";
            out << res;
            out.close();
        }
    }

    // 分片2
    res = "";
    if(dev::consensus::internal_groupId == sub2 && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(sub2 - 1)))
    {
        for (int i =0 ; i < 1000; i++) {
            if (i < 100) {
                tx = _injectionTest.createSpecialInnerTransactions(sub2, ledgerManager, "stateE", "stateG");
            } else if(i < 200) {
                tx = _injectionTest.createSpecialInnerTransactions(sub2, ledgerManager, "stateF", "stateH");
            } else {
                tx = _injectionTest.createSpecialInnerTransactions(sub2, ledgerManager, "stateG", "stateH");
            }
            if (i == 0) {
                res = "[\"" + tx + "\"";
            } else {
                res += ",\"" + tx + "\"";
            }
            std::this_thread::sleep_for(std::chrono::milliseconds((1)));
        }
        
        ofstream out;
        out.open(fileName, ios::in|ios::out|ios::app);
        if (out.is_open()) {
            res += "]";
            out << res;
            out.close();
        }
    }

    // 分片3
    res = "";
    if(dev::consensus::internal_groupId == cor && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(cor - 1)))
    {
        for (int i =0 ; i < 1000; i++) {
            if (i < 500) {
                tx = _injectionTest.createSpecialCrossTransactions(cor, sub1, sub2, ledgerManager, "stateA", "stateE");
            } else {
                tx = _injectionTest.createSpecialCrossTransactions(cor, sub1, sub2, ledgerManager, "stateB", "stateF");
            }
            if (i == 0) {
                res = "[\"" + tx + "\"";
            } else {
                res += ",\"" + tx + "\"";
            }
            std::this_thread::sleep_for(std::chrono::milliseconds((1)));
        }
        
        ofstream out;
        out.open(fileName, ios::in|ios::out|ios::app);
        if (out.is_open()) {
            res += "]";
            out << res;
            out.close();
        }
    }
}

void createDataSet(int cor, vector<int>& shardIds, std::shared_ptr<dev::ledger::LedgerManager> ledgerManager,
                   int txNum, int percent, std::shared_ptr<dev::rpc::Rpc> rpcService) {
    // 计算跨片交易及各分片的片内交易数
    string fileName;
    int subShardNum = shardIds.size() + 1;
    int crossTxNum = (txNum * percent) / 100;
    int innerTxNum = (txNum - crossTxNum) / subShardNum;
    int remain = (txNum - crossTxNum) % subShardNum;
    transactionInjectionTest _injectionTest(rpcService, 1);

    if (percent == 20) {
        fileName = "workload1.json";
    } else if (percent == 80) {
        fileName = "workload2.json";
    } else if (percent == 100) {
        fileName = "workload3.json";
    }

    // 生成子分片的片内交易
    for (int i = 0; i < subShardNum; i++) {
        int subId = shardIds[i];
        if(innerTxNum != 0 && dev::consensus::internal_groupId == subId && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(subId - 1)))
        {
            createInnerTransaction(subId, ledgerManager, innerTxNum, fileName, 0, _injectionTest);
        }
    }

    // 生成协调者分片的片内交易和跨片交易
    cout << "fileName:" << fileName << endl;
    if(dev::consensus::internal_groupId == cor && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(cor - 1)))
    {
        int nowInnerNum = innerTxNum + remain;
        int nowCrossNum = crossTxNum;
        if (nowInnerNum == 0) { //纯跨片交易 直接生成返回
            createCrossTransaction(cor, shardIds, ledgerManager, nowCrossNum, fileName, 0, _injectionTest);
            return;
        }
        bool firstTime = true;
        while(nowInnerNum != 0 || nowCrossNum != 0) {
            if (nowInnerNum == 0) {
                createCrossTransaction(cor, shardIds, ledgerManager, nowCrossNum, fileName, 3, _injectionTest);
                break;
            } else if (nowCrossNum == 0) {
                createInnerTransaction(cor, ledgerManager, nowInnerNum, fileName, 3, _injectionTest);
                break;
            }
            // srand((unsigned)time(NULL));
            int flag = rand() % 2;
            if (flag) { // 为1 生成跨片交易
                cout << "createDataSet 111" << endl;
                if (nowCrossNum < 100) {
                    // cout << "createDataSet 222" << endl;
                    if (firstTime == true) {
                        // cout << "进来了 firstTime = true" << endl; 
                        createCrossTransaction(cor, shardIds, ledgerManager, nowCrossNum, fileName, 1, _injectionTest);
                        firstTime = false;
                    } else {
                        createCrossTransaction(cor, shardIds, ledgerManager, nowCrossNum, fileName, 2, _injectionTest);
                    }
                    nowCrossNum = 0;
                } else {
                    // cout << "createDataSet 333" << endl;
                    cout << "firstTime:" << firstTime << endl;
                    if (firstTime == true) { // 第一次
                        // cout << "进来了 firstTime = true" << endl; 
                        createCrossTransaction(cor, shardIds, ledgerManager, 100, fileName, 1, _injectionTest);
                        firstTime = false;
                    } else {
                        createCrossTransaction(cor, shardIds, ledgerManager, 100, fileName, 2, _injectionTest);
                    }
                    nowCrossNum -= 100;
                }
            } else { // 为0 生成片内交易
                cout << "createDataSet 444" << endl;
                if (nowInnerNum < 100) {
                    // cout << "createDataSet 555" << endl;
                    if (firstTime == true) {
                        // cout << "进来了 firstTime = true" << endl; 
                        createInnerTransaction(cor, ledgerManager, nowInnerNum, fileName, 1, _injectionTest);
                        firstTime = false;
                    } else {
                        createInnerTransaction(cor, ledgerManager, nowInnerNum, fileName, 2, _injectionTest);
                    }
                    nowInnerNum = 0;
                } else {
                    // cout << "createDataSet 666" << endl;
                    // cout << "firstTime:" << firstTime << endl;
                    if (firstTime == true) {
                        // cout << "进来了 firstTime = true" << endl; 
                        createInnerTransaction(cor, ledgerManager, 100, fileName, 1, _injectionTest);
                        firstTime = false;
                    } else {
                        createInnerTransaction(cor, ledgerManager, 100, fileName, 2, _injectionTest);
                    }
                    nowInnerNum -= 100;
                }
            }
        }
    }
}

void swap(int &a, int &b) {
    a ^= b;       //a=a^b
    b ^= a;      //b=b^(a^b)=b^a^b=b^b^a=0^a=a
    a ^= b;     //a=(a^b)^a=a^b^a=a^a^b=0^b=b
}

int getLCA(int subId1, int subId2) {
    if ((subId1 <= 3 || subId1 == 7) && (subId2 <=3 || subId2 == 7)) {
        return 7;
    } else if (((subId1 >= 4 && subId1 <= 6) || subId1 == 8) && ((subId2 >= 4 && subId2 <= 6) || subId2 == 8)) {
        return 8;
    }
    return 9;
}

int createCrossId(int id) {
    srand((unsigned)time(NULL));
    int newId;

    if (id <= 3) {
        // 范围:[4-6,8,9]
        newId = (rand() % 5) + 4;
        if (newId >= 7) {
            newId++;
        }
    } else if (id <= 6) {
        // 范围:[1-3,7,9]
        newId = (rand() % 5) + 1;
        if (newId == 4) {
            newId = 7;
        } else if (newId == 5) {
            newId = 9;
        }
    } else if (id == 7) {
        // 范围:[4-6]
        newId = (rand() % 3) + 4;
    } else if (id == 8) {
        // 范围:[1-3]
        newId = (rand() % 3) + 1;
    }
    return newId;
}

void createRandomDataSet(std::shared_ptr<dev::ledger::LedgerManager> ledgerManager, int txNum, int percent, std::shared_ptr<dev::rpc::Rpc> rpcService) {
    // 计算跨片交易及各分片的片内交易数
    string fileName, newfileName;
    int shardsNum = dev::consensus::SHARDNUM;
    int crossTxNum = (txNum * percent) / 100;
    int innerTxNum = (txNum - crossTxNum) / shardsNum;
    int remain = (txNum - crossTxNum) % shardsNum;
    crossTxNum += remain;
    transactionInjectionTest _injectionTest(rpcService, 1);
    if (percent == 0) {
        fileName = "workload0.json";
    } else if (percent == 20) {
        fileName = "workload1.json";
    } else if (percent == 40) {
        fileName = "workload2.json";
    } else if (percent == 60) {
        fileName = "workload3.json";
    } else if (percent == 80) {
        fileName = "workload4.json";
    } else if (percent == 100) {
        fileName = "workload5.json";
    }

    // 生成子分片的片内交易
    if (innerTxNum != 0) {
        for (int i = 1; i <= shardsNum; i++) {
            flags[i - 1] = false;
            newfileName = "../node" + to_string((i-1)*4) + "/" + fileName;
            // std::cout << "fileName: " << newfileName << std::endl;
            createInnerTransaction(i, ledgerManager, innerTxNum, newfileName, 0, _injectionTest);
        }
    }

    // 生成随机跨片交易
    int nowCrossNum = crossTxNum;
    srand((unsigned)time(NULL));
    while(nowCrossNum != 0) {
        // 生成随机两个子分片ID 范围:[1,8]
        // 默认各个节点生成的随机数顺序一致
        int subId1 = (rand() % (shardsNum - 1)) + 1;
        int subId2 = (rand() % (shardsNum - 1)) + 1;
        while (subId2 == subId1) {
            subId2 = (rand() % (shardsNum - 1)) + 1;
        }
        // 查找最近公共祖先
        int corId = getLCA(subId1, subId2);
        std::cout << corId << ":" << subId1 << "-" << subId2 << std::endl;
        // 生成交易
        newfileName = "../node" + to_string((corId-1)*4) + "/" + fileName;
        if (nowCrossNum < 500) {
            createCrossTransaction(corId, subId1, subId2, ledgerManager, nowCrossNum, newfileName, _injectionTest);
            nowCrossNum = 0;
        } else {
            createCrossTransaction(corId, subId1, subId2, ledgerManager, 500, newfileName, _injectionTest);
            nowCrossNum -= 500;
        }
    }
}

// 生成局部性负载
void createLocalityDataSet(std::shared_ptr<dev::ledger::LedgerManager> ledgerManager, int txNum, int percent, std::shared_ptr<dev::rpc::Rpc> rpcService) {
    // 计算跨片交易及各分片的片内交易数
    string fileName, newfileName;
    int shardsNum = dev::consensus::SHARDNUM;
    int crossTxNum = (txNum * percent) / 100;
    int innerTxNum = (txNum - crossTxNum) / shardsNum;
    transactionInjectionTest _injectionTest(rpcService, 1);
    if (percent == 0) {
        fileName = "workload0.json";
    } else if (percent == 20) {
        fileName = "workload1.json";
    } else if (percent == 50) {
        fileName = "workload2.json";
    } else if (percent == 80) {
        fileName = "workload3.json";
    } else if (percent == 100) {
        fileName = "workload4.json";
    }

    // 生成子分片的片内交易
    if (innerTxNum != 0) {
        for (int i = 1; i <= shardsNum; i++) {
            flags[i - 1] = false;
            newfileName = "../node" + to_string((i-1)*4) + "/" + fileName;
            createInnerTransaction(i, ledgerManager, innerTxNum, newfileName, 0, _injectionTest);
        }
    }

    // 生成局部性跨片交易
    int allCrossNum = crossTxNum;
    // 跨层交易和局部性交易数
    int otherCrossNum = crossTxNum / 21;
    int localityCrossNum = otherCrossNum * 20;
    // 上层局部性交易数
    int upperCrossNum = localityCrossNum / 5;
    // 下层局部性交易数
    int downCrossNum = localityCrossNum / 5 * 2;
    
    // 生成局部性交易
    srand((unsigned)time(NULL));
    int nowCrossNum = upperCrossNum;
    std::cout << "开始生成上层局部性交易：" << std::endl;
    while(nowCrossNum != 0) {
        // 生成随机两个子分片ID 范围:[7,9]
        // 默认各个节点生成的随机数顺序一致
        int subId1 = (rand() % (9 - 7 + 1)) + 7;
        int subId2 = (rand() % (9 - 7 + 1)) + 7;
        while (subId2 == subId1) {
            subId2 = (rand() % (9 - 7 + 1)) + 7;
        }
        if (subId1 > subId2) {
            swap(subId1, subId2);
        }
        // 查找最近公共祖先
        int corId = 9;
        std::cout << corId << ":" << subId1 << "-" << subId2 << std::endl;
        // 生成交易
        newfileName = "../node" + to_string((corId-1)*4) + "/" + fileName;
        if (nowCrossNum < 500) {
            createCrossTransaction(corId, subId1, subId2, ledgerManager, nowCrossNum, newfileName, _injectionTest);
            nowCrossNum = 0;
        } else {
            createCrossTransaction(corId, subId1, subId2, ledgerManager, 500, newfileName, _injectionTest);
            nowCrossNum -= 500;
        }
    }

    std::cout << "开始生成下层局部性交易：" << std::endl;
    for (int i = 1; i < 7; i += 3) {
        nowCrossNum = downCrossNum;
        while(nowCrossNum != 0) {
            // 生成随机两个子分片ID 范围:[1,4] / [4,7] == [i, i+3]
            // 默认各个节点生成的随机数顺序一致
            int subId1 = (rand() % 4) + i;
            int subId2 = (rand() % 4) + i;
            while (subId2 == subId1) {
                subId2 = (rand() % 4) + i;
            }
            if (i == 1 && subId1 == 4) {
                subId1 = 7;
            } else if (i == 1 && subId2 == 4) {
                subId2 = 7;
            } else if (subId1 == 7) {
                subId1 = 8;
            } else if (subId2 == 7) {
                subId2 = 8;
            }

            if (subId1 > subId2) {
                swap(subId1, subId2);
            }

            int corId;
            if (i == 1) {
                corId = 7;
            } else if (i == 4) {
                corId = 8;
            }
            
            std::cout << corId << ":" << subId1 << "-" << subId2 << std::endl;
            // 生成交易
            newfileName = "../node" + to_string((corId-1)*4) + "/" + fileName;
            if (nowCrossNum < 500) {
                createCrossTransaction(corId, subId1, subId2, ledgerManager, nowCrossNum, newfileName, _injectionTest);
                nowCrossNum = 0;
            } else {
                createCrossTransaction(corId, subId1, subId2, ledgerManager, 500, newfileName, _injectionTest);
                nowCrossNum -= 500;
            }
        }
    }

    // 生成跨层交易
    nowCrossNum = otherCrossNum;
    std::cout << "开始生成跨层级交易：" << std::endl;
    while(nowCrossNum != 0) {
        // 生成随机两个子分片ID 范围:[1,9]
        // 默认各个节点生成的随机数顺序一致
        int subId1 = (rand() % shardsNum) + 1;
        int subId2 = createCrossId(subId1);
        if (subId1 > subId2) {
            swap(subId1, subId2);
        }

        // 查找最近公共祖先
        int corId = getLCA(subId1, subId2);
        std::cout << corId << ":" << subId1 << "-" << subId2 << std::endl;
        // 生成交易
        newfileName = "../node" + to_string((corId-1)*4) + "/" + fileName;
        if (nowCrossNum < 500) {
            createCrossTransaction(corId, subId1, subId2, ledgerManager, nowCrossNum, newfileName, _injectionTest);
            nowCrossNum = 0;
        } else {
            createCrossTransaction(corId, subId1, subId2, ledgerManager, 500, newfileName, _injectionTest);
            nowCrossNum -= 500;
        }
    }

}

int main(){

    dev::consensus::SHARDNUM = 9; // 初始化分片数目
    std::cout << "SHARDNUM = " << dev::consensus::SHARDNUM << std::endl;

    // 开始增加组间通信同步组
    boost::property_tree::ptree pt;
    boost::property_tree::read_ini("./configgroup.ini", pt);

    std::string jsonrpc_listen_ip = pt.get<std::string>("rpc.jsonrpc_listen_ip");
    std::string jsonrpc_listen_port = pt.get<std::string>("rpc.jsonrpc_listen_port");
    std::string nearest_upper_groupId = pt.get<std::string>("layer.nearest_upper_groupId");
    std::string nearest_lower_groupId = pt.get<std::string>("layer.nearest_lower_groupId");

    // 对dev::consensus::messageIDs进行初始化
    for(int i = 0; i < dev::consensus::SHARDNUM; i++)
    {
        dev::consensus::messageIDs.insert(std::make_pair(i + 1, 0));
        dev::plugin::flags.push_back(true);
    }

    // 对latest_candidate_tx_messageids进行初始化
    latest_candidate_tx_messageids = std::make_shared<tbb::concurrent_vector<unsigned long>>(dev::consensus::SHARDNUM);

    // ADD ON 22.11.16
    current_candidate_tx_messageids = std::make_shared<tbb::concurrent_vector<unsigned long>>(dev::consensus::SHARDNUM);
    // for (unsigned long i = 0; i < dev::consensus::SHARDNUM; i++) {
    //     current_candidate_tx_messageids->at(i) = 1;
    //     // cout << i << "===>" << current_candidate_tx_messageids->at(i) << endl;
    // }
    
    // ADD ON 22.11.20
    complete_candidate_tx_messageids = std::make_shared<tbb::concurrent_vector<unsigned long>>(dev::consensus::SHARDNUM);

    /* 以下为测试
    unsigned long message_id = 1;
    unsigned long source_shard_id = 3;
    for (unsigned long i = 1; i <= dev::consensus::SHARDNUM; i++) {
        cout << i << " : " << latest_candidate_tx_messageids->at(i - 1) << endl;
        if (message_id == latest_candidate_tx_messageids->at(i - 1) + 1) {
            cout << i << " is OK" << endl;
        }
    }
    cout << "测试cached_cs_tx: " << (cached_cs_tx->count("attempt_key") != 0) << endl;
    cout << "测试candidate_tx_queues: " << candidate_tx_queues->count("readwriteset") << endl;
    cout << "测试locking_key: " << locking_key->count("readwriteset") << endl;
    cout << "测试locking_key insert: " << locking_key->insert(std::make_pair("readwriteset", 1)).second << endl; 
    cout << "测试candidate_tx_queue insert: " << endl;
    unsigned long i = 0;
    dev::eth::Transaction::Ptr tx;
    dev::blockverifier::ExecutiveContext::Ptr executiveContext;
    dev::executive::Executive::Ptr executive; 
    dev::eth::Block block = dev::eth::Block();

    std::queue<executableTransaction> queue = std::queue<executableTransaction>();
    candidate_tx_queue _candidate_tx_queue { "readwriteset", queue };
    _candidate_tx_queue.queue.push(executableTransaction{i, tx, executiveContext, executive, block});
    candidate_tx_queues->insert(std::make_pair("readwriteset", _candidate_tx_queue));

    测试结束*/

    // 对dev::consensus::latest_commit_cs_tx进行初始化
    for(int i = 0; i < dev::consensus::SHARDNUM; i++)
    {
        dev::blockverifier::latest_commit_cs_tx.push_back(0);
    }

    GroupP2PService groupP2Pservice("./configgroup.ini");
    auto p2pService = groupP2Pservice.p2pInitializer()->p2pService();
    putGroupPubKeyIntoService(p2pService, pt);
    putGroupPubKeyIntoshardNodeId(pt); // 读取全网所有节点Ï
    p2pService->start();

    GROUP_ID groupId = std::stoi(pt.get<std::string>("group.global_group_id")); // 全局通信使用的groupid

    auto nodeid = asString(contents("conf/node.nodeid"));
    NodeID nodeId = NodeID(nodeid.substr(0, 128));
    nodeIdStr = toHex(nodeId);

    PROTOCOL_ID syncId = getGroupProtoclID(groupId, ProtocolID::InterGroup);

    std::cout << "syncId = " << syncId << std::endl;

    std::shared_ptr<dev::initializer::Initializer> initialize = std::make_shared<dev::initializer::Initializer>();
    // initialize->init_with_groupP2PService("./config.ini", p2pService);  // 启动3个群组
    initialize->init_with_groupP2PService("./config.ini", p2pService, syncId);  // 启动3个群组
    // initialize->init("./config.ini");  // 启动3个群组

    // ADD BY ZH
    group_protocolID = syncId;
    group_p2p_service = p2pService;


    dev::consensus::internal_groupId = std::stoi(pt.get<std::string>("group.internal_group_id")); // 片内使通信使用的groupID
    auto secureInitializer = initialize->secureInitializer();
    auto ledgerManager = initialize->ledgerInitializer()->ledgerManager();
    auto consensusP2Pservice = initialize->p2pInitializer()->p2pService();
    auto rpcService = std::make_shared<dev::rpc::Rpc>(initialize->ledgerInitializer(), consensusP2Pservice);
    auto blockchainManager = ledgerManager->blockChain(dev::consensus::internal_groupId);

    shared_ptr<dev::plugin::SyncThreadMaster> syncs = std::make_shared<dev::plugin::SyncThreadMaster>(p2pService, syncId, nodeId, dev::consensus::internal_groupId, rpcService);
    std::shared_ptr<ConsensusPluginManager> consensusPluginManager = std::make_shared<ConsensusPluginManager>(rpcService);
    // consensusPluginManager->m_deterministExecute->start(); // 启动交易处理线程
    
    
    consensusPluginManager->setAttribute(blockchainManager, rpcService);
    // consensusPluginManager->m_deterministExecute->setAttribute(blockchainManager);
    
    std::thread processBlocksThread(&dev::plugin::deterministExecute::processConsensusBlock, consensusPluginManager->m_deterministExecute);
    processBlocksThread.detach();
    
    std::thread executetxsThread(&dev::plugin::deterministExecute::deterministExecuteTx, consensusPluginManager->m_deterministExecute);
    executetxsThread.detach();
    
    syncs->setAttribute(blockchainManager);
    syncs->setAttribute(consensusPluginManager);
    
    // std::this_thread::sleep_for(std::chrono::milliseconds(5000));

    /* 
    // 测试发送交易（分片1的node1向本分片1发送一笔片内交易
    if(dev::consensus::internal_groupId == 1 && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(0)))
    {
        PLUGIN_LOG(INFO) << LOG_DESC("准备发送交易...")<< LOG_KV("nodeIdStr", nodeIdStr);
        transactionInjectionTest _injectionTest(rpcService, 1, ledgerManager);
        // _injectionTest.deployContractTransaction("./deploy.json", 1);
        std::this_thread::sleep_for(std::chrono::milliseconds(4000));
        // _injectionTest.injectionTransactions("./workload1.json", 1);
        // _injectionTest.injectionTransactions("./workload2.json", 1);
        // _injectionTest.injectionTransactions("./workload3.json", 1);
    }

    if(dev::consensus::internal_groupId == 2 && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(1)))
    {
        PLUGIN_LOG(INFO) << LOG_DESC("准备发送交易...")<< LOG_KV("nodeIdStr", nodeIdStr);
        transactionInjectionTest _injectionTest(rpcService, 2, ledgerManager);
        // _injectionTest.deployContractTransaction("./deploy.json", 2);
        std::this_thread::sleep_for(std::chrono::milliseconds(4000));
        // _injectionTest.injectionTransactions("./workload1.json", 2);
        // _injectionTest.injectionTransactions("./workload2.json", 2);
        // _injectionTest.injectionTransactions("./workload3.json", 2);
    }

    if(dev::consensus::internal_groupId == 3 && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(2)))
    {
        PLUGIN_LOG(INFO) << LOG_DESC("准备发送交易...")<< LOG_KV("nodeIdStr", nodeIdStr);
        transactionInjectionTest _injectionTest(rpcService, 3, ledgerManager);
        std::this_thread::sleep_for(std::chrono::milliseconds(4000));
        // _injectionTest.injectionTransactions("./workload1.json", 3);
        // _injectionTest.injectionTransactions("./workload2.json", 3);
        // _injectionTest.injectionTransactions("./workload3.json", 3);
    }

    if(dev::consensus::internal_groupId == 7 && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(6)))
    {
        PLUGIN_LOG(INFO) << LOG_DESC("准备发送交易...")<< LOG_KV("nodeIdStr", nodeIdStr);
        transactionInjectionTest _injectionTest(rpcService, 7, ledgerManager);
        // _injectionTest.deployContractTransaction("./deploy.json", 7);
        std::this_thread::sleep_for(std::chrono::milliseconds(4000));
        // _injectionTest.injectionTransactions("./workload1.json", 7);
        // _injectionTest.injectionTransactions("./workload2.json", 7);
        // _injectionTest.injectionTransactions("./workload3.json", 7);
    }

    if(dev::consensus::internal_groupId == 8 && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(7)))
    {
        PLUGIN_LOG(INFO) << LOG_DESC("准备发送交易...")<< LOG_KV("nodeIdStr", nodeIdStr);
        transactionInjectionTest _injectionTest(rpcService, 8, ledgerManager);
        // _injectionTest.deployContractTransaction("./deploy.json", 8);
        // std::this_thread::sleep_for(std::chrono::milliseconds(4000));
        // _injectionTest.injectionTransactions("./workload1.json", 8);
        // _injectionTest.injectionTransactions("./workload2.json", 8);
        // _injectionTest.injectionTransactions("./workload3.json", 8);
        
        // 测试文件覆盖功能
        // ofstream out;
        // out.open("test.json", ios::in|ios::out|ios::app);
        // if (out.is_open()) {
        //     out << "[\"dfada\"]";
        //     out.close();
        // }
        // out.open("test.json", ios::binary | ios::out | ios::in);
        // if (out.is_open()) {
        //     out.seekp(-1, ios::end);
        //     out << "123";
        //     out.close();
        // }

        // fstream fout;
        // fout.open("test.json", fstream::binary | fstream::out | fstream::in);
        // fout.tellp();
        // fout.seekp(-1, ios::end);
        // fout.write(",####]",4);
        // // fout << "123";
        // fout.close();
    }
    */

    std::this_thread::sleep_for(std::chrono::milliseconds(10000));
// dev::consensus::SHARDNUM
    for (int i = 1; i <= dev::consensus::SHARDNUM; i++) {
        if(dev::consensus::internal_groupId == i && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(i - 1))) {
            PLUGIN_LOG(INFO) << LOG_DESC("准备发送交易...")<< LOG_KV("nodeIdStr", nodeIdStr);
            transactionInjectionTest _injectionTest(rpcService, i, ledgerManager);
            // _injectionTest.deployContractTransaction("./deploy.json", i);
            
            // _injectionTest.injectionTransactions("./workload0.json", i);
            _injectionTest.injectionTransactions("./workload1.json", i);
            // _injectionTest.injectionTransactions("./workload2.json", i);
            // _injectionTest.injectionTransactions("./workload3.json", i);
            // _injectionTest.injectionTransactions("./workload4.json", i);
        }
    }

    // createDataSet(3, 1, 2, ledgerManager, 150000, 20, rpcService);
    // createDataSet(3, 1, 2, ledgerManager, 150000, 80, rpcService);
    // createDataSet(3, 1, 2, ledgerManager, 90000, 100, rpcService);
    // createHBDataSet(3, 1, 2, ledgerManager, rpcService);

    // int coorShardId = 7;
    // std::vector<int> subShardIds;
    // subShardIds.push_back(1);
    // subShardIds.push_back(2);
    // subShardIds.push_back(3);
    // createDataSet(coorShardId, subShardIds, ledgerManager, 15000, 100, rpcService);

    // createDataSet(7, 1, 2, ledgerManager, 15000, 100, rpcService);
    // createDataSet(8, 2, 3, ledgerManager, 15000, 100, rpcService);
    // createDataSet(7, 1, 2, ledgerManager, 15000, 80, rpcService);
    // createDataSet(8, 2, 3, ledgerManager, 15000, 80, rpcService);
    // createDataSet(7, 1, 2, ledgerManager, 15000, 20, rpcService);
    // createDataSet(8, 2, 3, ledgerManager, 15000, 20, rpcService);
    
    // 生成均匀负载
    // if(dev::consensus::internal_groupId == 1 && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(0))) {
    //     // createRandomDataSet(ledgerManager, 90000, 0, rpcService);
    //     // createRandomDataSet(ledgerManager, 90000, 20, rpcService);
    //     // createRandomDataSet(ledgerManager, 90000, 40, rpcService);
    //     // createRandomDataSet(ledgerManager, 90000, 60, rpcService);
    //     // createRandomDataSet(ledgerManager, 90000, 80, rpcService);
    //     createRandomDataSet(ledgerManager, 90000, 100, rpcService);
    // }

    // 生成局部性负载
    // if(dev::consensus::internal_groupId == 1 && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(0))) {
    //     // createLocalityDataSet(ledgerManager, 63000, 0, rpcService);
    //     // createLocalityDataSet(ledgerManager, 63000, 20, rpcService);
    //     // createLocalityDataSet(ledgerManager, 63000, 50, rpcService);
    //     // createLocalityDataSet(ledgerManager, 63000, 80, rpcService);
    //     createLocalityDataSet(ledgerManager, 63000, 100, rpcService);
    // }
    

    std::cout << "node " + jsonrpc_listen_ip + ":" + jsonrpc_listen_port + " start success." << std::endl;

    if(nearest_upper_groupId != "N/A")
    {
        std::cout << "nearest_upper_groupId = " << nearest_upper_groupId << std::endl;
    }
    else
    {
        std::cout<<"it's a root group" << std::endl;
    }

    if(nearest_lower_groupId != "N/A")
    {
        std::cout << "nearest_lower_groupId = " << nearest_lower_groupId << std::endl;
    }
    else
    {
        std::cout<<"it's a leaf group" << std::endl;
    }

    while (true)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000000));
    }
    return 0;
}
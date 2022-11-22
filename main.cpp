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
        // struct transaction
        // {
        //     int type; // 交易类型, 0 为片内交易, 1 为跨片子交易
        //     int source_shard_id;
        //     int destin_shard_id;
        //     int message_id;
        //     int readwritesetnum;
        //     std::string sub_tx_hash;
        //     std::string cross_tx_hash;
        //     std::string readwrite_key; // 多个读写集时候中间用'|'号分隔开，为了便于实验，先假设所有的片内交易只访问片内的一个读写集key，跨片交易的读写集可能有多个
        //     // 子交易句柄，这里先假设有了，拿到了就可以执行
        // };
        // struct candidate_tx_queue
        // {
        //     std::string blocked_readwriteset; // 队列所阻塞的读写集
        //     std::queue<transaction> queue; // 缓存的交易
        // };
        
        std::map<h256, transaction> crossTx; // 分片待处理的跨片子交易详细信息
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

        // 协调者分片存储对应跨片交易收集到的状态消息包
        std::shared_ptr<tbb::concurrent_unordered_map<std::string, std::vector<int>>> crossTx2ReceivedMsg = 
                                std::make_shared<tbb::concurrent_unordered_map<std::string, std::vector<int>>>();
        std::mutex m_crossTx2ReceivedMsgMutex;
            
        // 子分片存储对应跨片交易收集到的commit消息包数量
        std::shared_ptr<tbb::concurrent_unordered_map<std::string, int>> crossTx2CommitMsg = 
                                std::make_shared<tbb::concurrent_unordered_map<std::string, int>>();
        std::mutex m_crossTx2CommitMsgMutex;

        // 协调者分片存储对应子分片执行跨片交易成功收集的消息包数量
        std::shared_ptr<tbb::concurrent_unordered_map<std::string, std::vector<int>>> crossTx2ReceivedCommitMsg = 
                                std::make_shared<tbb::concurrent_unordered_map<std::string, std::vector<int>>>();
        std::mutex m_crossTx2ReceivedCommitMsgMutex;
        
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
        // 片内交易映射至交易状态集
        std::map<h256, transaction> innerTx;
        // 跨片交易映射至对应状态集
        std::shared_ptr<tbb::concurrent_unordered_map<std::string, std::string>> crossTx2StateAddress = 
                                std::make_shared<tbb::concurrent_unordered_map<std::string, std::string>>();
        std::mutex m_crossTx2AddressMutex;
        
        // ADD ON 22.11.16
        // 落后的跨片交易
        std::shared_ptr<tbb::concurrent_unordered_set<int>> lateCrossTxMessageId = 
                                std::make_shared<tbb::concurrent_unordered_set<int>>();
        std::mutex m_lateCrossTxMutex;


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

/*
void executeTxs()
{
    while (true)
    {
        if(dev::consensus::toExecute_transactions.size() > 0)
        {
            auto tx = dev::consensus::toExecute_transactions.front();
            auto tx_hash = tx->hash();
            BLOCKVERIFIER_LOG(INFO) << LOG_DESC("缓存交易的hash") << LOG_KV("tx_hash", tx_hash);
            /
                添加交易逻辑
            /


            
            dev::consensus::toExecute_transactions.pop();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

void startprocessThread()
{
    // 单独启动一个线程执行executeTxs()
    BLOCKVERIFIER_LOG(INFO) << LOG_DESC("startprocessThread...");
    std::thread executeTxs_Thread(executeTxs);
    executeTxs_Thread.detach();
    BLOCKVERIFIER_LOG(INFO) << LOG_DESC("startprocessThread...");;
}
*/
class CrossShardTest
{
    private:
        std::vector<std::string> m_transactionsRLP;
        std::string m_contractStr;

        std::shared_ptr<dev::rpc::Rpc> m_rpcService;
        shared_ptr<SyncThreadMaster> m_syncs;
        int32_t m_groupId;
        int32_t m_internal_groupId;

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

int main(){

    std::cout<< "* * * * * * * * * HieraChain v0.0.2 * * * * * * * * *" <<std::endl;

    dev::consensus::SHARDNUM = 3; // 初始化分片数目
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
        dev::consensus::messageIDs.insert(std::make_pair(i, 0));
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
    std::thread executetxsThread(&dev::plugin::deterministExecute::deterministExecuteTx, consensusPluginManager->m_deterministExecute);
    executetxsThread.detach();

    syncs->setAttribute(blockchainManager);
    syncs->setAttribute(consensusPluginManager);
    
    // startprocessThread();
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));


    // 测试发送交易（分片1的node1向本分片1发送一笔片内交易
    if(dev::consensus::internal_groupId == 1 && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(0)))
    {
        PLUGIN_LOG(INFO) << LOG_DESC("准备发送交易...")<< LOG_KV("nodeIdStr", nodeIdStr);
        transactionInjectionTest _injectionTest(rpcService, 1);
        _injectionTest.deployContractTransaction("./deploy.json", 1);
        // std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        // _injectionTest.injectionTransactions("./signedtxs.json", 1);

        // std::this_thread::sleep_for(std::chrono::milliseconds(4000));
        // _injectionTest.createInnerTransactions(1);

        /* 批量生产跨片交易
        std::string res;
        for (int i =0 ; i < 100000; i++) {
            auto tx = _injectionTest.createCrossTransactions_HB(1, 2, 3, i);
            if (i % 1000 == 0) { // 100笔写一次文件
                if (i == 0) {
                    res = "[\"" + tx + "\"";
                } else {
                    ofstream out;
                    out.open("crossTx_HB.json", ios::in|ios::out|ios::app);
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
            // std::this_thread::sleep_for(std::chrono::milliseconds((500)));
        }

        ofstream out;
        out.open("crossTx_HB.json", ios::in|ios::out|ios::app);
        if (out.is_open()) {
            res += "]";
            out << res;
            out.close();
        }
        */
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    if(dev::consensus::internal_groupId == 2 && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(1)))
    {
        PLUGIN_LOG(INFO) << LOG_DESC("准备发送交易...")<< LOG_KV("nodeIdStr", nodeIdStr);
        transactionInjectionTest _injectionTest(rpcService, 2);
        _injectionTest.deployContractTransaction("./deploy.json", 2);
        // std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        // _injectionTest.injectionTransactions("./signedtxs.json", 2);

        // std::this_thread::sleep_for(std::chrono::milliseconds(4000));
        // _injectionTest.createInnerTransactions(2);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    if(dev::consensus::internal_groupId == 3 && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(2)))
    {
        PLUGIN_LOG(INFO) << LOG_DESC("准备发送交易...")<< LOG_KV("nodeIdStr", nodeIdStr);
        transactionInjectionTest _injectionTest(rpcService, 3);
        _injectionTest.deployContractTransaction("./deploy.json", 3);
        std::this_thread::sleep_for(std::chrono::milliseconds(4000));
        // _injectionTest.injectionTransactions("./signedtxs.json", 3);
        _injectionTest.injectionTransactions("./crossTx.json", 3);

        // std::this_thread::sleep_for(std::chrono::milliseconds(4000));
        // _injectionTest.createInnerTransactions(3);
        // _injectionTest.createCrossTransactions(3, 1, 2);

        /* 批量生产跨片交易
        std::string res;
        for (int i =0 ; i < 100; i++) {
            auto tx = _injectionTest.createCrossTransactions(3, 1, 2);
            if (i % 100 == 0) { // 10笔写一次文件
                if (i == 0) {
                    res = "[\"" + tx + "\"";
                } else {
                    ofstream out;
                    out.open("crossTx.json", ios::in|ios::out|ios::app);
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
        out.open("crossTx.json", ios::in|ios::out|ios::app);
        if (out.is_open()) {
            res += "]";
            out << res;
            out.close();
        }
        */
    }

    /*
    // 测试发送交易（分片3的头节点向本分片发送一笔跨片交易
    if(dev::consensus::internal_groupId == 3 && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(2)))
    {
        PLUGIN_LOG(INFO) << LOG_DESC("准备发送交易...")<< LOG_KV("nodeIdstr", nodeid);
        transactionInjectionTest _injectionTest(rpcService, dev::consensus::internal_groupId);
        // _injectionTest.deployContractTransaction("./deploy.json", 1);
        _injectionTest.injectionTransactions("./signedtxs.json", dev::consensus::internal_groupId);
    }
    */

    // // 启动后等待客户端部署合约，将合约贴在文件后，输入回车符，程序继续往下运行
    // int flag;
    // cin >> flag;
    // std::cout << "开始两阶段提交跨片交易性能测试..." << endl;

    // CrossShardTest cst(rpcService, groupId, syncs);
    // cst.InjectTxRead_Write_Sets(); // 导入所有跨片交易读写集
    // cst.start(); // 启动 receive 和 work thread

    // cst.InjectTxDAddr();
    // if(internal_groupId == 1) // 群组1导入所有待发送的交易RLP码
    // {
    //     cst.InjectTxRLP();
    //     cst.InjextConAddress2txrlps(); // 导入跨片子交易
    //     std::cout << "测试交易导入成功..." << std::endl;
    //     std::cout << "准备发送交易..."<< std::endl;
    //     cst.sendTx(); // 向群组二节点发送交易（其实交易应该生成片2的交易，coordinator只是共识保存）
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

    //size_t duration = 0;
    while (true)
    {
        //duration ++;
        std::this_thread::sleep_for(std::chrono::milliseconds(1000000));
    }
    return 0;
}
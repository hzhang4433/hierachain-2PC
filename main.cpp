//#include <grpcpp/grpcpp.h>
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
#include <string>
#include <unistd.h>
#include <thread>

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
    }
}

namespace dev{
    namespace consensus{
        int internal_groupId; // 当前分片所在的groupID
        int SHARDNUM; // 分片总数
        int NODENUM; // 所有节点数目
        std::vector<dev::h512>forwardNodeId;
        std::vector<dev::h512>shardNodeId;
        std::map<int, int> messageIDs;
    }
}

namespace dev{
    namespace blockverifier{
        std::vector<int>latest_commit_cs_tx;
        std::map<std::string, std::shared_ptr<dev::eth::Transaction>> blocked_txs;
        std::map<std::string, std::shared_ptr<dev::eth::Block>> blocked_blocks;
    }
}

namespace dev{
    namespace rpc{
        std::vector<dev::h256> subCrossTxsHash; // 记录所有待处理的跨片子交易hash
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
                groupid = (int)((s[s_size - 1])[0] - '0');
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

    auto nodeIdstr = asString(contents("conf/node.nodeid"));
    NodeID nodeId = NodeID(nodeIdstr.substr(0, 128));
    PROTOCOL_ID syncId = getGroupProtoclID(groupId, ProtocolID::InterGroup);

    std::cout << "syncId = " << syncId << std::endl;

    std::shared_ptr<dev::initializer::Initializer> initialize = std::make_shared<dev::initializer::Initializer>();
    // initialize->init_with_groupP2PService("./config.ini", p2pService);  // 启动3个群组
    initialize->init_with_groupP2PService("./config.ini", p2pService, syncId);  // 启动3个群组
    // initialize->init("./config.ini");  // 启动3个群组

    dev::consensus::internal_groupId = std::stoi(pt.get<std::string>("group.internal_group_id")); // 片内使通信使用的groupID
    auto secureInitializer = initialize->secureInitializer();
    auto ledgerManager = initialize->ledgerInitializer()->ledgerManager();
    auto consensusP2Pservice = initialize->p2pInitializer()->p2pService();
    auto rpcService = std::make_shared<dev::rpc::Rpc>(initialize->ledgerInitializer(), consensusP2Pservice);
    auto blockchainManager = ledgerManager->blockChain(dev::consensus::internal_groupId);

    shared_ptr<dev::plugin::SyncThreadMaster> syncs = std::make_shared<dev::plugin::SyncThreadMaster>(p2pService, syncId, nodeId, dev::consensus::internal_groupId, rpcService);
    std::shared_ptr<ConsensusPluginManager> consensusPluginManager = std::make_shared<ConsensusPluginManager>(rpcService);
    syncs->setAttribute(blockchainManager);
    syncs->setAttribute(consensusPluginManager);
    // syncs->startThread(); // 不再启用轮循检查，通过回调函数异步通知

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

    size_t duration = 0;
    while (true)
    {
        duration ++;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return 0;
}
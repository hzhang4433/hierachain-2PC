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
#include <libplugin/InjectThreadMaster.h>
#include <libplugin/hieraShardTree.h>
#include "Common.h"
#include "argparse.h"


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
using namespace dev::consensus;

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

        // std::vector<bool> flags;

        // 交易延时记录
        std::shared_ptr<tbb::concurrent_unordered_map<string, int>> m_txid_to_starttime = 
                                std::make_shared<tbb::concurrent_unordered_map<string, int>>();
        std::shared_ptr<tbb::concurrent_unordered_map<string, int>> m_txid_to_endtime = 
                                std::make_shared<tbb::concurrent_unordered_map<string, int>>();


        std::shared_ptr<dev::plugin::HieraShardTree> hieraShardTree = make_shared<dev::plugin::HieraShardTree>();
        int injectSpeed = 5000;
        int total_injectNum = -1;

        // std::map<std::string, std::string> txRWSet;
        // std::map<int, std::vector<std::string>> processingTxD;
        // std::map<std::string, int> subTxRlp2ID;
        // tbb::concurrent_queue<std::vector<std::string>> receCommTxRlps;
        // std::map<std::string, std::vector<std::string>> conAddress2txrlps;
        // std::vector<std::string> disTxDepositAddrs;
        // std::map<std::string, int> subTxNum;
        // std::map<std::string, std::vector<std::string>> resendTxs;
        // std::vector<std::string> committedDisTxRlp;
        // std::vector<std::string> preCommittedDisTxRlp;
        // std::map<std::string, std::string>txRlp2ConAddress;
        // std::vector<std::string> coordinatorRlp;
        std::shared_ptr<ExecuteVMTestFixture> executiveContext;

        int global_txId = 0;
    }
}

namespace dev{
    namespace consensus{
        int internal_groupId; // 当前分片所在的groupID
        int hiera_shard_number; // 分片总数
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


int main(int argc, char const *argv[]){
    auto args = util::argparser("hiera-2PC");
    args.set_program_name("test")
        .add_help_option()
        // .add_sc_option("-v", "--version", "show version info", []() {
        //     std::cout << "version " << VERSION << std::endl;
        // })
        .add_option("-g", "--generate", "need to generate tx json or not") // 生成模式 / 注入模式
        .add_option<int>("-n", "--number", "inject tx number, default is 400000", 400000) // 注入交易总数
        .add_option<int>("-s", "--speed", "inject speed(tps), default is 5000", 5000) // 注入速度
        .add_option<int>("-c", "--cross", "cross rate(0 ~ 100), default is 20", 20) // 跨片比例，0 ~ 100
        .add_option<int>("-t", "--thread", "inject thread number, default is 1", 1) // 线程数
        // .add_option<util::StepRange>("-r", "--range", "range", util::range(0, 10, 2)) 
        // .add_named_argument<std::string>("input", "initialize file")
        // .add_named_argument<std::string>("output", "output file")
        .parse(argc, argv);
    int injectNumber = args.get_option_int("--number");
    int inject_threadNumber = args.get_option_int("--thread");
    injectSpeed = args.get_option_int("--speed") / inject_threadNumber;
    int cross_rate = args.get_option_int("--cross");
    if (cross_rate > 100 || cross_rate < 0) {
        cout << "cross rate should be in range [0,100]!";
        return 1;
    }
    if (args.has_option("--generate")) {
        cout << "need to generate tx json..." << endl;
    }
    cout << "inject tx Number: " << injectNumber << endl;
    cout << "thread Number: " << inject_threadNumber << endl;
    cout << "inject Speed(total): " << args.get_option_int("--speed") << endl;
    cout << "inject Speed(each thread): " << injectSpeed << endl;
    cout << "cross Rate: " << cross_rate << endl;
    

    // // 获取分片的层级结构
    string tree_struct_json_filename = "../tree.json";
    hieraShardTree->buildFromJson(tree_struct_json_filename);
    cout << "build hieraShardTree success..." << endl;
    cout << "hieraShardRTree strcuture: " << endl;
    hieraShardTree->print();
    cout << endl;

    // 在建树时，已经统计得到了分片总数并存在成员变量中，直接get获取
    dev::consensus::hiera_shard_number = dev::plugin::hieraShardTree->get_hiera_shard_number();
    cout << "hiera_shard_number = " << hiera_shard_number << std::endl;

    // 初始化变量
    initVariables();
    
    // 开始增加组间通信同步组
    GroupP2PService groupP2Pservice("./configgroup.ini");
    auto p2pService = groupP2Pservice.p2pInitializer()->p2pService();
    p2pService->start();

    boost::property_tree::ptree pt;
    boost::property_tree::read_ini("./configgroup.ini", pt);
    hieraShardTree->putGroupPubKeyIntoService(p2pService, pt); // 读取全网所有节点，并存入p2pService中
    // putGroupPubKeyIntoService(p2pService, pt);
    putGroupPubKeyIntoshardNodeId(pt); // 读取全网所有节点Ï
    
    // 获取当前节点id
    auto nodeid = asString(contents("conf/node.nodeid"));
    NodeID nodeId = NodeID(nodeid.substr(0, 128));
    nodeIdStr = toHex(nodeId);

    // 连接所有分片节点
    GROUP_ID groupId = std::stoi(pt.get<std::string>("group.global_group_id")); // 全局通信使用的groupid
    PROTOCOL_ID syncId = getGroupProtoclID(groupId, ProtocolID::InterGroup);
    std::shared_ptr<dev::initializer::Initializer> initialize = std::make_shared<dev::initializer::Initializer>();
    initialize->init_with_groupP2PService("./config.ini", p2pService, syncId);  // 启动群组

    // ADD BY ZH
    group_protocolID = syncId;
    group_p2p_service = p2pService;


    // 读取本分片的上层分片id和下层分片id
    dev::consensus::internal_groupId = std::stoi(pt.get<std::string>("group.internal_group_id")); // 片内使通信使用的groupID
    string upper_groupIds = "";
    string lower_groupIds = "";
    loadHieraInfo(&upper_groupIds, &lower_groupIds);


    auto secureInitializer = initialize->secureInitializer();
    auto ledgerManager = initialize->ledgerInitializer()->ledgerManager();
    auto consensusP2Pservice = initialize->p2pInitializer()->p2pService();
    auto rpcService = std::make_shared<dev::rpc::Rpc>(initialize->ledgerInitializer(), consensusP2Pservice);
    auto blockchainManager = ledgerManager->blockChain(dev::consensus::internal_groupId);

    shared_ptr<dev::plugin::SyncThreadMaster> syncs = std::make_shared<dev::plugin::SyncThreadMaster>(p2pService, syncId, nodeId, dev::consensus::internal_groupId, rpcService);
    std::shared_ptr<ConsensusPluginManager> consensusPluginManager = std::make_shared<ConsensusPluginManager>(rpcService);
    syncs->setAttribute(blockchainManager);
    syncs->setAttribute(consensusPluginManager);
    
    consensusPluginManager->setAttribute(blockchainManager, rpcService);
    
    // 启动区块处理线程
    std::thread processBlocksThread(&dev::plugin::deterministExecute::processConsensusBlock, consensusPluginManager->m_deterministExecute);
    processBlocksThread.detach();
    
    // 启动交易处理线程
    std::thread executetxsThread(&dev::plugin::deterministExecute::deterministExecuteTx, consensusPluginManager->m_deterministExecute);
    executetxsThread.detach();

    // 启动包处理线程
    std::thread msgHandlerThread(&dev::plugin::ConsensusPluginManager::receiveRemoteMsgWorker, consensusPluginManager);
    msgHandlerThread.detach();
    

// ==========================================发送交易==========================================
    shared_ptr<InjectThreadMaster> inject = make_shared<InjectThreadMaster>(rpcService, ledgerManager, injectNumber, cross_rate);
    // shared_ptr<InjectThreadMaster> inject = make_shared<InjectThreadMaster>(rpcService, ledgerManager, 10000, 10, 90);
    // shared_ptr<InjectThreadMaster> inject = make_shared<InjectThreadMaster>(rpcService, ledgerManager, 0, 100, 0);

    std::this_thread::sleep_for(std::chrono::milliseconds(10000));
    // nodeIdStr == toHex(dev::consensus::forwardNodeId.at(internal_groupId - 1))
    if(hieraShardTree->is_forward_node(internal_groupId, nodeIdStr)) { // 判断是否为头节点
        if(args.has_option("--generate")) {
          cout << "start genenrate tx..." << endl;
          // 生成片内交易负载---所有的节点都有片内交易负载
          string fileName = "intrashardworkload.json";
          transactionInjectionTest _injectionTest(rpcService, dev::consensus::internal_groupId, ledgerManager);
          _injectionTest.generateIntraShardWorkLoad(internal_groupId, fileName, 100000);
          
          // // 生成局部性跨片交易
          // // 只要判断当前节点是否有局部性跨片交易 
          // if (hieraShardTree->is_inter(internal_groupId)) {
          //   string fileName = "intershardworkload.json";
          //   transactionInjectionTest _injectionTest(rpcService, internal_groupId, ledgerManager);
          //   //todo Benchmark.cpp
          //   _injectionTest.generateInterShardWorkLoad(internal_groupId, lower_groupIds, fileName, 100000);
          // }
          
          // // 生成跨层交易
          // //只要判断当前节点是否有跨层交易
          // if (hieraShardTree->is_cross_layer(internal_groupId)) {
          //   string crosslayerworkload_filename = "shard"+ to_string(internal_groupId) +"_crosslayer_workload_10w.json";
          //   injectTxs _injectionTest(rpcService, internal_groupId, ledgerManager);
          //   //todo Benchmark.cpp
          //   _injectionTest.generateCrossLayerWorkLoad(internal_groupId, crosslayerworkload_filename, 100000, ledgerManager);
          // }
        } else {
          inject->startInjectThreads(inject_threadNumber); // 启动负载导入线程
        }
    }


// ==========================================测试部分==========================================
/*
    // 测试不同跨片交易比例下系统整体吞吐延迟
    // dev::consensus::hiera_shard_number

    // string intrashardworkload = "./intrashardworkload.json";
    // string intershardworkload = "./intershardworkload.json";
    // string crosslayerworkload = "./crosslayerworkload.json";

    // for (int i = 1; i <= dev::consensus::hiera_shard_number; i++) {
    //     // dev::consensus::internal_groupId == i
    //     // (7 == i || i <= 3 )
    //     if(dev::consensus::internal_groupId == i && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(i - 1))) {
    //         PLUGIN_LOG(INFO) << LOG_DESC("准备发送交易...")<< LOG_KV("nodeIdStr", nodeIdStr);
    //         transactionInjectionTest _injectionTest(rpcService, i, ledgerManager);

    //         if (i <= 6) {
    //             // std::this_thread::sleep_for(std::chrono::milliseconds(500));
    //             // 100
    //             // _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 0, 0, 0);
    //             // 80
    //             _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 2000, 0, 0);
    //             // 50
    //             // _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 5000, 0, 0);
    //             // 20
    //             // _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 8000, 0, 0);
    //             // 10
    //             // _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 9000, 0, 0);
    //             // 5
    //             // _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 9500, 0, 0);
                
    //             // _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 50000, 0, 0);
    //         } else if (i <= 8) {
    //             // _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 0, 20000, 0);

    //             _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 2000, 27360, 0);

    //             // _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 5000, 17100, 0);
    //             // _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 8000, 6840, 0);
    //             // _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 9000, 3420, 0);
    //             // _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 9500, 1710, 0);
    //             // _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 50000, 0, 0);
    //         } else {
    //             // _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 0, 10000, 2500);

    //             _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 2000, 13680, 2000);

    //             // _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 5000, 8550, 1250);
    //             // _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 8000, 3420, 500);
    //             // _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 9000, 1710, 250);
    //             // _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 9500, 855, 125);
                
    //             // _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 50000, 0, 0);
    //         }

    //         // _injectionTest.injectionTransactions("./workload0.json", i);
    //         // _injectionTest.injectionTransactions("./workload1.json", i);
    //         // _injectionTest.injectionTransactions("./workload2.json", i);
    //         // _injectionTest.injectionTransactions("./workload3.json", i);
    //         // _injectionTest.injectionTransactions("./workload4.json", i);
    //     }
    // }


    // if(nodeIdStr == toHex(dev::consensus::forwardNodeId.at(internal_groupId - 1))) {
    //     PLUGIN_LOG(INFO) << LOG_DESC("准备发送交易...")<< LOG_KV("nodeIdStr", nodeIdStr);
    //     transactionInjectionTest _injectionTest(rpcService, internal_groupId, ledgerManager);
        
    //     // 测试4分片 系统的延时吞吐变化（20%跨片）
    //     // if (internal_groupId <= 3) {
    //     //     // 20%
    //     //     _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 10000, 0, 0, 1);
    //     // } else if (internal_groupId == 7) {
    //     //     // 20%
    //     //     _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 10000, 10000, 0, 1);
    //     // }

    //     // 测试9分片 系统的延时吞吐变化（20%跨片）
    //     // if (internal_groupId <= 6) {
    //     //     // 20%
    //     //     _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 32000, 0, 0);
    //     //     // 80%
    //     //     // _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 8000, 0, 0);
    //     // } else if (internal_groupId <= 8) {
    //     //     // 20%
    //     //     _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 32000, 27360, 0);
    //     //     // 80%
    //     //     // _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 8000, 100000, 0);
    //     // } else {
    //     //     // 20%
    //     //     _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 32000, 13680, 2000);
    //     //     // 80%
    //     //     // _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 8000, 54720, 8000);
    //     // }

    //     // 测试13分片 系统的延时吞吐变化（20%跨片）
    //     if (internal_groupId <= 9) {
    //         _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 16000, 0, 0, 1);
    //     } else if (internal_groupId <= 12) {
    //         _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 16000, 12350, 0, 1);
    //     } else {
    //         _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 16000, 12350, 2600, 1);
    //     }

    // }

    
    // if(internal_groupId <= 8 && nodeIdStr == toHex(forwardNodeId.at(internal_groupId - 1))) {
    //     PLUGIN_LOG(INFO) << LOG_DESC("准备发送交易...")<< LOG_KV("nodeIdStr", nodeIdStr);
    //     transactionInjectionTest _injectionTest(rpcService, internal_groupId, ledgerManager);
    //     _injectionTest.injectionTransactions(intrashardworkload, intershardworkload, crosslayerworkload, 50000, 0, 0);
    // }
*/

// ==========================================负载部分==========================================
/*
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
    // 当前节点为头节点 各分片生成各自片内交易
    // if(nodeIdStr == toHex(forwardNodeId.at(internal_groupId - 1))) {
    //     createIntrashardDataSet(ledgerManager, 100000, rpcService);
    // }

    if(dev::consensus::internal_groupId == 1 && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(0))) {
        // createLocalityDataSet(ledgerManager, 63000, 0, rpcService);
        // createIntrashardDataSet(ledgerManager, 100000, rpcService);
        createIntershardDataSet(ledgerManager, 100000, rpcService);
        createCrossLayerDataSet(ledgerManager, 100000, rpcService);
        // createCrossshardDataSet(ledgerManager, 100000, rpcService);
    }
*/
    

    if(upper_groupIds != "N/A")
    {
        std::cout << "upper_groupIds = " << upper_groupIds << std::endl;
    }
    else
    {
        std::cout<<"it's a root group" << std::endl;
    }

    if(lower_groupIds != "N/A")
    {
        std::cout << "lower_groupIds = " << lower_groupIds << std::endl;
    }
    else
    {
        std::cout<<"it's a leaf group" << std::endl;
    }

    auto deterministExecute = consensusPluginManager->m_deterministExecute;
    while (true)
    {
        deterministExecute->average_latency();
        std::this_thread::sleep_for(std::chrono::seconds(1));
        // std::this_thread::sleep_for(std::chrono::milliseconds(1000000));
    }
    return 0;
}
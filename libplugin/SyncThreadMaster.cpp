#include <libplugin/SyncThreadMaster.h>
#include <libprotobasic/shard.pb.h>
#include <libsync/SyncMsgPacket.h>
#include <libplugin/Common.h>

using namespace std;
using namespace dev;
using namespace dev::p2p;
using namespace dev::sync;
using namespace dev::plugin;

void SyncThreadMaster::listenWorker()
{
    // listen latest block to send to group nodes
    int blockId = 0; // block that has been discovered
    int disTxID = 0; // 跨片交易 ID
    while(true)
    {
        int currentBlockNum = m_blockchainManager->number(); // 当前块高
        if(currentBlockNum > blockId)
        {
            blockId++;
            int txIndex = 0;
            std::shared_ptr<dev::eth::Block> currentBlock = m_blockchainManager->getBlockByNumber(blockId);

            for(size_t i = 0; i < currentBlock->getTransactionSize(); i++)
            {
                txIndex++;
                auto currentTx = (*currentBlock->transactions())[i];
                auto fromHex = currentTx->sender().hex();
                auto recAddr = currentTx->receiveAddress().hex();
                recAddr = "0x" + recAddr;
                auto rlp = currentTx->rlp();
                auto data = currentTx->data();

                // std::cout << "合约调用地址为:" << recAddr << std::endl;
                // std::cout << "块中的交易为:" << rlp << std::endl;

                bool isTxD = false;
                bool isDisTxRequest = false;
                bool isPreCommitted = false;
                bool isCommitted = false;

                // 将rlp转为string类型
                int len = rlp.size();
                unsigned char txrlpchar[len];
                for(int i = 0; i < len; i++)
                {
                    txrlpchar[i] = rlp.at(i);
                }

                std::string rlpstr;
                rlpstr.resize(len * 2);
                dev::plugin::transform trans;
                trans.hexstring_from_data(txrlpchar, len, &rlpstr[0]);    // unsigned char *data, int len, char *output
                rlpstr = "0x" + rlpstr;

                // 根据合约地址判断是否为跨片交易存证合约
                if(std::find(dev::plugin::disTxDepositAddrs.begin(), dev::plugin::disTxDepositAddrs.end(), recAddr) != dev::plugin::disTxDepositAddrs.end())
                {
                    // std::cout<< "协调者准备发起跨片交易" <<std::endl;
                    isTxD = true;
                }
                else
                {
                    std::cout<< "协调者发起的是片内交易" <<std::endl;
                }

                if(isTxD == true) // 若是跨片交易
                {
                    // 若协调者收齐了当前跨片交易所有 precommit 消息，检查当前交易的rlp是否在 committedDisTxRlp 中
                    if(std::find(dev::plugin::committedDisTxRlp.begin(), dev::plugin::committedDisTxRlp.end(), rlpstr) != dev::plugin::committedDisTxRlp.end())
                    {
                        isCommitted = true; // 当前分片应向所有参与者发送committed消息
                    }
                    else if(std::find(dev::plugin::preCommittedDisTxRlp.begin(), dev::plugin::preCommittedDisTxRlp.end(), rlpstr) != dev::plugin::preCommittedDisTxRlp.end())
                    {
                        isPreCommitted = true; // 当前分片应该向协调者回复precommitted消息
                    }
                    else
                    {
                        isDisTxRequest = true; // 当前分片应向所有参与者发送原始跨片交易请求
                    }

                    if(isPreCommitted)
                    {
                        // std::cout << "当前应向协调者发送 precommitted 消息" << std::endl;
                        //根据 preCommittedDisTxRlp 获取 precommitted 消息中的 合约地址
                        recAddr = dev::plugin::txRlp2ConAddress[rlpstr];
                        // 开始获取所有跨片子交易
                        // 准备将交易发送给相应的参与者
                        protos::SubPreCommitedDisTx subPreCommitedDisTx;
                        subPreCommitedDisTx.set_subtxrlp(rlpstr);
                        subPreCommitedDisTx.set_contractaddress(recAddr);

                        std::string serializedPCDT_str;
                        subPreCommitedDisTx.SerializeToString(&serializedPCDT_str);
                        auto txByte = asBytes(serializedPCDT_str);

                        SyncPreCommittedTxPacket retPacket;
                        retPacket.encode(txByte);
                        auto msg = retPacket.toMessage(m_protocolId);
                        m_service->asyncSendMessageByNodeID(m_pluginManager->getNodeId(0), msg, CallbackFuncWithSession(), dev::network::Options());
                        std::cout << "向协调者发送preCommitt消息..." << std::endl;
                    }

                    if(isDisTxRequest)
                    {
                        // std::cout << "当前交易是原始跨片请求" << std::endl;
                        // 开始获取所有跨片子交易
                        disTxID++;

                        std::vector<std::string> subTxRlps = dev::plugin::conAddress2txrlps[recAddr];
                        for(int i = 0; i < subTxRlps.size(); i++ )
                        {
                            std::string subTx = subTxRlps.at(i);
                            std::string destGroupId = subTx.substr(0, 1);
                            std::string subTxRlp = subTx.substr(1, subTx.length());
                            std::string readset = dev::plugin::txRWSet[subTxRlp];

                            // std::cout << "交易rlp = " << subTxRlp << std::endl;
                            // std::cout << "交易readset = " << readset << std::endl;

                            // 协调者将子交易在本地存档，用于收票
                            if(dev::plugin::processingTxD.count(disTxID) == 0)
                            {
                                std::vector<std::string> txrlps;
                                txrlps.push_back(subTxRlp);
                                dev::plugin::processingTxD.insert(std::make_pair(disTxID, txrlps));
                            }
                            else
                            {
                                std::vector<std::string> txrlps = dev::plugin::processingTxD[disTxID];
                                txrlps.push_back(subTxRlp);
                                dev::plugin::processingTxD.insert(std::make_pair(disTxID, txrlps));
                            }

                            dev::plugin::subTxRlp2ID.insert(std::make_pair(subTxRlp, disTxID)); // subTxRlp --> disTxID

                            // 准备将交易发送给相应的参与者
                            protos::RLPWithReadSet txWithReadset;
                            txWithReadset.set_subtxrlp(subTxRlp);
                            txWithReadset.set_readset(readset);
                            txWithReadset.set_contractaddress(recAddr);

                            std::string serializedTWRS_str;
                            txWithReadset.SerializeToString(&serializedTWRS_str);
                            auto txByte = asBytes(serializedTWRS_str);

                            SyncDistributedTxPacket retPacket;
                            retPacket.encode(txByte);
                            auto msg = retPacket.toMessage(m_protocolId);
                            m_service->asyncSendMessageByNodeID(m_pluginManager->getNodeId(atoi(destGroupId.c_str())), msg, CallbackFuncWithSession(), dev::network::Options());
                            std::cout << "协调者向所有参与者发送了跨片子交易..." << std::endl;
                        }
                    }

                    if(isCommitted)
                    {
                        std::cout << "当前交易应该committed" << std::endl;
                        // 从 committedDisTxRlp 中将 rlpstr 删除掉
                        auto iter = dev::plugin::committedDisTxRlp.begin();
                        size_t index = 0;
                        while (iter!= dev::plugin::committedDisTxRlp.end())
                        {
                            if(*iter == rlpstr) { break;}
                            else {iter++; index++;}
                        }
                        dev::plugin::committedDisTxRlp.erase(dev::plugin::committedDisTxRlp.begin() + index);

                        // 开始获取所有跨片子交易
                        std::vector<std::string> subTxRlps = dev::plugin::conAddress2txrlps[recAddr];
                        for(int i = 0; i < subTxRlps.size(); i++)
                        {
                            std::string subTx = subTxRlps.at(i);
                            std::string destGroupId = subTx.substr(0, 1);
                            std::string subTxRlp = subTx.substr(1, subTx.length());
                            std::string readset = dev::plugin::txRWSet[subTxRlp];

                            // std::cout << "交易rlp = " << subTxRlp << std::endl;
                            // std::cout << "交易readset = " << readset << std::endl;

                            // dev::plugin::subTxRlp2ID.insert(std::make_pair(subTxRlp, disTxID)); // subTxRlp --> disTxID

                            std::cout << "协调者再次共识成功, 准备发送Commit消息给所有参与者..." << std::endl;
                            protos::CommittedRLPWithReadSet committedRLPWithReadSet;
                            committedRLPWithReadSet.set_subtxrlp(subTxRlp);
                            committedRLPWithReadSet.set_readset(readset);
                            committedRLPWithReadSet.set_contractaddress(recAddr);

                            std::string serializedCRWRS_str;
                            committedRLPWithReadSet.SerializeToString(&serializedCRWRS_str);
                            auto txByte = asBytes(serializedCRWRS_str);

                            SyncCommittedTxPacket retPacket;
                            retPacket.encode(txByte);
                            auto msg = retPacket.toMessage(m_protocolId);
                            m_service->asyncSendMessageByNodeID(m_pluginManager->getNodeId(atoi(destGroupId.c_str())), msg, CallbackFuncWithSession(), dev::network::Options());
                            std::cout << "协调者Committed消息发送完毕..." << std::endl;
                        }
                    }
                }
            }
        }
    }
}


void SyncThreadMaster::receiveWorker()
{
    // receive and storage
    protos::Transaction msg_rs;
    // protos::RLPWithReadSet msg_txWithReadset;
    protos::SubCrossShardTx msg_txWithReadset;
    protos::SubPreCommitedDisTx msg_subPreCommitedDisTx;
    protos::CommittedRLPWithReadSet msg_committedRLPWithReadSet;


    while(true) {
        bool got_message = m_pluginManager->txs->try_pop(msg_rs);
        if(got_message == true)
        {
            auto txid = msg_rs.txid();
            auto from = msg_rs.from();
            auto to = msg_rs.to();
            auto value = msg_rs.value();
            auto data = msg_rs.data();

            //std::cout<< " Node Receive Data " << " txid = " << txid << " from = " << from << " to = " << to << " value = " << value << " data = "<< data <<std::endl;
            std::cout<< "收到的交易rlp码是:" << data <<std::endl;
            std::string rlpstr("f88fa003299b65ebb0418949841c37b07ffc654dfe2e1eff41f3b21e74ad3eaa16295785051f4d5c0083419ce08201f7940b50a31cd9e5c1c71ea11fcd2a1a909572b9d06780844f2be91f0102801ca095e31d9415b6557f5c2f34586eafbd9e8f86fb693c75ebcadb08111dc19252f5a018b6568460f1782a22aad26fe8a54be6bcb9d318e2ae3016f56fd11e3de9077c");    
            unsigned char rlpchar[rlpstr.length()/2];

            dev::plugin::transform trans;
            int length = trans.strhex_parse_hex(rlpstr, rlpchar);

            std::vector<unsigned char> txRlpbytes;
            for(int i = 0; i < length; i++)
            {
                txRlpbytes.push_back(rlpchar[i]);
            }
        }


        // 参与者轮循从队列中获取协调者发来的跨片子交易
        got_message = m_pluginManager->distxs->try_pop(msg_txWithReadset);
        if(got_message == true)
        { 
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

            // 对跨片交易进行缓存，缓存在KV Store中，其中Key为txrlp, value为proto::SubCrossShardTx格式的交易数据
            // cacheCrossShardTx(rlp, msg_txWithReadset);

            // 将交易投递给共识模块进行继续共识
            // std::cout << "接收到的 rlp = " << rlp << std::endl;
            // std::cout << "readset = " << readset << std::endl;
            // std::cout << "contractAddress = " << contractAddress << std::endl;
            // dev::plugin::preCommittedDisTxRlp.push_back(rlp);
            // dev::plugin::txRlp2ConAddress.insert(std::make_pair(rlp, contractAddress));
            // std::cout<< "参与者开始对跨片交易请求进行共识..." << std::endl;
            // 通过RPC接口将交易发送至本地服务器，并开始共识

            // m_rpc_service->sendRawTransaction(destinshardid, signeddata); // 通过调用本地的RPC接口发起新的共识
            m_rpc_service->sendSubCsRawTransaction(destinshardid, signeddata, 1); // 通过调用本地的RPC接口发起新的共识
        }

        got_message = m_pluginManager->precommit_txs->try_pop(msg_subPreCommitedDisTx);
        if(got_message == true)
        { 
            auto rlp = msg_subPreCommitedDisTx.subtxrlp();
            auto contractAddress = msg_subPreCommitedDisTx.contractaddress();

            // std::cout << "接收到的 precommit 交易的 rlp = " << rlp << std::endl;
            // std::cout << "接收到的 precommit 交易的 conAddr = " << contractAddress << std::endl;
            
            // 检查是否已经收齐所有参与者返回的投票
            // 1. 检查当前 rlp 所属的 disTxID
            int disTxID = dev::plugin::subTxRlp2ID[rlp];
            // std::cout << "disTxID = " << disTxID << std::endl;
            // 2. 从 dev::plugin::processingTxD 中删除 rlp
            std::vector<std::string> txrlps = dev::plugin::processingTxD[disTxID];
            // std::cout << "txrlps = " << txrlps << std::endl;

            auto iter = txrlps.begin();
            size_t index = 0;
            while (iter!= txrlps.end())
            {
                if(*iter == rlp) { break;}
                else {iter++; index++;}
            }

            txrlps.erase(txrlps.begin() + index);

            // 3. 检查processingTxD[disTxID] 中是否为空，若为空表示已经收齐所有投票
            if(txrlps.size() == 0)
            {
                dev::plugin::processingTxD.erase(disTxID);
                std::cout << "协调者节点收齐了所有preCommit消息, 准备再次共识..." << std::endl;

                // 从dev::plugin::resendTxs 中获取重新发送的交易 rlp
                std::vector<std::string> resendTx = dev::plugin::resendTxs[contractAddress];
                std::string txrlpstr = resendTx.at(0);

                if(resendTx.size() == 1)
                {
                    dev::plugin::resendTxs.erase(contractAddress);
                }
                else
                {
                    // 从 resendTx 中删除 txrlpstr
                    auto iter = resendTx.begin();
                    size_t index = 0;
                    while (iter!= resendTx.end())
                    {
                        if(*iter == txrlpstr) { break;}
                        else {iter++; index++;}
                    }
                    resendTx.erase(txrlps.begin() + index);
                    dev::plugin::resendTxs.insert(std::make_pair(contractAddress, txrlps));
                }
                // // 协调者再次调用跨片交易
                dev::plugin::coordinatorRlp.push_back(txrlpstr); // 协调者记录自己发送的跨片交易请求
                std::string response = m_rpc_service->sendRawTransaction(1, txrlpstr); // 通过一个节点将交易发送给协调者中的所有节点，共识、出块
                dev::plugin::committedDisTxRlp.push_back(txrlpstr);
            }
            else
            {
                dev::plugin::processingTxD.insert(std::make_pair(disTxID, txrlps));
            }
        }

        got_message = m_pluginManager->commit_txs->try_pop(msg_committedRLPWithReadSet);
        if(got_message == true)
        {
            auto rlp = msg_committedRLPWithReadSet.subtxrlp();
            auto readset = msg_committedRLPWithReadSet.readset();
            auto contractAddress = msg_committedRLPWithReadSet.contractaddress();

            std::cout<< "参与者收到了commit命令, 开始执行被阻塞交易..." << std::endl;
            // std::cout << "rlp = " << rlp << std::endl;
            // std::cout << "readset = " << readset << std::endl;
            // std::cout << "contractAddress = " << contractAddress << std::endl;

            std::vector<std::string> committed_txrlp;
            committed_txrlp.push_back(rlp);
            committed_txrlp.push_back(readset);
            committed_txrlp.push_back(contractAddress);

            // //参与者记录所有收到的 committed 交易， 交易rlp、交易地址、交易读集
            dev::plugin::receCommTxRlps.push(committed_txrlp); // 用队列去接收管道消息
        }

    }
}

bool SyncThreadMaster::startThread()
{
    typedef void* (*FUNC)(void*);
    FUNC receiveWorkerCallback = (FUNC)&SyncThreadMaster::receiveWorker;

    int ret = pthread_create(&receivethread, NULL, receiveWorkerCallback, this);

    if (ret != 0)
    {
        std::cout << "本地轮询P2P消息包线程启动失败..." << std::endl;
        return false;
    }
    else
    {
        std::cout << "本地轮询P2P消息包线程启动成功..." << std::endl;
    }

    // FUNC listenWorkercallback = (FUNC)&SyncThreadMaster::listenWorker;
    // ret = pthread_create(&listenthread, NULL, listenWorkercallback, this);

    // if (ret != 0)
    // {
    //     std::cout << "本地轮询最新区块线程启动失败..." << std::endl;
    //     return false;
    // }
    // else
    // {
    //     std::cout << "本地轮询最新区块线程启动成功..." << std::endl;
    //     return true;
    // }
}

void SyncThreadMaster::dowork(dev::sync::SyncPacketType const& packettype, byte const& data)
{
    
}

void SyncThreadMaster::dowork(dev::sync::SyncPacketType const& packettype, byte const& data, dev::network::NodeID const& destnodeId)
{

}

void SyncThreadMaster::sendMessage(bytes const& _blockRLP, dev::sync::SyncPacketType const& packetreadytype)
{

}

void SyncThreadMaster::sendMessage(bytes const& _blockRLP, dev::sync::SyncPacketType const& packettype, dev::network::NodeID const& destnodeId)
{

}

void SyncThreadMaster::start(byte const &pt, byte const& data)
{
    SyncPacketType packettype;
    switch (pt)
    {
        case 0x06:
            packettype = ParamRequestPacket;
            break;
        case 0x07:
            packettype = ParamResponsePacket;
            break;
        case 0x08:
            packettype = CheckpointPacket;
            break;
        case 0x09:
            packettype = ReadSetPacket;
            break;
        case 0x10:
            packettype = BlockForExecutePacket;
            break;
        case 0x11:
            packettype = BlockForStoragePacket;
            break;
        case 0x12:
            packettype = CommitStatePacket;
            break;
        default:
            std::cout << "unknown byte type!" << std::endl;
            break;
    }
    dowork(packettype, data);
}

void SyncThreadMaster::start(byte const &pt, byte const& data, dev::network::NodeID const& destnodeId)
{
    SyncPacketType packettype;
    switch (pt)
    {
        case 0x06:
            packettype = ParamRequestPacket;
            break;
        case 0x07:
            packettype = ParamResponsePacket;
            break;
        case 0x08:
            packettype = CheckpointPacket;
            break;
        case 0x09:
            packettype = ReadSetPacket;
            break;
        case 0x10:
            packettype = BlockForExecutePacket;
            break;
        case 0x11:
            packettype = BlockForStoragePacket;
            break;
        case 0x12:
            packettype = CommitStatePacket;
            break;
        default:
            std::cout << "unknown byte type!" << std::endl;
            break;
    }
    dowork(packettype, data, destnodeId);
}


void SyncThreadMaster::setAttribute(std::shared_ptr<dev::blockchain::BlockChainInterface> _blockchainManager)
{
    m_blockchainManager = _blockchainManager;
    m_msgEngine->setAttribute(_blockchainManager);
}


void SyncThreadMaster::setAttribute(std::shared_ptr<ConsensusPluginManager> _pluginManager)
{
    m_pluginManager = _pluginManager;
    m_msgEngine->setAttribute(_pluginManager);
}

void SyncThreadMaster::cacheCrossShardTx(std::string _rlpstr, protos::SubCrossShardTx _subcrossshardtx)
{
    std::lock_guard<std::mutex> lock(x_map_Mutex);
    m_cacheCrossShardTxMap.insert(std::make_pair( _rlpstr, _subcrossshardtx ));
}
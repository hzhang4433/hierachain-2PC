#include "ex_SyncMsgEngine.h"
#include <libsync/SyncMsgPacket.h>
#include <libprotobasic/shard.pb.h>

using namespace std;
using namespace dev;
using namespace dev::p2p;
using namespace dev::sync;
using namespace dev::plugin;

void ex_SyncMsgEngine::stop()
{
    if (m_service)
    {
        m_service->removeHandlerByProtocolID(m_protocolId);
    }
}

void ex_SyncMsgEngine::messageHandler(dev::p2p::NetworkException _e, std::shared_ptr <dev::p2p::P2PSession> _session, dev::p2p::P2PMessage::Ptr _msg)
{
    try
    {
        std::cout<< "tanghaibo 收到" << std::endl;

        SyncMsgPacket::Ptr packet = std::make_shared<SyncMsgPacket>();
        if(!packet->decode(_session, _msg))
        {
            SYNC_ENGINE_LOG(WARNING)
                    << LOG_BADGE("Rev") << LOG_BADGE("Packet") << LOG_DESC("Reject packet")
                    << LOG_KV("reason", "decode failed")
                    << LOG_KV("nodeId", _session->nodeID().abridged())
                    << LOG_KV("size", _msg->buffer()->size())
                    << LOG_KV("message", toHex(*_msg->buffer()));
            return;
        }

        if(packet->packetType == ParamRequestPacket)
        {
            RLP const& rlps = (*packet).rlp();
            bool size = rlps.isNull();
            std::cout << "ParamRequestPacket" << std::endl;
            if(size)
            {
                std::cout << "data is null!" << std::endl;
            }
            else
            {
                // 此处是处理 ParamRequestPacket 逻辑
            }
        }

        else if(packet->packetType == ParamResponsePacket)
        {
            RLP const& rlps = (*packet).rlp();
            bool size = rlps.isNull();
            std::cout << "ParamResponsePacket" << std::endl;
            if(size)
            {
                std::cout << "data is null" <<std::endl;
            }
            else
            {

            }
        }
        else if (packet->packetType == CheckpointPacket)
        {
            RLP const& rlps = (*packet).rlp();
            bool size = rlps.isNull();
            std::cout << "CheckpointPacket" << std::endl;
            if (size)
            {
                std::cout << "data is null!" << std::endl;
            }
            else
            {
            }
        }
        else if (packet->packetType == BlockForExecutePacket)
        {
            // std::cout << "receive readset message" << std::endl;
            RLP const& rlps = (*packet).rlp();
            bool size = rlps.isNull();
            std::cout << "BlockForExecutePacket" << std::endl;
            if (size)
            {
                std::cout << "data is null!" << std::endl;
            }
            else
            {
                // 准备执行交易
                try
                {
                    // std::string str = rlps[0].toString();
                    // protos::Transaction msg_rs;
                    // msg_rs.ParseFromString(str);
                    // m_pluginManager->processReceivedTx(msg_rs); // 存交易

                    // auto txRlp = msg_rs.data(); // 交易的rlp编码部分
                    // // 从 rlp 编码生成交易
                    // //从rlp编码中将交易解析出来 Transaction(bytesConstRef _rlp, CheckTransaction _checkSig)
                    // enum dev::eth::CheckTransaction checkTransaction;
                    // checkTransaction = 0;
                    // std::shared_ptr<dev::eth::Transaction> tx = std::make_shared<dev::eth::Transaction>( txRlp, checkTransaction );
                    // auto from = tx.from();
                    // auto to = tx.to();
                    // std::cout << "from =  " << from << "to = " << to << std::endl;

                }
                catch(const std::exception& e)
                {
                    std::cerr << e.what() << '\n';
                }
            }
        }
        else if(packet->packetType == BlockForStoragePacket)
        {
            RLP const& rlps = (*packet).rlp();
            bool size = rlps.isNull();
            std::cout << "BlockForStoragePacket" << std::endl;
            if(size)
            {
                std::cout << "data is null" << std::endl;
            }
            else
            {
                try
                {
                    std::string str = rlps[0].toString();
                    protos::Transaction msg_rs;
                    msg_rs.ParseFromString(str);
                    m_pluginManager->processReceivedTx(msg_rs); // 存交易

                    //auto txRlp = msg_rs.data(); // 交易的rlp编码部分

                    // std::string rlpstr("f88fa003299b65ebb0418949841c37b07ffc654dfe2e1eff41f3b21e74ad3eaa16295785051f4d5c0083419ce08201f7940b50a31cd9e5c1c71ea11fcd2a1a909572b9d06780844f2be91f0102801ca095e31d9415b6557f5c2f34586eafbd9e8f86fb693c75ebcadb08111dc19252f5a018b6568460f1782a22aad26fe8a54be6bcb9d318e2ae3016f56fd11e3de9077c");    
                    // unsigned char rlpchar[rlpstr.length()/2];

                    // dev::plugin::transform trans;
                    // int length = trans.strhex_parse_hex(rlpstr, rlpchar);

                    // std::vector<unsigned char> txRlpbytes;
                    // for(int i = 0; i < length; i++)
                    // {
                    //     txRlpbytes.push_back(rlpchar[i]);
                    // }

                    // // 根据rlp码生成交易(片内交易)
                    // dev::eth::CheckTransaction checkTransaction = dev::eth::CheckTransaction::Everything;
                    // shared_ptr<dev::eth::Transaction> tx = std::make_shared<dev::eth::Transaction>( txRlpbytes, checkTransaction);

                }
                catch(const std::exception& e)
                {
                    std::cerr << e.what() << '\n';
                }
            }
        }

        else if(packet->packetType == DistributedTxPacket) // 若是分布式事务
        {
            RLP const& rlps = (*packet).rlp();
            bool size = rlps.isNull();
            // std::cout << "DistributedTxPacket" << std::endl;
            if(size)
            {
                std::cout << "data is null" << std::endl;
            }
            else
            {
                try
                {
                    // std::cout << "节点接收到 DistributedTxPacket 消息" << std::endl;
                    PLUGIN_LOG(INFO) << LOG_DESC("开始对收到的 DistributedTxPacket 消息进行处理");
                    std::string str = rlps[0].toString();
                    // protos::RLPWithReadSet msg_rs;
                    // msg_rs.ParseFromString(str);

                    protos::SubCrossShardTx msg_rs;
                    msg_rs.ParseFromString(str);

                    m_pluginManager->processReceivedDisTx(msg_rs); // 存交易
                }
                catch(const std::exception& e)
                {
                    std::cerr << e.what() << '\n';
                }
            }
        }



        else if(packet->packetType == PreCommittedTxPacket) // 若是 分布式事务precommit消息
        {
            RLP const& rlps = (*packet).rlp();
            bool size = rlps.isNull();
            // std::cout << "PreCommittedTxPacket" << std::endl;
            if(size)
            {
                std::cout << "data is null" << std::endl;
            }
            else
            {
                try
                {
                    std::string str = rlps[0].toString();
                    protos::SubPreCommitedDisTx msg_rs;
                    msg_rs.ParseFromString(str);
                    m_pluginManager->processReceivedPreCommitedTx(msg_rs); // 存交易
                }
                catch(const std::exception& e)
                {
                    std::cerr << e.what() << '\n';
                }
            }
        }

        else if(packet->packetType == CommittedTxPacket) // 若是 分布式事务precommit消息
        {
            RLP const& rlps = (*packet).rlp();
            bool size = rlps.isNull();
            // std::cout << "CommittedTxPacket" << std::endl;
            if(size)
            {
                std::cout << "data is null" << std::endl;
            }
            else
            {
                try
                {
                    std::string str = rlps[0].toString();
                    protos::CommittedRLPWithReadSet msg_rs;
                    msg_rs.ParseFromString(str);
                    m_pluginManager->processReceivedCommitedTx(msg_rs); // 存交易
                }
                catch(const std::exception& e)
                {
                    std::cerr << e.what() << '\n';
                }
            }
        }
        
        else if(packet->packetType == CommitStatePacket)
        {
            RLP const& rlps = (*packet).rlp();
            bool size = rlps.isNull();
            if(size)
            {
                std::cout << "data is null" << std::endl;
            }
            else
            {

            }
        }
    }
    catch(std::exception const& e)
    {
        SYNC_ENGINE_LOG(WARNING) << LOG_BADGE("messageHandler exceptioned")
                                 << LOG_KV("errorInfo", boost::diagnostic_information(e));
    }
}

void ex_SyncMsgEngine::setAttribute(std::shared_ptr<dev::blockchain::BlockChainInterface> _blockchainManager)
{
    m_blockchainManager = _blockchainManager;
}

void ex_SyncMsgEngine::setAttribute(std::shared_ptr<ConsensusPluginManager> _pluginManager)
{
    m_pluginManager = _pluginManager;
}
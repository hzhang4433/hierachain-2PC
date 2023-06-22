#include "ex_SyncMsgEngine.h"
#include <libsync/SyncMsgPacket.h>
#include <libprotobasic/shard.pb.h>
#include <libconsensus/pbft/Common.h>

using namespace std;
using namespace dev;
using namespace dev::p2p;
using namespace dev::sync;
using namespace dev::plugin;
using namespace dev::consensus;

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

        if(packet->packetType == CrossTxPacket){ // 若是跨片交易包
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
                    PLUGIN_LOG(INFO) << LOG_DESC("开始对收到的 '跨片交易'消息包 进行处理");
                    std::string str = rlps[0].toString();
                    
                    protos::SubCrossShardTx msg_rs;
                    msg_rs.ParseFromString(str);

                    m_pluginManager->processReceivedCrossTx(msg_rs); // 存交易
                }
                catch(const std::exception& e)
                {
                    std::cerr << e.what() << '\n';
                }
            }
        }
        else if(packet->packetType == CrossTxReplyPacket) { // 若是跨片交易回执包
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
                    PLUGIN_LOG(INFO) << LOG_DESC("开始对收到的 '跨片交易回执'消息包 进行处理");

                     // 若非主节点，则将消息转发至主节点
                    if (nodeIdStr != toHex(forwardNodeId.at(internal_groupId - 1))) {
                      PLUGIN_LOG(INFO) << LOG_DESC("当前节点非主节点, 转发'跨片交易回执'消息包至主节点");
                      group_p2p_service->asyncSendMessageByNodeID(forwardNodeId.at(internal_groupId - 1), _msg, CallbackFuncWithSession(), dev::network::Options());
                      return;
                    }

                    std::string str = rlps[0].toString();
                    
                    protos::SubCrossShardTxReply msg_rs;
                    msg_rs.ParseFromString(str);

                    m_pluginManager->processReceivedCrossTxReply(msg_rs); // 收集并检查包的数量
                }
                catch(const std::exception& e)
                {
                    std::cerr << e.what() << '\n';
                }
            }
        }
        else if(packet->packetType == CrossTxCommitPacket) { // 若是跨片交易提交包
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
                    PLUGIN_LOG(INFO) << LOG_DESC("开始对收到的 '跨片交易提交'消息包 进行处理");
                    std::string str = rlps[0].toString();
                    
                    protos::SubCrossShardTxCommit msg_rs;
                    msg_rs.ParseFromString(str);

                    m_pluginManager->processReceivedCrossTxCommit(msg_rs); // 收集并检查包的数量
                }
                catch(const std::exception& e)
                {
                    std::cerr << e.what() << '\n';
                }
            }
        }
        else if(packet->packetType == CrossTxCommitReplyPacket) { // 若是跨片交易执行完成消息包
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
                    PLUGIN_LOG(INFO) << LOG_DESC("开始对收到的 '跨片交易提交回执'消息包 进行处理");
                    std::string str = rlps[0].toString();
                    
                    protos::SubCrossShardTxCommitReply msg_rs;
                    msg_rs.ParseFromString(str);

                    m_pluginManager->processReceivedCrossTxCommitReply(msg_rs); // 收集并检查包的数量
                }
                catch(const std::exception& e)
                {
                    std::cerr << e.what() << '\n';
                }
            }
        }
        else if(packet->packetType == AbortPacket) { // 若是abort消息包
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
                    PLUGIN_LOG(INFO) << LOG_DESC("开始对收到的 'Abort'消息包 进行处理");
                    std::string str = rlps[0].toString();
                    
                    protos::AbortMsg msg_rs;
                    msg_rs.ParseFromString(str);

                    m_pluginManager->processReceivedAbortMessage(msg_rs); // 收集并检查包的数量
                }
                catch(const std::exception& e)
                {
                    std::cerr << e.what() << '\n';
                }
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
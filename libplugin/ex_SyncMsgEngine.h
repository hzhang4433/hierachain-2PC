#pragma once

#include <libblockchain/BlockChainInterface.h>
#include <libdevcore/CommonData.h>
#include <libethcore/Protocol.h>
#include <libp2p/Common.h>
#include <libp2p/P2PInterface.h>
#include <libsync/Common.h>
#include <libplugin/ConsensusPluginManager.h>
#include <libplugin/transform.h>
#include <libplugin/Common.h>
#include <memory>
#include <thread>
#include <map>

namespace dev{
    namespace plugin {

        class ex_SyncMsgEngine : public std::enable_shared_from_this<ex_SyncMsgEngine>{
            /* 
            Input:
                _service: p2p网络传输接口
                _protocolId: 协议Id，同一个grouo内的节点通信协议 Id 相同
                _sharedId: 同一个company内的节点通信协议 SharedId 相同
                _nodeId: 本节点的 Id
            */

            public:
                ex_SyncMsgEngine(std::shared_ptr <dev::p2p::P2PInterface> _service, PROTOCOL_ID const &_protocolId, dev::network::NodeID const &_nodeId)
                :m_service(_service), m_protocolId(_protocolId), m_groupId(dev::eth::getGroupAndProtocol(_protocolId).first), m_nodeId(_nodeId) {
                m_service->registerHandlerByProtoclID(m_protocolId, boost::bind(&ex_SyncMsgEngine::messageHandler, this, _1, _2, _3));
                }

                void stop();

                ~ex_SyncMsgEngine(){ stop();};

                void messageHandler(dev::p2p::NetworkException _e, std::shared_ptr <dev::p2p::P2PSession> _sesion, dev::p2p::P2PMessage::Ptr _msg);

                void setAttribute(std::shared_ptr <dev::blockchain::BlockChainInterface> m_blockchainManager);

                void setAttribute(std::shared_ptr <ConsensusPluginManager> _pluginManager);

            protected:
                std::shared_ptr <dev::p2p::P2PInterface> m_service;
                std::map <uint64_t, uint64_t> checkpoint_count; // id count
                PROTOCOL_ID m_protocolId;
                GROUP_ID m_groupId;
                dev::network::NodeID m_nodeId;
                std::shared_ptr <dev::blockchain::BlockChainInterface> m_blockchainManager;
                std::shared_ptr <ConsensusPluginManager> m_pluginManager;
        };
    }
}
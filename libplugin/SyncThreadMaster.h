#pragma once

#include <libplugin/ex_SyncMsgEngine.h>
#include <libplugin/transform.h>
#include <librpc/Rpc.h>

using namespace dev::rpc;
using namespace dev::plugin;

namespace dev{
    namespace plugin {
        /*
        * m_service: 网络传输接口
        * m_protocolId: 同一个group内，m_protocolId相同
        * m_shaId:同一个company内传输消息的m_shaId相同
        * m_nodeId:本节点的nodeId
        * m_sharedId:本节点所属的company
        * m_group:本节点所属的group
        * m_rpc_service: 本节点所属的group的rpc服务
        */
       class SyncThreadMaster{
           public:
                SyncThreadMaster(std::shared_ptr<dev::p2p::P2PInterface> _service, PROTOCOL_ID const & _protocolId, dev::network::NodeID const& _nodeId, dev::GROUP_ID const& _interal_groupId, std::shared_ptr<dev::rpc::Rpc> _rpc_service)
                :m_service(_service), m_protocolId(_protocolId), m_nodeId(_nodeId)
                {
                    m_groupId = dev::eth::getGroupAndProtocol(_protocolId).first;
                    interal_groupId = _interal_groupId;
                    std::string threadName = "Sync-" + std::to_string(m_groupId);
                    counter = 0;
                    m_msgEngine = std::make_shared<ex_SyncMsgEngine>(_service, _protocolId, _nodeId);
                    m_rpc_service = _rpc_service;
                }

                void dowork(dev::sync::SyncPacketType const& packettype, byte const& data);

                void dowork(dev::sync::SyncPacketType const& packettype, byte const& data, dev::network::NodeID const& destnodeId);

                void listenWorker();

                void receiveWorker();

                // ADD BY THB, 启动队列监听线程
                bool startThread();

                void sendMessage(bytes const& _blockRLP, dev::sync::SyncPacketType const& packetreadytype);

                void sendMessage(bytes const& _blockRLP, dev::sync::SyncPacketType const& packettype, dev::network::NodeID const& destnodeId);

                void start(byte const &pt, byte const& data);

                void start(byte const &pt, byte const& data, dev::network::NodeID const& destnodeId);

                void setAttribute(std::shared_ptr <dev::blockchain::BlockChainInterface> m_blockchainManager);

                void setAttribute(std::shared_ptr <ConsensusPluginManager> _plugin);

                std::map <u256, std::string> contractMap;
                // std::map<std::string, std::string> read_write_sets;

                void cacheCrossShardTx(std::string _rlpstr, protos::SubCrossShardTx _subcrossshardtx);

            private:
                std::string m_name;
                std::unique_ptr <std::thread> m_work;
                std::shared_ptr <dev::p2p::P2PInterface> m_service;
                std::shared_ptr <ex_SyncMsgEngine> m_msgEngine;
                dev::PROTOCOL_ID m_protocolId;
                dev::GROUP_ID m_groupId;
                dev::GROUP_ID interal_groupId;
                dev::h512 m_nodeId;
                int counter = 0;
                std::mutex x_map_Mutex;

                /// listen block thread
                pthread_t listenthread;
                /// process receive workers
                pthread_t receivethread;
                /// get block info
                std::shared_ptr <dev::blockchain::BlockChainInterface> m_blockchainManager;
                std::shared_ptr <ConsensusPluginManager> m_pluginManager;
                std::shared_ptr<dev::rpc::Rpc> m_rpc_service;

                // ADD BY THB cacheCrossShardTxMap
                std::map<std::string, protos::SubCrossShardTx> m_cacheCrossShardTxMap;
       };
    }
}
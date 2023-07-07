#pragma once
#include <memory>
#include <atomic>
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_queue.h>
#include <libdevcore/CommonData.h>
#include <libdevcore/Address.h>
#include <libprotobasic/shard.pb.h>
#include <libplugin/Common.h>
#include <librpc/Rpc.h>
#include <libplugin/deterministExecute.h>

namespace dev {
    namespace plugin {
        class ConsensusPluginManager:public std::enable_shared_from_this<ConsensusPluginManager>{
            public:
                ConsensusPluginManager(std::shared_ptr<dev::rpc::Rpc> _service){
                    crossTxs = new tbb::concurrent_queue<protos::SubCrossShardTx>();
                    crossTxReplys = new tbb::concurrent_queue<protos::SubCrossShardTxReply>();
                    commitTxs =  new tbb::concurrent_queue<protos::SubCrossShardTxCommit>();
                    commitTxReplys = new tbb::concurrent_queue<protos::SubCrossShardTxCommitReply>();
                    abortTxs = new tbb::concurrent_queue<protos::AbortMsg>();


                    m_rpc_service = _service;
                    m_deterministExecute = std::make_shared<dev::plugin::deterministExecute>();
                }

                void pushReceivedCrossTx(protos::SubCrossShardTx _txrlp);
                void pushReceivedCrossTxReply(protos::SubCrossShardTxReply _txrlp);
                void pushReceivedCrossTxCommit(protos::SubCrossShardTxCommit _txrlp);
                void pushReceivedCrossTxCommitReply(protos::SubCrossShardTxCommitReply _txrlp);
                void pushReceivedAbortMessage(protos::AbortMsg _txrlp);

                void receiveRemoteMsgWorker();

                void processReceivedCrossTx(protos::SubCrossShardTx _txrlp);
                void processReceivedCrossTxReply(protos::SubCrossShardTxReply _txrlp);
                void processReceivedCrossTxCommit(protos::SubCrossShardTxCommit _txrlp);
                void processReceivedCrossTxCommitReply(protos::SubCrossShardTxCommitReply _txrlp);
                void processReceivedAbortMessage(protos::AbortMsg _txrlp);
                void sendCommitPacket(protos::SubCrossShardTxReply _txrlp);
                void sendCommitPacketToShard(protos::SubCrossShardTxReply _txrlp, unsigned long shardID);
                void replyToCoordinatorCommitOK(protos::SubCrossShardTxCommit _txrlp);
                int getRand(int a, int b);
                
            

                // int numOfNotFinishedDAGs();
                // int addNotFinishedDAGs(int _num);

                // u256 getLatestState(std::string _addr);
                
                // void updateNotLatest(std::string const _state);
                // void removeNotLatest(std::string const _state);
                // bool isLatest(std::string const _state);
                h512 getNodeId(int _index);
                void setAttribute(std::shared_ptr<dev::blockchain::BlockChainInterface> _blockchainManager, std::shared_ptr<dev::rpc::Rpc> _service);

                // receive crossTxs from coordinator
                tbb::concurrent_queue<protos::SubCrossShardTx> *crossTxs;

                // receive crossTxReplys from participant
                tbb::concurrent_queue<protos::SubCrossShardTxReply> *crossTxReplys;

                // receive commitTxs from coordinator
                tbb::concurrent_queue<protos::SubCrossShardTxCommit> *commitTxs;

                // receive commitTxReplys from participant
                tbb::concurrent_queue<protos::SubCrossShardTxCommitReply> *commitTxReplys;

                // receive abortTxs
                tbb::concurrent_queue<protos::AbortMsg> *abortTxs;



                std::shared_ptr<dev::rpc::Rpc> m_rpc_service;
                std::shared_ptr<dev::plugin::deterministExecute> m_deterministExecute;

            private:
                /// global set to record latest_state
                std::set<std::string> not_latest;
        };
    }
}


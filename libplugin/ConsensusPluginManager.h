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

namespace dev {
    namespace plugin {
        class ConsensusPluginManager:public std::enable_shared_from_this<ConsensusPluginManager>{
            public:
                ConsensusPluginManager(std::shared_ptr<dev::rpc::Rpc> _service){
                    readSetQueue = new tbb::concurrent_queue<protos::TxWithReadSet>();
                    txs = new tbb::concurrent_queue<protos::Transaction>();
                    // distxs = new tbb::concurrent_queue<protos::RLPWithReadSet>();
                    distxs = new tbb::concurrent_queue<protos::SubCrossShardTx>();
                    precommit_txs = new tbb::concurrent_queue<protos::SubPreCommitedDisTx>();
                    commit_txs = new tbb::concurrent_queue<protos::CommittedRLPWithReadSet>();
                    notFinishedDAG = 0;
                    m_rpc_service = _service;
                }
                void processReceivedWriteSet(protos::TxWithReadSet _rs);
                void processReceivedTx(protos::Transaction _tx);

                // void processReceivedDisTx(protos::RLPWithReadSet _txrlp);
                void processReceivedDisTx(protos::SubCrossShardTx _txrlp);

                void processReceivedPreCommitedTx(protos::SubPreCommitedDisTx _txrlp);

                void processReceivedCommitedTx(protos::CommittedRLPWithReadSet _txrlp);

                // ADD BY ZH
                void processReceivedCrossTx(protos::SubCrossShardTx _txrlp);
                void processReceivedCrossTxReply(protos::SubCrossShardTxReply _txrlp);
                void processReceivedCrossTxCommit(protos::SubCrossShardTxCommit _txrlp);
                void processReceivedCrossTxCommitReply(protos::SubCrossShardTxCommitReply _txrlp);

                int numOfNotFinishedDAGs();
                int addNotFinishedDAGs(int _num);

                u256 getLatestState(std::string _addr);
                
                void updateNotLatest(std::string const _state);
                void removeNotLatest(std::string const _state);
                bool isLatest(std::string const _state);
                h512 getNodeId(int _index);

                /// record already send DAG(wait exnode) key: dagId value: waitStateNum
                tbb::concurrent_unordered_map<int, int> DAGMap;

                /// record wait DAG key:dagId value:DAG
                std::map<int,protos::DAGWithReadSet> m_DAGWaitValue;

                /// record wait State
                std::map<std::string,std::queue<int>> m_waitValueQueue;

                tbb::concurrent_unordered_map<std::string,u256>testMap;

                /// receive writeResult from exnode
                tbb::concurrent_queue<protos::TxWithReadSet> *readSetQueue;

                // receive txs from leader
                tbb::concurrent_queue<protos::Transaction> *txs;

                // receive dis_txs from leader
                tbb::concurrent_queue<protos::SubCrossShardTx> *distxs;

                // receive precommit_txs from participant
                tbb::concurrent_queue<protos::SubPreCommitedDisTx> *precommit_txs;

                // receive precommit_txs from participant
                tbb::concurrent_queue<protos::CommittedRLPWithReadSet> *commit_txs;

                /// no need so much mutex delete later
                std::mutex x_latest_Mutex;
                std::mutex x_wait_Mutex;
                std::mutex x_snapshot_Mutex;
                std::mutex x_map_Mutex;

                std::atomic<int> notFinishedDAG;
                std::shared_ptr<dev::rpc::Rpc> m_rpc_service;;

            private:
                /// global set to record latest_state
                std::set<std::string> not_latest;
        };
    }
}


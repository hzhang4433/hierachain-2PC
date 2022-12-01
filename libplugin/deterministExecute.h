#pragma once

#include <libconsensus/pbft/Common.h>
#include <libplugin/executeVM.h>
#include <libsync/SyncMsgPacket.h>
#include <libprotobasic/shard.pb.h>
#include <libplugin/Common.h>
#include <mutex>
#include <libblockchain/BlockChainInterface.h>

using namespace std;

namespace dev{
    namespace plugin
    {
        class deterministExecute:public std::enable_shared_from_this<deterministExecute>
        {
            public:
                deterministExecute()
                {
                    std::string path = "./" + to_string(dev::consensus::internal_groupId);
                    dev::plugin::executiveContext = std::make_shared<ExecuteVMTestFixture>(path);
                }
                void deterministExecuteTx();
                void processConsensusBlock();
                // void start();
                void replyToCoordinator(dev::plugin::transaction txInfo, dev::PROTOCOL_ID& m_group_protocolID, std::shared_ptr<dev::p2p::Service> m_group_service);
                void checkForDeterministExecuteTxWookLoop();
                void checkDelayCommitPacket(dev::plugin::transaction txInfo);
                void setAttribute(std::shared_ptr<dev::blockchain::BlockChainInterface> _blockchainManager);
                std::string dataToHexString(bytes data);
                int checkTransactionType(std::string& hex_m_data_str, std::shared_ptr<dev::eth::Transaction> tx);
                void processInnerShardTx(std::string data_str, std::shared_ptr<dev::eth::Transaction> tx);
                void processCrossShardTx(std::string data_str, std::shared_ptr<dev::eth::Transaction> tx);
                void processSubShardTx(std::shared_ptr<dev::eth::Transaction> tx, int height);
                
                int popedTxNum = 0;
                int count = 0;
                std::shared_ptr <dev::blockchain::BlockChainInterface> m_blockchainManager;
                std::shared_ptr<tbb::concurrent_unordered_map<std::string, int>> state2Messageid;
                
            private:
                std::mutex m_cachedTx;
        };
    }
}
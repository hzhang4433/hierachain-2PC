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
                    key2Messageid = std::make_shared<tbb::concurrent_unordered_map<std::string, int>>();
                    key2Signdatas = std::make_shared<tbb::concurrent_unordered_map<std::string, std::string>>();
                    key2CrossTxHash = std::make_shared<tbb::concurrent_unordered_map<std::string, std::string>>();
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
                std::string createBatchTransaction(std::string signedDatas, int groupId);
                void tryToSendSubTxs();
                
                int popedTxNum = 0;
                int count = 0;
                std::shared_ptr <dev::blockchain::BlockChainInterface> m_blockchainManager;
                // 对应状态集收集的messageId
                std::shared_ptr<tbb::concurrent_unordered_map<std::string, int>> key2Messageid;
                // 对应状态集messageId对应的子交易
                std::shared_ptr<tbb::concurrent_unordered_map<std::string, std::string>> key2Signdatas;
                // 对应key值的crossTxHash值 用于标识
                std::shared_ptr<tbb::concurrent_unordered_map<std::string, std::string>> key2CrossTxHash;
                
                // 交易地址hash
                std::string innerContact_1 = "0x93911693669c9a4b83f702838bc3294e95951438";
                std::string innerContact_2 = "0x1d89f9c61addceff5d8cae494c3439667b657deb";
                std::string innerContact_3 = "0x4bb13eaebeed711234f0bc2c455d1e74d0cef0c8";
                std::string crossContact_3 = "0x2fa6307e464428209f02702f65180ad663aa4fd9";

            private:
                std::mutex m_cachedTx;
        };
    }
}
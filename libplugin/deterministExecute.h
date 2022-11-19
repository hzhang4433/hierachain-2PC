#pragma once

#include <libconsensus/pbft/Common.h>
#include <libplugin/executeVM.h>
#include <libsync/SyncMsgPacket.h>
#include <libprotobasic/shard.pb.h>
#include <libplugin/Common.h>

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
                // void start();
                void replyToCoordinator(dev::plugin::transaction txInfo, dev::PROTOCOL_ID& m_group_protocolID, std::shared_ptr<dev::p2p::Service> m_group_service);
                void checkForDeterministExecuteTxWookLoop();
                
                int popedTxNum = 0;
                int count = 0;
        };

        extern std::mutex m_block2UnExecMutex;
        extern std::mutex m_height2TxHashMutex;
        extern std::mutex m_doneCrossTxMutex;
        extern std::mutex m_lockKeyMutex;
    }
}
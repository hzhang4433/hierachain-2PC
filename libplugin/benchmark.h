#pragma once

#include <vector>
#include <string>
#include <json/json.h>
#include <librpc/Rpc.h>

using namespace std;

namespace dev
{
    namespace plugin
    {
        #define PLUGIN_LOG(LEVEL) LOG(LEVEL) << LOG_BADGE("PLUGIN") << LOG_BADGE("PLUGIN")
        class transactionInjectionTest
        {
            private:
                std::vector<std::string> signedTransactions;
                std::string signedDeployContractTransaction;
                std::shared_ptr<dev::rpc::Rpc> m_rpcService;
                int32_t m_internal_groupId;
            public:
                transactionInjectionTest(std::shared_ptr<dev::rpc::Rpc> _rpcService, int32_t _groupId)
                {
                    m_rpcService = _rpcService;
                    m_internal_groupId = _groupId;
                }
                ~transactionInjectionTest(){};
                void deployContractTransaction(std::string filename, int32_t _groupId);
                void injectionTransactions(std::string filename, int32_t _groupId);
        };
    }
}
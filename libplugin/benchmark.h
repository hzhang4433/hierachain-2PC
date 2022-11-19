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
                std::string innerContact_1 = "0x93911693669c9a4b83f702838bc3294e95951438";
                std::string innerContact_2 = "0x1d89f9c61addceff5d8cae494c3439667b657deb";
                std::string innerContact_3 = "0x4bb13eaebeed711234f0bc2c455d1e74d0cef0c8";
                std::string crossContact_3 = "0x2fa6307e464428209f02702f65180ad663aa4fd9";
            public:
                transactionInjectionTest(std::shared_ptr<dev::rpc::Rpc> _rpcService, int32_t _groupId)
                {
                    m_rpcService = _rpcService;
                    m_internal_groupId = _groupId;
                }
                ~transactionInjectionTest(){};
                void deployContractTransaction(std::string filename, int32_t _groupId);
                void injectionTransactions(std::string filename, int32_t _groupId);
                std::string createInnerTransactions(int32_t _groupId);
                std::string createCrossTransactions(int32_t coorGroupId, int32_t subGroupId1, int32_t subGroupId2);
                std::string createCrossTransactions_HB(int32_t coorGroupId, int32_t subGroupId1, int32_t subGroupId2, int32_t squId);
        };
    }
}
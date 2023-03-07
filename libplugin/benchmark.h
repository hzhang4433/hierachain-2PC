#pragma once

#include <vector>
#include <string>
#include <json/json.h>
#include <librpc/Rpc.h>
#include <time.h>

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
                vector<Transaction::Ptr> txs;

                std::string innerContact_1 = "0x93911693669c9a4b83f702838bc3294e95951438";
                std::string innerContact_2 = "0x1d89f9c61addceff5d8cae494c3439667b657deb";
                std::string innerContact_3 = "0x4bb13eaebeed711234f0bc2c455d1e74d0cef0c8";
                std::string crossContact_3 = "0x2fa6307e464428209f02702f65180ad663aa4fd9";
                shared_ptr<dev::ledger::LedgerManager> m_ledgerManager;

            public:
                transactionInjectionTest(std::shared_ptr<dev::rpc::Rpc> _rpcService, int32_t _groupId, shared_ptr<dev::ledger::LedgerManager> ledgerManager)
                {
                    m_rpcService = _rpcService;
                    m_internal_groupId = _groupId;
                    m_ledgerManager = ledgerManager;

                }
                transactionInjectionTest(std::shared_ptr<dev::rpc::Rpc> _rpcService, int32_t _groupId)
                {
                    m_rpcService = _rpcService;
                    m_internal_groupId = _groupId;
                }
                
                ~transactionInjectionTest(){};
                void deployContractTransaction(string filename, int32_t _groupId);
                void injectionTransactions(string filename, int32_t _groupId);
                void injectionTransactions(string& intrashardworkload_filename, string& intershardworkload_filename, int intratxNum, int intertxNum);
                void injectionTransactions(string& intrashardworkload_filename, string& intershardworkload_filename, string& crosslayerworkload_filename, int intratxNum, int intertxNum, int crosslayerNum);
                int getRand(int a, int b);
                string createInnerTransactions(int32_t _groupId, shared_ptr<dev::ledger::LedgerManager> ledgerManager);
                string createCrossTransactions(int32_t coorGroupId, int32_t subGroupId1, int32_t subGroupId2, shared_ptr<dev::ledger::LedgerManager> ledgerManager);
                string createCrossTransactions(int32_t coorGroupId, vector<int>& shardIds, shared_ptr<dev::ledger::LedgerManager> ledgerManager);
                string createCrossTransactions_HB(int32_t coorGroupId, int32_t subGroupId1, int32_t subGroupId2, int32_t squId);
                string dataToHexString(bytes data);
                string createSpecialInnerTransactions(int32_t _groupId, shared_ptr<dev::ledger::LedgerManager> ledgerManager, string state1, string state2);
                string createSpecialCrossTransactions(int32_t coorGroupId, int32_t subGroupId1, int32_t subGroupId2, shared_ptr<dev::ledger::LedgerManager> ledgerManager, string state1, string state2);

        };
    }
}
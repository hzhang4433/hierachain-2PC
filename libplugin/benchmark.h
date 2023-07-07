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
        #define PLUGIN_LOG(LEVEL) LOG(LEVEL) << LOG_BADGE("PLUGIN")
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
                void injectionTransactions(string& intrashardworkload_filename, string& intershardworkload_filename, string& crosslayerworkload_filename, int intratxNum, int intertxNum, int crosslayerNum, int threadId);
                int getRand(int a, int b);
                string createInnerTransactions(int32_t _groupId, shared_ptr<dev::ledger::LedgerManager> ledgerManager);
                string createCrossTransactions(int32_t coorGroupId, int32_t subGroupId1, int32_t subGroupId2, shared_ptr<dev::ledger::LedgerManager> ledgerManager);
                string createCrossTransactions(int32_t coorGroupId, vector<int>& shardIds, shared_ptr<dev::ledger::LedgerManager> ledgerManager);
                string createCrossTransactions_HB(int32_t coorGroupId, int32_t subGroupId1, int32_t subGroupId2, int32_t squId);
                string dataToHexString(bytes data);
                string createSpecialInnerTransactions(int32_t _groupId, shared_ptr<dev::ledger::LedgerManager> ledgerManager, string state1, string state2);
                string createSpecialCrossTransactions(int32_t coorGroupId, int32_t subGroupId1, int32_t subGroupId2, shared_ptr<dev::ledger::LedgerManager> ledgerManager, string state1, string state2);

        };

        //初始化时输入底层片内交易数量、跨片数/片内数 
        class Test_3shard
        {
            public:

            Test_3shard();
            Test_3shard(int num_3shard_intra, double rc)
            {
                intraTx4Three=num_3shard_intra;
                rate_cross=rc;
            };

            // 片内交易数
            int get_intra()
            {
                return intraTx4Three;
            };
            // 跨片交易数
            int get_inter()
            {
                int res = 3 * intraTx4Three * rate_cross;
                cout << "res=" << res << endl;
                //纯跨片
                if(intraTx4Three == 0)  return max_txnum;
                if(res > max_txnum) return max_txnum;
                return res;
            };

            //data
            //片内交易数量
            int intraTx4Three=0;
            //跨片交易/片内交易
            double rate_cross=0;
            //纯跨片是注入交易量
            int max_txnum=50000;
        };

        //初始化时输入底层片内交易数量、跨片数/片内数 
        class Test_9shard
        {
            public:

            Test_9shard();
            Test_9shard(int num_9shard_intra, double rc)
            {
                intraTx4nine=num_9shard_intra;
                rate_cross=rc;
            };

            //底层
            int get_6shard_intra()
            {
                return intraTx4nine;
            };
            //中间
            int get_2shard_cross()
            {
                // cout << "rate_cross=" << rate_cross << ", rate_cross_area=" << rate_cross_area << endl;
                // cout << "middle res1=" << 9*intraTx4nine*rate_cross
                //      << "middle res2=" << 9*intraTx4nine*rate_cross*(1-rate_cross_area)
                //      << "middle res3=" << 9*intraTx4nine*rate_cross*(1-rate_cross_area)*(2/5) << endl;
                int res = 9*intraTx4nine*rate_cross*(1-rate_cross_area)*(2.0/5);
                cout << "res=" << res << endl;
                //纯跨片
                if(intraTx4nine==0)  return int(max_txnum*(1-rate_cross_area)*(2.0/5));
                if(res > 50000) return 50000;
                return res;
            };
            //顶层
            int get_1shard_cross()
            {
                if(intraTx4nine==0)  return int(max_txnum*(1-rate_cross_area)*(1.0/5));
                int res = 9*intraTx4nine*rate_cross*(1-rate_cross_area)*(1.0/5);
                if(res > 50000) return 50000;
                return res;
            };
            //跨层
            int get_1shard_cross2()
            {
                if(intraTx4nine==0)  return int(max_txnum*rate_cross_area);
                int res = 9*intraTx4nine*rate_cross*rate_cross_area;
                if(res > 50000) return 50000;
                return res;
            };

            //data
            //片内交易数量
            int  intraTx4nine=0;
            //跨片交易/片内交易
            double rate_cross=0;
            //纯跨片是注入交易量
            int max_txnum=70000;
            //跨域比例
            double rate_cross_area=0.05;

        };

        //初始化时输入底层片内交易数量、跨片数/片内数 
        class Test_13shard
        {
            public:

            Test_13shard();
            Test_13shard(int num_9shard_intra,double rc)
            {
                intraTxdown=num_9shard_intra;
                rate_cross=rc;
            };

            //底层
            int get_down_intra()
            {
                return intraTxdown;
            };
            //中间
            int get_mid_cross()
            {
                // cout<<
                int res = 1.0*9*intraTxdown*rate_cross*(1-rate_cross_area)*0.25;
                //纯跨片
                if(intraTxdown==0)  return max_txnum;
                //不可超出限制
                return min(res,70000);
            };
            //顶层
            int get_top_cross()
            {
                if(intraTxdown==0)  return int(max_txnum*(1-rate_cross_area));
                int res =1.0*9*intraTxdown*rate_cross*(1-rate_cross_area)*0.25;
                return min(res,70000);
                
            };
            int get_top_cross2()
            {
                if(intraTxdown==0)  return int(max_txnum*rate_cross_area);
                int res =1.0*9*intraTxdown*rate_cross*(rate_cross_area);
                return min(res,70000);

            };

            //data
            //片内交易数量
            int  intraTxdown=0;
            //跨片交易/片内交易
            double rate_cross=0;
            //纯跨片是注入交易量
            int max_txnum=70000;
            //跨域比例
            double rate_cross_area=0.05;


        };








    }
}
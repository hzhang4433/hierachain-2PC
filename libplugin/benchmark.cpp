#include "benchmark.h"

using namespace std;
using namespace dev::plugin;

void transactionInjectionTest::deployContractTransaction(std::string filename, int32_t groupId)
{
    ifstream infile(filename, ios::binary); //deploy.json
    Json::Reader reader;
    Json::Value root;

    if(reader.parse(infile, root)) { signedDeployContractTransaction = root[0].asString(); }
    infile.close();
    std::string response = m_rpcService->sendRawTransaction(groupId, signedDeployContractTransaction);
    PLUGIN_LOG(INFO) << LOG_DESC("部署合约交易完成...");
}

void transactionInjectionTest::injectionTransactions(std::string filename, int32_t groupId)
{
    PLUGIN_LOG(INFO) << LOG_DESC("进入injectionTransactions");

    ifstream infile(filename, ios::binary); // signedtxs.json

    Json::Reader reader;
    Json::Value root;
    int64_t number = 0;

    if(reader.parse(infile, root))
    {
        number = root.size();
        for(int i = 0; i < number; i++)
        {
            std::string signedTransaction = root[i].asString();
            PLUGIN_LOG(INFO) << LOG_KV("signedTransaction", signedTransaction);
            signedTransactions.push_back(signedTransaction);
        }
    }
    infile.close();

    std::vector<std::string>::iterator iter;
    for(iter = signedTransactions.begin(); iter != signedTransactions.end(); iter++)
    {
        m_rpcService->sendRawTransaction(groupId, *iter);
    }
    PLUGIN_LOG(INFO) << LOG_DESC("跨片交易发送完成...");
}
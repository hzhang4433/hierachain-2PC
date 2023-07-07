#include "benchmark.h"
#include <libplugin/executeVM.h>
#include <libplugin/Common.h>
#include <librpc/Rpc.h>
#include <libdevcore/Address.h>
#include <libdevcore/CommonData.h>
#include <libethcore/ABI.h>

using namespace std;
using namespace dev::plugin;

int transactionInjectionTest::getRand(int a, int b) {
    srand((unsigned)time(NULL));
    return (rand() % (b - a + 1)) + a;
}

void transactionInjectionTest::deployContractTransaction(std::string filename, int32_t groupId) {
    // /*
    ifstream infile(filename, ios::binary); //deploy.json
    Json::Reader reader;
    Json::Value root;
    std::string deploytx_str = "";

    if(reader.parse(infile, root))
    {
        for(int i = 0; i < root.size(); i++)
        {
            std::string deployTx = root[i].asString();
            m_rpcService->sendRawTransaction(groupId, deployTx);
            
            /*
            deploytx_str = root[i].asString();

            Transaction::Ptr tx = std::make_shared<Transaction>(
                jsToBytes(deploytx_str, OnFailed::Throw), CheckTransaction::Everything);

            auto exec = executiveContext->getExecutive();
            auto vm = executiveContext->getExecutiveInstance();
            exec->setVM(vm);
            executiveContext->executeTransaction(exec, tx);

            Address newAddress = exec->newAddress();
            PLUGIN_LOG(INFO) << LOG_KV("Contract created at ", newAddress);

            
            u256 value = 0;
            u256 gasPrice = 0;
            u256 gas = 100000000;
            auto keyPair = KeyPair::create();
            Address caller = Address("1000000000000000000000000000000000000000");

            // add()
            bytes callDataToSet =
                fromHex(string("4f2be91f") + 
                        string(""));
            Transaction::Ptr setTx =
                std::make_shared<Transaction>(value, gasPrice, gas, newAddress, callDataToSet);
            auto sig = dev::crypto::Sign(keyPair, setTx->hash(WithoutSignature));
            setTx->updateSignature(sig);
            setTx->forceSender(caller);
            PLUGIN_LOG(INFO) << LOG_KV("setTx RLP", toHex(setTx->rlp()));
            m_rpcService->sendRawTransaction(groupId, toHex(setTx->rlp()));
            executiveContext->m_vminstance_pool.push(vm);
            */
        }
    }
    // */


    // /*
    // // if(reader.parse(infile, root)) { deploytx_str = root[0].asString(); }
    
    // // // m_rpcService->sendRawTransaction(groupId, deploytx_str);

    // // std::string path = "./" + to_string(groupId);
    // // auto executiveContext = std::make_shared<ExecuteVMTestFixture>(path);

    // // auto vm = executiveContext->getExecutiveInstance();
    // // auto exec1 = executiveContext->getExecutive();
    // // exec1->setVM(vm);

    // // Transaction::Ptr tx1 = std::make_shared<Transaction>(
    // //         jsToBytes(deploytx_str, OnFailed::Throw), CheckTransaction::Everything);

    // // PLUGIN_LOG(INFO) << LOG_KV("deploytx_str", deploytx_str);


    // // executiveContext->executeTransaction(exec1, tx1);

    // // Address newAddress = exec1->newAddress();
    // // PLUGIN_LOG(INFO) << LOG_KV("Contract created at: ", newAddress);

    // // if(reader.parse(infile, root)) { deploytx_str = root[1].asString(); }
    // // auto exec2 = executiveContext->getExecutive();

    // // Transaction::Ptr tx2 = std::make_shared<Transaction>(
    // //         jsToBytes(deploytx_str, OnFailed::Throw), CheckTransaction::Everything);

    // // PLUGIN_LOG(INFO) << LOG_KV("deploytx_str", deploytx_str);


    // // executiveContext->executeTransaction(exec2, tx2);

    // // if(reader.parse(infile, root)) { deploytx_str = root[2].asString(); }
    // // auto exec3 = executiveContext->getExecutive();

    // // PLUGIN_LOG(INFO) << LOG_KV("deploytx_str", deploytx_str);


    // // Transaction::Ptr tx3 = std::make_shared<Transaction>(
    // //         jsToBytes(deploytx_str, OnFailed::Throw), CheckTransaction::Everything);

    // // executiveContext->executeTransaction(exec3, tx3);

    // */

    // /*
    // // Deploy a contract
    // u256 value = 0;
    // u256 gasPrice = 0;
    // u256 gas = 100000000;
    // Address caller = Address("1000000000000000000000000000000000000000");
    // bytes code = fromHex(
    //     string("608060405234801561001057600080fd5b506103e86000806101000a81548163ffffffff021916908360030b63ffffffff160217905550610246806100456000396000f3fe60806040526004361061005c576000357c0100000000000000000000000000000000000000000000000000000000900480634f2be91f146100615780636d4ce63c146100785780638fa1842f146100a9578063c54124be14610171575b600080fd5b34801561006d57600080fd5b50610076610188565b005b34801561008457600080fd5b5061008d6101c4565b604051808260030b60030b815260200191505060405180910390f35b3480156100b557600080fd5b5061016f600480360360208110156100cc57600080fd5b81019080803590602001906401000000008111156100e957600080fd5b8201836020820111156100fb57600080fd5b8035906020019184600183028401116401000000008311171561011d57600080fd5b91908080601f016020809104026020016040519081016040528093929190818152602001838380828437600081840152601f19601f8201169050808301925050505050505091929192905050506101da565b005b34801561017d57600080fd5b506101866101dd565b005b60008081819054906101000a900460030b8092919060010191906101000a81548163ffffffff021916908360030b63ffffffff16021790555050565b60008060009054906101000a900460030b905090565b50565b60008081819054906101000a900460030b809291906001900391906101000a81548163ffffffff021916908360030b63ffffffff1602179055505056fea165627a7a7230582093d3530e595c9bad21dd7b85985e8b9604319eef6a7ef270eafc1d90946855010029") +
    //     string(""));

    // Transaction::Ptr tx = std::make_shared<Transaction>(value, gasPrice, gas, code);  // Use contract creation constructor
    // auto keyPair = KeyPair::create();
    // auto sig = dev::crypto::Sign(keyPair, tx->hash(WithoutSignature));
    // tx->updateSignature(sig);
    // tx->forceSender(caller);
    

    // PLUGIN_LOG(INFO) << LOG_KV("DeployTx RLP", toHex(tx->rlp()));

    // auto exec = executiveContext->getExecutive();
    // auto vm = executiveContext->getExecutiveInstance();
    // exec->setVM(vm);

    // executiveContext->executeTransaction(exec, tx);
    // Address newAddress = exec->newAddress();
    // cout << "Contract created at: " << newAddress << endl;
    // PLUGIN_LOG(INFO) << LOG_KV("Contract created at: ", newAddress);

    // // // set()
    // // bytes callDataToSet =
    // //     fromHex(string("0x60fe47b1") +  // set(0xaa)
    // //             string("00000000000000000000000000000000000000000000000000000000000000aa"));
    // // add()
    // bytes callDataToSet =
    //     fromHex(string("4f2be91f") +
    //             string(""));
    // Transaction::Ptr setTx =
    //     std::make_shared<Transaction>(value, gasPrice, gas, newAddress, callDataToSet);
    // sig = dev::crypto::Sign(keyPair, setTx->hash(WithoutSignature));
    // setTx->updateSignature(sig);
    // setTx->forceSender(caller);

    // auto exec1 = executiveContext->getExecutive();

    // executiveContext->executeTransaction(exec1, setTx);

    // PLUGIN_LOG(INFO) << LOG_KV("setTx RLP", toHex(setTx->rlp()));


    // // get()
    // bytes callDataToGet = fromHex(string("6d4ce63c") +  // get()
    //                               string(""));

    // Transaction::Ptr getTx =
    //     std::make_shared<Transaction>(value, gasPrice, gas, newAddress, callDataToGet);
    // sig = dev::crypto::Sign(keyPair, getTx->hash(WithoutSignature));
    // getTx->updateSignature(sig);
    // getTx->forceSender(caller);

    // auto exec2 = executiveContext->getExecutive();

    // executiveContext->executeTransaction(exec2, getTx);
    // PLUGIN_LOG(INFO) << LOG_KV("getTx RLP", toHex(getTx->rlp()));
    // // */

    // infile.close();
    PLUGIN_LOG(INFO) << LOG_DESC("部署合约交易完成...");
}

void transactionInjectionTest::injectionTransactions(std::string filename, int32_t groupId) {
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
            // PLUGIN_LOG(INFO) << LOG_KV("signedTransaction", signedTransaction);
            signedTransactions.push_back(signedTransaction);
        }
    }
    infile.close();

    number = 0;
    std::vector<std::string>::iterator iter;
    for(iter = signedTransactions.begin(); iter != signedTransactions.end(); iter++)
    {
        number++;
        m_rpcService->sendRawTransaction(groupId, *iter);

        string txid;
        Transaction::Ptr tx = std::make_shared<Transaction>(
            jsToBytes(*iter, OnFailed::Throw), CheckTransaction::Everything);
        string data_str = dataToHexString(tx->get_data());
        if(data_str.find("0x444555666", 0) != -1){ // 片内交易
            vector<string> dataItems;
            boost::split(dataItems, data_str, boost::is_any_of("|"), boost::token_compress_on);
            txid = dataItems.at(2).c_str();
            // PLUGIN_LOG(INFO) << LOG_DESC("片内交易")
            //                  << LOG_KV("txid", txid);
        }
        else if(data_str.find("0x111222333", 0) != -1){ // 跨片交易
            vector<string> dataItems;
            boost::split(dataItems, data_str, boost::is_any_of("|"), boost::token_compress_on);
            txid = dataItems.at(7).c_str();
            // PLUGIN_LOG(INFO) << LOG_DESC("跨片交易")
            //                  << LOG_KV("txid", txid);
        }

        // std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    PLUGIN_LOG(INFO) << LOG_DESC("injectionTransactions交易发送完成...")
                     << LOG_KV("number", number);

    // ifstream infile(filename, ios::binary); // signedtxs.json
    // Json::Reader reader;
    // Json::Value root;

    // string signedTransaction = "";
    // int inputTxsize = 40000;
    // if(reader.parse(infile, root))
    // {
    //     int number = root.size();
    //     for(int i = 0; i < number; i++)
    //     {
    //         signedTransaction = root[i].asString();
    //         Transaction::Ptr tx = std::make_shared<Transaction>(
    //             jsToBytes(signedTransaction, OnFailed::Throw), CheckTransaction::Everything);
    //         txs.push_back(tx);

    //         // 记录片内交易的读写集
    //         string data_str = dataToHexString(tx->get_data());

    //         // if(data_str.find("0x444555666", 0) != -1) {

    //         // // PLUGIN_LOG(INFO) << LOG_KV("载入交易时的data_str", data_str) << LOG_KV("txhash", tx->hash());

    //         //     // 发现原始片内交易，存储交易的读写集
    //         //     vector<string> dataItems;
    //         //     boost::split(dataItems, data_str, boost::is_any_of("_"), boost::token_compress_on);
    //         //     string readwriteset = dataItems.at(1);
    //         //     // dev::plugin::intrashardtxhash2rwkeys->insert(make_pair(tx->hash(), readwriteset)); // 记录原始片内交易的读写集信息(txhash --> readwriteset)
    //         // }

    //         if(i == inputTxsize) { // 倒入的交易总数目
    //             break;
    //         }
    //     }
    // }

    // // 往交易池中灌交易
    // auto txPool = m_ledgerManager->txPool(dev::consensus::internal_groupId);
    // for(int i = 0; i < inputTxsize; i++) {
    //     txPool->submitTransactions(txs.at(i)); // 交易直接往交易池中发送
    // }
    // PLUGIN_LOG(INFO) << LOG_KV("往交易池中灌入的交易数目为", inputTxsize);
    // infile.close();
}

void transactionInjectionTest::injectionTransactions(string& intrashardworkload_filename, string& intershardworkload_filename, string& crosslayerworkload_filename
                                                    , int intratxNum, int intertxNum, int crosslayerNum, int threadId)
{
    int SPEED;
    if (dev::consensus::internal_groupId == 9) {
      SPEED = 1000;
    } else  {
      SPEED = 5000;
    }
    int baseNum = (threadId - 1) * 50000;
    
    // 只导入片内交易(只需转发节点负责)
    if(intratxNum != 0 && intertxNum == 0 && crosslayerNum == 0){
        vector<string> txids;
        vector<string> txRLPS;
        int inputTxsize = intratxNum;
        PLUGIN_LOG(INFO) << LOG_DESC("开始导入交易....")
                         << LOG_KV("即将导入的'片内交易'总数", intratxNum);
                        //  << LOG_KV("threadId", threadId);

        ifstream infile(intrashardworkload_filename, ios::binary); // signedtxs.json
        Json::Reader reader;
        Json::Value root;

        // 加载交易
        if(reader.parse(infile, root)) {
            for(int i = baseNum; i < inputTxsize + baseNum; i++) {
                string txrlp = root[i].asString();
                txRLPS.push_back(txrlp);

                // 解析交易data字段，获取交易txid
                Transaction::Ptr tx = std::make_shared<Transaction>(
                    jsToBytes(txrlp, OnFailed::Throw), CheckTransaction::Everything);
                string data_str = dataToHexString(tx->get_data());
                vector<string> dataItems;
                boost::split(dataItems, data_str, boost::is_any_of("|"), boost::token_compress_on);
                string txid = dataItems.at(2).c_str();
                // PLUGIN_LOG(INFO) << LOG_DESC("片内交易")
                //                  << LOG_KV("txid", txid);
                txids.push_back(txid);
            }
        }
        infile.close();

        // 正式投递到交易池
        for(int i = 0; i < inputTxsize; i++) {
            if(i != 0 && i % SPEED == 0){
                std::this_thread::sleep_for(std::chrono::seconds(1)); // 暂停1秒
            }
            m_rpcService->sendRawTransaction(dev::consensus::internal_groupId, txRLPS.at(i));
            string txid = txids.at(i);
            struct timeval tv;
            gettimeofday(&tv, NULL);
            int time_sec = (int)tv.tv_sec;
            m_txid_to_starttime->insert(make_pair(txid, time_sec)); // 记录txid的开始时间
        }
    }
    // 只导入跨片交易
    else if(intratxNum == 0 && (intertxNum != 0 || crosslayerNum != 0)){ 
        vector<string> txids;
        vector<string> txRLPS;
        int inputTxsize = intertxNum + crosslayerNum;
        PLUGIN_LOG(INFO) << LOG_DESC("开始导入交易....")
                         << LOG_KV("即将导入的'跨片交易'总数", intertxNum + crosslayerNum);
        if (intertxNum != 0) {
            ifstream infile(intershardworkload_filename, ios::binary); // signedtxs.json
            Json::Reader reader;
            Json::Value root;

            // 加载交易
            if(reader.parse(infile, root)) {
                for(int i = baseNum; i < intertxNum + baseNum; i++) {
                    string txrlp = root[i].asString();
                    txRLPS.push_back(txrlp);

                    // 解析交易data字段，获取交易txid
                    Transaction::Ptr tx = std::make_shared<Transaction>(
                        jsToBytes(txrlp, OnFailed::Throw), CheckTransaction::Everything);
                    string data_str = dataToHexString(tx->get_data());
                    vector<string> dataItems;
                    boost::split(dataItems, data_str, boost::is_any_of("|"), boost::token_compress_on);
                    string txid = dataItems.at(7).c_str();
                    txids.push_back(txid);
                }
            }

            infile.close();
        }

        if (crosslayerNum != 0) {
            ifstream infile(crosslayerworkload_filename, ios::binary); // signedtxs.json
            Json::Reader reader;
            Json::Value root;

            // 加载交易
            if(reader.parse(infile, root)) {
                for(int i = baseNum; i < crosslayerNum + baseNum; i++) {
                    string txrlp = root[i].asString();
                    txRLPS.push_back(txrlp);

                    // 解析交易data字段，获取交易txid
                    Transaction::Ptr tx = std::make_shared<Transaction>(
                        jsToBytes(txrlp, OnFailed::Throw), CheckTransaction::Everything);
                    string data_str = dataToHexString(tx->get_data());
                    vector<string> dataItems;
                    boost::split(dataItems, data_str, boost::is_any_of("|"), boost::token_compress_on);
                    string txid = dataItems.at(7).c_str();
                    txids.push_back(txid);
                }
            }

            infile.close();
        }

        // 正式投递到交易池
        for(int i = 0; i < inputTxsize; i++) {
            if(i != 0 && i % SPEED == 0){
                std::this_thread::sleep_for(std::chrono::seconds(1)); // 暂停1秒
            }
            m_rpcService->sendRawTransaction(dev::consensus::internal_groupId, txRLPS.at(i));
            // PLUGIN_LOG(INFO) << LOG_KV("已经导入的跨片交易总数", i);
            string txid = txids.at(i);
            struct timeval tv;
            gettimeofday(&tv, NULL);
            int time_sec = (int)tv.tv_sec;
            m_txid_to_starttime->insert(make_pair(txid, time_sec)); // 记录txid的开始时间
        }
    }
    // 片内交易+跨片交易
    else if(intratxNum != 0 && (intertxNum != 0 || crosslayerNum != 0)){
        vector<string> txids;
        vector<string> txRLPS;
        int inputTxsize = intratxNum+intertxNum+crosslayerNum;
        PLUGIN_LOG(INFO) << LOG_DESC("开始导入交易....")
                         << LOG_KV("即将导入的 '片内+跨片' 交易总数", inputTxsize);

        // 导入片内交易
        ifstream infile1(intrashardworkload_filename, ios::binary);
        Json::Reader reader;
        Json::Value root;
        // 加载交易
        if(reader.parse(infile1, root)) {
            for(int i = baseNum; i < intratxNum + baseNum; i++) {
                string txrlp = root[i].asString();
                txRLPS.push_back(txrlp);
            }
        }
        infile1.close();

        // 导入跨片交易
        if (intertxNum != 0) {
            ifstream infile2(intershardworkload_filename, ios::binary);
            // 加载交易
            if(reader.parse(infile2, root)) {
                for(int i = baseNum; i < intertxNum + baseNum; i++) {
                    string txrlp = root[i].asString();
                    txRLPS.push_back(txrlp);
                }
            }
            infile2.close();
        }
        if (crosslayerNum != 0) {
            ifstream infile2(crosslayerworkload_filename, ios::binary);
            // 加载交易
            if(reader.parse(infile2, root)) {
                for(int i = baseNum; i < crosslayerNum + baseNum; i++) {
                    string txrlp = root[i].asString();
                    txRLPS.push_back(txrlp);
                }
            }
            infile2.close();
        }

        // // 将txRLPS打乱
        // auto start = txRLPS.begin();
        // auto end = txRLPS.end();
        // srand(time(NULL));
        // random_shuffle(start, end);

        // 将txRLPS中的交易导入交易池, 且记录下开始时间
        for(int i = 0; i < inputTxsize; i++){
            if(i != 0 && i % SPEED == 0){
                std::this_thread::sleep_for(std::chrono::seconds(1)); // 暂停1秒
            }

            string txrlp = txRLPS.at(i);
            m_rpcService->sendRawTransaction(dev::consensus::internal_groupId, txrlp);

            // 获取交易id, 记录交易开始时间
            string txid;
            Transaction::Ptr tx = std::make_shared<Transaction>(
                jsToBytes(txrlp, OnFailed::Throw), CheckTransaction::Everything);
            string data_str = dataToHexString(tx->get_data());
            if(data_str.find("0x444555666", 0) != -1){ // 片内交易
                vector<string> dataItems;
                boost::split(dataItems, data_str, boost::is_any_of("|"), boost::token_compress_on);
                txid = dataItems.at(2).c_str();
            }
            else if(data_str.find("0x111222333", 0) != -1){ // 跨片交易
                vector<string> dataItems;
                boost::split(dataItems, data_str, boost::is_any_of("|"), boost::token_compress_on);
                txid = dataItems.at(7).c_str();
            }

            struct timeval tv;
            gettimeofday(&tv, NULL);
            int time_sec = (int)tv.tv_sec;
            m_txid_to_starttime->insert(make_pair(txid, time_sec));
        }
    }
}

std::string transactionInjectionTest::createInnerTransactions(int32_t _groupId, std::shared_ptr<dev::ledger::LedgerManager> ledgerManager) {
    
    std::string requestLabel = "0x444555666";
    std::string flag = "|";
    std::string txid = "I" + to_string(global_txId++);
    std::string stateAddress = "state" + to_string((rand() % 1000) + 1)
                            + "_state" + to_string((rand() % 1000) + 1);
    
    PLUGIN_LOG(INFO) << LOG_DESC("createInnerTransactions...")
                     << LOG_KV("txId", txid)
                     << LOG_KV("stateAddress", stateAddress);

    // std::string hex_m_testdata_str = requestLabel + flag + std::to_string(sourceshardid) + flag + std::to_string(destinshardid)
    //                                     + flag + readwritekey + flag + requestmessageid + flag + std::to_string(coordinatorshardid);

    std::string hex_m_data_str = requestLabel + flag + stateAddress + flag + txid + flag;

    // 自己构造交易
    std::string str_address;
    if (_groupId == 1) {
        // PLUGIN_LOG(INFO) << LOG_DESC("GroupID为1...");
        str_address = innerContact_1;
    } else if (_groupId == 2) {
        // PLUGIN_LOG(INFO) << LOG_DESC("GroupID为2...");
        str_address = innerContact_2;
    } else if (_groupId == 3) {
        // PLUGIN_LOG(INFO) << LOG_DESC("GroupID为3...");
        str_address = innerContact_3;
    } else {
        str_address = innerContact_3;
    }
    dev::Address contactAddress(str_address);
    dev::eth::ContractABI abi;
    bytes data = abi.abiIn("add(string)", hex_m_data_str);  // add
    // bytes data = [];

    Transaction tx(0, 1000, 0, contactAddress, data);
    tx.setNonce(tx.nonce() + u256(utcTime()));
    tx.setGroupId(_groupId);
    // tx.setBlockLimit(u256(ledgerManager->blockChain(_groupId)->number()) + 500);
    tx.setBlockLimit(500);
    
    auto keyPair = KeyPair::create();
    auto sig = dev::crypto::Sign(keyPair, tx.hash(WithoutSignature));
    tx.updateSignature(sig);

    auto rlp = tx.rlp();
    PLUGIN_LOG(INFO) << LOG_DESC("交易生成完毕...")
                     << LOG_KV("rlp", toHex(rlp));

    // m_rpcService->sendRawTransaction(_groupId, toHex(rlp)); // 通过调用本地的RPC接口发起新的共识
    // PLUGIN_LOG(INFO) << LOG_DESC("发送完毕...");

    return toHex(rlp);
}

std::string transactionInjectionTest::createCrossTransactions(int32_t coorGroupId, int32_t subGroupId1, int32_t subGroupId2, 
                        std::shared_ptr<dev::ledger::LedgerManager> ledgerManager) {
    std::string requestLabel = "0x111222333";
    std::string flag = "|";
    std::string txid = "C" + to_string(global_txId++);
    // std::string txid = "L" + to_string(global_txId++);
    // std::string stateAddress = "state1";
    // srand((unsigned)time(0));

    std::string stateAddress1 = "state" + to_string((rand() % 1000) + 1);
    std::string stateAddress2 = "state" + to_string((rand() % 1000) + 1);


    PLUGIN_LOG(INFO) << LOG_DESC("createCrossTransactions...")
                     << LOG_KV("stateAddress", stateAddress1)
                     << LOG_KV("stateAddress", stateAddress2)
                     << LOG_KV("txid", txid);

    auto keyPair = KeyPair::create();

    // 生成子交易1
    std::string str_address;
    if (subGroupId1 == 1) {
        str_address = innerContact_1;
    } else if (subGroupId1 == 2) {
        str_address = innerContact_2;
    } else if (subGroupId1 == 3) {
        str_address = innerContact_3;
    } else {
        str_address = innerContact_3;
    }
    dev::Address subAddress1(str_address);
    dev::eth::ContractABI abi;
    bytes data = abi.abiIn("add(string)");  // add
    // bytes data = [];

    Transaction subTx1(0, 1000, 0, subAddress1, data);
    subTx1.setNonce(subTx1.nonce() + u256(utcTime()));
    subTx1.setGroupId(subGroupId1);
    subTx1.setBlockLimit(u256(ledgerManager->blockChain(dev::consensus::internal_groupId)->number()) + 500);

    
    auto subSig1 = dev::crypto::Sign(keyPair, subTx1.hash(WithoutSignature));
    subTx1.updateSignature(subSig1);

    auto subrlp1 = subTx1.rlp();
    std::string signTx1 = toHex(subrlp1);

    // 生成子交易2
    if (subGroupId2 == 1) {
        str_address = innerContact_1;
    } else if (subGroupId2 == 2) {
        str_address = innerContact_2;
    } else if (subGroupId2 == 3) {
        str_address = innerContact_3;
    } else {
        str_address = innerContact_3;
    }
    dev::Address subAddress2(str_address);
    // dev::eth::ContractABI abi;
    data = abi.abiIn("add(string)");  // sub
    // bytes data = [];

    Transaction subTx2(0, 1000, 0, subAddress2, data);
    subTx2.setNonce(subTx2.nonce() + u256(utcTime()));
    subTx2.setGroupId(subGroupId2);
    subTx2.setBlockLimit(u256(ledgerManager->blockChain(dev::consensus::internal_groupId)->number()) + 500);

    
    // auto keyPair = KeyPair::create();
    auto subSig2 = dev::crypto::Sign(keyPair, subTx2.hash(WithoutSignature));
    subTx2.updateSignature(subSig2);

    auto subrlp = subTx2.rlp();
    std::string signTx2 = toHex(subrlp);

    // 生成跨片交易
    std::string hex_m_data_str = requestLabel
                                + flag + std::to_string(subGroupId1) + flag + signTx1 + flag + stateAddress1 
                                + flag + std::to_string(subGroupId2) + flag + signTx2 + flag + stateAddress2
                                + flag + txid
                                + flag;

    str_address = crossContact_3;
    dev::Address crossAddress(str_address);
    // dev::eth::ContractABI abi;
    data = abi.abiIn("set(string)", hex_m_data_str);  // set
    // bytes data = [];

    Transaction tx(0, 1000, 0, crossAddress, data);
    tx.setNonce(tx.nonce() + u256(utcTime()));
    tx.setGroupId(coorGroupId);
    tx.setBlockLimit(500);

    
    // auto keyPair = KeyPair::create();
    auto sig = dev::crypto::Sign(keyPair, tx.hash(WithoutSignature));
    tx.updateSignature(sig);

    auto rlp = tx.rlp();
    PLUGIN_LOG(INFO) << LOG_DESC("跨片交易生成完毕...")
                     << LOG_KV("rlp", toHex(rlp));

    // m_rpcService->sendRawTransaction(coorGroupId, toHex(rlp)); // 通过调用本地的RPC接口发起新的共识
    // PLUGIN_LOG(INFO) << LOG_DESC("发送完毕...");

    return toHex(rlp);
}

std::string transactionInjectionTest::createCrossTransactions(int32_t coorGroupId, vector<int>& shardIds, 
                        std::shared_ptr<dev::ledger::LedgerManager> ledgerManager) {
    std::string requestLabel = "0x111222333";
    std::string flag = "|";
    // std::string stateAddress = "state1";
    // srand((unsigned)time(0));
    int subShardNum = shardIds.size();
    string allStateAddress = "";
    auto keyPair = KeyPair::create();

    // 跨片交易data字段
    std::string hex_m_data_str = requestLabel;
    // std::string hex_m_data_str = requestLabel
    //                             + flag + std::to_string(subGroupId1) + flag + signTx1 + flag + stateAddress1 
    //                             + flag + std::to_string(subGroupId2) + flag + signTx2 + flag + stateAddress2
    //                             + flag;

    for (int i = 0; i < subShardNum; i++) {
        int subId = shardIds[i];
        std::string stateAddress = "state" + to_string((rand() % 1000) + 1);
        // 生成子交易1
        std::string str_address;
        if (subId == 1) {
            str_address = innerContact_1;
        } else if (subId == 2) {
            str_address = innerContact_2;
        } else if (subId == 3) {
            str_address = innerContact_3;
        } else {
            str_address = innerContact_3;
        }
        dev::Address subAddress(str_address);
        dev::eth::ContractABI abi;
        bytes data = abi.abiIn("add(string)");  // add
        // bytes data = [];

        Transaction subTx(0, 1000, 0, subAddress, data);
        subTx.setNonce(subTx.nonce() + u256(utcTime()));
        subTx.setGroupId(subId);
        subTx.setBlockLimit(u256(ledgerManager->blockChain(dev::consensus::internal_groupId)->number()) + 500);

        auto subSig = dev::crypto::Sign(keyPair, subTx.hash(WithoutSignature));
        subTx.updateSignature(subSig);

        auto subrlp = subTx.rlp();
        std::string signTx = toHex(subrlp);

        hex_m_data_str += (flag + std::to_string(subId) + flag + signTx + flag + stateAddress);
    }

    hex_m_data_str += flag;

    // 生成跨片交易
    string str_address = crossContact_3;
    dev::Address crossAddress(str_address);
    dev::eth::ContractABI abi;
    bytes data = abi.abiIn("set(string)", hex_m_data_str);  // set

    Transaction tx(0, 1000, 0, crossAddress, data);
    tx.setNonce(tx.nonce() + u256(utcTime()));
    tx.setGroupId(coorGroupId);
    tx.setBlockLimit(u256(ledgerManager->blockChain(coorGroupId)->number()) + 500);

    auto sig = dev::crypto::Sign(keyPair, tx.hash(WithoutSignature));
    tx.updateSignature(sig);

    auto rlp = tx.rlp();
    PLUGIN_LOG(INFO) << LOG_DESC("跨片交易生成完毕...")
                     << LOG_KV("rlp", toHex(rlp));

    // m_rpcService->sendRawTransaction(coorGroupId, toHex(rlp)); // 通过调用本地的RPC接口发起新的共识
    // PLUGIN_LOG(INFO) << LOG_DESC("发送完毕...");

    return toHex(rlp);
}

std::string transactionInjectionTest::createSpecialInnerTransactions(int32_t _groupId, 
                                                    std::shared_ptr<dev::ledger::LedgerManager> ledgerManager,
                                                    std::string state1, std::string state2) {
    
    std::string requestLabel = "0x444555666";
    std::string flag = "|";
    std::string stateAddress = state1 + "_" + state2;
    
    PLUGIN_LOG(INFO) << LOG_DESC("createInnerTransactions...")
                     << LOG_KV("stateAddress", stateAddress);

    // std::string hex_m_testdata_str = requestLabel + flag + std::to_string(sourceshardid) + flag + std::to_string(destinshardid)
    //                                     + flag + readwritekey + flag + requestmessageid + flag + std::to_string(coordinatorshardid);

    std::string hex_m_data_str = requestLabel + flag + stateAddress + flag;

    // 自己构造交易
    std::string str_address;
    if (_groupId == 1) {
        // PLUGIN_LOG(INFO) << LOG_DESC("GroupID为1...");
        str_address = innerContact_1;
    } else if (_groupId == 2) {
        // PLUGIN_LOG(INFO) << LOG_DESC("GroupID为2...");
        str_address = innerContact_2;
    } else if (_groupId == 3) {
        // PLUGIN_LOG(INFO) << LOG_DESC("GroupID为3...");
        str_address = innerContact_3;
    }
    dev::Address contactAddress(str_address);
    dev::eth::ContractABI abi;
    bytes data = abi.abiIn("add(string)", hex_m_data_str);  // add
    // bytes data = [];

    Transaction tx(0, 1000, 0, contactAddress, data);
    tx.setNonce(tx.nonce() + u256(utcTime()));
    tx.setGroupId(_groupId);
    tx.setBlockLimit(u256(ledgerManager->blockChain(_groupId)->number()) + 500);
    
    auto keyPair = KeyPair::create();
    auto sig = dev::crypto::Sign(keyPair, tx.hash(WithoutSignature));
    tx.updateSignature(sig);

    auto rlp = tx.rlp();
    PLUGIN_LOG(INFO) << LOG_DESC("交易生成完毕...")
                     << LOG_KV("rlp", toHex(rlp));

    // m_rpcService->sendRawTransaction(_groupId, toHex(rlp)); // 通过调用本地的RPC接口发起新的共识
    // PLUGIN_LOG(INFO) << LOG_DESC("发送完毕...");

    return toHex(rlp);
}

std::string transactionInjectionTest::createSpecialCrossTransactions(int32_t coorGroupId, 
                                                    int32_t subGroupId1, int32_t subGroupId2, 
                                                    std::shared_ptr<dev::ledger::LedgerManager> ledgerManager,
                                                    std::string state1, std::string state2) {
    std::string requestLabel = "0x111222333";
    std::string flag = "|";
    // std::string stateAddress = "state1";
    // srand((unsigned)time(0));

    std::string stateAddress1 = state1 + "_" + state2;
    std::string stateAddress2 = state1 + "_" + state2;


    // PLUGIN_LOG(INFO) << LOG_DESC("createCrossTransactions...")
    //                  << LOG_KV("stateAddress", stateAddress1)
    //                  << LOG_KV("stateAddress", stateAddress2);

    auto keyPair = KeyPair::create();

    // 生成子交易1
    std::string str_address;
    if (subGroupId1 == 1) {
        str_address = innerContact_1;
    } else if (subGroupId1 == 2) {
        str_address = innerContact_2;
    } else if (subGroupId1 == 3) {
        str_address = innerContact_3;
    }
    dev::Address subAddress1(str_address);
    dev::eth::ContractABI abi;
    bytes data = abi.abiIn("add(string)");  // add
    // bytes data = [];

    Transaction subTx1(0, 1000, 0, subAddress1, data);
    subTx1.setNonce(subTx1.nonce() + u256(utcTime()));
    subTx1.setGroupId(subGroupId1);
    subTx1.setBlockLimit(u256(ledgerManager->blockChain(dev::consensus::internal_groupId)->number()) + 500);

    
    auto subSig1 = dev::crypto::Sign(keyPair, subTx1.hash(WithoutSignature));
    subTx1.updateSignature(subSig1);

    auto subrlp1 = subTx1.rlp();
    std::string signTx1 = toHex(subrlp1);

    // 生成子交易2
    if (subGroupId2 == 1) {
        str_address = innerContact_1;
    } else if (subGroupId2 == 2) {
        str_address = innerContact_2;
    } else if (subGroupId2 == 3) {
        str_address = innerContact_3;
    }
    dev::Address subAddress2(str_address);
    // dev::eth::ContractABI abi;
    data = abi.abiIn("add(string)");  // sub
    // bytes data = [];

    Transaction subTx2(0, 1000, 0, subAddress2, data);
    subTx2.setNonce(subTx2.nonce() + u256(utcTime()));
    subTx2.setGroupId(subGroupId2);
    subTx2.setBlockLimit(u256(ledgerManager->blockChain(dev::consensus::internal_groupId)->number()) + 500);

    
    // auto keyPair = KeyPair::create();
    auto subSig2 = dev::crypto::Sign(keyPair, subTx2.hash(WithoutSignature));
    subTx2.updateSignature(subSig2);

    auto subrlp = subTx2.rlp();
    std::string signTx2 = toHex(subrlp);

    // 生成跨片交易
    std::string hex_m_data_str = requestLabel
                                + flag + std::to_string(subGroupId1) + flag + signTx1 + flag + stateAddress1 
                                + flag + std::to_string(subGroupId2) + flag + signTx2 + flag + stateAddress2
                                + flag;

    str_address = crossContact_3;
    dev::Address crossAddress(str_address);
    // dev::eth::ContractABI abi;
    data = abi.abiIn("set(string)", hex_m_data_str);  // set
    // bytes data = [];

    Transaction tx(0, 1000, 0, crossAddress, data);
    tx.setNonce(tx.nonce() + u256(utcTime()));
    tx.setGroupId(coorGroupId);
    tx.setBlockLimit(u256(ledgerManager->blockChain(coorGroupId)->number()) + 500);

    
    // auto keyPair = KeyPair::create();
    auto sig = dev::crypto::Sign(keyPair, tx.hash(WithoutSignature));
    tx.updateSignature(sig);

    auto rlp = tx.rlp();
    PLUGIN_LOG(INFO) << LOG_DESC("跨片交易生成完毕...")
                     << LOG_KV("rlp", toHex(rlp));

    // m_rpcService->sendRawTransaction(coorGroupId, toHex(rlp)); // 通过调用本地的RPC接口发起新的共识
    // PLUGIN_LOG(INFO) << LOG_DESC("发送完毕...");

    return toHex(rlp);
}

std::string transactionInjectionTest::createCrossTransactions_HB(int32_t coorGroupId, int32_t subGroupId1, int32_t subGroupId2, int32_t squId) {
    std::string requestLabel = "0x111222333";
    std::string flag = "|";
    std::string stateAddress = "0x362de179294eb3070a36d13ed00c61f59bcfb542_0x728a02ac510f6802813fece0ed12e7f774dab69d";
    auto keyPair = KeyPair::create();

    // 生成子交易1
    std::string str_address = "0x362de179294eb3070a36d13ed00c61f59bcfb542";
    dev::Address subAddress1(str_address);
    dev::eth::ContractABI abi;
    bytes data = abi.abiIn("add(string)");  // add

    Transaction subTx1(0, 1000, 0, subAddress1, data);
    subTx1.setNonce(subTx1.nonce() + u256(utcTime()));
    subTx1.setGroupId(subGroupId1);
    
    auto subSig1 = dev::crypto::Sign(keyPair, subTx1.hash(WithoutSignature));
    subTx1.updateSignature(subSig1);

    auto subrlp1 = subTx1.rlp();
    std::string signTx1 = toHex(subrlp1);

    // 生成子交易2
    str_address = "0x728a02ac510f6802813fece0ed12e7f774dab69d";
    dev::Address subAddress2(str_address);
    data = abi.abiIn("add(string)");  // add

    Transaction subTx2(0, 1000, 0, subAddress2, data);
    subTx2.setNonce(subTx2.nonce() + u256(utcTime()));
    subTx2.setGroupId(subGroupId2);
    
    auto subSig2 = dev::crypto::Sign(keyPair, subTx2.hash(WithoutSignature));
    subTx2.updateSignature(subSig2);

    auto subrlp = subTx2.rlp();
    std::string signTx2 = toHex(subrlp);


    // 生成跨片交易
    std::string hex_m_data_str = requestLabel + flag + std::to_string(squId)
                                + flag + std::to_string(subGroupId1) + flag + signTx1 + flag + stateAddress 
                                + flag + std::to_string(subGroupId2) + flag + signTx2 + flag + stateAddress;
    
    PLUGIN_LOG(INFO) << LOG_DESC("in createCrossTransactions_HB...")
                     << LOG_KV("dataStr", hex_m_data_str);
    
    str_address = "0xf9cd680d54778346cc0b018fb45fdaff031c0125";
    dev::Address crossAddress(str_address);
    data = abi.abiIn("set(string)", hex_m_data_str);  // set

    Transaction tx(0, 1000, 0, crossAddress, data);
    tx.setNonce(tx.nonce() + u256(utcTime()));
    tx.setGroupId(coorGroupId);
    
    auto sig = dev::crypto::Sign(keyPair, tx.hash(WithoutSignature));
    tx.updateSignature(sig);

    auto rlp = tx.rlp();
    PLUGIN_LOG(INFO) << LOG_DESC("跨片交易生成完毕...")
                     << LOG_KV("rlp", toHex(rlp));
    
    return toHex(rlp);
}

//bytes转string
string transactionInjectionTest::dataToHexString(bytes data)
{
    string res2 = "";
    string temp;
    stringstream ioss;

    int count = 0;
    for(auto const &ele:data)
    {
        count++;
        ioss << std::hex << ele;

        if(count > 30)
        {
            ioss >> temp;
            res2 += temp;
            temp.clear();
            ioss.clear();
            count = 0;
        }
    }
    ioss >> temp;
    res2 += temp;
    
    return res2;
}
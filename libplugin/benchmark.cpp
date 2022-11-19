#include "benchmark.h"
#include <libplugin/executeVM.h>
#include <libplugin/Common.h>
#include <librpc/Rpc.h>
#include <libdevcore/Address.h>
#include <libdevcore/CommonData.h>
#include <libethcore/ABI.h>

using namespace std;
using namespace dev::plugin;

void transactionInjectionTest::deployContractTransaction(std::string filename, int32_t groupId)
{
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
            // PLUGIN_LOG(INFO) << LOG_KV("signedTransaction", signedTransaction);
            signedTransactions.push_back(signedTransaction);
        }
    }
    infile.close();

    std::vector<std::string>::iterator iter;
    for(iter = signedTransactions.begin(); iter != signedTransactions.end(); iter++)
    {
        m_rpcService->sendRawTransaction(groupId, *iter);
        // std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    PLUGIN_LOG(INFO) << LOG_DESC("injectionTransactions交易发送完成...");
}

std::string transactionInjectionTest::createInnerTransactions(int32_t _groupId) {
    
    std::string requestLabel = "0x444555666";
    std::string flag = "|";
    std::string stateAddress = "state333";

    // std::string hex_m_testdata_str = requestLabel + flag + std::to_string(sourceshardid) + flag + std::to_string(destinshardid)
    //                                     + flag + readwritekey + flag + requestmessageid + flag + std::to_string(coordinatorshardid);

    std::string hex_m_data_str = requestLabel + flag + stateAddress + flag;

    /*
    auto data_str_bytes = hex_m_data_str.c_str();
    int bytelen = strlen(data_str_bytes);

    bytes hex_m_data;
    for(int i = 0; i < bytelen; i++)
    {
        hex_m_data.push_back((uint8_t)data_str_bytes[i]);
    }
    */

    // 自己构造交易
    std::string str_address;
    if (_groupId == 1) {
        PLUGIN_LOG(INFO) << LOG_DESC("GroupID为1...");
        str_address = innerContact_1;
    } else if (_groupId == 2) {
        PLUGIN_LOG(INFO) << LOG_DESC("GroupID为2...");
        str_address = innerContact_2;
    } else if (_groupId == 3) {
        PLUGIN_LOG(INFO) << LOG_DESC("GroupID为3...");
        str_address = innerContact_3;
    }
    dev::Address contactAddress(str_address);
    dev::eth::ContractABI abi;
    bytes data = abi.abiIn("add(string)", hex_m_data_str);  // add
    // bytes data = [];

    Transaction tx(0, 1000, 0, contactAddress, data);
    tx.setNonce(tx.nonce() + u256(utcTime()));
    tx.setGroupId(_groupId);
    
    auto keyPair = KeyPair::create();
    auto sig = dev::crypto::Sign(keyPair, tx.hash(WithoutSignature));
    tx.updateSignature(sig);

    auto rlp = tx.rlp();
    PLUGIN_LOG(INFO) << LOG_DESC("交易生成完毕...")
                     << LOG_KV("rlp", toHex(rlp));

    m_rpcService->sendRawTransaction(_groupId, toHex(rlp)); // 通过调用本地的RPC接口发起新的共识

    PLUGIN_LOG(INFO) << LOG_DESC("发送完毕...");

    return toHex(rlp);
}

std::string transactionInjectionTest::createCrossTransactions(int32_t coorGroupId, int32_t subGroupId1, int32_t subGroupId2) {
    std::string requestLabel = "0x111222333";
    std::string flag = "|";
    std::string stateAddress = "state1";
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
    
    // auto keyPair = KeyPair::create();
    auto subSig2 = dev::crypto::Sign(keyPair, subTx2.hash(WithoutSignature));
    subTx2.updateSignature(subSig2);

    auto subrlp = subTx2.rlp();
    std::string signTx2 = toHex(subrlp);


    // 生成跨片交易
    std::string hex_m_data_str = requestLabel
                                + flag + std::to_string(subGroupId1) + flag + signTx1 + flag + stateAddress 
                                + flag + std::to_string(subGroupId2) + flag + signTx2 + flag + stateAddress
                                + flag;

    
    str_address = crossContact_3;
    dev::Address crossAddress(str_address);
    // dev::eth::ContractABI abi;
    data = abi.abiIn("set(string)", hex_m_data_str);  // set
    // bytes data = [];

    Transaction tx(0, 1000, 0, crossAddress, data);
    tx.setNonce(tx.nonce() + u256(utcTime()));
    tx.setGroupId(coorGroupId);
    
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

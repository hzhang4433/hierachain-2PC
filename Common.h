#pragma once

#include "libplugin/benchmark.h"
#include <libinitializer/Initializer.h>
#include <libinitializer/P2PInitializer.h>
// #include 
using namespace dev;
using namespace dev::initializer;
using namespace dev::plugin;
using namespace std;


void putGroupPubKeyIntoshardNodeId(boost::property_tree::ptree const& _pt)
{
    size_t index = 0;
    for (auto it : _pt.get_child("group"))
    {
        if (it.first.find("groups.") == 0)
        {
            std::vector<std::string> s;
            try
            {
                boost::split(s, it.second.data(), boost::is_any_of(":"), boost::token_compress_on);

                // 对分片中的所有节点id进行遍历，加入到列表中
                size_t s_size = s.size();
                for(size_t i = 0; i < s_size - 1; i++)
                {
                    h512 node;
                    node = h512(s[i]);
                    dev::consensus::shardNodeId.push_back(node);

                    if(index % 4 == 0)
                    {
                        dev::consensus::forwardNodeId.push_back(node);
                    }
                    index++;
                }
            }
            catch (std::exception& e)
            {
                exit(1);
            }
        }
    }
}

void putGroupPubKeyIntoService(std::shared_ptr<Service> service, boost::property_tree::ptree const& _pt)
{
    std::map<GROUP_ID, h512s> groupID2NodeList;
    h512s nodelist;
    int groupid;
    for (auto it : _pt.get_child("group"))
    {
        if (it.first.find("groups.") == 0)
        {
            std::vector<std::string> s;
            try
            {
                boost::split(s, it.second.data(), boost::is_any_of(":"), boost::token_compress_on);

                // 对分片中的所有节点id进行遍历，加入到列表中
                int s_size = s.size();
                for(int i = 0; i < s_size - 1; i++)
                {
                    h512 node;
                    node = h512(s[i]);
                    nodelist.push_back(node);
                }
                groupid = (int)((s[s_size - 1])[0] - '0');  // 放到同一个总的groupid中
            }
            catch (std::exception& e)
            {
                exit(1);
            }
        }
    }
    groupID2NodeList.insert(std::make_pair(groupid, nodelist)); // 都是同一个groupid，所以插入一次就好了
    std::cout << groupID2NodeList[groupid] << std::endl;
    service->setGroupID2NodeList(groupID2NodeList);
}

class GroupP2PService
{
public:
    GroupP2PService(std::string const& _path)
    {
        boost::property_tree::ptree pt;
        boost::property_tree::read_ini(_path, pt);
        m_secureInitializer = std::make_shared<SecureInitializer>();
        m_secureInitializer->initConfig(pt);
        m_p2pInitializer = std::make_shared<P2PInitializer>();
        m_p2pInitializer->setSSLContext(m_secureInitializer->SSLContext(SecureInitializer::Usage::ForP2P));
        m_p2pInitializer->setKeyPair(m_secureInitializer->keyPair());
        m_p2pInitializer->initConfig(pt);
    }
    P2PInitializer::Ptr p2pInitializer() { return m_p2pInitializer; }
    ~GroupP2PService()
    {
        if (m_p2pInitializer)
        {
            m_p2pInitializer->stop();
        }
    }

private:
    P2PInitializer::Ptr m_p2pInitializer;
    SecureInitializer::Ptr m_secureInitializer;
};

namespace dev
{
    namespace plugin
    {
      std::vector<bool> flags;
    }
    
}

void initVariables() {
  // 对dev::consensus::messageIDs进行初始化
    for(int i = 0; i < dev::consensus::hiera_shard_number; i++)
    {
        dev::consensus::messageIDs.insert(std::make_pair(i + 1, 0));
        dev::plugin::flags.push_back(true);
        // 对dev::consensus::latest_commit_cs_tx进行初始化
        dev::blockverifier::latest_commit_cs_tx.push_back(0);
    }
    // 对latest_candidate_tx_messageids进行初始化
    latest_candidate_tx_messageids = std::make_shared<tbb::concurrent_vector<unsigned long>>(dev::consensus::hiera_shard_number);
    // ADD ON 22.11.16
    current_candidate_tx_messageids = std::make_shared<tbb::concurrent_vector<unsigned long>>(dev::consensus::hiera_shard_number);
    // ADD ON 22.11.20
    complete_candidate_tx_messageids = std::make_shared<tbb::concurrent_vector<unsigned long>>(dev::consensus::hiera_shard_number);
}

/*
status:
    0 ==> 一次生成全部交易
    1 ==> 生成部分交易，这次为第一次
    2 ==> 生成部分交易，这次为中间一次
    3 ==> 生成部分交易，这次为最后一次
*/ 
void createInnerTransaction(int groupId, shared_ptr<dev::ledger::LedgerManager> ledgerManager, int txNum, 
                            string fileName, int status, transactionInjectionTest _injectionTest) 
{
    // 批量生产片内交易
    cout << "createInnerTransaction, txNum = " << txNum << ", status = " << status << endl;
    std::string res;
    for (int i =0 ; i < txNum; i++) {
        auto tx = _injectionTest.createInnerTransactions(groupId);
        if (i % 100 == 0) { // 100笔写一次文件
            if ((status == 0 || status == 1) && i == 0) {
                res = "[\"" + tx + "\"";
            } else {
                ofstream out;
                out.open(fileName, ios::in|ios::out|ios::app);
                if (out.is_open()) {
                    out << res;
                    out.close();
                    res = "";
                }
                res = ",\"" + tx + "\"";
            }
        } else {
            res = res + ",\"" + tx + "\"";
        }
        std::this_thread::sleep_for(std::chrono::milliseconds((1)));
    }
    
    ofstream out;
    out.open(fileName, ios::in|ios::out|ios::app);
    if (out.is_open()) {
        if (status == 0 || status == 3) {
            res += "]";
        }
        out << res;
        out.close();
    }
    
}

void createCrossTransaction(int cor, int sub1, int sub2, shared_ptr<dev::ledger::LedgerManager> ledgerManager, int txNum, string fileName, int status, transactionInjectionTest _injectionTest) 
{    
    // 批量生产跨片交易
    cout << "createCrossTransaction, txNum = " << txNum << ", status = " << status << endl;

    std::string res;
    for (int i =0 ; i < txNum; i++) {
        auto tx = _injectionTest.createCrossTransactions(cor, sub1, sub2);
        if (i % 100 == 0) { // 100笔写一次文件
            if ((status == 0 || status == 1) && i == 0) {
                res = "[\"" + tx + "\"";
            } else {
                ofstream out;
                out.open(fileName, ios::in|ios::out|ios::app);
                if (out.is_open()) {
                    out << res;
                    out.close();
                    res = "";
                }
                res = ",\"" + tx + "\"";
            }
        } else {
            res = res + ",\"" + tx + "\"";
        }
        std::this_thread::sleep_for(std::chrono::milliseconds((1)));
    }

    ofstream out;
    out.open(fileName, ios::in|ios::out|ios::app);
    if (out.is_open()) {
        if (status == 0 || status == 3) {
            res += "]";
        }
        out << res;
        out.close();
    }
    
}

// 两个及两个以上子分片
void createCrossTransaction(int cor, vector<int>& shardIds, shared_ptr<dev::ledger::LedgerManager> ledgerManager, 
                            int txNum, string fileName, int status, transactionInjectionTest _injectionTest) 
{    
    // 批量生产跨片交易
    cout << "createCrossTransaction, txNum = " << txNum << ", status = " << status << endl;

    std::string res;
    for (int i =0 ; i < txNum; i++) {
        auto tx = _injectionTest.createCrossTransactions(cor, shardIds);
        if (i % 100 == 0) { // 100笔写一次文件
            if ((status == 0 || status == 1) && i == 0) {
                res = "[\"" + tx + "\"";
            } else {
                ofstream out;
                out.open(fileName, ios::in|ios::out|ios::app);
                if (out.is_open()) {
                    out << res;
                    out.close();
                    res = "";
                }
                res = ",\"" + tx + "\"";
            }
        } else {
            res = res + ",\"" + tx + "\"";
        }
        std::this_thread::sleep_for(std::chrono::milliseconds((1)));
    }

    ofstream out;
    out.open(fileName, ios::in|ios::out|ios::app);
    if (out.is_open()) {
        if (status == 0 || status == 3) {
            res += "]";
        }
        out << res;
        out.close();
    }
    
}

// 无status参数
void createCrossTransaction(int cor, int sub1, int sub2, shared_ptr<dev::ledger::LedgerManager> ledgerManager, int txNum, string fileName, transactionInjectionTest _injectionTest) 
{    
    // std::cout << "in new createCrossTransaction txNum = " << txNum << std::endl;
    // 批量生产跨片交易
    std::string res;
    for (int i =0; i < txNum; i++) {
        auto tx = _injectionTest.createCrossTransactions(cor, sub1, sub2);
        if (i % 100 == 0) { // 100笔写一次文件
            if (i == 0) { // 根据文件之前写入情况设置第一个字符
                if (flags[cor - 1]) {
                    res = "[\"" + tx + "\"";
                } else {
                    res = ",\"" + tx + "\"";
                }
            } else if (i == 100) { // 第一次写入，或需覆盖
                // std::cout << "flags" << cor - 1 << ":" << flags[cor - 1] << std::endl;
                if (flags[cor - 1]) { // 文件之前未被写过
                    ofstream out;
                    out.open(fileName, ios::in|ios::out|ios::app);
                    if (out.is_open()) {
                        out << res;
                        out.close();
                    }
                    flags[cor - 1] = false;
                } else { // 文件之前被写过
                    // std::cout << "开始覆盖写" << std::endl;
                    ofstream out;
                    out.open(fileName, ios::binary | ios::out | ios::in);
                    if (out.is_open()) {
                        out.seekp(-1, ios::end);
                        out << res;
                        out.close();
                    }
                }
                res = ",\"" + tx + "\"";
            } else { // 不是第一次写，直接续写无需覆盖
                ofstream out;
                out.open(fileName, ios::in|ios::out|ios::app);
                if (out.is_open()) {
                    out << res;
                    out.close();
                }
                res = ",\"" + tx + "\"";
            }
        } else {
            res = res + ",\"" + tx + "\"";
        }
        std::this_thread::sleep_for(std::chrono::milliseconds((1)));
    }

    // 收尾工作
    ofstream out;
    res += "]";
    if (txNum == 100) {
        out.open(fileName, ios::binary | ios::out | ios::in);
        if (out.is_open()) {
            out.seekp(-1, ios::end);
            out << res;
            out.close();
        }
    } else {
        out.open(fileName, ios::in|ios::out|ios::app);
        if (out.is_open()) {
            out << res;
            out.close();
        }
    }
    
}

void createDataSet(int cor, int sub1, int sub2, std::shared_ptr<dev::ledger::LedgerManager> ledgerManager,
                   int txNum, int percent, std::shared_ptr<dev::rpc::Rpc> rpcService) {
    // 计算跨片交易及各分片的片内交易数
    string fileName;
    int crossTxNum = (txNum * percent) / 100;
    int innerTxNum = (txNum - crossTxNum) / 3;
    int remain = (txNum - crossTxNum) % 3;
    transactionInjectionTest _injectionTest(rpcService, 1);

    if (percent == 20) {
        fileName = "workload1.json";
    } else if (percent == 50) {
        fileName = "workload2.json";
    } else if (percent == 80) {
        fileName = "workload3.json";
    } else if (percent == 100) {
        fileName = "workload4.json";
    }

    // 生成子分片1的片内交易
    if(innerTxNum != 0 && dev::consensus::internal_groupId == sub1 && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(sub1 - 1)))
    {
        createInnerTransaction(sub1, ledgerManager, innerTxNum, fileName, 0, _injectionTest);
    }

    // 生成子分片2的片内交易
    if(innerTxNum != 0 && dev::consensus::internal_groupId == sub2 && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(sub2 - 1)))
    {
        createInnerTransaction(sub2, ledgerManager, innerTxNum, fileName, 0, _injectionTest);
    }

    // 生成协调者分片的片内交易和跨片交易
    cout << "fileName:" << fileName << endl;
    if(dev::consensus::internal_groupId == cor && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(cor - 1)))
    {
        int nowInnerNum = innerTxNum + remain;
        int nowCrossNum = crossTxNum;
        if (nowInnerNum == 0) { //纯跨片交易 直接生成返回
            createCrossTransaction(cor, sub1, sub2, ledgerManager, nowCrossNum, fileName, 0, _injectionTest);
            return;
        }
        bool firstTime = true;
        while(nowInnerNum != 0 || nowCrossNum != 0) {
            if (nowInnerNum == 0) {
                createCrossTransaction(cor, sub1, sub2, ledgerManager, nowCrossNum, fileName, 3, _injectionTest);
                break;
            } else if (nowCrossNum == 0) {
                createInnerTransaction(cor, ledgerManager, nowInnerNum, fileName, 3, _injectionTest);
                break;
            }
            // srand((unsigned)time(NULL));
            int flag = rand() % 2;
            if (flag) { // 为1 生成跨片交易
                cout << "createDataSet 111" << endl;
                if (nowCrossNum < 100) {
                    // cout << "createDataSet 222" << endl;
                    if (firstTime == true) {
                        // cout << "进来了 firstTime = true" << endl; 
                        createCrossTransaction(cor, sub1, sub2, ledgerManager, nowCrossNum, fileName, 1, _injectionTest);
                        firstTime = false;
                    } else {
                        createCrossTransaction(cor, sub1, sub2, ledgerManager, nowCrossNum, fileName, 2, _injectionTest);
                    }
                    nowCrossNum = 0;
                } else {
                    // cout << "createDataSet 333" << endl;
                    cout << "firstTime:" << firstTime << endl;
                    if (firstTime == true) { // 第一次
                        // cout << "进来了 firstTime = true" << endl; 
                        createCrossTransaction(cor, sub1, sub2, ledgerManager, 100, fileName, 1, _injectionTest);
                        firstTime = false;
                    } else {
                        createCrossTransaction(cor, sub1, sub2, ledgerManager, 100, fileName, 2, _injectionTest);
                    }
                    nowCrossNum -= 100;
                }
            } else { // 为0 生成片内交易
                cout << "createDataSet 444" << endl;
                if (nowInnerNum < 100) {
                    // cout << "createDataSet 555" << endl;
                    if (firstTime == true) {
                        // cout << "进来了 firstTime = true" << endl; 
                        createInnerTransaction(cor, ledgerManager, nowInnerNum, fileName, 1, _injectionTest);
                        firstTime = false;
                    } else {
                        createInnerTransaction(cor, ledgerManager, nowInnerNum, fileName, 2, _injectionTest);
                    }
                    nowInnerNum = 0;
                } else {
                    // cout << "createDataSet 666" << endl;
                    // cout << "firstTime:" << firstTime << endl;
                    if (firstTime == true) {
                        // cout << "进来了 firstTime = true" << endl; 
                        createInnerTransaction(cor, ledgerManager, 100, fileName, 1, _injectionTest);
                        firstTime = false;
                    } else {
                        createInnerTransaction(cor, ledgerManager, 100, fileName, 2, _injectionTest);
                    }
                    nowInnerNum -= 100;
                }
            }
        }
    }
}

void createHBDataSet(int cor, int sub1, int sub2, std::shared_ptr<dev::ledger::LedgerManager> ledgerManager,
                     std::shared_ptr<dev::rpc::Rpc> rpcService) {
    transactionInjectionTest _injectionTest(rpcService, 1);
    std::string res = "";
    std::string fileName = "workload.json";
    std::string tx = "";
    // 分片1
    if(dev::consensus::internal_groupId == sub1 && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(sub1 - 1)))
    {
        for (int i = 0 ; i < 1000; i++) {
            if (i < 500) {
                tx = _injectionTest.createSpecialInnerTransactions(sub1, "stateA", "stateC");
            } else {
                tx = _injectionTest.createSpecialInnerTransactions(sub1, "stateA", "stateD");
            }
            if (i == 0) {
                res = "[\"" + tx + "\"";
            } else {
                res += ",\"" + tx + "\"";
            }
            std::this_thread::sleep_for(std::chrono::milliseconds((1)));
        }
        
        ofstream out;
        out.open(fileName, ios::in|ios::out|ios::app);
        if (out.is_open()) {
            res += "]";
            out << res;
            out.close();
        }
    }

    // 分片2
    res = "";
    if(dev::consensus::internal_groupId == sub2 && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(sub2 - 1)))
    {
        for (int i =0 ; i < 1000; i++) {
            if (i < 100) {
                tx = _injectionTest.createSpecialInnerTransactions(sub2, "stateE", "stateG");
            } else if(i < 200) {
                tx = _injectionTest.createSpecialInnerTransactions(sub2, "stateF", "stateH");
            } else {
                tx = _injectionTest.createSpecialInnerTransactions(sub2, "stateG", "stateH");
            }
            if (i == 0) {
                res = "[\"" + tx + "\"";
            } else {
                res += ",\"" + tx + "\"";
            }
            std::this_thread::sleep_for(std::chrono::milliseconds((1)));
        }
        
        ofstream out;
        out.open(fileName, ios::in|ios::out|ios::app);
        if (out.is_open()) {
            res += "]";
            out << res;
            out.close();
        }
    }

    // 分片3
    res = "";
    if(dev::consensus::internal_groupId == cor && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(cor - 1)))
    {
        for (int i =0 ; i < 1000; i++) {
            if (i < 500) {
                tx = _injectionTest.createSpecialCrossTransactions(cor, sub1, sub2, "stateA", "stateE");
            } else {
                tx = _injectionTest.createSpecialCrossTransactions(cor, sub1, sub2, "stateB", "stateF");
            }
            if (i == 0) {
                res = "[\"" + tx + "\"";
            } else {
                res += ",\"" + tx + "\"";
            }
            std::this_thread::sleep_for(std::chrono::milliseconds((1)));
        }
        
        ofstream out;
        out.open(fileName, ios::in|ios::out|ios::app);
        if (out.is_open()) {
            res += "]";
            out << res;
            out.close();
        }
    }
}

void createDataSet(int cor, vector<int>& shardIds, std::shared_ptr<dev::ledger::LedgerManager> ledgerManager,
                   int txNum, int percent, std::shared_ptr<dev::rpc::Rpc> rpcService) {
    // 计算跨片交易及各分片的片内交易数
    string fileName;
    int subShardNum = shardIds.size() + 1;
    int crossTxNum = (txNum * percent) / 100;
    int innerTxNum = (txNum - crossTxNum) / subShardNum;
    int remain = (txNum - crossTxNum) % subShardNum;
    transactionInjectionTest _injectionTest(rpcService, 1);

    if (percent == 20) {
        fileName = "workload1.json";
    } else if (percent == 80) {
        fileName = "workload2.json";
    } else if (percent == 100) {
        fileName = "workload3.json";
    }

    // 生成子分片的片内交易
    for (int i = 0; i < subShardNum; i++) {
        int subId = shardIds[i];
        if(innerTxNum != 0 && dev::consensus::internal_groupId == subId && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(subId - 1)))
        {
            createInnerTransaction(subId, ledgerManager, innerTxNum, fileName, 0, _injectionTest);
        }
    }

    // 生成协调者分片的片内交易和跨片交易
    cout << "fileName:" << fileName << endl;
    if(dev::consensus::internal_groupId == cor && nodeIdStr == toHex(dev::consensus::forwardNodeId.at(cor - 1)))
    {
        int nowInnerNum = innerTxNum + remain;
        int nowCrossNum = crossTxNum;
        if (nowInnerNum == 0) { //纯跨片交易 直接生成返回
            createCrossTransaction(cor, shardIds, ledgerManager, nowCrossNum, fileName, 0, _injectionTest);
            return;
        }
        bool firstTime = true;
        while(nowInnerNum != 0 || nowCrossNum != 0) {
            if (nowInnerNum == 0) {
                createCrossTransaction(cor, shardIds, ledgerManager, nowCrossNum, fileName, 3, _injectionTest);
                break;
            } else if (nowCrossNum == 0) {
                createInnerTransaction(cor, ledgerManager, nowInnerNum, fileName, 3, _injectionTest);
                break;
            }
            // srand((unsigned)time(NULL));
            int flag = rand() % 2;
            if (flag) { // 为1 生成跨片交易
                cout << "createDataSet 111" << endl;
                if (nowCrossNum < 100) {
                    // cout << "createDataSet 222" << endl;
                    if (firstTime == true) {
                        // cout << "进来了 firstTime = true" << endl; 
                        createCrossTransaction(cor, shardIds, ledgerManager, nowCrossNum, fileName, 1, _injectionTest);
                        firstTime = false;
                    } else {
                        createCrossTransaction(cor, shardIds, ledgerManager, nowCrossNum, fileName, 2, _injectionTest);
                    }
                    nowCrossNum = 0;
                } else {
                    // cout << "createDataSet 333" << endl;
                    cout << "firstTime:" << firstTime << endl;
                    if (firstTime == true) { // 第一次
                        // cout << "进来了 firstTime = true" << endl; 
                        createCrossTransaction(cor, shardIds, ledgerManager, 100, fileName, 1, _injectionTest);
                        firstTime = false;
                    } else {
                        createCrossTransaction(cor, shardIds, ledgerManager, 100, fileName, 2, _injectionTest);
                    }
                    nowCrossNum -= 100;
                }
            } else { // 为0 生成片内交易
                cout << "createDataSet 444" << endl;
                if (nowInnerNum < 100) {
                    // cout << "createDataSet 555" << endl;
                    if (firstTime == true) {
                        // cout << "进来了 firstTime = true" << endl; 
                        createInnerTransaction(cor, ledgerManager, nowInnerNum, fileName, 1, _injectionTest);
                        firstTime = false;
                    } else {
                        createInnerTransaction(cor, ledgerManager, nowInnerNum, fileName, 2, _injectionTest);
                    }
                    nowInnerNum = 0;
                } else {
                    // cout << "createDataSet 666" << endl;
                    // cout << "firstTime:" << firstTime << endl;
                    if (firstTime == true) {
                        // cout << "进来了 firstTime = true" << endl; 
                        createInnerTransaction(cor, ledgerManager, 100, fileName, 1, _injectionTest);
                        firstTime = false;
                    } else {
                        createInnerTransaction(cor, ledgerManager, 100, fileName, 2, _injectionTest);
                    }
                    nowInnerNum -= 100;
                }
            }
        }
    }
}

void swap(int &a, int &b) {
    a ^= b;       //a=a^b
    b ^= a;      //b=b^(a^b)=b^a^b=b^b^a=0^a=a
    a ^= b;     //a=(a^b)^a=a^b^a=a^a^b=0^b=b
}

int getLCA(int subId1, int subId2) {
    if ((subId1 <= 3 || subId1 == 7) && (subId2 <=3 || subId2 == 7)) {
        return 7;
    } else if (((subId1 >= 4 && subId1 <= 6) || subId1 == 8) && ((subId2 >= 4 && subId2 <= 6) || subId2 == 8)) {
        return 8;
    }
    return 9;
}

int createCrossId(int id) {
    srand((unsigned)time(NULL));
    int newId;

    if (id <= 3) {
        // 范围:[4-6,8,9]
        newId = (rand() % 5) + 4;
        if (newId >= 7) {
            newId++;
        }
    } else if (id <= 6) {
        // 范围:[1-3,7,9]
        newId = (rand() % 5) + 1;
        if (newId == 4) {
            newId = 7;
        } else if (newId == 5) {
            newId = 9;
        }
    } else if (id == 7) {
        // 范围:[4-6]
        newId = (rand() % 3) + 4;
    } else if (id == 8) {
        // 范围:[1-3]
        newId = (rand() % 3) + 1;
    } else if (id ==9) {
        // 范围:[1-6]
        newId = (rand() % 6) + 1;
    }
    return newId;
}

void createRandomDataSet(std::shared_ptr<dev::ledger::LedgerManager> ledgerManager, int txNum, int percent, std::shared_ptr<dev::rpc::Rpc> rpcService) {
    // 计算跨片交易及各分片的片内交易数
    string fileName, newfileName;
    int shardsNum = dev::consensus::hiera_shard_number;
    int crossTxNum = (txNum * percent) / 100;
    int innerTxNum = (txNum - crossTxNum) / shardsNum;
    int remain = (txNum - crossTxNum) % shardsNum;
    crossTxNum += remain;
    transactionInjectionTest _injectionTest(rpcService, 1);
    if (percent == 0) {
        fileName = "workload0.json";
    } else if (percent == 20) {
        fileName = "workload1.json";
    } else if (percent == 40) {
        fileName = "workload2.json";
    } else if (percent == 60) {
        fileName = "workload3.json";
    } else if (percent == 80) {
        fileName = "workload4.json";
    } else if (percent == 100) {
        fileName = "workload5.json";
    }

    // 生成子分片的片内交易
    if (innerTxNum != 0) {
        for (int i = 1; i <= shardsNum; i++) {
            flags[i - 1] = false;
            newfileName = "../node" + to_string((i-1)*4) + "/" + fileName;
            // std::cout << "fileName: " << newfileName << std::endl;
            createInnerTransaction(i, ledgerManager, innerTxNum, newfileName, 0, _injectionTest);
        }
    }

    // 生成随机跨片交易
    int nowCrossNum = crossTxNum;
    srand((unsigned)time(NULL));
    while(nowCrossNum != 0) {
        // 生成随机两个子分片ID 范围:[1,8]
        // 默认各个节点生成的随机数顺序一致
        int subId1 = (rand() % (shardsNum - 1)) + 1;
        int subId2 = (rand() % (shardsNum - 1)) + 1;
        while (subId2 == subId1) {
            subId2 = (rand() % (shardsNum - 1)) + 1;
        }
        // 查找最近公共祖先
        int corId = getLCA(subId1, subId2);
        std::cout << corId << ":" << subId1 << "-" << subId2 << std::endl;
        // 生成交易
        newfileName = "../node" + to_string((corId-1)*4) + "/" + fileName;
        if (nowCrossNum < 500) {
            createCrossTransaction(corId, subId1, subId2, ledgerManager, nowCrossNum, newfileName, _injectionTest);
            nowCrossNum = 0;
        } else {
            createCrossTransaction(corId, subId1, subId2, ledgerManager, 500, newfileName, _injectionTest);
            nowCrossNum -= 500;
        }
    }
}

// 生成局部性负载
void createLocalityDataSet(std::shared_ptr<dev::ledger::LedgerManager> ledgerManager, int txNum, int percent, std::shared_ptr<dev::rpc::Rpc> rpcService) {
    // 计算跨片交易及各分片的片内交易数
    string fileName, newfileName;
    int shardsNum = dev::consensus::hiera_shard_number;
    int crossTxNum = (txNum * percent) / 100;
    int innerTxNum = (txNum - crossTxNum) / shardsNum;
    transactionInjectionTest _injectionTest(rpcService, 1);
    if (percent == 0) {
        fileName = "workload0.json";
    } else if (percent == 20) {
        fileName = "workload1.json";
    } else if (percent == 50) {
        fileName = "workload2.json";
    } else if (percent == 80) {
        fileName = "workload3.json";
    } else if (percent == 100) {
        fileName = "workload4.json";
    }

    // 生成子分片的片内交易
    if (innerTxNum != 0) {
        for (int i = 1; i <= shardsNum; i++) {
            flags[i - 1] = false;
            newfileName = "../node" + to_string((i-1)*4) + "/" + fileName;
            createInnerTransaction(i, ledgerManager, innerTxNum, newfileName, 0, _injectionTest);
        }
    }

    // 生成局部性跨片交易
    int allCrossNum = crossTxNum;
    // 跨层交易和局部性交易数
    int otherCrossNum = crossTxNum / 21;
    int localityCrossNum = otherCrossNum * 20;
    // 上层局部性交易数
    int upperCrossNum = localityCrossNum / 5;
    // 下层局部性交易数
    int downCrossNum = localityCrossNum / 5 * 2;
    
    // 生成局部性交易
    srand((unsigned)time(NULL));
    int nowCrossNum = upperCrossNum;
    std::cout << "开始生成上层局部性交易：" << std::endl;
    while(nowCrossNum != 0) {
        // 生成随机两个子分片ID 范围:[7,9]
        // 默认各个节点生成的随机数顺序一致
        int subId1 = (rand() % (9 - 7 + 1)) + 7;
        int subId2 = (rand() % (9 - 7 + 1)) + 7;
        while (subId2 == subId1) {
            subId2 = (rand() % (9 - 7 + 1)) + 7;
        }
        if (subId1 > subId2) {
            swap(subId1, subId2);
        }
        // 查找最近公共祖先
        int corId = 9;
        std::cout << corId << ":" << subId1 << "-" << subId2 << std::endl;
        // 生成交易
        newfileName = "../node" + to_string((corId-1)*4) + "/" + fileName;
        if (nowCrossNum < 500) {
            createCrossTransaction(corId, subId1, subId2, ledgerManager, nowCrossNum, newfileName, _injectionTest);
            nowCrossNum = 0;
        } else {
            createCrossTransaction(corId, subId1, subId2, ledgerManager, 500, newfileName, _injectionTest);
            nowCrossNum -= 500;
        }
    }

    std::cout << "开始生成下层局部性交易：" << std::endl;
    for (int i = 1; i < 7; i += 3) {
        nowCrossNum = downCrossNum;
        while(nowCrossNum != 0) {
            // 生成随机两个子分片ID 范围:[1,4] / [4,7] == [i, i+3]
            // 默认各个节点生成的随机数顺序一致
            int subId1 = (rand() % 4) + i;
            int subId2 = (rand() % 4) + i;
            while (subId2 == subId1) {
                subId2 = (rand() % 4) + i;
            }
            if (i == 1 && subId1 == 4) {
                subId1 = 7;
            } else if (i == 1 && subId2 == 4) {
                subId2 = 7;
            } else if (subId1 == 7) {
                subId1 = 8;
            } else if (subId2 == 7) {
                subId2 = 8;
            }

            if (subId1 > subId2) {
                swap(subId1, subId2);
            }

            int corId;
            if (i == 1) {
                corId = 7;
            } else if (i == 4) {
                corId = 8;
            }
            
            std::cout << corId << ":" << subId1 << "-" << subId2 << std::endl;
            // 生成交易
            newfileName = "../node" + to_string((corId-1)*4) + "/" + fileName;
            if (nowCrossNum < 500) {
                createCrossTransaction(corId, subId1, subId2, ledgerManager, nowCrossNum, newfileName, _injectionTest);
                nowCrossNum = 0;
            } else {
                createCrossTransaction(corId, subId1, subId2, ledgerManager, 500, newfileName, _injectionTest);
                nowCrossNum -= 500;
            }
        }
    }

    // 生成跨层交易
    nowCrossNum = otherCrossNum;
    std::cout << "开始生成跨层级交易：" << std::endl;
    while(nowCrossNum != 0) {
        // 生成随机两个子分片ID 范围:[1,9]
        // 默认各个节点生成的随机数顺序一致
        int subId1 = (rand() % shardsNum) + 1;
        int subId2 = createCrossId(subId1);
        if (subId1 > subId2) {
            swap(subId1, subId2);
        }

        // 查找最近公共祖先
        int corId = getLCA(subId1, subId2);
        std::cout << corId << ":" << subId1 << "-" << subId2 << std::endl;
        // 生成交易
        newfileName = "../node" + to_string((corId-1)*4) + "/" + fileName;
        if (nowCrossNum < 500) {
            createCrossTransaction(corId, subId1, subId2, ledgerManager, nowCrossNum, newfileName, _injectionTest);
            nowCrossNum = 0;
        } else {
            createCrossTransaction(corId, subId1, subId2, ledgerManager, 500, newfileName, _injectionTest);
            nowCrossNum -= 500;
        }
    }

}

// 生成纯跨层交易
// 9分片
// void createCrossLayerDataSet(std::shared_ptr<dev::ledger::LedgerManager> ledgerManager, int txNum, std::shared_ptr<dev::rpc::Rpc> rpcService) {
//     // 计算跨片交易及各分片的片内交易数
//     string fileName, newfileName;
//     int shardsNum = dev::consensus::hiera_shard_number;
//     transactionInjectionTest _injectionTest(rpcService, 1);
    
//     fileName = "crosslayerworkload.json";

//     // 生成跨层交易
//     int nowCrossNum = txNum;
//     std::cout << "开始生成跨层级交易：" << std::endl;
//     while(nowCrossNum != 0) {
//         // 生成随机两个子分片ID 范围:[1,9]
//         // 默认各个节点生成的随机数顺序一致
//         int subId1 = (rand() % shardsNum) + 1;
//         int subId2 = createCrossId(subId1);
//         if (subId1 > subId2) {
//             swap(subId1, subId2);
//         }

//         // 查找最近公共祖先
//         int corId = 9;
//         std::cout << corId << ":" << subId1 << "-" << subId2 << std::endl;
//         // 生成交易
//         newfileName = "../node" + to_string((corId-1)*4) + "/" + fileName;
//         if (nowCrossNum < 500) {
//             createCrossTransaction(corId, subId1, subId2, ledgerManager, nowCrossNum, newfileName, _injectionTest);
//             nowCrossNum = 0;
//         } else {
//             createCrossTransaction(corId, subId1, subId2, ledgerManager, 500, newfileName, _injectionTest);
//             nowCrossNum -= 500;
//         }
//     }
// }

int createCrossId_13(int id) {
    srand((unsigned)time(NULL));
    int newId = 0;

    if (id <= 3) {
        // 范围:[4-9, 11-12]
        newId = (rand() % 8) + 4;
        if (newId >= 10) {
            newId++;
        }
    } else if (id <= 6) {
        // 范围:[1-3, 7-9, 10, 12]
        newId = (rand() % 8) + 1;
        if (newId == 4) {
            newId = 10;
        } else if (newId ==5) {
            newId = 11;
        } else if (newId >= 6) {
            newId++;
        }
    } else if (id <= 9) {
        // 范围:[1-6, 10-11]
        newId = (rand() % 8) + 1;
        if (newId >= 7) {
            newId += 3;
        }
    } else if (id == 10) {
        // 范围:[4-9]
        newId = (rand() % 6) + 4;
    } else if (id == 11) {
        // 范围:[1-3, 7-9]
        newId = (rand() % 6) + 1;
        if (newId >= 4) {
            newId += 3;
        }
    } else if (id == 12) {
        // 范围:[1-6]
        newId = (rand() % 6) + 1;
    }
    return newId;
}

// 13分片 
void createCrossLayerDataSet(std::shared_ptr<dev::ledger::LedgerManager> ledgerManager, int txNum, std::shared_ptr<dev::rpc::Rpc> rpcService) {
    // 计算跨片交易及各分片的片内交易数
    string fileName, newfileName;
    int shardsNum = dev::consensus::hiera_shard_number;
    transactionInjectionTest _injectionTest(rpcService, 1);
    
    fileName = "crosslayerworkload.json";

    // 生成跨层交易
    int nowCrossNum = txNum;
    std::cout << "开始生成跨层级交易：" << std::endl;
    while(nowCrossNum != 0) {
        // 生成随机两个子分片ID 范围:[1,12]
        // 默认各个节点生成的随机数顺序一致
        int subId1 = (rand() % 12) + 1;
        int subId2 = createCrossId_13(subId1);
        if (subId1 > subId2) {
            swap(subId1, subId2);
        }

        // 查找最近公共祖先
        int corId = 13;
        std::cout << corId << ":" << subId1 << "-" << subId2 << std::endl;
        // 生成交易
        newfileName = "../node" + to_string((corId-1)*4) + "/" + fileName;
        if (nowCrossNum < 500) {
            createCrossTransaction(corId, subId1, subId2, ledgerManager, nowCrossNum, newfileName, _injectionTest);
            nowCrossNum = 0;
        } else {
            createCrossTransaction(corId, subId1, subId2, ledgerManager, 500, newfileName, _injectionTest);
            nowCrossNum -= 500;
        }
    }
}

// 生成纯片内交易
void createIntrashardDataSet(std::shared_ptr<dev::ledger::LedgerManager> ledgerManager, int txNum, std::shared_ptr<dev::rpc::Rpc> rpcService) {
    // 计算跨片交易及各分片的片内交易数
    string fileName, newfileName;
    int shardsNum = dev::consensus::hiera_shard_number;
    transactionInjectionTest _injectionTest(rpcService, dev::consensus::internal_groupId);
    fileName = "intrashardworkload.json";
    // 生成子分片的片内交易
    createInnerTransaction(dev::consensus::internal_groupId, ledgerManager, txNum, fileName, 0, _injectionTest);
}

// 生成纯局部性负载
// 3分片
void createIntershardDataSet(std::shared_ptr<dev::ledger::LedgerManager> ledgerManager, int txNum, std::shared_ptr<dev::rpc::Rpc> rpcService) {
    string fileName, newfileName;
    int shardsNum = dev::consensus::hiera_shard_number;
    transactionInjectionTest _injectionTest(rpcService, 1);
    fileName = "intershardworkload.json";
    
    // 生成跨片交易
    std::cout << "开始生成跨片交易：" << std::endl;
    
    int nowCrossNum = txNum;
    while(nowCrossNum != 0) {
        // 生成随机两个子分片ID 范围:[1,3]
        // 默认各个节点生成的随机数顺序一致
        int subId1 = (rand() % 3) + 1;
        int subId2 = (rand() % 3) + 1;
        while (subId2 == subId1) {
            subId2 = (rand() % 3) + 1;
        }
        if (subId1 > subId2) {
            swap(subId1, subId2);
        }
        int corId = 3;
        
        std::cout << corId << ":" << subId1 << "-" << subId2 << std::endl;
        // 生成交易
        newfileName = "../node" + to_string((corId-1)*4) + "/" + fileName;
        if (nowCrossNum < 500) {
            createCrossTransaction(corId, subId1, subId2, ledgerManager, nowCrossNum, newfileName, _injectionTest);
            nowCrossNum = 0;
        } else {
            createCrossTransaction(corId, subId1, subId2, ledgerManager, 500, newfileName, _injectionTest);
            nowCrossNum -= 500;
        }
    }
}


// 9分片
// void createIntershardDataSet(std::shared_ptr<dev::ledger::LedgerManager> ledgerManager, int txNum, std::shared_ptr<dev::rpc::Rpc> rpcService) {
//     // 计算跨片交易及各分片的片内交易数
//     string fileName, newfileName;
//     int shardsNum = dev::consensus::hiera_shard_number;
//     transactionInjectionTest _injectionTest(rpcService, 1);
//     fileName = "intershardworkload.json";
    
//     // 生成局部性交易
//     srand((unsigned)time(NULL));
//     int nowCrossNum = txNum;
//     std::cout << "开始生成上层局部性交易：" << std::endl;
//     while(nowCrossNum != 0) {
//         // 生成随机两个子分片ID 范围:[7,9]
//         // 默认各个节点生成的随机数顺序一致
//         int subId1 = (rand() % (9 - 7 + 1)) + 7;
//         int subId2 = (rand() % (9 - 7 + 1)) + 7;
//         while (subId2 == subId1) {
//             subId2 = (rand() % (9 - 7 + 1)) + 7;
//         }
//         if (subId1 > subId2) {
//             swap(subId1, subId2);
//         }
//         // 查找最近公共祖先
//         int corId = 9;
//         std::cout << corId << ":" << subId1 << "-" << subId2 << std::endl;
//         // 生成交易
//         newfileName = "../node" + to_string((corId-1)*4) + "/" + fileName;
//         if (nowCrossNum < 500) {
//             createCrossTransaction(corId, subId1, subId2, ledgerManager, nowCrossNum, newfileName, _injectionTest);
//             nowCrossNum = 0;
//         } else {
//             createCrossTransaction(corId, subId1, subId2, ledgerManager, 500, newfileName, _injectionTest);
//             nowCrossNum -= 500;
//         }
//     }

//     std::cout << "开始生成下层局部性交易：" << std::endl;
//     for (int i = 1; i < 7; i += 3) {
//         nowCrossNum = txNum;
//         while(nowCrossNum != 0) {
//             // 生成随机两个子分片ID 范围:[1,4] / [4,7] == [i, i+3]
//             // 默认各个节点生成的随机数顺序一致
//             int subId1 = (rand() % 4) + i;
//             int subId2 = (rand() % 4) + i;
//             while (subId2 == subId1) {
//                 subId2 = (rand() % 4) + i;
//             }

//             if (subId1 > subId2) {
//                 swap(subId1, subId2);
//             }

//             if (i == 1 && subId2 == 4) {
//                 subId2 = 7;
//             } else if (i == 2 && subId2 == 7) {
//                 subId2 = 8;
//             }

//             int corId;
//             if (i == 1) {
//                 corId = 7;
//             } else if (i == 4) {
//                 corId = 8;
//             }
            
//             std::cout << corId << ":" << subId1 << "-" << subId2 << std::endl;
//             // 生成交易
//             newfileName = "../node" + to_string((corId-1)*4) + "/" + fileName;
//             if (nowCrossNum < 500) {
//                 createCrossTransaction(corId, subId1, subId2, ledgerManager, nowCrossNum, newfileName, _injectionTest);
//                 nowCrossNum = 0;
//             } else {
//                 createCrossTransaction(corId, subId1, subId2, ledgerManager, 500, newfileName, _injectionTest);
//                 nowCrossNum -= 500;
//             }
//         }
//     }

// }

// 13分片
// void createIntershardDataSet(std::shared_ptr<dev::ledger::LedgerManager> ledgerManager, int txNum, std::shared_ptr<dev::rpc::Rpc> rpcService) {
//     // 计算跨片交易及各分片的片内交易数
//     string fileName, newfileName;
//     int shardsNum = dev::consensus::hiera_shard_number;
//     transactionInjectionTest _injectionTest(rpcService, 1);
//     fileName = "intershardworkload.json";
    
//     // 生成局部性交易
//     srand((unsigned)time(NULL));
//     int nowCrossNum = txNum;
//     std::cout << "开始生成上层局部性交易：" << std::endl;
//     while(nowCrossNum != 0) {
//         // 生成随机两个子分片ID 范围:[10,12]
//         // 默认各个节点生成的随机数顺序一致
//         int subId1 = (rand() % 3) + 10;
//         int subId2 = (rand() % 3) + 10;
//         while (subId2 == subId1) {
//             subId2 = (rand() % 3) + 10;
//         }
//         if (subId1 > subId2) {
//             swap(subId1, subId2);
//         }
//         // 查找最近公共祖先
//         int corId = 13;
//         std::cout << corId << ":" << subId1 << "-" << subId2 << std::endl;
//         // 生成交易
//         newfileName = "../node" + to_string((corId-1)*4) + "/" + fileName;
//         if (nowCrossNum < 500) {
//             createCrossTransaction(corId, subId1, subId2, ledgerManager, nowCrossNum, newfileName, _injectionTest);
//             nowCrossNum = 0;
//         } else {
//             createCrossTransaction(corId, subId1, subId2, ledgerManager, 500, newfileName, _injectionTest);
//             nowCrossNum -= 500;
//         }
//     }

//     std::cout << "开始生成下层局部性交易：" << std::endl;
//     for (int i = 1; i < 10; i += 3) {
//         nowCrossNum = txNum;
//         while(nowCrossNum != 0) {
//             // 生成随机两个子分片ID 范围:[1,3] / [4,6] / [7,9] == [i, i+3]
//             // 默认各个节点生成的随机数顺序一致
//             int subId1 = (rand() % 3) + i;
//             int subId2 = (rand() % 3) + i;
//             while (subId2 == subId1) {
//                 subId2 = (rand() % 3) + i;
//             }

//             if (subId1 > subId2) {
//                 swap(subId1, subId2);
//             }

//             int corId;
//             if (i == 1) {
//                 corId = 10;
//             } else if (i == 4) {
//                 corId = 11;
//             } else if (i == 7) {
//                 corId = 12;
//             }
            
//             std::cout << corId << ":" << subId1 << "-" << subId2 << std::endl;
//             // 生成交易
//             newfileName = "../node" + to_string((corId-1)*4) + "/" + fileName;
//             if (nowCrossNum < 500) {
//                 createCrossTransaction(corId, subId1, subId2, ledgerManager, nowCrossNum, newfileName, _injectionTest);
//                 nowCrossNum = 0;
//             } else {
//                 createCrossTransaction(corId, subId1, subId2, ledgerManager, 500, newfileName, _injectionTest);
//                 nowCrossNum -= 500;
//             }
//         }
//     }
// }

// 生成AHL跨片交易-shardsNum为协调者
void createCrossshardDataSet(std::shared_ptr<dev::ledger::LedgerManager> ledgerManager, int txNum, std::shared_ptr<dev::rpc::Rpc> rpcService) {
    // 计算跨片交易及各分片的片内交易数
    string fileName, newfileName;
    int shardsNum = dev::consensus::hiera_shard_number;
    transactionInjectionTest _injectionTest(rpcService, 1);
    
    fileName = "intershardworkload.json";

    // 生成跨层交易
    int nowCrossNum = txNum;
    std::cout << "开始生成AHL跨片交易：" << std::endl;
    while(nowCrossNum != 0) {
        // 生成随机两个子分片ID 范围:[1, shardsNum-1]
        // 默认各个节点生成的随机数顺序一致
        int subId1 = (rand() % (shardsNum - 1)) + 1;
        int subId2 = (rand() % (shardsNum - 1)) + 1;
        while (subId1 == subId2) {
            subId2 = (rand() % (shardsNum - 1)) + 1;
        }

        if (subId1 > subId2) {
            swap(subId1, subId2);
        }

        // 查找最近公共祖先
        int corId = shardsNum;
        std::cout << corId << ":" << subId1 << "-" << subId2 << std::endl;
        // 生成交易
        newfileName = "../node" + to_string((corId-1)*4) + "/" + fileName;
        if (nowCrossNum < 500) {
            createCrossTransaction(corId, subId1, subId2, ledgerManager, nowCrossNum, newfileName, _injectionTest);
            nowCrossNum = 0;
        } else {
            createCrossTransaction(corId, subId1, subId2, ledgerManager, 500, newfileName, _injectionTest);
            nowCrossNum -= 500;
        }
    }
}


// by yzm
// 这里加载了当前节点所在分片的子分片和最近祖先分片
void loadHieraInfo(string* upper_groupIds, string* lower_groupIds)
{
    PLUGIN_LOG(INFO) << LOG_DESC("start loadHieraInfo...");
    
    int shard_id = dev::consensus::internal_groupId; // 当前节点所在的分片id
    cout << "shard_id:" << shard_id << endl;
    
    pair<string, string> info = dev::plugin::hieraShardTree->get_shard_info_by_internal_groupId(shard_id);
    *upper_groupIds = info.first;
    *lower_groupIds = info.second;
}



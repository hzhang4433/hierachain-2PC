#pragma once

#include <libmptstate/MPTStateFactory.h>
#include <libexecutive/VMFactory.h>
#include <libblockverifier/ExecutiveContext.h>
#include <libexecutive/Common.h>
#include <libethcore/BlockHeader.h>
#include <libdevcore/CommonJS.h>

namespace dev
{
    namespace plugin
    {
        #define PLUGIN_LOG(LEVEL) LOG(LEVEL) << LOG_BADGE("PLUGIN") << LOG_BADGE("PLUGIN")
        extern std::shared_ptr<dev::eth::EVMInterface> m_vminstance; // 执行时候使用的VM

        class ExecuteVMTestFixture : public std::enable_shared_from_this<ExecuteVMTestFixture>
        {
            public:
                typedef std::shared_ptr <ExecuteVMTestFixture> Ptr;

                static dev::h256 fakeCallBack(int64_t) { return dev::h256(); }

                ExecuteVMTestFixture(std::string _path, int _executive_instance_num = 1)
                    : m_mptStates(std::make_shared<dev::mptstate::MPTState>(
                        u256(0), dev::mptstate::MPTState::openDB(_path, h256("0x2234")), dev::mptstate::BaseState::Empty)) 
                        {
                            m_executive_instance_num = _executive_instance_num;
                            for(int i = 0; i < m_executive_instance_num; i++)
                            {
                                auto vm = dev::eth::VMFactory::create();
                                m_vminstance_pool.push(vm);
                            }
                        }

                std::shared_ptr<dev::eth::EVMInterface> getExecutiveInstance() 
                {
                    while (m_vminstance_pool.size() == 0)
                    {
                        PLUGIN_LOG(INFO) << LOG_DESC("wait executive...");
                        sleep(1);
                    }
                    // Executive::Ptr exeptr = m_executive_pool.back();
                    auto vm = m_vminstance_pool.front();
                    m_vminstance_pool.pop();
                    return vm;
                }

                dev::executive::EnvInfo initEnvInfo() {
                    dev::executive::EnvInfo envInfo{fakeBlockHeader(), fakeCallBack, 0};
                    dev::blockverifier::ExecutiveContext::Ptr executiveContext =
                            std::make_shared<dev::blockverifier::ExecutiveContext>();
                    executiveContext->setMemoryTableFactory(std::make_shared<dev::storage::MemoryTableFactory>());
                    envInfo.setPrecompiledEngine(executiveContext);
                    return envInfo;
                }

                dev::executive::EnvInfo initEnvInfo(uint64_t _gasLimit) {
                    dev::executive::EnvInfo envInfo{fakeBlockHeader(), fakeCallBack, 0};
                    dev::blockverifier::ExecutiveContext::Ptr executiveContext = std::make_shared<blockverifier::ExecutiveContext>();
                    executiveContext->setTxGasLimit(_gasLimit);
                    executiveContext->setMemoryTableFactory(std::make_shared<storage::MemoryTableFactory>());
                    envInfo.setPrecompiledEngine(executiveContext);
                    return envInfo;
                }

                std::shared_ptr <dev::mptstate::MPTState> m_mptStates;
                int m_executive_instance_num = -1;
                std::queue<std::shared_ptr<dev::eth::EVMInterface>> m_vminstance_pool;

                void executeTransaction(dev::executive::Executive::Ptr _e, eth::Transaction::Ptr tx) {
                    PLUGIN_LOG(INFO) << LOG_DESC("xxxx");
                    _e->initialize(tx);
                    PLUGIN_LOG(INFO) << LOG_DESC("yyyy");
                    if (!_e->execute()) {
                        _e->go();
                    }
                    PLUGIN_LOG(INFO) << LOG_DESC("zzzz");
                    _e->finalize();
                }

                dev::executive::Executive::Ptr getExecutive(){
                    dev::executive::Executive::Ptr e = std::make_shared<dev::executive::Executive>(m_mptStates, initEnvInfo());
                    return e;
                }

                dev::executive::Executive::Ptr getExecutive(dev::executive::EnvInfo _EnvInfo){
                    dev::executive::Executive::Ptr e = std::make_shared<dev::executive::Executive>(m_mptStates, _EnvInfo);
                    return e;
                }

            private:
                dev::eth::BlockHeader fakeBlockHeader()
                {
                    dev::eth::BlockHeader fakeHeader;
                    fakeHeader.setParentHash(crypto::Hash("parent"));
                    fakeHeader.setRoots(crypto::Hash("transactionRoot"), crypto::Hash("receiptRoot"), crypto::Hash("stateRoot"));
                    fakeHeader.setLogBloom(eth::LogBloom(0));
                    fakeHeader.setNumber(int64_t(0));
                    fakeHeader.setGasLimit(u256(3000000000000000000));
                    fakeHeader.setGasUsed(u256(1000000));
                    uint64_t current_time = utcTime();
                    fakeHeader.setTimestamp(current_time);
                    fakeHeader.appendExtraDataArray(jsToBytes("0x1020"));
                    fakeHeader.setSealer(u256("0x00"));
                    std::vector<h512> sealer_list;
                    for (unsigned int i = 0; i < 10; i++)
                    {
                        sealer_list.push_back(toPublic(Secret::random()));
                    }
                    fakeHeader.setSealerList(sealer_list);
                    return fakeHeader;
                }
        };
    } // namespace name
} // namespace name

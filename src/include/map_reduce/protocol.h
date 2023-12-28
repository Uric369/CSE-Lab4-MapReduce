#include <atomic>
#include <mutex>
#include <string>
#include <utility>
#include <vector>
#include "distributed/client.h"
#include "librpc/client.h"
#include "librpc/server.h"

// Lab4: Free to modify this file

namespace mapReduce {
    struct KeyVal {
        KeyVal(const std::string &key, const std::string &val) : key(key), val(val) {}
        KeyVal() {}
        std::string key;
        std::string val;
    };

    enum mr_tasktype { NONE = 0, MAP, REDUCE };

    std::vector<KeyVal> Map(const std::string &content);

    std::string Reduce(const std::string &key, const std::vector<std::string> &values);

    const std::string ASK_TASK = "ask task";
    const std::string SUBMIT_TASK = "submit task";

    struct MR_CoordinatorConfig {
        uint16_t port;
        std::string ip_address;
        std::string resultFile;
        std::shared_ptr<chfs::ChfsClient> client;

        MR_CoordinatorConfig(std::string ip_address, uint16_t port, std::shared_ptr<chfs::ChfsClient> client,
                             std::string resultFile)
                : port(port), ip_address(std::move(ip_address)), resultFile(resultFile), client(std::move(client)) {}
    };

    class SequentialMapReduce {
    public:
        SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client, const std::vector<std::string> &files,
                            std::string resultFile);
        void doWork();

    private:
        std::shared_ptr<chfs::ChfsClient> chfs_client;
        std::vector<std::string> files;
        std::string outPutFile;
    };

    class Coordinator {
    public:
        Coordinator(MR_CoordinatorConfig config, const std::vector<std::string> &files, int nReduce);
        std::tuple<int, int, std::string> askTask(int);
        int submitTask(int taskType, int index);
        bool Done();

    private:
        std::vector<std::string> files;
        std::mutex mtx;
        bool isFinished;
        std::unique_ptr<chfs::RpcServer> rpc_server;
        std::vector<int> map_task;
        int finished_map_task{0};
        std::vector<int> reduce_task;
        // Helper method to pop a task from a task queue.
        std::tuple<int, int, std::string> popTask(std::vector<int>& task_queue, mr_tasktype task_type);

        // Helper method to push tasks into the task queue.
        void initializeTaskQueue(std::vector<int>& task_queue, int size);
    };

    class Worker {
    public:
        explicit Worker(MR_CoordinatorConfig config);
        void doWork();
        void stop();
        void writeToFile(const std::string &filename, const std::vector<uint8_t> &content);
        std::string readFromFile(const std::string &filename);

    private:
        void doMap(int index, const std::string &filename);
        void doReduce(int index, int nfiles);
        void doSubmit(mr_tasktype taskType, int index);

        std::string outPutFile;
        std::unique_ptr<chfs::RpcClient> mr_client;
        std::shared_ptr<chfs::ChfsClient> chfs_client;
        std::unique_ptr<std::thread> work_thread;
        bool shouldStop = false;
    };
    std::map<std::string, int> CountMap(const std::string &content);
    std::vector<uint8_t> SerializeCountMap(const std::map<std::string, int> &count_map);
    void DeserializeCountMap(const std::vector<uint8_t> &content, std::map<std::string, int> &count_map);
}  // namespace mapReduce
#include <mutex>
#include <string>
#include <vector>

#include "map_reduce/protocol.h"

namespace mapReduce {
    std::tuple<int, int, std::string> Coordinator::askTask(int) {
        std::unique_lock<std::mutex> uniqueLock(this->mtx);

        if (!map_task.empty()) {
            return popTask(map_task, MAP);
        }
        if (finished_map_task != files.size()) {
            return {NONE, -1, ""};
        }
        if (!reduce_task.empty()) {
            return popTask(reduce_task, REDUCE);
        }
        return {NONE, -1, ""};
    }

    int Coordinator::submitTask(int taskType, int index) {
        std::unique_lock<std::mutex> uniqueLock(this->mtx);

        auto task_type = static_cast<mr_tasktype>(taskType);
        if (task_type == MAP) {
            finished_map_task++;
        } else if (task_type == REDUCE) {
            isFinished = finished_map_task == files.size() && reduce_task.empty();
        }
        return 0;
    }

    bool Coordinator::Done() {
        std::unique_lock<std::mutex> uniqueLock(this->mtx);
        return this->isFinished;
    }

// create a Coordinator.
// nReduce is the number of reduce tasks to use.
    Coordinator::Coordinator(MR_CoordinatorConfig config, const std::vector<std::string> &files, int nReduce) {
        this->files = files;
        this->isFinished = false;
        // Lab4: Your code goes here (Optional).

        initializeTaskQueue(map_task, files.size());
        reduce_task.emplace_back(0);

        for (int i = 0; i < files.size(); ++i) {
            config.client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, "intermediate_" + std::to_string(i));
        }

        rpc_server = std::make_unique<chfs::RpcServer>(config.ip_address, config.port);
        rpc_server->bind(ASK_TASK, [this](int i) { return this->askTask(i); });
        rpc_server->bind(SUBMIT_TASK, [this](int taskType, int index) { return this->submitTask(taskType, index); });
        rpc_server->run(true, 1);
    }

    void Coordinator::initializeTaskQueue(std::vector<int>& task_queue, int size) {
        for (int i = 0; i < size; ++i) {
            task_queue.emplace_back(i);
        }
    }

    std::tuple<int, int, std::string> Coordinator::popTask(std::vector<int>& task_queue, mr_tasktype task_type) {
        auto index = task_queue.back();
        task_queue.pop_back();
        std::string file_info = (task_type == MAP) ? files[index] : std::to_string(files.size());
        return {static_cast<int>(task_type), index, file_info};
    }

}  // namespace mapReduce
#include <string>
#include <unordered_map>
#include "map_reduce/protocol.h"

namespace mapReduce {

    Worker::Worker(MR_CoordinatorConfig config) {
        mr_client = std::make_unique<chfs::RpcClient>(config.ip_address, config.port, true);
        outPutFile = config.resultFile;
        chfs_client = config.client;
        work_thread = std::make_unique<std::thread>(&Worker::doWork, this);
        // Lab4: Your code goes here (Optional).
    }

    void Worker::doMap(int index, const std::string &filename) {
        auto content = readFromFile(filename);
        auto map = CountMap(content);
        auto intermediateFile = "intermediate_" + std::to_string(index);
        writeToFile(intermediateFile, SerializeCountMap(map));
    }

    void Worker::doReduce(int index, int totalFiles) {
        std::map<std::string, int> aggregatedMap;
        for (int i = 0; i < totalFiles; ++i) {
            auto tempFilename = "intermediate_" + std::to_string(i);
            auto tempFileID = chfs_client->lookup(1, tempFilename).unwrap();
            auto [_, tempFileAttr] = chfs_client->get_type_attr(tempFileID).unwrap();
            auto content = chfs_client->read_file(tempFileID, 0, tempFileAttr.size).unwrap();
            DeserializeCountMap(content, aggregatedMap);
        }
        auto outputID = chfs_client->lookup(1, outPutFile).unwrap();
        chfs_client->write_file(outputID, 0, SerializeCountMap(aggregatedMap));
    }


    void Worker::doSubmit(mr_tasktype taskType, int index) {
        // Lab4: Your code goes here.
        mr_client->call(SUBMIT_TASK, static_cast<int>(taskType), index);
    }

    void Worker::stop() {
        shouldStop = true;
        work_thread->join();
    }

    void Worker::doWork() {
        while (!shouldStop) {
            auto task_info = mr_client->call(ASK_TASK, 0);
            if (task_info.is_err()) {
                continue;
            }
            auto [taskType, taskIndex, taskData] = task_info.unwrap()->as<std::tuple<int, int, std::string>>();
            auto workType = static_cast<mr_tasktype>(taskType);
            if (workType == NONE) {
                continue;
            } else if (workType == MAP) {
                doMap(taskIndex, taskData);
            } else if (workType == REDUCE) {
                doReduce(taskIndex, std::stoi(taskData));
            }
            doSubmit(workType, taskIndex);
        }
    }


    std::string Worker::readFromFile(const std::string &filename) {
        auto file_inode_id = chfs_client->lookup(1, filename).unwrap();
        auto [type, attr] = chfs_client->get_type_attr(file_inode_id).unwrap();
        auto content = chfs_client->read_file(file_inode_id, 0, attr.size).unwrap();
        return std::string(content.begin(), content.end());
    }

    void Worker::writeToFile(const std::string &filename, const std::vector<uint8_t> &content) {
        auto file_inode_id = chfs_client->lookup(1, filename).unwrap();
        chfs_client->write_file(file_inode_id, 0, content);
    }
}  // namespace mapReduce
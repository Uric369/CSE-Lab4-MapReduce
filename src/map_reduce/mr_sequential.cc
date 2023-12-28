#include <algorithm>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "map_reduce/protocol.h"
namespace mapReduce {
    SequentialMapReduce::SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client,
                                             const std::vector<std::string> &files_, std::string resultFile) {
        chfs_client = std::move(client);
        files = files_;
        outPutFile = resultFile;
        // Your code goes here (optional)
    }

    void SequentialMapReduce::doWork() {
        // Assuming Map and Reduce are defined elsewhere in the class.
        std::map<std::string, std::vector<std::string>> map_result;
        // Reserve space for map_result if possible; this is optional and can be skipped if not needed.

        for (const auto &filename : files) {
            auto file_inode_id = chfs_client->lookup(1, filename).unwrap();
            auto [type, attr] = chfs_client->get_type_attr(file_inode_id).unwrap();
            auto content = chfs_client->read_file(file_inode_id, 0, attr.size).unwrap();
            auto str_content = std::string(content.begin(), content.end());

            for (auto &[key, value] : Map(str_content)) {
                map_result[std::move(key)].emplace_back(std::move(value));
            }
        }

        std::ostringstream result_ss;
        for (auto &[key, values] : map_result) {
            result_ss << key << ' ' << Reduce(key, values) << '\n';
        }

        auto result_content = result_ss.str();
        auto result = std::vector<uint8_t>(result_content.begin(), result_content.end());

        auto output_file_inode_id = chfs_client->lookup(1, outPutFile).unwrap();
        chfs_client->write_file(output_file_inode_id, 0, result);
    }

}  // namespace mapReduce
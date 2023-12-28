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
        std::map<std::string, std::vector<std::string>> intermediateResults;

        for (const auto &file : files) {
            auto fileID = chfs_client->lookup(1, file).unwrap();
            auto [type, attributes] = chfs_client->get_type_attr(fileID).unwrap();
            auto fileData = chfs_client->read_file(fileID, 0, attributes.size).unwrap();
            std::string fileContent(fileData.begin(), fileData.end());

            for (auto &[extractedKey, extractedValue] : Map(fileContent)) {
                intermediateResults[std::move(extractedKey)].emplace_back(std::move(extractedValue));
            }
        }

        std::ostringstream combinedResults;
        for (auto &[key, values] : intermediateResults) {
            combinedResults << key << ' ' << Reduce(key, values) << '\n';
        }

        std::string resultsString = combinedResults.str();
        std::vector<uint8_t> resultsData(resultsString.begin(), resultsString.end());

        auto resultFileID = chfs_client->lookup(1, outPutFile).unwrap();
        chfs_client->write_file(resultFileID, 0, resultsData);
    }

}  // namespace mapReduce
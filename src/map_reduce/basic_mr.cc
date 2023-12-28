#include <algorithm>
#include <iostream>
#include <string>
#include <vector>

#include "map_reduce/protocol.h"

namespace mapReduce {
//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//

    bool IsAlpha(const char ch) {
        return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z');
    }

    std::map<std::string, int> CountMap(const std::string &text) {
        auto validChar = [](const char c) {
            return std::isalpha(c) || std::isspace(c);
        };

        std::string modifiedText;
        std::transform(text.begin(), text.end(), std::back_inserter(modifiedText), [&](const char c) {
            return validChar(c) ? c : ' ';
        });

        std::istringstream stream(modifiedText);
        std::map<std::string, int> wordFreq;
        std::string currentWord;

        while (stream >> currentWord) {
            wordFreq[currentWord]++;
        }

        return wordFreq;
    }

// Serialize the count map to a vector of uint8_t.
    std::vector<uint8_t> SerializeCountMap(const std::map<std::string, int> &count_map) {
        std::stringstream ss;
        for (const auto &[key, value] : count_map) {
            ss << key << " " << value << "\n";  // Use '\0' as a separator between key and value
        }
        std::string content = ss.str();
        return std::vector<uint8_t>(content.begin(), content.end());
    }


    void DeserializeCountMap(const std::vector<uint8_t> &serializedData, std::map<std::string, int> &resultMap) {
        std::stringstream stream(std::string(serializedData.begin(), serializedData.end()));
        std::string key;
        int value;
        std::istream_iterator<std::string> iterator(stream), endIterator;
        while (iterator != endIterator) {
            key = *iterator++;
            if (iterator == endIterator) break;
            std::stringstream valueStream(*iterator++);
            if (!(valueStream >> value)) {
                continue;
            }
            if (!key.empty() && key.front() == '\0') {
                continue;
            }
            resultMap[key] += value;
        }
    }


    // The map function processes the content and returns a vector of key-value pairs.
    std::vector<KeyVal> Map(const std::string &content) {
        std::vector<KeyVal> key_values;

        std::string::const_iterator start = content.begin(), end = content.end();
        std::string word;
        // Iterate over each character in the content
        for (auto it = start; it != end; ++it) {
            // If the character is alphabetic, add it to the current word
            if (IsAlpha(*it)) {
                word.push_back(*it);
            }
                // If the character is non-alphabetic and we have a word, emit it
            else if (!word.empty()) {
                key_values.emplace_back(word, "1"); // We emit "1" for each occurrence
                word.clear(); // Clear the word for the next one
            }
        }

        // Emit the last word if the content ends with an alphabetic character
        if (!word.empty()) {
            key_values.emplace_back(word, "1");
        }

        return key_values;
    }

    // The reduce function sums up all the counts for each word
    std::string Reduce(const std::string &key, const std::vector<std::string> &values) {
        int sum = std::accumulate(values.begin(), values.end(), 0, [](int total, const std::string &number) {
            return total + std::stoi(number);
        });

        return std::to_string(sum);
    }
}  // namespace mapReduce
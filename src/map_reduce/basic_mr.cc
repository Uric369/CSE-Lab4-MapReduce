#include <algorithm>
#include <fstream>
#include <iostream>
#include <sstream>
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

std::map<std::string, int> CountMap(const std::string &content) {
  auto punctuation_free_content = std::string();
  punctuation_free_content.resize(content.size());
  std::transform(content.begin(), content.end(), punctuation_free_content.begin(), [](const char ch) {
    if (IsAlpha(ch)) {
      return ch;
    }
    return ' ';
  });
  while (!IsAlpha(punctuation_free_content.back())) {
    punctuation_free_content.pop_back();
  }
  // count words
  auto ss = std::stringstream(punctuation_free_content);
  auto word = std::string();
  auto count_map = std::map<std::string, int>{};
  while (!ss.eof()) {
    ss >> word;
    count_map[word]++;
  }
  return count_map;
}

std::vector<uint8_t> SerializeCountMap(const std::map<std::string, int> &count_map) {
  std::stringstream ss;
  for (const auto &[key, value] : count_map) {
    ss << key << " " << value << "\n";
  }
  std::string content = ss.str();
  return std::vector<uint8_t>{content.begin(), content.end()};
}

void DeserializeCountMap(const std::vector<uint8_t> &content, std::map<std::string, int> &count_map) {
  std::string str_content(content.begin(), content.end());
  std::stringstream ss(str_content);
  std::string word;
  int count;
  while (!ss.eof()) {
    ss >> word >> count;
    if (word.front() == '\0') {
      continue;
    }
    count_map[word] += count;

    //    LOG_FORMAT_INFO("{} {}", word, count_map[word]);
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
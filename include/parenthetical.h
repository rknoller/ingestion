#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <map>

// Represents a row from search_parenthetical table
struct Parenthetical {
    int id;
    std::string text;
    double score;
    int described_opinion_id;
    int describing_opinion_id;
    int group_id;
    
    std::string toString() const {
        std::ostringstream oss;
        oss << "Parenthetical{id=" << id 
            << ", score=" << score
            << ", described_opinion_id=" << described_opinion_id
            << ", describing_opinion_id=" << describing_opinion_id
            << ", group_id=" << group_id
            << ", text=\"" << (text.length() > 50 ? text.substr(0, 50) + "..." : text) << "\"}";
        return oss.str();
    }
    
    std::string toCsv() const {
        std::ostringstream oss;
        // Escape quotes in text field
        std::string escaped_text = text;
        size_t pos = 0;
        while ((pos = escaped_text.find('"', pos)) != std::string::npos) {
            escaped_text.replace(pos, 1, "\"\"");
            pos += 2;
        }
        
        oss << id << ","
            << "\"" << escaped_text << "\","
            << score << ","
            << described_opinion_id << ","
            << describing_opinion_id << ","
            << group_id;
        return oss.str();
    }
};

// CSV reader for parenthetical records with streaming support
class ParentheticalReader {
public:
    explicit ParentheticalReader(const std::string& filename);
    ~ParentheticalReader();
    
    // Read next batch of records (streaming)
    std::vector<Parenthetical> readBatch(size_t batch_size);
    
    // Check if more records are available
    bool hasMore() const;

private:
    std::ifstream file_;
    std::vector<std::string> header_;
    bool header_parsed_;
    
    void parseHeader(const std::string& header_line);
    Parenthetical parseCsvLine(const std::string& line);
    std::vector<std::string> splitCsvLine(const std::string& line);
    size_t getColumn(const std::string& name) const;
    
    std::map<std::string, size_t> column_map_;
};

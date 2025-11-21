#pragma once

#include <string>
#include <vector>
#include <map>
#include <optional>
#include <fstream>

// Represents a row from search_opinionscited table (citation map)
struct OpinionCited {
    int id;
    int depth;
    int cited_opinion_id;
    int citing_opinion_id;
    
    std::string toString() const;
    std::string toCsv() const; // For outputting to bad records file
};

// CSV reader for opinion citation data - streaming implementation
class OpinionCitedReader {
public:
    explicit OpinionCitedReader(const std::string& filename);
    ~OpinionCitedReader();
    
    // Read all records from CSV file (for small files)
    std::vector<OpinionCited> readAll();
    
    // Read next batch of records (streaming mode)
    std::vector<OpinionCited> readBatch(size_t batch_size);
    
    // Check if there are more records to read
    bool hasMore() const;
    
    // Get total lines read so far
    size_t getTotalLinesRead() const { return total_lines_read_; }
    
    // Parse a single CSV line into OpinionCited
    OpinionCited parseCsvLine(const std::string& line);

private:
    std::string filename_;
    std::vector<std::string> header_;
    std::map<std::string, size_t> column_map_;
    std::ifstream file_;
    bool header_parsed_;
    size_t total_lines_read_;
    
    void parseHeader(const std::string& header_line);
    std::optional<std::string> getColumn(const std::vector<std::string>& cols, const std::string& name);
    std::vector<std::string> splitCsvLine(const std::string& line);
};

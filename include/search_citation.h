#pragma once

#include <string>
#include <vector>
#include <map>
#include <optional>
#include <fstream>

// Represents a row from search_citation table
struct SearchCitation {
    int id;
    int volume;
    std::string reporter;
    std::string page;
    int type;
    int cluster_id;
    
    std::string toString() const;
    std::string toCsv() const; // For outputting to bad records file
};

// CSV reader for search_citation data - streaming implementation
class SearchCitationReader {
public:
    explicit SearchCitationReader(const std::string& filename);
    ~SearchCitationReader();
    
    // Read next batch of records (streaming mode)
    std::vector<SearchCitation> readBatch(size_t batch_size);
    
    // Check if there are more records to read
    bool hasMore() const;
    
    // Get total lines read so far
    size_t getTotalLinesRead() const { return total_lines_read_; }
    
    // Parse a single CSV line into SearchCitation
    SearchCitation parseCsvLine(const std::string& line);

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

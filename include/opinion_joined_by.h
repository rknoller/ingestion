#pragma once

#include <string>
#include <vector>
#include <map>
#include <optional>
#include <fstream>

// Represents a row from search_opinion_joined_by table
struct OpinionJoinedBy {
    int id;
    int opinion_id;
    int person_id;
    
    std::string toString() const;
    std::string toCsv() const; // For outputting to bad records file
};

// CSV reader for opinion joined_by data
class OpinionJoinedByReader {
public:
    explicit OpinionJoinedByReader(const std::string& filename);
    
    // Read all records from CSV file
    std::vector<OpinionJoinedBy> readAll();
    
    // Parse a single CSV line into OpinionJoinedBy
    OpinionJoinedBy parseCsvLine(const std::string& line);

private:
    std::string filename_;
    std::vector<std::string> header_;
    std::map<std::string, size_t> column_map_;
    
    void parseHeader(const std::string& header_line);
    std::optional<std::string> getColumn(const std::vector<std::string>& cols, const std::string& name);
    std::vector<std::string> splitCsvLine(const std::string& line);
};

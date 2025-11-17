#pragma once

#include <string>
#include <vector>
#include <map>
#include <optional>
#include <fstream>

// Represents a row from search_opinioncluster_panel table
struct OpinionClusterPanel {
    int id;
    int opinioncluster_id;
    int person_id;
    
    std::string toString() const;
    std::string toCsv() const; // For outputting to bad records file
};

// CSV reader for opinion cluster panel data
class OpinionClusterPanelReader {
public:
    explicit OpinionClusterPanelReader(const std::string& filename);
    
    // Read all records from CSV file
    std::vector<OpinionClusterPanel> readAll();
    
    // Parse a single CSV line into OpinionClusterPanel
    OpinionClusterPanel parseCsvLine(const std::string& line);

private:
    std::string filename_;
    std::vector<std::string> header_;
    std::map<std::string, size_t> column_map_;
    
    void parseHeader(const std::string& header_line);
    std::optional<std::string> getColumn(const std::vector<std::string>& cols, const std::string& name);
    std::vector<std::string> splitCsvLine(const std::string& line);
};

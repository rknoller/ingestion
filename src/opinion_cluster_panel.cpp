#include "opinion_cluster_panel.h"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <iostream>

using std::optional;
using std::string;
using std::vector;

static inline string trim(const string& s) {
    size_t start = 0;
    while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) start++;
    size_t end = s.size();
    while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) end--;
    return s.substr(start, end - start);
}

static inline int parse_int_safe(const string& s, int default_val = 0) {
    auto t = trim(s);
    if (t.empty()) return default_val;
    try {
        return std::stoi(t);
    } catch (...) {
        return default_val;
    }
}

string OpinionClusterPanel::toString() const {
    std::ostringstream oss;
    oss << "OpinionClusterPanel{";
    oss << "id=" << id;
    oss << ", opinioncluster_id=" << opinioncluster_id;
    oss << ", person_id=" << person_id;
    oss << "}";
    return oss.str();
}

string OpinionClusterPanel::toCsv() const {
    std::ostringstream oss;
    oss << id << "," << opinioncluster_id << "," << person_id;
    return oss.str();
}

OpinionClusterPanelReader::OpinionClusterPanelReader(const string& filename) 
    : filename_(filename) {}

void OpinionClusterPanelReader::parseHeader(const string& header_line) {
    header_ = splitCsvLine(header_line);
    column_map_.clear();
    for (size_t i = 0; i < header_.size(); ++i) {
        column_map_[trim(header_[i])] = i;
    }
}

optional<string> OpinionClusterPanelReader::getColumn(const vector<string>& cols, const string& name) {
    auto it = column_map_.find(name);
    if (it == column_map_.end()) return std::nullopt;
    size_t idx = it->second;
    if (idx >= cols.size()) return std::nullopt;
    return cols[idx];
}

// CSV parsing with quote handling for values like "3","1975844","5540"
vector<string> OpinionClusterPanelReader::splitCsvLine(const string& line) {
    vector<string> result;
    string current;
    bool in_quotes = false;
    
    for (size_t i = 0; i < line.size(); ++i) {
        char c = line[i];
        
        // Handle backslash escapes
        if (c == '\\' && i + 1 < line.size() && line[i + 1] == '"') {
            // Backslash-escaped quote: treat as literal quote character
            current += '"';
            ++i; // skip the quote
            continue;
        }
        
        if (c == '"') {
            if (in_quotes && i + 1 < line.size() && line[i + 1] == '"') {
                // Escaped quote "" -> single quote
                current += '"';
                ++i;
            } else {
                // Toggle quote state (opening or closing quote)
                in_quotes = !in_quotes;
            }
        } else if (c == ',' && !in_quotes) {
            // Comma outside quotes = field separator
            result.push_back(current);
            current.clear();
        } else {
            current += c;
        }
    }
    result.push_back(current);
    return result;
}

OpinionClusterPanel OpinionClusterPanelReader::parseCsvLine(const string& line) {
    auto cols = splitCsvLine(line);
    
    if (cols.size() < 3) {
        throw std::runtime_error("Panel record has insufficient columns (expected 3)");
    }
    
    OpinionClusterPanel panel;
    
    // Extract required fields by column name
    auto id_str = getColumn(cols, "id");
    auto cluster_id_str = getColumn(cols, "opinioncluster_id");
    auto person_id_str = getColumn(cols, "person_id");
    
    if (!id_str || !cluster_id_str || !person_id_str) {
        throw std::runtime_error("Panel record missing required key columns (id, opinioncluster_id, person_id)");
    }
    
    panel.id = parse_int_safe(*id_str, 0);
    panel.opinioncluster_id = parse_int_safe(*cluster_id_str, 0);
    panel.person_id = parse_int_safe(*person_id_str, 0);
    
    if (panel.id == 0) {
        throw std::runtime_error("Panel record has invalid id=0");
    }
    
    return panel;
}

vector<OpinionClusterPanel> OpinionClusterPanelReader::readAll() {
    vector<OpinionClusterPanel> panels;
    
    std::ifstream file(filename_);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open panel CSV file: " + filename_);
    }
    
    // Read and parse header
    string header_line;
    if (!std::getline(file, header_line)) {
        throw std::runtime_error("Panel CSV file is empty or missing header");
    }
    
    parseHeader(header_line);
    
    // Verify required columns exist
    if (column_map_.find("id") == column_map_.end() ||
        column_map_.find("opinioncluster_id") == column_map_.end() ||
        column_map_.find("person_id") == column_map_.end()) {
        throw std::runtime_error("Panel CSV missing required columns (id, opinioncluster_id, person_id)");
    }
    
    // Read all data lines
    string line;
    size_t line_number = 1; // Start at 1 (header is line 0)
    while (std::getline(file, line)) {
        line_number++;
        
        // Skip empty lines
        if (trim(line).empty()) {
            continue;
        }
        
        try {
            panels.push_back(parseCsvLine(line));
        } catch (const std::exception& e) {
            std::cerr << "Warning: Failed to parse line " << line_number 
                      << ": " << e.what() << std::endl;
            // Continue processing other lines
        }
    }
    
    return panels;
}

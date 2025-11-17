#include "opinion_joined_by.h"

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

string OpinionJoinedBy::toString() const {
    std::ostringstream oss;
    oss << "OpinionJoinedBy{";
    oss << "id=" << id;
    oss << ", opinion_id=" << opinion_id;
    oss << ", person_id=" << person_id;
    oss << "}";
    return oss.str();
}

string OpinionJoinedBy::toCsv() const {
    std::ostringstream oss;
    oss << id << "," << opinion_id << "," << person_id;
    return oss.str();
}

OpinionJoinedByReader::OpinionJoinedByReader(const string& filename) 
    : filename_(filename) {}

void OpinionJoinedByReader::parseHeader(const string& header_line) {
    header_ = splitCsvLine(header_line);
    column_map_.clear();
    for (size_t i = 0; i < header_.size(); ++i) {
        column_map_[trim(header_[i])] = i;
    }
}

optional<string> OpinionJoinedByReader::getColumn(const vector<string>& cols, const string& name) {
    auto it = column_map_.find(name);
    if (it == column_map_.end()) return std::nullopt;
    size_t idx = it->second;
    if (idx >= cols.size()) return std::nullopt;
    return cols[idx];
}

// CSV parsing with quote handling for values like "3","1975844","5540"
vector<string> OpinionJoinedByReader::splitCsvLine(const string& line) {
    vector<string> result;
    string current;
    bool in_quotes = false;
    
    for (size_t i = 0; i < line.size(); ++i) {
        char c = line[i];
        
        // Handle backslash escapes
        if (c == '\\' && i + 1 < line.size() && line[i + 1] == '"') {
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

OpinionJoinedBy OpinionJoinedByReader::parseCsvLine(const string& line) {
    auto cols = splitCsvLine(line);
    
    if (cols.size() < 3) {
        throw std::runtime_error("JoinedBy record has insufficient columns (expected 3)");
    }
    
    OpinionJoinedBy record;
    
    // Extract required fields by column name
    auto id_str = getColumn(cols, "id");
    auto opinion_id_str = getColumn(cols, "opinion_id");
    auto person_id_str = getColumn(cols, "person_id");
    
    if (!id_str || !opinion_id_str || !person_id_str) {
        throw std::runtime_error("JoinedBy record missing required key columns (id, opinion_id, person_id)");
    }
    
    record.id = parse_int_safe(*id_str, 0);
    record.opinion_id = parse_int_safe(*opinion_id_str, 0);
    record.person_id = parse_int_safe(*person_id_str, 0);
    
    if (record.id == 0) {
        throw std::runtime_error("JoinedBy record has invalid id=0");
    }
    
    return record;
}

vector<OpinionJoinedBy> OpinionJoinedByReader::readAll() {
    vector<OpinionJoinedBy> records;
    
    std::ifstream file(filename_);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open joined_by CSV file: " + filename_);
    }
    
    // Read and parse header
    string header_line;
    if (!std::getline(file, header_line)) {
        throw std::runtime_error("JoinedBy CSV file is empty or missing header");
    }
    
    parseHeader(header_line);
    
    // Verify required columns exist
    if (column_map_.find("id") == column_map_.end() ||
        column_map_.find("opinion_id") == column_map_.end() ||
        column_map_.find("person_id") == column_map_.end()) {
        throw std::runtime_error("JoinedBy CSV missing required columns (id, opinion_id, person_id)");
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
            records.push_back(parseCsvLine(line));
        } catch (const std::exception& e) {
            std::cerr << "Warning: Failed to parse line " << line_number 
                      << ": " << e.what() << std::endl;
            // Continue processing other lines
        }
    }
    
    return records;
}

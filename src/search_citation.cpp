#include "search_citation.h"

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

string SearchCitation::toString() const {
    std::ostringstream oss;
    oss << "SearchCitation{";
    oss << "id=" << id;
    oss << ", volume=" << volume;
    oss << ", reporter=\"" << reporter << "\"";
    oss << ", page=\"" << page << "\"";
    oss << ", type=" << type;
    oss << ", cluster_id=" << cluster_id;
    oss << "}";
    return oss.str();
}

string SearchCitation::toCsv() const {
    std::ostringstream oss;
    oss << id << "," << volume << ",\"" << reporter << "\",\"" << page << "\"," << type << "," << cluster_id;
    return oss.str();
}

SearchCitationReader::SearchCitationReader(const string& filename) 
    : filename_(filename), header_parsed_(false), total_lines_read_(0) {
    file_.open(filename_);
    if (!file_.is_open()) {
        throw std::runtime_error("Failed to open search_citation CSV file: " + filename_);
    }
}

SearchCitationReader::~SearchCitationReader() {
    if (file_.is_open()) {
        file_.close();
    }
}

bool SearchCitationReader::hasMore() const {
    return file_.good() && !file_.eof();
}

void SearchCitationReader::parseHeader(const string& header_line) {
    header_ = splitCsvLine(header_line);
    column_map_.clear();
    for (size_t i = 0; i < header_.size(); ++i) {
        column_map_[trim(header_[i])] = i;
    }
}

optional<string> SearchCitationReader::getColumn(const vector<string>& cols, const string& name) {
    auto it = column_map_.find(name);
    if (it == column_map_.end()) return std::nullopt;
    size_t idx = it->second;
    if (idx >= cols.size()) return std::nullopt;
    return cols[idx];
}

// CSV parsing with quote handling
vector<string> SearchCitationReader::splitCsvLine(const string& line) {
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

SearchCitation SearchCitationReader::parseCsvLine(const string& line) {
    auto cols = splitCsvLine(line);
    
    if (cols.size() < 6) {
        throw std::runtime_error("Citation record has insufficient columns (expected 6)");
    }
    
    SearchCitation record;
    
    // Extract required fields by column name
    auto id_str = getColumn(cols, "id");
    auto volume_str = getColumn(cols, "volume");
    auto reporter_str = getColumn(cols, "reporter");
    auto page_str = getColumn(cols, "page");
    auto type_str = getColumn(cols, "type");
    auto cluster_id_str = getColumn(cols, "cluster_id");
    
    if (!id_str || !volume_str || !reporter_str || !page_str || !type_str || !cluster_id_str) {
        throw std::runtime_error("Citation record missing required columns");
    }
    
    record.id = parse_int_safe(*id_str, 0);
    record.volume = parse_int_safe(*volume_str, 0);
    record.reporter = trim(*reporter_str);
    record.page = trim(*page_str);
    record.type = parse_int_safe(*type_str, 0);
    record.cluster_id = parse_int_safe(*cluster_id_str, 0);
    
    if (record.id == 0) {
        throw std::runtime_error("Citation record has invalid id=0");
    }
    
    return record;
}

vector<SearchCitation> SearchCitationReader::readBatch(size_t batch_size) {
    vector<SearchCitation> records;
    records.reserve(batch_size);
    
    // Parse header if not already done
    if (!header_parsed_) {
        string header_line;
        if (!std::getline(file_, header_line)) {
            throw std::runtime_error("Citation CSV file is empty or missing header");
        }
        
        parseHeader(header_line);
        
        // Verify required columns exist
        if (column_map_.find("id") == column_map_.end() ||
            column_map_.find("volume") == column_map_.end() ||
            column_map_.find("reporter") == column_map_.end() ||
            column_map_.find("page") == column_map_.end() ||
            column_map_.find("type") == column_map_.end() ||
            column_map_.find("cluster_id") == column_map_.end()) {
            throw std::runtime_error("Citation CSV missing required columns");
        }
        
        header_parsed_ = true;
    }
    
    // Read batch_size lines
    string line;
    size_t count = 0;
    while (count < batch_size && std::getline(file_, line)) {
        total_lines_read_++;
        
        // Skip empty lines
        if (trim(line).empty()) {
            continue;
        }
        
        try {
            records.push_back(parseCsvLine(line));
            count++;
        } catch (const std::exception& e) {
            std::cerr << "Warning: Failed to parse line " << (total_lines_read_ + 1) 
                      << ": " << e.what() << std::endl;
            // Continue processing other lines
        }
    }
    
    return records;
}

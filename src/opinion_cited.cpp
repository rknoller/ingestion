#include "opinion_cited.h"

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

string OpinionCited::toString() const {
    std::ostringstream oss;
    oss << "OpinionCited{";
    oss << "id=" << id;
    oss << ", depth=" << depth;
    oss << ", cited_opinion_id=" << cited_opinion_id;
    oss << ", citing_opinion_id=" << citing_opinion_id;
    oss << "}";
    return oss.str();
}

string OpinionCited::toCsv() const {
    std::ostringstream oss;
    oss << id << "," << depth << "," << cited_opinion_id << "," << citing_opinion_id;
    return oss.str();
}

OpinionCitedReader::OpinionCitedReader(const string& filename) 
    : filename_(filename), header_parsed_(false), total_lines_read_(0) {
    file_.open(filename_);
    if (!file_.is_open()) {
        throw std::runtime_error("Failed to open citation CSV file: " + filename_);
    }
}

OpinionCitedReader::~OpinionCitedReader() {
    if (file_.is_open()) {
        file_.close();
    }
}

bool OpinionCitedReader::hasMore() const {
    return file_.good() && !file_.eof();
}

void OpinionCitedReader::parseHeader(const string& header_line) {
    header_ = splitCsvLine(header_line);
    column_map_.clear();
    for (size_t i = 0; i < header_.size(); ++i) {
        column_map_[trim(header_[i])] = i;
    }
}

optional<string> OpinionCitedReader::getColumn(const vector<string>& cols, const string& name) {
    auto it = column_map_.find(name);
    if (it == column_map_.end()) return std::nullopt;
    size_t idx = it->second;
    if (idx >= cols.size()) return std::nullopt;
    return cols[idx];
}

// CSV parsing with quote handling
vector<string> OpinionCitedReader::splitCsvLine(const string& line) {
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

OpinionCited OpinionCitedReader::parseCsvLine(const string& line) {
    auto cols = splitCsvLine(line);
    
    if (cols.size() < 4) {
        throw std::runtime_error("Citation record has insufficient columns (expected 4)");
    }
    
    OpinionCited record;
    
    // Extract required fields by column name
    auto id_str = getColumn(cols, "id");
    auto depth_str = getColumn(cols, "depth");
    auto cited_opinion_id_str = getColumn(cols, "cited_opinion_id");
    auto citing_opinion_id_str = getColumn(cols, "citing_opinion_id");
    
    if (!id_str || !depth_str || !cited_opinion_id_str || !citing_opinion_id_str) {
        throw std::runtime_error("Citation record missing required columns");
    }
    
    record.id = parse_int_safe(*id_str, 0);
    record.depth = parse_int_safe(*depth_str, 0);
    record.cited_opinion_id = parse_int_safe(*cited_opinion_id_str, 0);
    record.citing_opinion_id = parse_int_safe(*citing_opinion_id_str, 0);
    
    if (record.id == 0) {
        throw std::runtime_error("Citation record has invalid id=0");
    }
    
    return record;
}

vector<OpinionCited> OpinionCitedReader::readBatch(size_t batch_size) {
    vector<OpinionCited> records;
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
            column_map_.find("depth") == column_map_.end() ||
            column_map_.find("cited_opinion_id") == column_map_.end() ||
            column_map_.find("citing_opinion_id") == column_map_.end()) {
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

vector<OpinionCited> OpinionCitedReader::readAll() {
    vector<OpinionCited> records;
    
    // Use streaming approach for consistency
    while (hasMore()) {
        auto batch = readBatch(10000);
        if (batch.empty()) break;
        records.insert(records.end(), batch.begin(), batch.end());
    }
    
    return records;
}

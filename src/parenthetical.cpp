#include "parenthetical.h"
#include <iostream>
#include <algorithm>
#include <map>
#include <stdexcept>

// Helper function to trim whitespace
static std::string trim(const std::string& str) {
    size_t first = str.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) return "";
    size_t last = str.find_last_not_of(" \t\r\n");
    return str.substr(first, last - first + 1);
}

// Helper function to safely parse integers
static int parse_int_safe(const std::string& s) {
    std::string trimmed = trim(s);
    if (trimmed.empty()) return 0;
    try {
        return std::stoi(trimmed);
    } catch (...) {
        return 0;
    }
}

// Helper function to safely parse doubles
static double parse_double_safe(const std::string& s) {
    std::string trimmed = trim(s);
    if (trimmed.empty()) return 0.0;
    try {
        return std::stod(trimmed);
    } catch (...) {
        return 0.0;
    }
}

ParentheticalReader::ParentheticalReader(const std::string& filename) 
    : header_parsed_(false) {
    file_.open(filename);
    if (!file_.is_open()) {
        throw std::runtime_error("Failed to open file: " + filename);
    }
}

ParentheticalReader::~ParentheticalReader() {
    if (file_.is_open()) {
        file_.close();
    }
}

bool ParentheticalReader::hasMore() const {
    return file_.is_open() && !file_.eof() && file_.good();
}

std::vector<Parenthetical> ParentheticalReader::readBatch(size_t batch_size) {
    std::vector<Parenthetical> records;
    
    if (!file_.is_open() || file_.eof()) {
        return records;
    }
    
    // Parse header if not already done
    if (!header_parsed_) {
        std::string header_line;
        if (std::getline(file_, header_line)) {
            parseHeader(header_line);
        } else {
            return records;
        }
    }
    
    // Read batch_size records
    std::string line;
    while (records.size() < batch_size && std::getline(file_, line)) {
        if (line.empty()) continue;
        
        try {
            Parenthetical record = parseCsvLine(line);
            records.push_back(record);
        } catch (const std::exception& e) {
            std::cerr << "Warning: Failed to parse line: " << e.what() << std::endl;
            continue;
        }
    }
    
    return records;
}

void ParentheticalReader::parseHeader(const std::string& header_line) {
    header_ = splitCsvLine(header_line);
    header_parsed_ = true;
    
    column_map_.clear();
    for (size_t i = 0; i < header_.size(); ++i) {
        column_map_[trim(header_[i])] = i;
    }
}

size_t ParentheticalReader::getColumn(const std::string& name) const {
    auto it = column_map_.find(name);
    if (it == column_map_.end()) {
        throw std::runtime_error("Column not found: " + name);
    }
    return it->second;
}

Parenthetical ParentheticalReader::parseCsvLine(const std::string& line) {
    std::vector<std::string> cols = splitCsvLine(line);
    
    if (cols.size() < 6) {
        throw std::runtime_error("Invalid CSV line: insufficient columns");
    }
    
    Parenthetical record;
    record.id = parse_int_safe(cols[getColumn("id")]);
    record.text = trim(cols[getColumn("text")]);
    record.score = parse_double_safe(cols[getColumn("score")]);
    record.described_opinion_id = parse_int_safe(cols[getColumn("described_opinion_id")]);
    record.describing_opinion_id = parse_int_safe(cols[getColumn("describing_opinion_id")]);
    record.group_id = parse_int_safe(cols[getColumn("group_id")]);
    
    return record;
}

std::vector<std::string> ParentheticalReader::splitCsvLine(const std::string& line) {
    std::vector<std::string> result;
    std::string current;
    bool in_quotes = false;
    bool escape_next = false;
    
    for (size_t i = 0; i < line.size(); ++i) {
        char c = line[i];
        
        if (escape_next) {
            current += c;
            escape_next = false;
            continue;
        }
        
        if (c == '\\') {
            escape_next = true;
            continue;
        }
        
        if (c == '"') {
            // Check for double-quote escape
            if (in_quotes && i + 1 < line.size() && line[i + 1] == '"') {
                current += '"';
                ++i; // Skip next quote
                continue;
            }
            in_quotes = !in_quotes;
            continue;
        }
        
        if (c == ',' && !in_quotes) {
            result.push_back(current);
            current.clear();
            continue;
        }
        
        current += c;
    }
    
    // Add last field
    result.push_back(current);
    
    return result;
}

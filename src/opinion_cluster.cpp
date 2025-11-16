#include "opinion_cluster.h"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <limits>
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

static inline string lower(string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){ return std::tolower(c); });
    return s;
}

static inline bool parse_bool(const string& s) {
    auto v = lower(trim(s));
    return v == "true" || v == "t" || v == "1" || v == "yes";
}

template <typename T>
static inline optional<T> parse_optional_int(const string& s) {
    auto t = trim(s);
    if (t.empty()) return std::nullopt;
    try {
        long long v = std::stoll(t);
        return static_cast<T>(v);
    } catch (...) {
        return std::nullopt;
    }
}

// Safe parsing with defaults
static inline int parse_int_safe(const string& s, int default_val = 0) {
    auto t = trim(s);
    if (t.empty()) return default_val;
    try {
        return std::stoi(t);
    } catch (...) {
        return default_val;
    }
}

static inline bool parse_bool_safe(const string& s, bool default_val = false) {
    auto t = trim(s);
    if (t.empty()) return default_val;
    try {
        auto v = lower(t);
        return v == "true" || v == "t" || v == "1" || v == "yes";
    } catch (...) {
        return default_val;
    }
}

string OpinionCluster::toString() const {
    std::ostringstream oss;
    oss << "OpinionCluster{";
    oss << "id=" << id;
    oss << ", case_name='" << case_name << "'";
    oss << ", date_filed='" << date_filed << "'";
    oss << ", docket_id=" << docket_id;
    oss << ", precedential_status='" << precedential_status << "'";
    oss << ", citation_count=" << citation_count;
    oss << ", blocked=" << (blocked ? "true" : "false");
    oss << "}";
    return oss.str();
}

OpinionClusterReader::OpinionClusterReader(const string& filename) : filename_(filename) {}

void OpinionClusterReader::parseHeader(const string& header_line) {
    header_ = splitCsvLine(header_line);
    column_map_.clear();
    for (size_t i = 0; i < header_.size(); ++i) {
        column_map_[trim(header_[i])] = i;
    }
}

optional<string> OpinionClusterReader::getColumn(const vector<string>& cols, const string& name) {
    auto it = column_map_.find(name);
    if (it == column_map_.end()) return std::nullopt;
    size_t idx = it->second;
    if (idx >= cols.size()) return std::nullopt;
    return cols[idx];
}

bool OpinionClusterReader::isValidRow(const vector<string>& cols) {
    // We need at least the key columns: id (1), date_created (3), date_modified (4), date_filed (5), docket_id (21)
    // If we have at least 21 columns, we can extract the critical keys
    return cols.size() >= 21;
}

// RFC 4180-style CSV parsing with lenient quote handling
vector<string> OpinionClusterReader::splitCsvLine(const string& line) {
    vector<string> result;
    bool in_quotes = false;
    string current;
    
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
                // Escaped quote ""
                current += '"';
                ++i;
            } else if (in_quotes) {
                // Check if this quote is followed by comma or end-of-line (field terminator)
                if (i + 1 >= line.size() || line[i + 1] == ',') {
                    in_quotes = false; // This is the closing quote
                } else {
                    // Treat as literal quote (malformed CSV)
                    current += '"';
                }
            } else {
                // Opening quote
                in_quotes = true;
            }
        } else if (c == ',' && !in_quotes) {
            result.push_back(current);
            current.clear();
        } else {
            current += c;
        }
    }
    result.push_back(current);
    return result;
}

OpinionCluster OpinionClusterReader::parseCsvLine(const string& line) {
    auto cols = splitCsvLine(line);
    
    if (!isValidRow(cols)) {
        std::ostringstream oss;
        oss << "Invalid row: insufficient columns. Expected at least 21 (for keys), got " << cols.size();
        throw std::runtime_error(oss.str());
    }
    
    OpinionCluster cluster;
    
    // Helper to get column by name with default empty string
    auto get = [&](const string& name) -> string {
        auto val = getColumn(cols, name);
        return val ? trim(*val) : "";
    };
    
    // Helper to get optional string (nullable columns in DB)
    auto get_opt_str = [&](const string& name) -> optional<string> {
        auto val = getColumn(cols, name);
        if (!val || trim(*val).empty()) return std::nullopt;
        return trim(*val);
    };
    
    // Helper to get column by index with default
    auto get_by_idx = [&](size_t idx, const string& default_val = "") -> string {
        if (idx >= cols.size()) return default_val;
        return trim(cols[idx]);
    };
    
    // Parse PRIMARY KEY and FOREIGN KEY fields first
    cluster.id = parse_int_safe(get("id"), 0);
    cluster.docket_id = parse_int_safe(get("docket_id"), 0);
    
    // Validate primary key - must be valid
    if (cluster.id <= 0) {
        throw std::runtime_error("Invalid primary key: id is missing or <= 0");
    }
    
    // For foreign keys: if missing or zero, set to MAX int (placeholder for missing references)
    if (cluster.docket_id <= 0) {
        cluster.docket_id = std::numeric_limits<int>::max();
    }
    
    // Parse NOT NULL text fields with empty string defaults (PostgreSQL NOT NULL text = '')
    cluster.judges = get("judges");
    cluster.date_created = get("date_created");
    cluster.date_modified = get("date_modified");
    cluster.date_filed = get("date_filed");
    cluster.case_name_short = get("case_name_short");
    cluster.case_name = get("case_name");
    cluster.case_name_full = get("case_name_full");
    cluster.scdb_id = get("scdb_id");
    cluster.source = get("source");
    cluster.procedural_history = get("procedural_history");
    cluster.attorneys = get("attorneys");
    cluster.nature_of_suit = get("nature_of_suit");
    cluster.posture = get("posture");
    cluster.syllabus = get("syllabus");
    cluster.precedential_status = get("precedential_status");
    cluster.correction = get("correction");
    cluster.cross_reference = get("cross_reference");
    cluster.disposition = get("disposition");
    cluster.filepath_json_harvard = get("filepath_json_harvard");
    cluster.headnotes = get("headnotes");
    cluster.history = get("history");
    cluster.other_dates = get("other_dates");
    cluster.summary = get("summary");
    cluster.arguments = get("arguments");
    cluster.headmatter = get("headmatter");
    cluster.filepath_pdf_harvard = get("filepath_pdf_harvard");
    
    // Parse nullable fields (can be NULL in DB)
    cluster.slug = get_opt_str("slug");
    cluster.date_blocked = get_opt_str("date_blocked");
    
    // Parse NOT NULL integer/boolean fields with safe defaults
    cluster.citation_count = parse_int_safe(get("citation_count"), 0);
    cluster.blocked = parse_bool_safe(get("blocked"), false);
    cluster.date_filed_is_approximate = parse_bool_safe(get("date_filed_is_approximate"), false);
    // Parse nullable integer fields (can be NULL in DB)
    cluster.scdb_decision_direction = parse_optional_int<int>(get("scdb_decision_direction"));
    cluster.scdb_votes_majority = parse_optional_int<int>(get("scdb_votes_majority"));
    cluster.scdb_votes_minority = parse_optional_int<int>(get("scdb_votes_minority"));
    
    return cluster;
}

// Helper to check if a string looks like a date (YYYY-MM-DD pattern)
static bool looksLikeDate(const string& s, size_t start, size_t end) {
    // Allow for optional leading quote
    if (start < end && s[start] == '"') start++;
    
    // Need at least YYYY-MM-DD = 10 chars
    if (end - start < 10) return false;
    
    // Check YYYY-MM-DD pattern
    for (size_t i = start; i < start + 4 && i < end; ++i) {
        if (!std::isdigit(s[i])) return false;
    }
    if (start + 4 < end && s[start + 4] != '-') return false;
    if (start + 5 < end && !std::isdigit(s[start + 5])) return false;
    if (start + 6 < end && !std::isdigit(s[start + 6])) return false;
    if (start + 7 < end && s[start + 7] != '-') return false;
    if (start + 8 < end && !std::isdigit(s[start + 8])) return false;
    if (start + 9 < end && !std::isdigit(s[start + 9])) return false;
    
    return true;
}

// Extract raw records using chunked reading with delimiter detection
// Delimiter: newline + comma + text/null + comma + date
vector<string> OpinionClusterReader::extractRawRecords(size_t max_records) {
    std::ifstream file(filename_, std::ios::binary);
    if (!file) {
        throw std::runtime_error("Could not open file: " + filename_);
    }
    
    // Skip header line
    string header_line;
    std::getline(file, header_line);
    parseHeader(header_line);
    
    const size_t CHUNK_SIZE = 1024 * 1024; // 1MB
    vector<string> records;
    records.reserve(max_records);
    
    string leftover;
    vector<size_t> delimiter_positions;
    delimiter_positions.reserve(1000);
    
    while (records.size() < max_records && !file.eof()) {
        vector<char> buffer(CHUNK_SIZE);
        file.read(buffer.data(), CHUNK_SIZE);
        size_t bytes_read = file.gcount();
        
        if (bytes_read == 0) break;
        
        string chunk = leftover + string(buffer.data(), bytes_read);
        delimiter_positions.clear();
        delimiter_positions.push_back(0);
        
        // Find record boundaries using special pattern: \n + ID + , + date_created + , + date_modified
        // Be tolerant: fields may be quoted or unquoted; date fields may be timestamps.
        // We'll skip exactly two CSV fields after the ID and check that the next field looks like a date.
        auto skipCsvField = [&chunk](size_t pos) -> size_t {
            bool in_quotes = false;
            while (pos < chunk.size()) {
                char ch = chunk[pos];
                if (ch == '"') {
                    // Handle doubled quotes inside quoted field
                    if (in_quotes && pos + 1 < chunk.size() && chunk[pos + 1] == '"') { pos += 2; continue; }
                    in_quotes = !in_quotes;
                    pos++;
                    continue;
                }
                if (!in_quotes && ch == ',') { pos++; break; }
                pos++;
            }
            return pos; // points to start of next field (or chunk.size())
        };

        for (size_t i = 0; i < chunk.size(); ++i) {
            if (chunk[i] != '\n') continue;

            size_t pos = i + 1;
            if (pos >= chunk.size()) continue;

            // ID may be quoted or unquoted
            if (chunk[pos] == '"') {
                pos++;
                if (pos >= chunk.size() || !std::isdigit(static_cast<unsigned char>(chunk[pos]))) continue;
                while (pos < chunk.size() && std::isdigit(static_cast<unsigned char>(chunk[pos]))) pos++;
                if (pos >= chunk.size() || chunk[pos] != '"') continue; // require closing quote
                pos++; // move past closing quote
            } else {
                if (!std::isdigit(static_cast<unsigned char>(chunk[pos]))) continue;
                while (pos < chunk.size() && std::isdigit(static_cast<unsigned char>(chunk[pos]))) pos++;
            }

            if (pos >= chunk.size() || chunk[pos] != ',') continue;
            pos++; // after comma, at start of date_created

            // Skip date_created, date_modified, judges (quoted/unquoted/empty)
            pos = skipCsvField(pos);
            if (pos >= chunk.size()) continue;
            pos = skipCsvField(pos);
            if (pos >= chunk.size()) continue;
            pos = skipCsvField(pos);
            if (pos >= chunk.size()) continue;

            // Now at start of date_filed – validate YYYY-MM-DD
            if (looksLikeDate(chunk, pos, chunk.size())) {
                delimiter_positions.push_back(i + 1);
            }
        }
        
        // Extract records
        for (size_t j = 0; j + 1 < delimiter_positions.size() && records.size() < max_records; ++j) {
            size_t rec_start = delimiter_positions[j];
            size_t rec_end = delimiter_positions[j + 1];
            
            if (rec_start < rec_end && rec_end <= chunk.size()) {
                string record = chunk.substr(rec_start, rec_end - rec_start);
                // Remove trailing newline if present
                if (!record.empty() && record.back() == '\n') {
                    record.pop_back();
                }
                if (!record.empty()) {
                    records.push_back(record);
                }
            }
        }
        
        // Save leftover for next chunk
        if (delimiter_positions.size() >= 2) {
            size_t last_delimiter = delimiter_positions.back();
            if (last_delimiter < chunk.size()) {
                leftover = chunk.substr(last_delimiter);
            } else {
                leftover.clear();
            }
        } else {
            leftover = chunk;
        }
        
        if (records.size() >= max_records) break;
    }
    
    file.close();
    return records;
}

void OpinionClusterReader::initStream() {
    if (streamed_initialized_) return;
    file_stream_ = std::ifstream(filename_, std::ios::binary);
    if (!file_stream_) {
        throw std::runtime_error("Could not open file: " + filename_);
    }
    // Read and parse header once
    string header_line;
    std::getline(file_stream_, header_line);
    parseHeader(header_line);
    leftover_.clear();
    eof_ = false;
    streamed_initialized_ = true;
}

bool OpinionClusterReader::readNextBatch(vector<string>& outRecords, size_t max_records, size_t chunk_bytes) {
    if (!streamed_initialized_) initStream();
    outRecords.clear();
    outRecords.reserve(max_records);
    if (eof_) return false;

    vector<size_t> delimiter_positions;
    delimiter_positions.reserve(2048);

    while (outRecords.size() < max_records && !file_stream_.eof()) {
        vector<char> buffer(chunk_bytes);
        file_stream_.read(buffer.data(), chunk_bytes);
        size_t bytes_read = file_stream_.gcount();
        if (bytes_read == 0) { eof_ = true; break; }

        string chunk = leftover_ + string(buffer.data(), bytes_read);
        delimiter_positions.clear();
        delimiter_positions.push_back(0);

        // Find record boundaries using special pattern: \n + ID + , + date_created + , + date_modified
        // Be tolerant: fields may be quoted or unquoted; date fields may be timestamps.
        // We'll skip exactly two CSV fields after the ID and check that the next field looks like a date.
        auto skipCsvField = [&chunk](size_t pos) -> size_t {
            bool in_quotes = false;
            while (pos < chunk.size()) {
                char ch = chunk[pos];
                if (ch == '"') {
                    if (in_quotes && pos + 1 < chunk.size() && chunk[pos + 1] == '"') { pos += 2; continue; }
                    in_quotes = !in_quotes;
                    pos++;
                    continue;
                }
                if (!in_quotes && ch == ',') { pos++; break; }
                pos++;
            }
            return pos;
        };

        for (size_t i = 0; i < chunk.size(); ++i) {
            if (chunk[i] != '\n') continue;

            size_t pos = i + 1;
            if (pos >= chunk.size()) continue;

            // ID may be quoted or unquoted
            if (chunk[pos] == '"') {
                pos++;
                if (pos >= chunk.size() || !std::isdigit(static_cast<unsigned char>(chunk[pos]))) continue;
                while (pos < chunk.size() && std::isdigit(static_cast<unsigned char>(chunk[pos]))) pos++;
                if (pos >= chunk.size() || chunk[pos] != '"') continue;
                pos++;
            } else {
                if (!std::isdigit(static_cast<unsigned char>(chunk[pos]))) continue;
                while (pos < chunk.size() && std::isdigit(static_cast<unsigned char>(chunk[pos]))) pos++;
            }

            if (pos >= chunk.size() || chunk[pos] != ',') continue;
            pos++; // at date_created

            // Skip date_created, date_modified, judges (quoted/unquoted/empty)
            pos = skipCsvField(pos);
            if (pos >= chunk.size()) continue;
            pos = skipCsvField(pos);
            if (pos >= chunk.size()) continue;
            pos = skipCsvField(pos);
            if (pos >= chunk.size()) continue;

            // Now at start of date_filed – validate YYYY-MM-DD
            if (looksLikeDate(chunk, pos, chunk.size())) {
                delimiter_positions.push_back(i + 1);
            }
        }

        // Extract into outRecords
        for (size_t j = 0; j + 1 < delimiter_positions.size() && outRecords.size() < max_records; ++j) {
            size_t rec_start = delimiter_positions[j];
            size_t rec_end = delimiter_positions[j + 1];
            if (rec_start < rec_end && rec_end <= chunk.size()) {
                string record = chunk.substr(rec_start, rec_end - rec_start);
                if (!record.empty() && record.back() == '\n') record.pop_back();
                if (!record.empty()) outRecords.push_back(std::move(record));
            }
        }

        // Prepare leftover for next read
        if (delimiter_positions.size() >= 2) {
            size_t last_delim = delimiter_positions.back();
            if (last_delim < chunk.size()) {
                leftover_ = chunk.substr(last_delim);
            } else {
                leftover_.clear();
            }
        } else {
            leftover_ = std::move(chunk);
        }

        if (outRecords.size() >= max_records) break;
    }

    if (file_stream_.eof()) eof_ = true;
    return !outRecords.empty();
}

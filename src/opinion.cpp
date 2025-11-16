#include "opinion.h"

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

// ---- Opinion pretty printer ----
std::string Opinion::toString() const {
    std::ostringstream oss;
    oss << "Opinion{";
    oss << "id=" << id;
    oss << ", date_created='" << date_created << "'";
    oss << ", date_modified='" << date_modified << "'";
    oss << ", type='" << type << "'";
    oss << ", sha1='" << sha1 << "'";
    if (download_url) oss << ", download_url='" << *download_url << "'";
    oss << ", local_path='" << local_path << "'";
    oss << ", extracted_by_ocr=" << (extracted_by_ocr ? "true" : "false");
    if (author_id) oss << ", author_id=" << *author_id;
    oss << ", cluster_id=" << cluster_id;
    oss << ", per_curiam=" << (per_curiam ? "true" : "false");
    if (page_count) oss << ", page_count=" << *page_count;
    if (ordering_key) oss << ", ordering_key=" << *ordering_key;
    if (main_version_id) oss << ", main_version_id=" << *main_version_id;
    oss << ", author_str='" << author_str << "'";
    oss << ", joined_by_str='" << joined_by_str << "'";
    oss << "}";
    return oss.str();
}

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

// ---- OpinionReader basics ----
OpinionReader::OpinionReader(const std::string& filename) : filename_(filename) {}

void OpinionReader::parseHeader(const std::string& header_line) {
    header_ = splitCsvLine(header_line);
    column_map_.clear();
    for (size_t i = 0; i < header_.size(); ++i) {
        column_map_[trim(header_[i])] = i;
    }
}

std::optional<std::string> OpinionReader::getColumn(const std::vector<std::string>& cols, const std::string& name) {
    auto it = column_map_.find(name);
    if (it == column_map_.end()) return std::nullopt;
    size_t idx = it->second;
    if (idx >= cols.size()) return std::nullopt;
    return cols[idx];
}

// Helper: does substring starting at start look like a timestamp (YYYY-MM-DD ...)?
static bool looksLikeTimestamp(const std::string& s, size_t start, size_t end) {
    // Find end of field (comma or end)
    size_t field_end = start;
    while (field_end < end && s[field_end] != '\n' && s[field_end] != ',') field_end++;
    size_t len = field_end - start;
    if (len < 10) return false; // need at least YYYY-MM-DD
    // Check YYYY-MM-DD
    for (size_t i = 0; i < 4; ++i) { if (start + i >= end || !std::isdigit((unsigned char)s[start + i])) return false; }
    if (start + 4 >= end || s[start + 4] != '-') return false;
    if (start + 5 >= end || !std::isdigit((unsigned char)s[start + 5])) return false;
    if (start + 6 >= end || !std::isdigit((unsigned char)s[start + 6])) return false;
    if (start + 7 >= end || s[start + 7] != '-') return false;
    if (start + 8 >= end || !std::isdigit((unsigned char)s[start + 8])) return false;
    if (start + 9 >= end || !std::isdigit((unsigned char)s[start + 9])) return false;
    return true;
}

void OpinionReader::initStream() {
    if (streamed_initialized_) return;
    file_stream_ = std::ifstream(filename_, std::ios::binary);
    if (!file_stream_) throw std::runtime_error("Could not open file: " + filename_);
    std::string header_line;
    std::getline(file_stream_, header_line);
    parseHeader(header_line);
    leftover_.clear();
    eof_ = false;
    streamed_initialized_ = true;
}

bool OpinionReader::readNextBatch(std::vector<std::string>& outRecords, size_t max_records, size_t chunk_bytes) {
    if (!streamed_initialized_) initStream();
    outRecords.clear();
    outRecords.reserve(max_records);
    if (eof_) return false;

    std::vector<size_t> delimiter_positions;
    delimiter_positions.reserve(2048);

    // Quote balance tracking to avoid splitting inside quoted fields
    while (outRecords.size() < max_records && !file_stream_.eof()) {
        std::vector<char> buffer(chunk_bytes);
        file_stream_.read(buffer.data(), chunk_bytes);
        size_t bytes_read = file_stream_.gcount();
        if (bytes_read == 0) { eof_ = true; break; }

        std::string chunk = leftover_ + std::string(buffer.data(), bytes_read);
        delimiter_positions.clear();
        delimiter_positions.push_back(0);

        bool in_quotes = false;
        for (size_t i = 0; i < chunk.size(); ++i) {
            char c = chunk[i];
            if (c == '"') {
                // Handle doubled quotes
                if (in_quotes && i + 1 < chunk.size() && chunk[i + 1] == '"') { i++; continue; }
                in_quotes = !in_quotes;
            }
            if (c != '\n' || in_quotes) continue;

            size_t pos = i + 1; // candidate record start
            if (pos >= chunk.size()) continue;

            // Skip optional CR and whitespace
            while (pos < chunk.size() && (chunk[pos] == '\r' || chunk[pos] == ' ' || chunk[pos] == '\t')) pos++;

            // ID may be quoted or unquoted
            if (pos < chunk.size() && chunk[pos] == '"') {
                pos++;
                if (pos >= chunk.size() || !std::isdigit((unsigned char)chunk[pos])) continue;
                while (pos < chunk.size() && std::isdigit((unsigned char)chunk[pos])) pos++;
                if (pos >= chunk.size() || chunk[pos] != '"') continue; // need closing quote
                pos++;
            } else {
                if (pos >= chunk.size() || !std::isdigit((unsigned char)chunk[pos])) continue;
                while (pos < chunk.size() && std::isdigit((unsigned char)chunk[pos])) pos++;
            }

            // Skip optional whitespace before comma
            while (pos < chunk.size() && (chunk[pos] == ' ' || chunk[pos] == '\t')) pos++;
            if (pos >= chunk.size() || chunk[pos] != ',') continue;
            pos++; // start of date_created field

            // Skip an optional opening quote on the timestamp and whitespace
            while (pos < chunk.size() && (chunk[pos] == '"' || chunk[pos] == ' ' || chunk[pos] == '\t')) pos++;

            if (looksLikeTimestamp(chunk, pos, chunk.size())) {
                delimiter_positions.push_back(i + 1);
            }
        }

        // Slice out records
        for (size_t j = 0; j + 1 < delimiter_positions.size() && outRecords.size() < max_records; ++j) {
            size_t rec_start = delimiter_positions[j];
            size_t rec_end = delimiter_positions[j + 1];
            if (rec_start < rec_end && rec_end <= chunk.size()) {
                std::string record = chunk.substr(rec_start, rec_end - rec_start);
                if (!record.empty() && record.back() == '\n') record.pop_back();
                if (!record.empty()) outRecords.push_back(std::move(record));
            }
        }

        // Prepare leftover
        if (delimiter_positions.size() >= 2) {
            size_t last_delim = delimiter_positions.back();
            if (last_delim < chunk.size()) leftover_ = chunk.substr(last_delim); else leftover_.clear();
        } else {
            leftover_ = std::move(chunk);
        }

        if (outRecords.size() >= max_records) break;
    }

    if (file_stream_.eof()) eof_ = true;
    return !outRecords.empty();
}

// Safe parsing with defaults (mirrors helpers in opinion_cluster.cpp)
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

bool OpinionReader::isValidRow(const vector<string>& cols) {
    // Must have at least 2 columns
    if (cols.size() < 2) return false;

    // id must be a number
    auto id_col = getColumn(cols, "id");
    if (!id_col || id_col->empty()) return false;
    try {
        std::stoi(trim(*id_col));
    } catch (...) {
        return false;
    }

    // date_created must exist and be non-empty
    auto date_col = getColumn(cols, "date_created");
    if (!date_col || trim(*date_col).empty()) return false;

    return true;
}

// RFC-4180-ish CSV splitter with lenient handling of malformed quotes and backslash escapes.
vector<string> OpinionReader::splitCsvLine(const string& line) {
    vector<string> out;
    string field;
    bool in_quotes = false;
    for (size_t i = 0; i < line.size(); ++i) {
        char c = line[i];

        // Treat backslash-escaped quote (\") as a literal quote in content
        if (c == '\\' && i + 1 < line.size() && line[i + 1] == '"') {
            field.push_back('"');
            ++i; // skip the quote
            continue;
        }

        if (c == '"') {
            if (in_quotes) {
                // Escaped doubled quote
                if (i + 1 < line.size() && line[i + 1] == '"') {
                    field.push_back('"');
                    ++i;
                } else if (i + 1 >= line.size() || line[i + 1] == ',') {
                    // Close quote only if followed by comma or end-of-line
                    in_quotes = false;
                } else {
                    // Malformed: keep as literal
                    field.push_back('"');
                }
            } else {
                in_quotes = true; // opening quote
            }
        } else if (c == ',' && !in_quotes) {
            out.push_back(field);
            field.clear();
        } else {
            field.push_back(c);
        }
    }
    out.push_back(field);
    return out;
}

Opinion OpinionReader::parseCsvLine(const string& line) {
    auto cols = splitCsvLine(line);
    
    // Helper to safely get column value with default
    auto get = [&](const string& name, const string& default_val = "") -> string {
        auto val = getColumn(cols, name);
        return val ? *val : default_val;
    };
    
    Opinion o{};
    // Mandatory fields with safe defaults
    o.id = parse_int_safe(get("id"), 0);
    o.date_created = get("date_created");
    o.date_modified = get("date_modified");
    o.type = get("type");
    o.sha1 = get("sha1");
    
    // Optional string fields
    auto download_url_val = get("download_url");
    o.download_url = trim(download_url_val).empty() ? std::nullopt : optional<string>{download_url_val};
    o.local_path = get("local_path");
    o.plain_text = get("plain_text");
    o.html = get("html");
    o.html_lawbox = get("html_lawbox");
    o.html_columbia = get("html_columbia");
    o.html_with_citations = get("html_with_citations");
    
    // Boolean fields with safe defaults (false)
    o.extracted_by_ocr = parse_bool_safe(get("extracted_by_ocr"), false);
    o.per_curiam = parse_bool_safe(get("per_curiam"), false);
    
    // Optional integer fields
    o.author_id = parse_optional_int<int>(get("author_id"));
    o.cluster_id = parse_int_safe(get("cluster_id"), 0);
    o.page_count = parse_optional_int<int>(get("page_count"));
    o.ordering_key = parse_optional_int<int>(get("ordering_key"));
    o.main_version_id = parse_optional_int<int>(get("main_version_id"));
    
    // String fields
    o.author_str = get("author_str");
    o.joined_by_str = get("joined_by_str");
    o.xml_harvard = get("xml_harvard");
    o.html_anon_2020 = get("html_anon_2020");
    
    return o;
}

vector<Opinion> OpinionReader::readOpinions(size_t max_lines) {
    std::ifstream in(filename_);
    if (!in) {
        throw std::runtime_error("Failed to open file: " + filename_);
    }

    vector<Opinion> result;
    result.reserve(max_lines);

    // Read records, merging physical lines when inside quotes
    string physical;
    string record;
    bool in_quotes = false;
    size_t records_read = 0;
    bool header_parsed = false;

    while (std::getline(in, physical)) {
        if (!record.empty()) record.push_back('\n');
        record += physical;

        // Determine if we are balanced: count quotes not including escaped quotes
        size_t quote_count = 0;
        for (size_t i = 0; i < physical.size(); ++i) {
            if (physical[i] == '"') {
                if (i + 1 < physical.size() && physical[i + 1] == '"') {
                    ++i; // skip escaped
                } else {
                    quote_count++;
                }
            }
        }
        if (quote_count % 2 == 1) in_quotes = !in_quotes;

        if (!in_quotes) {
            // complete record
            if (!header_parsed) {
                // Parse header
                parseHeader(record);
                header_parsed = true;
                record.clear();
                continue;
            }

            auto cols = splitCsvLine(record);
            
            // Skip rows with too many columns (more than header)
            if (cols.size() > header_.size()) {
                std::cerr << "Warning: skipping row with " << cols.size() 
                          << " columns (expected " << header_.size() << ")\n";
                record.clear();
                continue;
            }
            
            // Validate mandatory fields
            if (!isValidRow(cols)) {
                std::cerr << "Warning: skipping invalid row (missing id or date_created)\n";
                record.clear();
                continue;
            }

            try {
                Opinion o = parseCsvLine(record);
                result.push_back(std::move(o));
                records_read++;
            } catch (const std::exception& e) {
                std::cerr << "Warning: failed to parse row: " << e.what() << "\n";
            }
            
            record.clear();
            if (records_read >= max_lines) break;
        }
    }
    return result;
}

// Helper: check if a string looks like a timestamp at given position
// Expected pattern: YYYY-MM-DD HH:MM:SS... (at least 10 chars for date part)
static bool looksLikeTimestamp(const string& buffer, size_t pos, size_t& timestamp_end) {
    if (pos >= buffer.size() || buffer.size() - pos < 10) return false;
    
    // Check for pattern: digit{4}-digit{2}-digit{2}
    if (!std::isdigit(buffer[pos]) || !std::isdigit(buffer[pos+1]) || 
        !std::isdigit(buffer[pos+2]) || !std::isdigit(buffer[pos+3])) return false;
    if (buffer[pos+4] != '-') return false;
    if (!std::isdigit(buffer[pos+5]) || !std::isdigit(buffer[pos+6])) return false;
    if (buffer[pos+7] != '-') return false;
    if (!std::isdigit(buffer[pos+8]) || !std::isdigit(buffer[pos+9])) return false;
    
    // Find the next comma after the timestamp
    timestamp_end = pos + 10;
    while (timestamp_end < buffer.size() && buffer[timestamp_end] != ',') {
        timestamp_end++;
    }
    
    return true;
}

vector<string> OpinionReader::extractRawRecords(size_t max_records) {
    std::ifstream in(filename_, std::ios::binary);
    if (!in) {
        throw std::runtime_error("Failed to open file: " + filename_);
    }

    vector<string> records;
    records.reserve(max_records);

    const size_t CHUNK_SIZE = 1024 * 1024; // 1MB chunks
    vector<char> chunk_buffer(CHUNK_SIZE);
    string leftover; // Incomplete record from previous chunk
    bool skip_header = true;

    while (in && records.size() < max_records) {
        // Read next chunk
        in.read(chunk_buffer.data(), CHUNK_SIZE);
        std::streamsize bytes_read = in.gcount();
        if (bytes_read == 0) break;

        // Combine leftover from previous chunk with new data
        string buffer = leftover + string(chunk_buffer.data(), bytes_read);
        leftover.clear();

        // Skip header on first chunk
        size_t start_pos = 0;
        if (skip_header) {
            size_t header_end = buffer.find('\n');
            if (header_end == string::npos) {
                // Header spans multiple chunks (unlikely but handle it)
                leftover = buffer;
                continue;
            }
            start_pos = header_end + 1;
            skip_header = false;
        }

        // Find all record delimiters in this chunk
        // Pattern: \n<number>,<date>,
        vector<size_t> delimiter_positions;
        
        // Check if first record starts at start_pos (right after header)
        // We need to verify it matches the pattern by checking backwards
        if (start_pos > 0 && start_pos < buffer.size()) {
            // The record should start after a \n, so check if start_pos-1 is \n
            // For first chunk after header, this is guaranteed, so we add it
            delimiter_positions.push_back(start_pos);
        }

        for (size_t pos = start_pos; pos < buffer.size(); ++pos) {
            // Step 1: Find \n
            if (buffer[pos] != '\n') continue;
            if (pos + 1 >= buffer.size()) break; // Need at least one char after \n
            
            // Step 2: Find position of next comma (after the ID)
            size_t first_comma = pos + 1;
            while (first_comma < buffer.size() && buffer[first_comma] != ',' && buffer[first_comma] != '\n') {
                first_comma++;
            }
            if (first_comma >= buffer.size() || buffer[first_comma] != ',') continue;
            
            // Step 3: Verify that ONLY digits (and optional quotes) exist between \n and first comma
            bool all_digits = true;
            bool has_digit = false;
            for (size_t i = pos + 1; i < first_comma; ++i) {
                if (std::isdigit(buffer[i])) {
                    has_digit = true;
                } else if (buffer[i] != '"') {
                    // Found a non-digit, non-quote character
                    all_digits = false;
                    break;
                }
            }
            if (!all_digits || !has_digit) continue;
            
            // Step 4: Find position of next comma (after the timestamp)
            size_t second_comma = first_comma + 1;
            while (second_comma < buffer.size() && buffer[second_comma] != ',') {
                second_comma++;
            }
                if (second_comma >= buffer.size()) continue;
            
            // Step 5: Verify string between two commas is at least a date structure
            size_t timestamp_start = first_comma + 1;
            size_t timestamp_length = second_comma - timestamp_start;
            if (timestamp_length < 10) continue; // Minimum "YYYY-MM-DD" is 10 chars
            
            // Skip leading quote if present
            size_t date_pos = timestamp_start;
            if (buffer[date_pos] == '"') {
                date_pos++;
                if (date_pos >= second_comma) continue; // Just a quote, no date
            }
            
            // Check for basic date pattern: YYYY-MM-DD
            if (date_pos + 9 < buffer.size() &&
                std::isdigit(buffer[date_pos]) &&
                std::isdigit(buffer[date_pos + 1]) &&
                std::isdigit(buffer[date_pos + 2]) &&
                std::isdigit(buffer[date_pos + 3]) &&
                buffer[date_pos + 4] == '-' &&
                std::isdigit(buffer[date_pos + 5]) &&
                std::isdigit(buffer[date_pos + 6]) &&
                buffer[date_pos + 7] == '-' &&
                std::isdigit(buffer[date_pos + 8]) &&
                std::isdigit(buffer[date_pos + 9])) {
                // Found a valid record delimiter
                delimiter_positions.push_back(pos + 1); // Record starts after the \n
            }
        }

        // Extract complete records from this chunk
        // Need at least 2 delimiter positions to extract a complete record
        if (delimiter_positions.size() >= 2) {
            for (size_t i = 0; i < delimiter_positions.size() - 1 && records.size() < max_records; ++i) {
                size_t rec_start = delimiter_positions[i];
                size_t rec_end = delimiter_positions[i + 1] - 1; // -1 to exclude the \n before next record
            
            string record = buffer.substr(rec_start, rec_end - rec_start);
            // Trim trailing whitespace
            while (!record.empty() && (record.back() == '\n' || record.back() == '\r' || std::isspace(static_cast<unsigned char>(record.back())))) {
                record.pop_back();
            }
            if (!record.empty()) {
                records.push_back(record);
            }
            }
        }

        // Handle the last partial record in this chunk
        if (!delimiter_positions.empty()) {
            size_t last_delimiter = delimiter_positions.back();
            if (last_delimiter < buffer.size()) {
                // Save incomplete record for next iteration
                leftover = buffer.substr(last_delimiter);
            }
        } else {
            // No delimiters found in this chunk, carry everything forward
            leftover = buffer.substr(start_pos);
        }
    }

    // Process any remaining data in leftover
    if (!leftover.empty() && records.size() < max_records) {
        // Trim trailing whitespace
        while (!leftover.empty() && (leftover.back() == '\n' || leftover.back() == '\r' || std::isspace(static_cast<unsigned char>(leftover.back())))) {
            leftover.pop_back();
        }
        if (!leftover.empty()) {
            records.push_back(leftover);
        }
    }

    return records;
}

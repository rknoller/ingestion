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
static inline int parse_int_safe(const string& s, int default_val = std::numeric_limits<int>::max()) {
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

string Opinion::toString() const {
    std::ostringstream oss;
    oss << "Opinion{";
    oss << "id=" << id;
    oss << ", type='" << type << "'";
    oss << ", sha1='" << sha1 << "'";
    oss << ", cluster_id=" << cluster_id;
    oss << ", per_curiam=" << (per_curiam ? "true" : "false");
    oss << ", extracted_by_ocr=" << (extracted_by_ocr ? "true" : "false");
    if (page_count) oss << ", page_count=" << *page_count;
    if (ordering_key) oss << ", ordering_key=" << *ordering_key;
    if (main_version_id) oss << ", main_version_id=" << *main_version_id;
    oss << "}";
    return oss.str();
}

OpinionReader::OpinionReader(const string& filename) : filename_(filename) {}

void OpinionReader::parseHeader(const string& header_line) {
    header_ = splitCsvLine(header_line);
    column_map_.clear();
    for (size_t i = 0; i < header_.size(); ++i) {
        column_map_[trim(header_[i])] = i;
    }
}

optional<string> OpinionReader::getColumn(const vector<string>& cols, const string& name) {
    auto it = column_map_.find(name);
    if (it == column_map_.end()) return std::nullopt;
    size_t idx = it->second;
    if (idx >= cols.size()) return std::nullopt;
    return cols[idx];
}

bool OpinionReader::isValidRow(const vector<string>& cols) {
    // Must have at least 2 columns
    if (cols.size() < 2) return false;
    
    // Check if first column (id) is a valid number
    auto id_col = getColumn(cols, "id");
    if (!id_col || id_col->empty()) return false;
    try {
        std::stoi(trim(*id_col));
    } catch (...) {
        return false;
    }
    
    // Check if second column (date_created) exists and is non-empty
    auto date_col = getColumn(cols, "date_created");
    if (!date_col || trim(*date_col).empty()) return false;
    
    return true;
}

// RFC-4180-ish CSV splitter: handles quotes, doubled quotes and commas/newlines inside quotes.
vector<string> OpinionReader::splitCsvLine(const string& line) {
    vector<string> out;
    string field;
    bool in_quotes = false;
    for (size_t i = 0; i < line.size(); ++i) {
        char c = line[i];
        if (in_quotes) {
            if (c == '"') {
                // Lookahead for escaped quote
                if (i + 1 < line.size() && line[i + 1] == '"') {
                    field.push_back('"');
                    ++i; // skip the escaped quote
                } else {
                    in_quotes = false;
                }
            } else {
                field.push_back(c);
            }
        } else {
            if (c == ',') {
                out.push_back(field);
                field.clear();
            } else if (c == '"') {
                in_quotes = true;
            } else {
                field.push_back(c);
            }
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

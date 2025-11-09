#pragma once

#include <string>
#include <vector>
#include <map>
#include <chrono>
#include <optional>

class Opinion {
public:
    int id;
    std::string date_created;
    std::string date_modified;
    std::string type;
    std::string sha1;
    std::optional<std::string> download_url;
    std::string local_path;
    std::string plain_text;
    std::string html;
    std::string html_lawbox;
    std::string html_columbia;
    std::string html_with_citations;
    bool extracted_by_ocr;
    std::optional<int> author_id;
    int cluster_id;
    bool per_curiam;
    std::optional<int> page_count;
    std::string author_str;
    std::string joined_by_str;
    std::string xml_harvard;
    std::string html_anon_2020;
    std::optional<int> ordering_key;
    std::optional<int> main_version_id;

    // For debugging/display
    std::string toString() const;
};

// CSV reader class with dynamic header parsing
class OpinionReader {
public:
    explicit OpinionReader(const std::string& filename);
    std::vector<Opinion> readOpinions(size_t max_lines = 100);
    
    // Public for testing
    Opinion parseCsvLine(const std::string& line);
    std::vector<std::string> splitCsvLine(const std::string& line);
    
private:
    std::string filename_;
    std::vector<std::string> header_;
    std::map<std::string, size_t> column_map_;
    
    void parseHeader(const std::string& header_line);
    std::optional<std::string> getColumn(const std::vector<std::string>& cols, const std::string& name);
    bool isValidRow(const std::vector<std::string>& cols);
};
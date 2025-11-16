#pragma once

#include <string>
#include <vector>
#include <map>
#include <optional>
#include <fstream>

class OpinionCluster {
public:
    // integer NOT NULL (primary key)
    int id;
    
    // text NOT NULL
    std::string judges;
    
    // timestamp with time zone NOT NULL
    std::string date_created;
    std::string date_modified;
    
    // date NOT NULL
    std::string date_filed;
    
    // character varying(75) - nullable
    std::optional<std::string> slug;
    
    // text NOT NULL
    std::string case_name_short;
    std::string case_name;
    std::string case_name_full;
    
    // character varying(10) NOT NULL
    std::string scdb_id;
    std::string source;
    
    // text NOT NULL
    std::string procedural_history;
    std::string attorneys;
    std::string nature_of_suit;
    std::string posture;
    std::string syllabus;
    
    // integer NOT NULL
    int citation_count;
    
    // character varying(50) NOT NULL
    std::string precedential_status;
    
    // date - nullable
    std::optional<std::string> date_blocked;
    
    // boolean NOT NULL
    bool blocked;
    
    // integer NOT NULL (foreign key)
    int docket_id;
    
    // integer - nullable
    std::optional<int> scdb_decision_direction;
    std::optional<int> scdb_votes_majority;
    std::optional<int> scdb_votes_minority;
    
    // boolean NOT NULL
    bool date_filed_is_approximate;
    
    // text NOT NULL
    std::string correction;
    std::string cross_reference;
    std::string disposition;
    
    // character varying(1000) NOT NULL
    std::string filepath_json_harvard;
    
    // text NOT NULL
    std::string headnotes;
    std::string history;
    std::string other_dates;
    std::string summary;
    std::string arguments;
    std::string headmatter;
    
    // character varying(100) NOT NULL
    std::string filepath_pdf_harvard;

    // For debugging/display
    std::string toString() const;
};

// CSV reader class for opinion clusters
class OpinionClusterReader {
public:
    explicit OpinionClusterReader(const std::string& filename);
    
    // Extract raw CSV records (multi-line aware, detects newline + comma + text/null + comma + date)
    std::vector<std::string> extractRawRecords(size_t max_records = 100);
    
    // Streaming API for large files
    // Initialize internal stream and parse header once
    void initStream();
    // Read next batch of raw records into outRecords. Returns false when EOF reached and no more records.
    bool readNextBatch(std::vector<std::string>& outRecords, size_t max_records = 1000, size_t chunk_bytes = 1024 * 1024);
    bool eof() const { return eof_; }
    
    // Public for testing
    OpinionCluster parseCsvLine(const std::string& line);
    std::vector<std::string> splitCsvLine(const std::string& line);
    void parseHeader(const std::string& header_line);
    
    // Get the parsed CSV header columns (after initStream or extractRawRecords)
    const std::vector<std::string>& getHeader() const { return header_; }
    
private:
    std::string filename_;
    std::vector<std::string> header_;
    std::map<std::string, size_t> column_map_;
    
    // Streaming state
    bool streamed_initialized_ = false;
    bool eof_ = false;
    std::ifstream file_stream_;
    std::string leftover_;
    
    std::optional<std::string> getColumn(const std::vector<std::string>& cols, const std::string& name);
    bool isValidRow(const std::vector<std::string>& cols);
};

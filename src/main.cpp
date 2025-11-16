#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <exception>
#include <optional>
#include "opinion.h"
#include "opinion_db.h"

int main(int argc, char** argv) {

    // CLI parsing: ingestion_app <opinions.csv> [--no-db] [--limit=N]
    std::string csvPath;
    bool skip_db = false;
    size_t limit = 100; // default record limit (parse-only mode)
    size_t batch_records = 5000; // default batch size for streaming DB insertion
    size_t chunk_bytes = 1024 * 1024; // 1MB chunk reads

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--no-db") {
            skip_db = true;
        } else if (arg == "--limit" && i + 1 < argc) {
            try {
                limit = static_cast<size_t>(std::stoull(argv[++i]));
            } catch (...) {
                std::cerr << "Invalid value for --limit: " << argv[i] << "\n";
                return 1;
            }
        } else if (arg.rfind("--limit=", 0) == 0) {
            try {
                limit = static_cast<size_t>(
                    std::stoull(arg.substr(std::string("--limit=").size())));
            } catch (...) {
                std::cerr << "Invalid value for --limit: " << arg << "\n";
                return 1;
            }
        } else if (arg == "--batch" && i + 1 < argc) {
            try { batch_records = static_cast<size_t>(std::stoull(argv[++i])); }
            catch (...) { std::cerr << "Invalid --batch value" << std::endl; return 1; }
        } else if (arg.rfind("--batch=",0)==0) {
            try { batch_records = static_cast<size_t>(std::stoull(arg.substr(8))); }
            catch (...) { std::cerr << "Invalid --batch value" << std::endl; return 1; }
        } else if (arg == "--chunk" && i + 1 < argc) {
            try { chunk_bytes = static_cast<size_t>(std::stoull(argv[++i])); }
            catch (...) { std::cerr << "Invalid --chunk value" << std::endl; return 1; }
        } else if (arg.rfind("--chunk=",0)==0) {
            try { chunk_bytes = static_cast<size_t>(std::stoull(arg.substr(8))); }
            catch (...) { std::cerr << "Invalid --chunk value" << std::endl; return 1; }
        } else if (!arg.empty() && arg[0] == '-') {
            std::cerr << "Unknown option: " << arg << "\n";
            std::cout << "Usage: ingestion_app <opinions.csv> [--no-db] [--limit=N]\n";
            return 1;
        } else if (csvPath.empty()) {
            csvPath = arg; // first non-option argument is the CSV path
        } else {
            std::cerr << "Unexpected extra argument: " << arg << "\n";
            std::cout << "Usage: ingestion_app <opinions.csv> [--no-db] [--limit=N]\n";
            return 1;
        }
    }

    if (csvPath.empty()) {
        std::cout << "Usage: ingestion_app <opinions.csv> [--no-db] [--limit=N]\n";
        std::cout << "  --no-db     Skip database insertion (just parse and display)\n";
        std::cout << "  --limit=N   Maximum number of records to extract (default 100)\n";
        return 0;
    }
    
    std::cout << "Reading raw opinion records from: " << csvPath << "\n";
    std::cout << "Record limit (parse-only): " << limit << ", batch_records=" << batch_records << ", chunk_bytes=" << chunk_bytes << "\n";
    
    try {
        OpinionReader reader(csvPath);
        
        // Streaming ingestion path (parse-only handled later)
        // Parse-only simple mode
        if (skip_db) {
            reader.initStream();
            std::vector<std::string> raw_records;
            if (!reader.readNextBatch(raw_records, limit, chunk_bytes)) { std::cout << "No records found." << std::endl; return 0; }
            std::vector<Opinion> opinions; opinions.reserve(raw_records.size());
            for (size_t i = 0; i < raw_records.size(); ++i) {
                try { opinions.push_back(reader.parseCsvLine(raw_records[i])); }
                catch (const std::exception& e) { std::cerr << "Parse failure rec=" << i << ": " << e.what() << std::endl; }
            }
            std::cout << "Parsed " << opinions.size() << " opinions" << std::endl;
            for (size_t i = 0; i < opinions.size() && i < 2; ++i) {
                std::cout << "=== Opinion " << i << " ===\n" << opinions[i].toString() << "\n";
            }
            return 0;
        }

        // Streaming DB insertion mode
        OpinionDatabase db("localhost", 5432, "courtlistener", "postgres", "postgres");
        if (!db.testConnection()) { std::cerr << "Database connection failed" << std::endl; return 1; }
        std::cout << "DB connection OK" << std::endl;

        reader.initStream();
        size_t batch_index = 0;
        std::vector<std::string> raw_records; raw_records.reserve(batch_records);
        std::vector<Opinion> opinions; opinions.reserve(batch_records);
        while (reader.readNextBatch(raw_records, batch_records, chunk_bytes)) {
            opinions.clear();
            for (size_t i = 0; i < raw_records.size(); ++i) {
                try { opinions.push_back(reader.parseCsvLine(raw_records[i])); }
                catch (const std::exception& e) { std::cerr << "Parse failure batch=" << (batch_index+1) << " rec=" << i << ": " << e.what() << std::endl; }
            }
            std::cout << "Batch " << (batch_index+1) << " parsed=" << opinions.size() << " raw=" << raw_records.size() << std::endl;
            if (!opinions.empty()) {
                try { db.insertOpinions(opinions); }
                catch (const std::exception& e) { std::cerr << "DB insertion error batch=" << (batch_index+1) << ": " << e.what() << std::endl; }
            }
            batch_index++;
            if (reader.eof()) break;
        }
        std::cout << "Opinion streaming ingestion finished after " << batch_index << " batches" << std::endl;
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "Fatal error: " << ex.what() << std::endl;
        return 1;
    }
    return 0;
}

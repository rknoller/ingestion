#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <exception>
#include <optional>
#include "opinion_cluster.h"
#include "opinion_cluster_db.h"

int main(int argc, char** argv) {

    // CLI parsing: cluster_ingestion_app <clusters.csv> [--no-db] [--limit=N] [--bad-records=file.csv]
    std::string csvPath;
    std::string bad_records_file; // optional output file for bad records
    bool skip_db = false;
    size_t limit = 100; // default record limit (for parse-only mode)
    size_t batch_records = 5000; // records per DB batch
    size_t chunk_bytes = 1024 * 1024; // 1MB default chunk

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
            catch (...) { std::cerr << "Invalid --batch value\n"; return 1; }
        } else if (arg.rfind("--batch=", 0) == 0) {
            try { batch_records = static_cast<size_t>(std::stoull(arg.substr(8))); }
            catch (...) { std::cerr << "Invalid --batch value\n"; return 1; }
        } else if (arg == "--chunk" && i + 1 < argc) {
            try { chunk_bytes = static_cast<size_t>(std::stoull(argv[++i])); }
            catch (...) { std::cerr << "Invalid --chunk value\n"; return 1; }
        } else if (arg.rfind("--chunk=", 0) == 0) {
            try { chunk_bytes = static_cast<size_t>(std::stoull(arg.substr(8))); }
            catch (...) { std::cerr << "Invalid --chunk value\n"; return 1; }
        } else if (arg == "--bad-records" && i + 1 < argc) {
            bad_records_file = argv[++i];
        } else if (arg.rfind("--bad-records=", 0) == 0) {
            bad_records_file = arg.substr(14);
        } else if (!arg.empty() && arg[0] == '-') {
            std::cerr << "Unknown option: " << arg << "\n";
            std::cout << "Usage: cluster_ingestion_app <clusters.csv> [--no-db] [--limit=N] [--bad-records=file.csv]\n";
            return 1;
        } else if (csvPath.empty()) {
            csvPath = arg; // first non-option argument is the CSV path
        } else {
            std::cerr << "Unexpected extra argument: " << arg << "\n";
            std::cout << "Usage: cluster_ingestion_app <clusters.csv> [--no-db] [--limit=N] [--bad-records=file.csv]\n";
            return 1;
        }
    }

    if (csvPath.empty()) {
        std::cout << "Usage: cluster_ingestion_app <clusters.csv> [--no-db] [--limit=N] [--bad-records=file.csv]\n";
        std::cout << "  --no-db              Skip database insertion (just parse and display)\n";
        std::cout << "  --limit=N            Maximum number of records to extract (default 100)\n";
        std::cout << "  --bad-records=FILE   Save bad records to CSV file\n";
        return 0;
    }
    
    std::cout << "Reading raw cluster records from: " << csvPath << "\n";
    std::cout << "Record limit: " << limit << " (parse-only), batch_records=" << batch_records
              << ", chunk_bytes=" << chunk_bytes << "\n";
    
    try {
        OpinionClusterReader reader(csvPath);
        
        // Streaming mode: iterate through file in chunks and process batches
        reader.initStream();
        
        // If parse-only (skip_db), we'll read a single batch of size 'limit' and print
        if (skip_db) {
            std::vector<std::string> raw_records;
            if (!reader.readNextBatch(raw_records, limit, chunk_bytes)) {
                std::cout << "No records found." << std::endl; return 0;
            }
            std::cout << "Extracted " << raw_records.size() << " raw cluster records\n";
            std::vector<OpinionCluster> clusters; clusters.reserve(raw_records.size());
            std::vector<std::string> bad_records; std::vector<std::string> bad_reasons;
            std::cout << "Parsing cluster records...\n";
            for (size_t i = 0; i < raw_records.size(); ++i) {
                try { clusters.push_back(reader.parseCsvLine(raw_records[i])); }
                catch (const std::exception& e) {
                    std::string msg = e.what();
                    if (msg.find("key") != std::string::npos || msg.find("insufficient columns") != std::string::npos) {
                        bad_records.push_back(raw_records[i]); bad_reasons.push_back(msg);
                    }
                }
            }
            std::cout << "Successfully parsed " << clusters.size() << " clusters\n";
            if (!bad_records.empty()) std::cout << "Bad records: " << bad_records.size() << "\n";
            std::cout << "Showing first 2 parsed cluster records:\n";
            for (size_t i = 0; i < clusters.size() && i < 2; ++i) {
                std::cout << "=== Cluster " << i << " ===\n" << clusters[i].toString() << "\n\n";
            }
            std::cout << "Skipping database insertion (--no-db flag)\n";
            return 0;
        }
        
        // With DB: stream all batches and insert incrementally
        std::cout << "Connecting to PostgreSQL...\n";
        OpinionClusterDatabase db("localhost", 5432, "courtlistener", "postgres", "postgres");
        if (!db.testConnection()) { std::cerr << "Failed to connect to database.\n"; return 1; }
        std::cout << "Connection successful!\n";
        

        
        // Open bad records file if specified
        std::ofstream bad_records_stream;
        if (!bad_records_file.empty()) {
            bad_records_stream.open(bad_records_file);
            if (!bad_records_stream.is_open()) {
                std::cerr << "Failed to open bad records file: " << bad_records_file << "\n";
                return 1;
            }
            // Write CSV header
            bad_records_stream << "reason,raw_record\n";
            std::cout << "Bad records will be saved to: " << bad_records_file << "\n";
        }
        
    size_t total_inserted = 0, total_bad = 0, batch_index = 0;
    size_t total_processed = 0; // good + bad (parsed) records across all batches
    size_t failed_batches = 0;  // number of batches whose DB insertion failed entirely
        std::vector<std::string> raw_records; raw_records.reserve(batch_records);
        std::vector<OpinionCluster> clusters; clusters.reserve(batch_records);
        std::vector<std::string> bad_records; bad_records.reserve(64);
        std::vector<std::string> bad_reasons; bad_reasons.reserve(64);
        
        while (reader.readNextBatch(raw_records, batch_records, chunk_bytes)) {
            clusters.clear(); bad_records.clear(); bad_reasons.clear();
            size_t batch_start_offset = total_processed; // offset BEFORE processing this batch
            for (size_t i = 0; i < raw_records.size(); ++i) {
                try { 
                    auto cluster = reader.parseCsvLine(raw_records[i]);
                    
                    // Debug: analyze records with wrong column count or specific IDs
                    auto cols = reader.splitCsvLine(raw_records[i]);
                    if ((cols.size() != 36 && cluster.id == 8027875) || cluster.id == 2131251) {
                        std::cout << "\n=== ANALYZING BAD RECORD id=" << cluster.id << " ===\n";
                        std::cout << "Column count: " << cols.size() << " (expected 36)\n";
                        std::cout << "Raw record length: " << raw_records[i].length() << " bytes\n";
                        
                        // Count quotes in raw record
                        int quote_count = 0;
                        for (char c : raw_records[i]) {
                            if (c == '"') quote_count++;
                        }
                        std::cout << "Total quotes in raw record: " << quote_count << " (should be even for balanced quotes)\n";
                        
                        // Count newlines in raw record
                        int newline_count = 0;
                        for (char c : raw_records[i]) {
                            if (c == '\n') newline_count++;
                        }
                        std::cout << "Newlines in raw record: " << newline_count << "\n";
                        
                        // Print entire raw record for examination
                        std::cout << "\n=== COMPLETE RAW RECORD ===\n";
                        std::cout << raw_records[i] << "\n";
                        std::cout << "=== END COMPLETE RAW RECORD ===\n\n";
                    }
                    
                    clusters.push_back(cluster);
                }
                catch (const std::exception& e) {
                    std::string msg = e.what();
                    if (msg.find("key") != std::string::npos || msg.find("insufficient columns") != std::string::npos) {
                        bad_records.push_back(raw_records[i]); bad_reasons.push_back(msg);
                    }
                }
            }
            // Update processed count (parsed good + bad in this batch)
            total_processed += clusters.size() + bad_records.size();
            bool batch_insert_failed = false;
            if (!clusters.empty()) {
                try {
                    db.insertClusters(clusters);
                    total_inserted += clusters.size();
                } catch (const std::exception& ex) {
                    batch_insert_failed = true;
                    failed_batches++;
                    std::cerr << "DB insertion failure for batch " << (batch_index + 1)
                              << ": starting_offset=" << batch_start_offset
                              << ", batch_records_attempted=" << clusters.size()
                              << ", reason=" << ex.what() << "\n";
                }
            }
            
            // Write bad records to file if specified
            if (bad_records_stream.is_open() && !bad_records.empty()) {
                for (size_t i = 0; i < bad_records.size(); ++i) {
                    // Escape the reason and record for CSV format
                    std::string escaped_reason = bad_reasons[i];
                    std::string escaped_record = bad_records[i];
                    // Replace quotes with double quotes for CSV escaping
                    size_t pos = 0;
                    while ((pos = escaped_reason.find('"', pos)) != std::string::npos) {
                        escaped_reason.replace(pos, 1, "\"\"");
                        pos += 2;
                    }
                    pos = 0;
                    while ((pos = escaped_record.find('"', pos)) != std::string::npos) {
                        escaped_record.replace(pos, 1, "\"\"");
                        pos += 2;
                    }
                    bad_records_stream << "\"" << escaped_reason << "\",\"" << escaped_record << "\"\n";
                }
            }
            
            total_bad += bad_records.size();
            batch_index++;
            std::cout << "Batch " << batch_index << ": inserted=" << (batch_insert_failed ? 0 : clusters.size())
                      << ", bad=" << bad_records.size()
                      << ", start_offset=" << batch_start_offset
                      << ", processed_total=" << total_processed
                      << (batch_insert_failed ? " [INSERT FAILED]" : "")
                      << " (total_inserted=" << total_inserted << ", total_bad=" << total_bad << ")\n";
        }
        
        if (bad_records_stream.is_open()) {
            bad_records_stream.close();
            std::cout << "Bad records saved to: " << bad_records_file << "\n";
        }
        
        std::cout << "Done. Total inserted: " << total_inserted
                  << ", total bad: " << total_bad
                  << ", failed batches: " << failed_batches
                  << ", total processed (good+bad): " << total_processed << "\n";
        
    } catch (const std::exception& ex) {
        std::cerr << "Error: " << ex.what() << "\n";
        return 1;
    }
    return 0;
}

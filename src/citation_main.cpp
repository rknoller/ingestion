#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <exception>
#include "opinion_cited.h"
#include "opinion_cited_db.h"

int main(int argc, char** argv) {

    // CLI parsing: citation_ingestion_app <citation-map.csv> [--no-db] [--batch=N] [--bad-records=file.csv]
    std::string csvPath;
    std::string bad_records_file; // optional output file for bad records
    bool skip_db = false;
    size_t batch_records = 5000; // records per DB batch

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--no-db") {
            skip_db = true;
        } else if (arg == "--batch" && i + 1 < argc) {
            try { batch_records = static_cast<size_t>(std::stoull(argv[++i])); }
            catch (...) { std::cerr << "Invalid --batch value\n"; return 1; }
        } else if (arg.rfind("--batch=", 0) == 0) {
            try { batch_records = static_cast<size_t>(std::stoull(arg.substr(8))); }
            catch (...) { std::cerr << "Invalid --batch value\n"; return 1; }
        } else if (arg == "--bad-records" && i + 1 < argc) {
            bad_records_file = argv[++i];
        } else if (arg.rfind("--bad-records=", 0) == 0) {
            bad_records_file = arg.substr(14);
        } else if (!arg.empty() && arg[0] == '-') {
            std::cerr << "Unknown option: " << arg << "\n";
            std::cout << "Usage: citation_ingestion_app <citation-map.csv> [--no-db] [--batch=N] [--bad-records=file.csv]\n";
            return 1;
        } else if (csvPath.empty()) {
            csvPath = arg; // first non-option argument is the CSV path
        } else {
            std::cerr << "Unexpected extra argument: " << arg << "\n";
            std::cout << "Usage: citation_ingestion_app <citation-map.csv> [--no-db] [--batch=N] [--bad-records=file.csv]\n";
            return 1;
        }
    }

    if (csvPath.empty()) {
        std::cout << "Usage: citation_ingestion_app <citation-map.csv> [--no-db] [--batch=N] [--bad-records=file.csv]\n";
        std::cout << "  --no-db              Skip database insertion (just parse and display)\n";
        std::cout << "  --batch=N            Batch size for DB insertion (default 5000)\n";
        std::cout << "  --bad-records=FILE   Save bad records to CSV file (FK violations)\n";
        return 0;
    }
    
    std::cout << "Reading citation records from: " << csvPath << "\n";
    
    try {
        OpinionCitedReader reader(csvPath);
        
        // Parse-only mode (skip_db) - read first batch only for display
        if (skip_db) {
            std::cout << "Reading first batch for display...\n";
            std::vector<OpinionCited> sample_records = reader.readBatch(10);
            std::cout << "Showing first " << sample_records.size() << " parsed citation records:\n";
            for (size_t i = 0; i < sample_records.size(); ++i) {
                std::cout << "  " << sample_records[i].toString() << "\n";
            }
            std::cout << "\nSkipping database insertion (--no-db flag)\n";
            std::cout << "Note: File will be processed in batches of " << batch_records 
                      << " when run with database.\n";
            return 0;
        }
        
        // Database insertion mode
        std::cout << "\nConnecting to PostgreSQL...\n";
        OpinionCitedDatabase db("localhost", 5432, "courtlistener", "postgres", "postgres");
        
        if (!db.testConnection()) { 
            std::cerr << "Failed to connect to database.\n"; 
            return 1; 
        }
        std::cout << "Connection successful!\n";
        
        // Load valid opinion IDs for FK validation
        std::cout << "Loading valid opinion IDs from database for FK validation...\n";
        db.loadValidOpinionIds();
        
        // Open bad records file if specified
        std::ofstream bad_records_stream;
        if (!bad_records_file.empty()) {
            bad_records_stream.open(bad_records_file);
            if (!bad_records_stream.is_open()) {
                std::cerr << "Failed to open bad records file: " << bad_records_file << "\n";
                return 1;
            }
            // Write CSV header
            bad_records_stream << "id,depth,cited_opinion_id,citing_opinion_id,reason\n";
            std::cout << "Bad records will be saved to: " << bad_records_file << "\n";
        }
        
        size_t total_inserted = 0, total_rejected = 0;
        size_t batch_count = 0;
        size_t total_records_processed = 0;
        
        // DEBUG: Collect all bad records for debugging
        std::vector<OpinionCited> all_bad_records;
        std::vector<std::string> all_bad_reasons;
        
        // Process in batches using streaming
        std::cout << "\nProcessing records in batches of " << batch_records << "...\n";
        
        while (reader.hasMore()) {
            // Read next batch from CSV
            std::vector<OpinionCited> batch = reader.readBatch(batch_records);
            
            if (batch.empty()) {
                break; // No more records
            }
            
            size_t batch_start = total_records_processed;
            total_records_processed += batch.size();
            
            // Insert batch with FK validation
            std::vector<OpinionCited> rejected_records;
            std::vector<std::string> rejection_reasons;
            
            size_t inserted = db.insertCitations(batch, rejected_records, rejection_reasons);
            total_inserted += inserted;
            total_rejected += rejected_records.size();
            
            // DEBUG: Collect all rejected records into global debug vectors
            all_bad_records.insert(all_bad_records.end(), rejected_records.begin(), rejected_records.end());
            all_bad_reasons.insert(all_bad_reasons.end(), rejection_reasons.begin(), rejection_reasons.end());
            
            // Save rejected records to bad records file
            if (bad_records_stream.is_open() && !rejected_records.empty()) {
                for (size_t j = 0; j < rejected_records.size(); ++j) {
                    bad_records_stream << rejected_records[j].toCsv() << ","
                                      << "\"" << rejection_reasons[j] << "\"\n";
                }
                bad_records_stream.flush(); // Ensure data is written immediately
            } else if (!rejected_records.empty() && bad_records_file.empty()) {
                // Show first few rejected records if no output file specified
                if (batch_count == 0) {
                    std::cout << "\nSample rejected records (first 5):\n";
                    for (size_t j = 0; j < rejected_records.size() && j < 5; ++j) {
                        std::cout << "  " << rejected_records[j].toString() 
                                  << " - " << rejection_reasons[j] << "\n";
                    }
                    std::cout << "  (Use --bad-records=file.csv to save all rejected records)\n\n";
                }
            }
            
            batch_count++;
            std::cout << "Batch " << batch_count 
                      << ": inserted=" << inserted
                      << ", rejected=" << rejected_records.size()
                      << " (records " << batch_start << "-" << (total_records_processed-1) << ")\n";
        }
        
        if (bad_records_stream.is_open()) {
            bad_records_stream.close();
        }
        
        std::cout << "\n=== SUMMARY ===\n";
        std::cout << "Total records:      " << total_records_processed << "\n";
        std::cout << "Total inserted:     " << total_inserted << "\n";
        std::cout << "Total rejected:     " << total_rejected << " (FK violations)\n";
        std::cout << "Batches processed:  " << batch_count << "\n";
        
        // DEBUG: Show statistics about bad records
        std::cout << "\n=== DEBUG: BAD RECORDS COLLECTED ===\n";
        std::cout << "Bad records in memory: " << all_bad_records.size() << "\n";
        
        // Show first 10 bad records for debugging
        if (!all_bad_records.empty()) {
            std::cout << "\nFirst 10 bad records:\n";
            for (size_t i = 0; i < all_bad_records.size() && i < 10; ++i) {
                std::cout << "  [" << i << "] " << all_bad_records[i].toString() 
                          << "\n      Reason: " << all_bad_reasons[i] << "\n";
            }
        }
        
        if (!bad_records_file.empty()) {
            std::cout << "\nBad records saved to: " << bad_records_file << "\n";
        }
        
        // DEBUG: Set breakpoint here to inspect all_bad_records and all_bad_reasons vectors
        
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}

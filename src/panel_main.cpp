#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <exception>
#include "opinion_cluster_panel.h"
#include "opinion_cluster_panel_db.h"

int main(int argc, char** argv) {

    // CLI parsing: panel_ingestion_app <panels.csv> [--no-db] [--bad-records=file.csv] [--batch=N]
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
            std::cout << "Usage: panel_ingestion_app <panels.csv> [--no-db] [--batch=N] [--bad-records=file.csv]\n";
            return 1;
        } else if (csvPath.empty()) {
            csvPath = arg; // first non-option argument is the CSV path
        } else {
            std::cerr << "Unexpected extra argument: " << arg << "\n";
            std::cout << "Usage: panel_ingestion_app <panels.csv> [--no-db] [--batch=N] [--bad-records=file.csv]\n";
            return 1;
        }
    }

    if (csvPath.empty()) {
        std::cout << "Usage: panel_ingestion_app <panels.csv> [--no-db] [--batch=N] [--bad-records=file.csv]\n";
        std::cout << "  --no-db              Skip database insertion (just parse and display)\n";
        std::cout << "  --batch=N            Batch size for DB insertion (default 5000)\n";
        std::cout << "  --bad-records=FILE   Save bad records to CSV file (FK violations)\n";
        return 0;
    }
    
    std::cout << "Reading panel records from: " << csvPath << "\n";
    
    try {
        OpinionClusterPanelReader reader(csvPath);
        
        // Read all records from CSV
        std::cout << "Reading all panel records from CSV...\n";
        std::vector<OpinionClusterPanel> panels = reader.readAll();
        std::cout << "Loaded " << panels.size() << " panel records from CSV\n";
        
        if (panels.empty()) {
            std::cout << "No records found in CSV file.\n";
            return 0;
        }
        
        // Parse-only mode (skip_db)
        if (skip_db) {
            std::cout << "Showing first 10 parsed panel records:\n";
            for (size_t i = 0; i < panels.size() && i < 10; ++i) {
                std::cout << "  " << panels[i].toString() << "\n";
            }
            std::cout << "\nSkipping database insertion (--no-db flag)\n";
            return 0;
        }
        
        // Database insertion mode
        std::cout << "\nConnecting to PostgreSQL...\n";
        OpinionClusterPanelDatabase db("localhost", 5432, "courtlistener", "postgres", "postgres");
        
        if (!db.testConnection()) { 
            std::cerr << "Failed to connect to database.\n"; 
            return 1; 
        }
        std::cout << "Connection successful!\n";
        
        // Load valid cluster IDs for FK validation
        std::cout << "Loading valid cluster IDs from database for FK validation...\n";
        db.loadValidClusterIds();
        
        // Open bad records file if specified
        std::ofstream bad_records_stream;
        if (!bad_records_file.empty()) {
            bad_records_stream.open(bad_records_file);
            if (!bad_records_stream.is_open()) {
                std::cerr << "Failed to open bad records file: " << bad_records_file << "\n";
                return 1;
            }
            // Write CSV header
            bad_records_stream << "id,opinioncluster_id,person_id,reason\n";
            std::cout << "Bad records will be saved to: " << bad_records_file << "\n";
        }
        
        size_t total_inserted = 0, total_rejected = 0;
        size_t batch_count = 0;
        
        // DEBUG: Collect all bad records for debugging
        std::vector<OpinionClusterPanel> all_bad_records;
        std::vector<std::string> all_bad_reasons;
        
        // Process in batches
        std::cout << "\nProcessing records in batches of " << batch_records << "...\n";
        for (size_t i = 0; i < panels.size(); i += batch_records) {
            size_t batch_end = std::min(i + batch_records, panels.size());
            std::vector<OpinionClusterPanel> batch(panels.begin() + i, panels.begin() + batch_end);
            
            // Insert batch with FK validation
            std::vector<OpinionClusterPanel> rejected_panels;
            std::vector<std::string> rejection_reasons;
            
            size_t inserted = db.insertPanels(batch, rejected_panels, rejection_reasons);
            total_inserted += inserted;
            total_rejected += rejected_panels.size();
            
            // DEBUG: Collect all rejected records into global debug vectors
            all_bad_records.insert(all_bad_records.end(), rejected_panels.begin(), rejected_panels.end());
            all_bad_reasons.insert(all_bad_reasons.end(), rejection_reasons.begin(), rejection_reasons.end());
            
            // Save rejected records to bad records file
            if (bad_records_stream.is_open() && !rejected_panels.empty()) {
                for (size_t j = 0; j < rejected_panels.size(); ++j) {
                    bad_records_stream << rejected_panels[j].toCsv() << ","
                                      << "\"" << rejection_reasons[j] << "\"\n";
                }
                bad_records_stream.flush(); // Ensure data is written immediately
            } else if (!rejected_panels.empty() && bad_records_file.empty()) {
                // Show first few rejected records if no output file specified
                if (batch_count == 0) {
                    std::cout << "\nSample rejected records (first 5):\n";
                    for (size_t j = 0; j < rejected_panels.size() && j < 5; ++j) {
                        std::cout << "  " << rejected_panels[j].toString() 
                                  << " - " << rejection_reasons[j] << "\n";
                    }
                    std::cout << "  (Use --bad-records=file.csv to save all rejected records)\n\n";
                }
            }
            
            batch_count++;
            std::cout << "Batch " << batch_count 
                      << ": inserted=" << inserted
                      << ", rejected=" << rejected_panels.size()
                      << " (records " << i << "-" << (batch_end-1) << ")\n";
        }
        
        if (bad_records_stream.is_open()) {
            bad_records_stream.close();
        }
        
        std::cout << "\n=== SUMMARY ===\n";
        std::cout << "Total records:      " << panels.size() << "\n";
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

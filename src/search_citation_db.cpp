#include "search_citation_db.h"
#include <iostream>
#include <sstream>

SearchCitationDatabase::SearchCitationDatabase(
    const std::string& host, int port,
    const std::string& dbname, const std::string& user,
    const std::string& password) {
    
    std::ostringstream oss;
    oss << "host=" << host
        << " port=" << port
        << " dbname=" << dbname
        << " user=" << user
        << " password=" << password;
    connection_string_ = oss.str();
}

bool SearchCitationDatabase::testConnection() {
    try {
        pqxx::connection conn(connection_string_);
        return conn.is_open();
    } catch (const std::exception& e) {
        std::cerr << "Connection test failed: " << e.what() << std::endl;
        return false;
    }
}

void SearchCitationDatabase::loadValidClusterIds() {
    valid_cluster_ids_.clear();
    
    try {
        pqxx::connection conn(connection_string_);
        pqxx::work txn(conn);
        
        std::string query = "SELECT id FROM search_opinioncluster";
        pqxx::result res = txn.exec(query);
        
        for (const auto& row : res) {
            int cluster_id = row[0].as<int>();
            valid_cluster_ids_.insert(cluster_id);
        }
        
        txn.commit();
        
        std::cout << "Loaded " << valid_cluster_ids_.size() 
                  << " valid cluster IDs from database\n";
                  
    } catch (const std::exception& e) {
        std::cerr << "Failed to load valid cluster IDs: " << e.what() << std::endl;
        throw;
    }
}

bool SearchCitationDatabase::isValidClusterId(int cluster_id) const {
    return valid_cluster_ids_.find(cluster_id) != valid_cluster_ids_.end();
}

bool SearchCitationDatabase::createPlaceholderCluster(int cluster_id, std::vector<int>& search_opinioncluster_placeholders) {
    try {
        pqxx::connection conn(connection_string_);
        pqxx::work txn(conn);
        
        // Create placeholder cluster with all required NOT NULL fields
        std::ostringstream query;
        query << "INSERT INTO search_opinioncluster ("
              << "id, date_created, date_modified, judges, date_filed, "
              << "case_name_short, case_name, case_name_full, scdb_id, source, "
              << "procedural_history, attorneys, nature_of_suit, posture, syllabus, "
              << "citation_count, precedential_status, blocked, docket_id, "
              << "date_filed_is_approximate, correction, cross_reference, disposition, "
              << "filepath_json_harvard, headnotes, history, other_dates, summary, "
              << "arguments, headmatter, filepath_pdf_harvard"
              << ") VALUES ("
              << cluster_id << ", "
              << "NOW(), NOW(), '', '0001-01-01', "
              << "'Placeholder', 'Placeholder Case', 'Placeholder Case', '', 'C', "
              << "'', '', '', '', '', "
              << "0, 'Published', false, 1, "
              << "false, '', '', '', "
              << "'', '', '', '', '', "
              << "'', '', ''"
              << ") ON CONFLICT (id) DO NOTHING";
        
        pqxx::result res = txn.exec(query.str());
        txn.commit();
        
        // Add to valid cluster IDs cache
        valid_cluster_ids_.insert(cluster_id);
        
        // Track this placeholder creation
        search_opinioncluster_placeholders.push_back(cluster_id);
        
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "Failed to create placeholder cluster for id=" << cluster_id 
                  << ": " << e.what() << std::endl;
        return false;
    }
}

std::pair<size_t, size_t> SearchCitationDatabase::insertCitations(
    const std::vector<SearchCitation>& records,
    std::vector<SearchCitation>& rejected_records,
    std::vector<std::string>& rejection_reasons,
    std::vector<int>& search_opinioncluster_placeholders) {
    
    rejected_records.clear();
    rejection_reasons.clear();
    
    size_t inserted = 0;
    size_t placeholders_created = 0;
    
    // Insert records line by line and collect failures
    try {
        pqxx::connection conn(connection_string_);
        
        for (const auto& record : records) {
            try {
                // Start a new transaction for each record
                pqxx::work txn(conn);
                
                // Build insert query with upsert to handle duplicates
                std::ostringstream query;
                query << "INSERT INTO search_citation "
                      << "(id, volume, reporter, page, type, cluster_id) VALUES ("
                      << record.id << ", "
                      << record.volume << ", "
                      << txn.quote(record.reporter) << ", "
                      << txn.quote(record.page) << ", "
                      << record.type << ", "
                      << record.cluster_id
                      << ") ON CONFLICT (cluster_id, volume, reporter, page) "
                      << "DO UPDATE SET id = EXCLUDED.id, type = EXCLUDED.type";
                
                pqxx::result res = txn.exec(query.str());
                txn.commit();
                
                // Successfully inserted
                inserted++;
                
            } catch (const std::exception& e) {
                // Individual record insertion failed - PostgreSQL will tell us why
                std::string error_msg = e.what();
                
                // Check if it's an FK violation on cluster_id
                if ((error_msg.find("foreign key") != std::string::npos || 
                     error_msg.find("violates foreign key constraint") != std::string::npos) &&
                    error_msg.find("cluster_id") != std::string::npos) {
                    
                    // Try to create placeholder and retry insert
                    if (createPlaceholderCluster(record.cluster_id, search_opinioncluster_placeholders)) {
                        placeholders_created++;
                        try {
                            // Retry the insert with upsert
                            pqxx::work retry_txn(conn);
                            std::ostringstream retry_query;
                            retry_query << "INSERT INTO search_citation "
                                       << "(id, volume, reporter, page, type, cluster_id) VALUES ("
                                       << record.id << ", "
                                       << record.volume << ", "
                                       << retry_txn.quote(record.reporter) << ", "
                                       << retry_txn.quote(record.page) << ", "
                                       << record.type << ", "
                                       << record.cluster_id
                                       << ") ON CONFLICT (cluster_id, volume, reporter, page) "
                                       << "DO UPDATE SET id = EXCLUDED.id, type = EXCLUDED.type";
                            
                            retry_txn.exec(retry_query.str());
                            retry_txn.commit();
                            
                            inserted++;
                            continue; // Success - move to next record
                            
                        } catch (const std::exception& retry_e) {
                            // Retry also failed
                            std::string retry_error = retry_e.what();
                            std::cerr << "REJECTED: Record " << record.toString() 
                                      << "\n  Retry failed after creating placeholder: " << retry_error << std::endl;
                            rejected_records.push_back(record);
                            std::ostringstream reason;
                            reason << "FK violation, placeholder created but retry failed: " << retry_error;
                            rejection_reasons.push_back(reason.str());
                        }
                    } else {
                        // Failed to create placeholder
                        std::cerr << "REJECTED: Record " << record.toString() 
                                  << "\n  Failed to create placeholder: " << error_msg << std::endl;
                        rejected_records.push_back(record);
                        std::ostringstream reason;
                        reason << "FK violation, failed to create placeholder: " << error_msg;
                        rejection_reasons.push_back(reason.str());
                    }
                } else {
                    // Other types of errors - print to stderr for visibility
                    std::cerr << "REJECTED: Record " << record.toString() 
                              << "\n  PostgreSQL error: " << error_msg << std::endl;
                    rejected_records.push_back(record);
                    std::ostringstream reason;
                    
                    if (error_msg.find("duplicate key") != std::string::npos) {
                        reason << "Duplicate key violation: " << error_msg;
                    } else {
                        reason << "DB error: " << error_msg;
                    }
                    
                    rejection_reasons.push_back(reason.str());
                }
            }
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Database connection failed: " << e.what() << std::endl;
        
        // Connection failed - reject all remaining records
        for (const auto& record : records) {
            rejected_records.push_back(record);
            std::ostringstream reason;
            reason << "Connection error: " << e.what();
            rejection_reasons.push_back(reason.str());
        }
        
        return {inserted, placeholders_created};
    }
    
    return {inserted, placeholders_created};
}

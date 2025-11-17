#include "opinion_cluster_panel_db.h"
#include <iostream>
#include <sstream>

OpinionClusterPanelDatabase::OpinionClusterPanelDatabase(
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

bool OpinionClusterPanelDatabase::testConnection() {
    try {
        pqxx::connection conn(connection_string_);
        return conn.is_open();
    } catch (const std::exception& e) {
        std::cerr << "Connection test failed: " << e.what() << std::endl;
        return false;
    }
}

void OpinionClusterPanelDatabase::loadValidClusterIds() {
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

bool OpinionClusterPanelDatabase::isValidClusterId(int cluster_id) const {
    return valid_cluster_ids_.find(cluster_id) != valid_cluster_ids_.end();
}

bool OpinionClusterPanelDatabase::createPlaceholderCluster(int cluster_id) {
    try {
        pqxx::connection conn(connection_string_);
        pqxx::work txn(conn);
        
        // Create minimal placeholder with required fields
        std::ostringstream query;
        query << "INSERT INTO search_opinioncluster ("
              << "id, judges, date_created, date_modified, date_filed, "
              << "case_name_short, case_name, case_name_full, scdb_id, source, "
              << "procedural_history, attorneys, nature_of_suit, posture, syllabus, "
              << "citation_count, precedential_status, blocked, docket_id, "
              << "date_filed_is_approximate, correction, cross_reference, disposition, "
              << "filepath_json_harvard, headnotes, history, other_dates, summary, "
              << "arguments, headmatter, filepath_pdf_harvard"
              << ") VALUES ("
              << cluster_id << ", "
              << "'PLACEHOLDER', "  // judges
              << "NOW(), NOW(), '1900-01-01', "  // dates
              << "'PLACEHOLDER', 'PLACEHOLDER', 'PLACEHOLDER', "  // case names
              << "'', 'R', "  // scdb_id, source
              << "'', '', '', '', '', "  // text fields
              << "0, 'Unknown', false, 1, "  // citation_count, precedential_status, blocked, docket_id
              << "false, '', '', '', "  // date_filed_is_approximate, correction, cross_reference, disposition
              << "'', '', '', '', '', '', '', ''"  // remaining text fields
              << ") ON CONFLICT (id) DO NOTHING";
        
        pqxx::result res = txn.exec(query.str());
        txn.commit();
        
        // Add to valid cluster IDs cache
        valid_cluster_ids_.insert(cluster_id);
        
        std::cout << "Created placeholder cluster for id=" << cluster_id << std::endl;
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "Failed to create placeholder cluster for id=" << cluster_id 
                  << ": " << e.what() << std::endl;
        return false;
    }
}

size_t OpinionClusterPanelDatabase::insertPanels(
    const std::vector<OpinionClusterPanel>& panels,
    std::vector<OpinionClusterPanel>& rejected_panels,
    std::vector<std::string>& rejection_reasons) {
    
    rejected_panels.clear();
    rejection_reasons.clear();
    
    size_t inserted = 0;
    
    // Insert records line by line and collect failures
    try {
        pqxx::connection conn(connection_string_);
        
        for (const auto& panel : panels) {
            try {
                // Start a new transaction for each record
                pqxx::work txn(conn);
                
                // Build insert query with upsert to handle duplicates
                std::ostringstream query;
                query << "INSERT INTO search_opinioncluster_panel "
                      << "(id, opinioncluster_id, person_id) VALUES ("
                      << panel.id << ", "
                      << panel.opinioncluster_id << ", "
                      << panel.person_id
                      << ") ON CONFLICT (opinioncluster_id, person_id) "
                      << "DO UPDATE SET id = EXCLUDED.id";
                
                pqxx::result res = txn.exec(query.str());
                txn.commit();
                
                // Successfully inserted
                inserted++;
                
            } catch (const std::exception& e) {
                // Individual record insertion failed - PostgreSQL will tell us why
                std::string error_msg = e.what();
                
                // Check if it's an FK violation on opinioncluster_id
                if ((error_msg.find("foreign key") != std::string::npos || 
                     error_msg.find("violates foreign key constraint") != std::string::npos) &&
                    error_msg.find("opinioncluster_id") != std::string::npos) {
                    
                    std::cout << "FK violation detected for cluster_id=" << panel.opinioncluster_id 
                              << ", creating placeholder..." << std::endl;
                    
                    // Try to create placeholder and retry insert
                    if (createPlaceholderCluster(panel.opinioncluster_id)) {
                        try {
                            // Retry the insert with upsert
                            pqxx::work retry_txn(conn);
                            std::ostringstream retry_query;
                            retry_query << "INSERT INTO search_opinioncluster_panel "
                                       << "(id, opinioncluster_id, person_id) VALUES ("
                                       << panel.id << ", "
                                       << panel.opinioncluster_id << ", "
                                       << panel.person_id
                                       << ") ON CONFLICT (opinioncluster_id, person_id) "
                                       << "DO UPDATE SET id = EXCLUDED.id";
                            
                            retry_txn.exec(retry_query.str());
                            retry_txn.commit();
                            
                            inserted++;
                            std::cout << "Successfully inserted after creating placeholder" << std::endl;
                            continue; // Success - move to next record
                            
                        } catch (const std::exception& retry_e) {
                            // Retry also failed
                            rejected_panels.push_back(panel);
                            std::ostringstream reason;
                            reason << "FK violation, placeholder created but retry failed: " << retry_e.what();
                            rejection_reasons.push_back(reason.str());
                        }
                    } else {
                        // Failed to create placeholder
                        rejected_panels.push_back(panel);
                        std::ostringstream reason;
                        reason << "FK violation, failed to create placeholder: " << error_msg;
                        rejection_reasons.push_back(reason.str());
                    }
                } else {
                    // Other types of errors
                    rejected_panels.push_back(panel);
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
        for (const auto& panel : panels) {
            rejected_panels.push_back(panel);
            std::ostringstream reason;
            reason << "Connection error: " << e.what();
            rejection_reasons.push_back(reason.str());
        }
        
        return inserted;
    }
    
    return inserted;
}

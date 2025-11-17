#include "opinion_joined_by_db.h"
#include <iostream>
#include <sstream>

OpinionJoinedByDatabase::OpinionJoinedByDatabase(
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

bool OpinionJoinedByDatabase::testConnection() {
    try {
        pqxx::connection conn(connection_string_);
        return conn.is_open();
    } catch (const std::exception& e) {
        std::cerr << "Connection test failed: " << e.what() << std::endl;
        return false;
    }
}

void OpinionJoinedByDatabase::loadValidOpinionIds() {
    valid_opinion_ids_.clear();
    
    try {
        pqxx::connection conn(connection_string_);
        pqxx::work txn(conn);
        
        std::string query = "SELECT id FROM search_opinion";
        pqxx::result res = txn.exec(query);
        
        for (const auto& row : res) {
            int opinion_id = row[0].as<int>();
            valid_opinion_ids_.insert(opinion_id);
        }
        
        txn.commit();
        
        std::cout << "Loaded " << valid_opinion_ids_.size() 
                  << " valid opinion IDs from database\n";
                  
    } catch (const std::exception& e) {
        std::cerr << "Failed to load valid opinion IDs: " << e.what() << std::endl;
        throw;
    }
}

bool OpinionJoinedByDatabase::isValidOpinionId(int opinion_id) const {
    return valid_opinion_ids_.find(opinion_id) != valid_opinion_ids_.end();
}

bool OpinionJoinedByDatabase::createPlaceholderOpinion(int opinion_id) {
    try {
        pqxx::connection conn(connection_string_);
        pqxx::work txn(conn);
        
        // First ensure we have a placeholder cluster (id=1) that we can reference
        // This is needed because cluster_id is NOT NULL in search_opinion
        // Include ALL required NOT NULL fields based on actual schema
        std::string ensure_cluster = 
            "INSERT INTO search_opinioncluster ("
            "id, date_created, date_modified, judges, date_filed, "
            "case_name_short, case_name, case_name_full, scdb_id, source, "
            "procedural_history, attorneys, nature_of_suit, posture, syllabus, "
            "citation_count, precedential_status, blocked, docket_id, "
            "date_filed_is_approximate, correction, cross_reference, disposition, "
            "filepath_json_harvard, headnotes, history, other_dates, summary, "
            "arguments, headmatter, filepath_pdf_harvard"
            ") VALUES ("
            "1, NOW(), NOW(), '', '0001-01-01', "
            "'Placeholder', 'Placeholder Case', 'Placeholder Case', '', 'C', "
            "'', '', '', '', '', "
            "0, 'Published', false, 1, "
            "false, '', '', '', "
            "'', '', '', '', '', "
            "'', '', ''"
            ") ON CONFLICT (id) DO NOTHING";
        txn.exec(ensure_cluster);
        
        // Create minimal placeholder with all required NOT NULL fields for search_opinion
        std::ostringstream query;
        query << "INSERT INTO search_opinion ("
              << "id, date_created, date_modified, type, sha1, "
              << "download_url, local_path, plain_text, html, html_lawbox, "
              << "html_columbia, html_with_citations, extracted_by_ocr, "
              << "cluster_id, per_curiam, author_str, joined_by_str, "
              << "xml_harvard, html_anon_2020"
              << ") VALUES ("
              << opinion_id << ", "
              << "NOW(), NOW(), '010', "  // type = '010' for Combined Opinion
              << "'PLACEHOLDER_" << opinion_id << "', "  // sha1 must be unique
              << "'', '', '', '', '', "  // download_url, local_path, plain_text, html, html_lawbox
              << "'', '', false, "  // html_columbia, html_with_citations, extracted_by_ocr
              << "1, false, '', '', "  // cluster_id (references placeholder), per_curiam, author_str, joined_by_str
              << "'', ''"  // xml_harvard, html_anon_2020
              << ") ON CONFLICT (id) DO NOTHING";
        
        pqxx::result res = txn.exec(query.str());
        txn.commit();
        
        // Add to valid opinion IDs cache
        valid_opinion_ids_.insert(opinion_id);
        
        std::cout << "Created placeholder opinion for id=" << opinion_id << std::endl;
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "Failed to create placeholder opinion for id=" << opinion_id 
                  << ": " << e.what() << std::endl;
        return false;
    }
}

size_t OpinionJoinedByDatabase::insertJoinedBy(
    const std::vector<OpinionJoinedBy>& records,
    std::vector<OpinionJoinedBy>& rejected_records,
    std::vector<std::string>& rejection_reasons) {
    
    rejected_records.clear();
    rejection_reasons.clear();
    
    size_t inserted = 0;
    
    // Insert records line by line and collect failures
    try {
        pqxx::connection conn(connection_string_);
        
        for (const auto& record : records) {
            try {
                // Start a new transaction for each record
                pqxx::work txn(conn);
                
                // Build insert query with upsert to handle duplicates
                std::ostringstream query;
                query << "INSERT INTO search_opinion_joined_by "
                      << "(id, opinion_id, person_id) VALUES ("
                      << record.id << ", "
                      << record.opinion_id << ", "
                      << record.person_id
                      << ") ON CONFLICT (opinion_id, person_id) "
                      << "DO UPDATE SET id = EXCLUDED.id";
                
                pqxx::result res = txn.exec(query.str());
                txn.commit();
                
                // Successfully inserted
                inserted++;
                
            } catch (const std::exception& e) {
                // Individual record insertion failed - PostgreSQL will tell us why
                std::string error_msg = e.what();
                
                // Check if it's an FK violation on opinion_id
                if ((error_msg.find("foreign key") != std::string::npos || 
                     error_msg.find("violates foreign key constraint") != std::string::npos) &&
                    error_msg.find("opinion_id") != std::string::npos) {
                    
                    std::cout << "FK violation detected for opinion_id=" << record.opinion_id 
                              << ", creating placeholder..." << std::endl;
                    
                    // Try to create placeholder and retry insert
                    if (createPlaceholderOpinion(record.opinion_id)) {
                        try {
                            // Retry the insert with upsert
                            pqxx::work retry_txn(conn);
                            std::ostringstream retry_query;
                            retry_query << "INSERT INTO search_opinion_joined_by "
                                       << "(id, opinion_id, person_id) VALUES ("
                                       << record.id << ", "
                                       << record.opinion_id << ", "
                                       << record.person_id
                                       << ") ON CONFLICT (opinion_id, person_id) "
                                       << "DO UPDATE SET id = EXCLUDED.id";
                            
                            retry_txn.exec(retry_query.str());
                            retry_txn.commit();
                            
                            inserted++;
                            std::cout << "Successfully inserted after creating placeholder" << std::endl;
                            continue; // Success - move to next record
                            
                        } catch (const std::exception& retry_e) {
                            // Retry also failed
                            rejected_records.push_back(record);
                            std::ostringstream reason;
                            reason << "FK violation, placeholder created but retry failed: " << retry_e.what();
                            rejection_reasons.push_back(reason.str());
                        }
                    } else {
                        // Failed to create placeholder
                        rejected_records.push_back(record);
                        std::ostringstream reason;
                        reason << "FK violation, failed to create placeholder: " << error_msg;
                        rejection_reasons.push_back(reason.str());
                    }
                } else {
                    // Other types of errors
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
        
        return inserted;
    }
    
    return inserted;
}

#include "parenthetical_db.h"
#include <iostream>
#include <sstream>

ParentheticalDatabase::ParentheticalDatabase(
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

bool ParentheticalDatabase::testConnection() {
    try {
        pqxx::connection conn(connection_string_);
        return conn.is_open();
    } catch (const std::exception& e) {
        std::cerr << "Connection test failed: " << e.what() << std::endl;
        return false;
    }
}

void ParentheticalDatabase::loadValidGroupIds() {
    valid_group_ids_.clear();
    
    try {
        pqxx::connection conn(connection_string_);
        pqxx::work txn(conn);
        
        std::string query = "SELECT id FROM search_parentheticalgroup";
        pqxx::result res = txn.exec(query);
        
        for (const auto& row : res) {
            int group_id = row[0].as<int>();
            valid_group_ids_.insert(group_id);
        }
        
        txn.commit();
        
        std::cout << "Loaded " << valid_group_ids_.size() 
                  << " valid group IDs from database\n";
                  
    } catch (const std::exception& e) {
        std::cerr << "Failed to load valid group IDs: " << e.what() << std::endl;
        throw;
    }
}

bool ParentheticalDatabase::isValidGroupId(int group_id) const {
    return valid_group_ids_.find(group_id) != valid_group_ids_.end();
}

bool ParentheticalDatabase::createPlaceholderGroup(int group_id, std::vector<int>& search_parentheticalgroup_placeholders) {
    try {
        pqxx::connection conn(connection_string_);
        
        // CRITICAL: We need to create a complete set of base placeholders (id=1) that reference each other
        // This is complex due to circular FK constraints between the three tables
        
        // Step 1: Check if base structures already exist
        bool base_exists = false;
        try {
            pqxx::work check_txn(conn);
            pqxx::result res = check_txn.exec("SELECT 1 FROM search_parentheticalgroup WHERE id = 1");
            if (!res.empty()) {
                base_exists = true;
            }
            check_txn.commit();
        } catch (...) {
            base_exists = false;
        }
        
        // Step 2: If base doesn't exist, create it by temporarily disabling triggers
        if (!base_exists) {
            try {
                pqxx::work txn(conn);
                
                // Disable triggers to bypass FK checks temporarily (requires superuser or table owner)
                txn.exec("ALTER TABLE search_parentheticalgroup DISABLE TRIGGER ALL");
                txn.exec("ALTER TABLE search_parenthetical DISABLE TRIGGER ALL");
                txn.exec("ALTER TABLE search_opinion DISABLE TRIGGER ALL");
                
                // Create the base placeholder opinion (id=1) - may fail if cluster_id=1 doesn't exist
                try {
                    txn.exec("INSERT INTO search_opinion (id, date_created, date_modified, type, sha1, local_path, "
                            "plain_text, html, html_lawbox, html_columbia, html_with_citations, "
                            "extracted_by_ocr, cluster_id, per_curiam, author_str, joined_by_str, "
                            "xml_harvard, html_anon_2020) VALUES "
                            "(1, NOW(), NOW(), '010', 'PLACEHOLDER_1', '', '', '', '', '', '', false, 1, false, '', '', '', '') "
                            "ON CONFLICT (id) DO NOTHING");
                } catch (...) {}
                
                // Create base group (id=1) with representative_id=1 (will exist after next step)
                try {
                    txn.exec("INSERT INTO search_parentheticalgroup (id, score, size, opinion_id, representative_id) "
                            "VALUES (1, 0.0, 0, 1, 1) ON CONFLICT (id) DO NOTHING");
                } catch (...) {}
                
                // Create base parenthetical (id=1) referencing group_id=1
                try {
                    txn.exec("INSERT INTO search_parenthetical (id, text, score, described_opinion_id, describing_opinion_id, group_id) "
                            "VALUES (1, 'PLACEHOLDER', 0.0, 1, 1, 1) ON CONFLICT (id) DO NOTHING");
                } catch (...) {}
                
                // Re-enable triggers
                txn.exec("ALTER TABLE search_opinion ENABLE TRIGGER ALL");
                txn.exec("ALTER TABLE search_parenthetical ENABLE TRIGGER ALL");
                txn.exec("ALTER TABLE search_parentheticalgroup ENABLE TRIGGER ALL");
                
                // Commit
                txn.commit();
            } catch (const std::exception& e) {
                // If base creation fails, log but continue - maybe another process created it
                std::cerr << "Warning: Could not create base placeholders (may already exist): " << e.what() << std::endl;
            }
        }
        
        // Step 3: Now create the actual placeholder group for the requested group_id
        pqxx::work txn(conn);
        std::ostringstream query;
        query << "INSERT INTO search_parentheticalgroup ("
              << "id, score, size, opinion_id, representative_id"
              << ") VALUES ("
              << group_id << ", "
              << "0.0, 0, 1, 1"
              << ") ON CONFLICT (id) DO NOTHING";
        
        txn.exec(query.str());
        txn.commit();
        
        // Add to valid group IDs cache
        valid_group_ids_.insert(group_id);
        
        // Track this placeholder creation
        search_parentheticalgroup_placeholders.push_back(group_id);
        
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "Failed to create placeholder group for id=" << group_id 
                  << ": " << e.what() << std::endl;
        return false;
    }
}

bool ParentheticalDatabase::createPlaceholderOpinion(int opinion_id) {
    try {
        pqxx::connection conn(connection_string_);
        pqxx::work txn(conn);
        
        // Check if opinion already exists
        pqxx::result check = txn.exec("SELECT 1 FROM search_opinion WHERE id = " + std::to_string(opinion_id));
        if (!check.empty()) {
            txn.commit();
            return true; // Already exists
        }
        
        // Create placeholder opinion with cluster_id=1 (assumed to exist)
        std::ostringstream query;
        query << "INSERT INTO search_opinion ("
              << "id, date_created, date_modified, type, sha1, local_path, "
              << "plain_text, html, html_lawbox, html_columbia, html_with_citations, "
              << "extracted_by_ocr, cluster_id, per_curiam, author_str, joined_by_str, "
              << "xml_harvard, html_anon_2020"
              << ") VALUES ("
              << opinion_id << ", "
              << "NOW(), NOW(), '010', 'PLACEHOLDER_" << opinion_id << "', '', "
              << "'', '', '', '', '', false, 1, false, '', '', '', ''"
              << ") ON CONFLICT (id) DO NOTHING";
        
        txn.exec(query.str());
        txn.commit();
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "Failed to create placeholder opinion for id=" << opinion_id 
                  << ": " << e.what() << std::endl;
        return false;
    }
}

std::pair<size_t, size_t> ParentheticalDatabase::insertParentheticals(
    const std::vector<Parenthetical>& records,
    std::vector<Parenthetical>& rejected_records,
    std::vector<std::string>& rejection_reasons,
    std::vector<int>& search_parentheticalgroup_placeholders) {
    
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
                query << "INSERT INTO search_parenthetical "
                      << "(id, text, score, described_opinion_id, describing_opinion_id, group_id) VALUES ("
                      << record.id << ", "
                      << txn.quote(record.text) << ", "
                      << record.score << ", "
                      << record.described_opinion_id << ", "
                      << record.describing_opinion_id << ", "
                      << record.group_id
                      << ") ON CONFLICT (id) "
                      << "DO UPDATE SET text = EXCLUDED.text, score = EXCLUDED.score, "
                      << "described_opinion_id = EXCLUDED.described_opinion_id, "
                      << "describing_opinion_id = EXCLUDED.describing_opinion_id, "
                      << "group_id = EXCLUDED.group_id";
                
                pqxx::result res = txn.exec(query.str());
                txn.commit();
                
                // Successfully inserted
                inserted++;
                
            } catch (const std::exception& e) {
                // Individual record insertion failed - PostgreSQL will tell us why
                std::string error_msg = e.what();
                
                // Check if it's any FK violation
                if (error_msg.find("foreign key") != std::string::npos || 
                    error_msg.find("violates foreign key constraint") != std::string::npos) {
                    
                    // Determine which FK(s) are missing and create placeholders for ALL of them
                    bool needs_described = error_msg.find("described_opinion_id") != std::string::npos;
                    bool needs_describing = error_msg.find("describing_opinion_id") != std::string::npos;
                    bool needs_group = error_msg.find("group_id") != std::string::npos;
                    
                    // Create all necessary placeholders
                    bool described_ok = true;
                    bool describing_ok = true;
                    bool group_ok = true;
                    
                    if (needs_described) {
                        described_ok = createPlaceholderOpinion(record.described_opinion_id);
                    }
                    if (needs_describing) {
                        describing_ok = createPlaceholderOpinion(record.describing_opinion_id);
                    }
                    if (needs_group) {
                        group_ok = createPlaceholderGroup(record.group_id, search_parentheticalgroup_placeholders);
                        if (group_ok) placeholders_created++;
                    }
                    
                    // If we couldn't determine the specific FK, try creating all placeholders
                    if (!needs_described && !needs_describing && !needs_group) {
                        // Generic FK error - create all placeholders to be safe
                        createPlaceholderOpinion(record.described_opinion_id);
                        createPlaceholderOpinion(record.describing_opinion_id);
                        if (createPlaceholderGroup(record.group_id, search_parentheticalgroup_placeholders)) {
                            placeholders_created++;
                        }
                    }
                    
                    // Now retry the insert
                    try {
                        pqxx::work retry_txn(conn);
                        std::ostringstream retry_query;
                        retry_query << "INSERT INTO search_parenthetical "
                                   << "(id, text, score, described_opinion_id, describing_opinion_id, group_id) VALUES ("
                                   << record.id << ", "
                                   << retry_txn.quote(record.text) << ", "
                                   << record.score << ", "
                                   << record.described_opinion_id << ", "
                                   << record.describing_opinion_id << ", "
                                   << record.group_id
                                   << ") ON CONFLICT (id) "
                                   << "DO UPDATE SET text = EXCLUDED.text, score = EXCLUDED.score, "
                                   << "described_opinion_id = EXCLUDED.described_opinion_id, "
                                   << "describing_opinion_id = EXCLUDED.describing_opinion_id, "
                                   << "group_id = EXCLUDED.group_id";
                        
                        retry_txn.exec(retry_query.str());
                        retry_txn.commit();
                        
                        inserted++;
                        continue; // Success - move to next record
                        
                    } catch (const std::exception& retry_e) {
                        std::string retry_error = retry_e.what();
                        std::cerr << "REJECTED: Record " << record.toString() 
                                  << "\n  Retry failed after creating placeholders: " << retry_error << std::endl;
                        rejected_records.push_back(record);
                        std::ostringstream reason;
                        reason << "FK violation, placeholder created but retry failed: " << retry_error;
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

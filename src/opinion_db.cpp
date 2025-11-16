#include "opinion_db.h"
#include <iostream>
#include <sstream>

OpinionDatabase::OpinionDatabase(const std::string& host, int port,
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

bool OpinionDatabase::testConnection() {
    try {
        pqxx::connection conn(connection_string_);
        return conn.is_open();
    } catch (const std::exception& e) {
        std::cerr << "Connection test failed: " << e.what() << std::endl;
        return false;
    }
}

std::string OpinionDatabase::formatOptionalInt(const std::optional<int>& val) {
    if (val.has_value()) {
        return std::to_string(val.value());
    }
    return "NULL";
}

std::string OpinionDatabase::formatOptionalString(const std::optional<std::string>& val) {
    if (val.has_value() && !val.value().empty()) {
        return val.value();
    }
    return "";
}

void OpinionDatabase::createPlaceholderCluster(pqxx::transaction_base& txn, int cluster_id, int docket_id) {
    // Create a minimal valid opinion cluster with the missing cluster_id
    // Use defaults respecting field size constraints:
    // - scdb_id: varchar(10)
    // - source: varchar(10)
    // - precedential_status: varchar(50)
    // - filepath_json_harvard: varchar(1000)
    // - filepath_pdf_harvard: varchar(100)
    std::string query = R"(
        INSERT INTO search_opinioncluster (
            id, judges, date_created, date_modified, date_filed, 
            case_name_short, case_name, case_name_full, scdb_id, source,
            procedural_history, attorneys, nature_of_suit, posture, syllabus,
            citation_count, precedential_status, blocked, docket_id,
            date_filed_is_approximate, correction, cross_reference, disposition,
            filepath_json_harvard, headnotes, history, other_dates, summary,
            arguments, headmatter, filepath_pdf_harvard
        ) VALUES (
            $1, $2, NOW(), NOW(), CURRENT_DATE,
            $3, $4, $5, $6, $7,
            $8, $9, $10, $11, $12,
            $13, $14, $15, $16,
            $17, $18, $19, $20,
            $21, $22, $23, $24, $25,
            $26, $27, $28
        )
        ON CONFLICT (id) DO NOTHING
    )";
    
    txn.exec_params(query,
        cluster_id,                    // id
        "",                            // judges
        "NA",                          // case_name_short
        "NA",                          // case_name
        "NA",                          // case_name_full
        "",                            // scdb_id (varchar(10) - keep empty)
        "NA",                          // source (varchar(10) - "NA" fits)
        "",                            // procedural_history
        "",                            // attorneys
        "",                            // nature_of_suit
        "",                            // posture
        "",                            // syllabus
        0,                             // citation_count
        "Unknown",                     // precedential_status (varchar(50))
        false,                         // blocked
        docket_id,                     // docket_id (2147483647 or opinion's value)
        false,                         // date_filed_is_approximate
        "",                            // correction
        "",                            // cross_reference
        "",                            // disposition
        "",                            // filepath_json_harvard (varchar(1000))
        "",                            // headnotes
        "",                            // history
        "",                            // other_dates
        "",                            // summary
        "",                            // arguments
        "",                            // headmatter
        ""                             // filepath_pdf_harvard (varchar(100))
    );
}

void OpinionDatabase::insertOpinion(const Opinion& opinion) {
    try {
        pqxx::connection conn(connection_string_);
        pqxx::work txn(conn);
        
        // Prepare parameterized query - including id from CSV data
        std::string query = R"(
            INSERT INTO search_opinion (
                id, date_created, date_modified, type, sha1, download_url,
                local_path, plain_text, html, html_lawbox, html_columbia,
                html_with_citations, extracted_by_ocr, author_id, cluster_id,
                per_curiam, page_count, author_str, joined_by_str,
                xml_harvard, html_anon_2020, ordering_key, main_version_id
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23
            )
        )";
        
        txn.exec_params(query,
            opinion.id,
            opinion.date_created,
            opinion.date_modified,
            opinion.type,
            opinion.sha1,
            formatOptionalString(opinion.download_url),
            opinion.local_path,
            opinion.plain_text,
            opinion.html,
            opinion.html_lawbox,
            opinion.html_columbia,
            opinion.html_with_citations,
            opinion.extracted_by_ocr,
            opinion.author_id.has_value() ? pqxx::params(opinion.author_id.value()) : pqxx::params(),
            opinion.cluster_id,
            opinion.per_curiam,
            opinion.page_count.has_value() ? pqxx::params(opinion.page_count.value()) : pqxx::params(),
            opinion.author_str,
            opinion.joined_by_str,
            opinion.xml_harvard,
            opinion.html_anon_2020,
            opinion.ordering_key.has_value() ? pqxx::params(opinion.ordering_key.value()) : pqxx::params(),
            opinion.main_version_id.has_value() ? pqxx::params(opinion.main_version_id.value()) : pqxx::params()
        );
        
        txn.commit();
        std::cout << "Successfully inserted opinion ID: " << opinion.id << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Failed to insert opinion: " << e.what() << std::endl;
        throw;
    }
}

void OpinionDatabase::insertOpinions(const std::vector<Opinion>& opinions) {
    if (opinions.empty()) {
        std::cout << "No opinions to insert." << std::endl;
        return;
    }
    
    try {
        pqxx::connection conn(connection_string_);
        pqxx::work txn(conn);
        // Ensure constraints are checked immediately per row
        txn.exec("SET CONSTRAINTS ALL IMMEDIATE");
        
        std::string query = R"(
            INSERT INTO search_opinion (
                id, date_created, date_modified, type, sha1, download_url,
                local_path, plain_text, html, html_lawbox, html_columbia,
                html_with_citations, extracted_by_ocr, author_id, cluster_id,
                per_curiam, page_count, author_str, joined_by_str,
                xml_harvard, html_anon_2020, ordering_key, main_version_id
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23
            )
            ON CONFLICT (id) DO NOTHING
        )";
        
        int success_count = 0;
        int failure_count = 0;
        int fk_violations = 0;
        int not_null_violations = 0;
        int unique_violations = 0;
        int other_errors = 0;
        int placeholder_clusters_created = 0;
        std::vector<std::string> failure_samples;
        const size_t max_samples = 5;
        
        for (const auto& opinion : opinions) {
            try {
                // Per-record subtransaction to isolate failures
                pqxx::subtransaction sub(txn, "insert_opinion_" + std::to_string(opinion.id));
                
                sub.exec_params(query,
                    opinion.id,
                    opinion.date_created,
                    opinion.date_modified,
                    opinion.type,
                    opinion.sha1,
                    formatOptionalString(opinion.download_url),
                    opinion.local_path,
                    opinion.plain_text,
                    opinion.html,
                    opinion.html_lawbox,
                    opinion.html_columbia,
                    opinion.html_with_citations,
                    opinion.extracted_by_ocr,
                    opinion.author_id,
                    opinion.cluster_id,
                    opinion.per_curiam,
                    opinion.page_count,
                    opinion.author_str,
                    opinion.joined_by_str,
                    opinion.xml_harvard,
                    opinion.html_anon_2020,
                    opinion.ordering_key,
                    opinion.main_version_id
                );
                sub.commit();
                success_count++;
                
            } catch (const std::exception& e) {
                failure_count++;
                std::string msg = e.what();
                
                // Check if this is a FK violation on cluster_id
                bool is_cluster_fk = (msg.find("foreign key") != std::string::npos) && 
                                     (msg.find("cluster_id") != std::string::npos);
                
                if (is_cluster_fk) {
                    // Attempt to create placeholder cluster and retry
                    try {
                        // Create placeholder in its own subtransaction (silent, count later)
                        pqxx::subtransaction placeholder_sub(txn, "placeholder_cluster_" + std::to_string(opinion.cluster_id));
                        createPlaceholderCluster(placeholder_sub, opinion.cluster_id, 2147483647);
                        placeholder_sub.commit();
                        placeholder_clusters_created++;
                        
                        // Retry the opinion insert in another subtransaction
                        pqxx::subtransaction retry_sub(txn, "retry_opinion_" + std::to_string(opinion.id));
                        retry_sub.exec_params(query,
                            opinion.id,
                            opinion.date_created,
                            opinion.date_modified,
                            opinion.type,
                            opinion.sha1,
                            formatOptionalString(opinion.download_url),
                            opinion.local_path,
                            opinion.plain_text,
                            opinion.html,
                            opinion.html_lawbox,
                            opinion.html_columbia,
                            opinion.html_with_citations,
                            opinion.extracted_by_ocr,
                            opinion.author_id,
                            opinion.cluster_id,
                            opinion.per_curiam,
                            opinion.page_count,
                            opinion.author_str,
                            opinion.joined_by_str,
                            opinion.xml_harvard,
                            opinion.html_anon_2020,
                            opinion.ordering_key,
                            opinion.main_version_id
                        );
                        retry_sub.commit();
                        
                        // Success after retry
                        success_count++;
                        failure_count--; // Don't count as failure
                        continue; // Skip the categorization below
                    } catch (const std::exception& retry_ex) {
                        std::cerr << "Failed to create placeholder or retry for opinion id=" << opinion.id 
                                  << ": " << retry_ex.what() << "\n";
                        // Fall through to categorize as FK failure
                    }
                }
                
                // Categorize failure
                if (msg.find("foreign key") != std::string::npos) {
                    fk_violations++;
                } else if (msg.find("not-null") != std::string::npos || msg.find("violates not-null") != std::string::npos) {
                    not_null_violations++;
                } else if (msg.find("duplicate key") != std::string::npos || msg.find("unique constraint") != std::string::npos) {
                    unique_violations++;
                } else {
                    other_errors++;
                }
                
                // Collect sample failures
                if (failure_samples.size() < max_samples) {
                    std::ostringstream sample;
                    sample << "id=" << opinion.id << " cluster_id=" << opinion.cluster_id << " msg=" << msg;
                    failure_samples.push_back(sample.str());
                }
            }
        }
        
        txn.commit();
        
        // Print batch statistics
        std::cout << "DB batch: inserted=" << success_count 
                  << " failed=" << failure_count 
                  << " attempted=" << opinions.size()
                  << " [fk=" << fk_violations
                  << ", not_null=" << not_null_violations
                  << ", unique=" << unique_violations
                  << ", other=" << other_errors << "]";
        
        if (placeholder_clusters_created > 0) {
            std::cout << " placeholder_clusters=" << placeholder_clusters_created;
        }
        std::cout << "\n";
        
        if (!failure_samples.empty()) {
            std::cout << "Sample failures:\n";
            for (const auto& s : failure_samples) {
                std::cout << "  " << s << "\n";
            }
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Batch insert transaction failed: " << e.what() << std::endl;
        throw;
    }
}

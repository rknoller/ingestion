#include "opinion_cluster_db.h"
#include <iostream>
#include <sstream>
#include <vector>
#include <algorithm>

OpinionClusterDatabase::OpinionClusterDatabase(const std::string& host, int port,
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

bool OpinionClusterDatabase::testConnection() {
    try {
        pqxx::connection conn(connection_string_);
        return conn.is_open();
    } catch (const std::exception& e) {
        std::cerr << "Connection test failed: " << e.what() << std::endl;
        return false;
    }
}

std::string OpinionClusterDatabase::formatOptionalString(const std::optional<std::string>& val) {
    if (val.has_value() && !val.value().empty()) {
        return val.value();
    }
    return "";
}

void OpinionClusterDatabase::insertCluster(const OpinionCluster& cluster) {
    try {
        pqxx::connection conn(connection_string_);
        pqxx::work txn(conn);
        
        // Prepare parameterized query - all 36 fields including id
        std::string query = R"(
            INSERT INTO search_opinioncluster (
                id, judges, date_created, date_modified, date_filed, slug,
                case_name_short, case_name, case_name_full, scdb_id, source,
                procedural_history, attorneys, nature_of_suit, posture, syllabus,
                citation_count, precedential_status, date_blocked, blocked, docket_id,
                scdb_decision_direction, scdb_votes_majority, scdb_votes_minority,
                date_filed_is_approximate, correction, cross_reference, disposition,
                filepath_json_harvard, headnotes, history, other_dates, summary,
                arguments, headmatter, filepath_pdf_harvard
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
                $31, $32, $33, $34, $35, $36
            )
            ON CONFLICT (id) DO NOTHING
        )";
        
        txn.exec_params(query,
            cluster.id,
            cluster.judges,
            cluster.date_created,
            cluster.date_modified,
            cluster.date_filed,
            cluster.slug.has_value() ? cluster.slug.value() : pqxx::params(),
            cluster.case_name_short,
            cluster.case_name,
            cluster.case_name_full,
            cluster.scdb_id,
            cluster.source,
            cluster.procedural_history,
            cluster.attorneys,
            cluster.nature_of_suit,
            cluster.posture,
            cluster.syllabus,
            cluster.citation_count,
            cluster.precedential_status,
            cluster.date_blocked.has_value() ? cluster.date_blocked.value() : pqxx::params(),
            cluster.blocked,
            cluster.docket_id,
            cluster.scdb_decision_direction.has_value() ? pqxx::params(cluster.scdb_decision_direction.value()) : pqxx::params(),
            cluster.scdb_votes_majority.has_value() ? pqxx::params(cluster.scdb_votes_majority.value()) : pqxx::params(),
            cluster.scdb_votes_minority.has_value() ? pqxx::params(cluster.scdb_votes_minority.value()) : pqxx::params(),
            cluster.date_filed_is_approximate,
            cluster.correction,
            cluster.cross_reference,
            cluster.disposition,
            cluster.filepath_json_harvard,
            cluster.headnotes,
            cluster.history,
            cluster.other_dates,
            cluster.summary,
            cluster.arguments,
            cluster.headmatter,
            cluster.filepath_pdf_harvard
        );
        
        txn.commit();
        std::cout << "Successfully inserted cluster ID: " << cluster.id << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Failed to insert cluster: " << e.what() << std::endl;
        throw;
    }
}

void OpinionClusterDatabase::insertClusters(const std::vector<OpinionCluster>& clusters) {
    if (clusters.empty()) {
        std::cout << "No clusters to insert." << std::endl;
        return;
    }
    
    try {
        pqxx::connection conn(connection_string_);
        pqxx::work txn(conn);
        // Ensure DEFERRABLE constraints (like FK on docket_id) are checked immediately per row,
        // so a single bad row won't cause the entire outer transaction to fail at commit time.
        txn.exec("SET CONSTRAINTS ALL IMMEDIATE");
        
        std::string query = R"(
            INSERT INTO search_opinioncluster (
                id, judges, date_created, date_modified, date_filed, slug,
                case_name_short, case_name, case_name_full, scdb_id, source,
                procedural_history, attorneys, nature_of_suit, posture, syllabus,
                citation_count, precedential_status, date_blocked, blocked, docket_id,
                scdb_decision_direction, scdb_votes_majority, scdb_votes_minority,
                date_filed_is_approximate, correction, cross_reference, disposition,
                filepath_json_harvard, headnotes, history, other_dates, summary,
                arguments, headmatter, filepath_pdf_harvard
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
                $31, $32, $33, $34, $35, $36
            )
            ON CONFLICT (id) DO NOTHING
        )";
        
    int success_count = 0;
    int failure_count = 0;
    int fk_violations = 0;
    int not_null_violations = 0;
    int unique_violations = 0;
    int other_errors = 0;
    std::vector<std::string> failure_samples;
    const size_t max_samples = 5;
        for (const auto& cluster : clusters) {
            try {
                // Use a subtransaction (savepoint) so one bad record doesn't abort the whole batch
                pqxx::subtransaction subtxn(txn);
                subtxn.exec_params(query,
                    cluster.id,
                    cluster.judges,
                    cluster.date_created,
                    cluster.date_modified,
                    cluster.date_filed,
                    cluster.slug,
                    cluster.case_name_short,
                    cluster.case_name,
                    cluster.case_name_full,
                    cluster.scdb_id,
                    cluster.source,
                    cluster.procedural_history,
                    cluster.attorneys,
                    cluster.nature_of_suit,
                    cluster.posture,
                    cluster.syllabus,
                    cluster.citation_count,
                    cluster.precedential_status,
                    cluster.date_blocked,
                    cluster.blocked,
                    cluster.docket_id,
                    cluster.scdb_decision_direction,
                    cluster.scdb_votes_majority,
                    cluster.scdb_votes_minority,
                    cluster.date_filed_is_approximate,
                    cluster.correction,
                    cluster.cross_reference,
                    cluster.disposition,
                    cluster.filepath_json_harvard,
                    cluster.headnotes,
                    cluster.history,
                    cluster.other_dates,
                    cluster.summary,
                    cluster.arguments,
                    cluster.headmatter,
                    cluster.filepath_pdf_harvard
                );
                subtxn.commit();
                success_count++;
            } catch (const std::exception& e) {
                // Skip this record, continue with next
                failure_count++;
                std::string msg = e.what();
                std::string lower = msg;
                std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c){ return std::tolower(c); });
                if (lower.find("foreign key") != std::string::npos) {
                    fk_violations++;
                } else if (lower.find("null value in column") != std::string::npos || lower.find("not-null constraint") != std::string::npos) {
                    not_null_violations++;
                } else if (lower.find("duplicate key value") != std::string::npos || lower.find("unique constraint") != std::string::npos) {
                    unique_violations++;
                } else {
                    other_errors++;
                }
                if (failure_samples.size() < max_samples) {
                    std::ostringstream oss;
                    oss << "fail id=" << cluster.id << ": " << msg;
                    failure_samples.push_back(oss.str());
                }
            }
        }
        
    txn.commit();
    // Batch-level statistics
    std::cout << "DB batch: inserted=" << success_count
              << " failed=" << failure_count
              << " attempted=" << clusters.size()
              << " [fk=" << fk_violations
              << ", notnull=" << not_null_violations
              << ", unique=" << unique_violations
              << ", other=" << other_errors
              << "]" << std::endl;
    if (!failure_samples.empty()) {
        std::cout << "  sample failures (up to " << max_samples << "):" << std::endl;
        for (const auto& s : failure_samples) {
            std::cout << "    - " << s << std::endl;
        }
    }
        
    } catch (const std::exception& e) {
        std::string error_msg = std::string("Batch insertion failed: ") + e.what();
        throw std::runtime_error(error_msg);
    }
}

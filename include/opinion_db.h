#pragma once

#include "opinion.h"
#include <pqxx/pqxx>
#include <string>
#include <vector>

class OpinionDatabase {
public:
    // Constructor with connection parameters
    OpinionDatabase(const std::string& host, int port, 
                    const std::string& dbname, const std::string& user, 
                    const std::string& password);
    
    // Insert a single opinion record
    void insertOpinion(const Opinion& opinion);
    
    // Insert multiple opinion records in a transaction
    void insertOpinions(const std::vector<Opinion>& opinions);
    
    // Test connection
    bool testConnection();

private:
    std::string connection_string_;
    
    // Helper to escape and format optional values
    std::string formatOptionalInt(const std::optional<int>& val);
    std::string formatOptionalString(const std::optional<std::string>& val);
    
    // Create placeholder opinion cluster for missing FK (can work with work or subtransaction)
    void createPlaceholderCluster(pqxx::transaction_base& txn, int cluster_id, int docket_id);
};

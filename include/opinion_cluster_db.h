#pragma once

#include "opinion_cluster.h"
#include <pqxx/pqxx>
#include <string>
#include <vector>

class OpinionClusterDatabase {
public:
    // Constructor with connection parameters
    OpinionClusterDatabase(const std::string& host, int port, 
                          const std::string& dbname, const std::string& user, 
                          const std::string& password);
    
    // Insert a single cluster record
    void insertCluster(const OpinionCluster& cluster);
    
    // Insert multiple cluster records in a transaction
    void insertClusters(const std::vector<OpinionCluster>& clusters);
    
    // Test connection
    bool testConnection();

private:
    std::string connection_string_;
    
    // Helper to format optional values
    std::string formatOptionalString(const std::optional<std::string>& val);
};

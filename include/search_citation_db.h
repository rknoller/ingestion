#pragma once

#include "search_citation.h"
#include <pqxx/pqxx>
#include <string>
#include <vector>
#include <set>

class SearchCitationDatabase {
public:
    // Constructor with connection parameters
    SearchCitationDatabase(const std::string& host, int port, 
                          const std::string& dbname, const std::string& user, 
                          const std::string& password);
    
    // Load all valid cluster IDs from search_opinioncluster table
    void loadValidClusterIds();
    
    // Check if a cluster ID exists (foreign key validation)
    bool isValidClusterId(int cluster_id) const;
    
    // Insert citation records with FK handling
    // Returns pair: (number of records successfully inserted, number of placeholders created)
    std::pair<size_t, size_t> insertCitations(const std::vector<SearchCitation>& records,
                          std::vector<SearchCitation>& rejected_records,
                          std::vector<std::string>& rejection_reasons,
                          std::vector<int>& search_opinioncluster_placeholders);
    
    // Create placeholder record in search_opinioncluster for missing cluster ID
    bool createPlaceholderCluster(int cluster_id, std::vector<int>& search_opinioncluster_placeholders);
    
    // Test connection
    bool testConnection();

private:
    std::string connection_string_;
    std::set<int> valid_cluster_ids_; // Cache of valid cluster IDs
};

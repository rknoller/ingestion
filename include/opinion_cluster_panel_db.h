#pragma once

#include "opinion_cluster_panel.h"
#include <pqxx/pqxx>
#include <string>
#include <vector>
#include <set>

class OpinionClusterPanelDatabase {
public:
    // Constructor with connection parameters
    OpinionClusterPanelDatabase(const std::string& host, int port, 
                                const std::string& dbname, const std::string& user, 
                                const std::string& password);
    
    // Load all valid opinioncluster IDs from search_opinioncluster table
    void loadValidClusterIds();
    
    // Check if a cluster ID exists (foreign key validation)
    bool isValidClusterId(int cluster_id) const;
    
    // Insert panel records that pass FK validation
    // Returns number of records successfully inserted
    size_t insertPanels(const std::vector<OpinionClusterPanel>& panels,
                        std::vector<OpinionClusterPanel>& rejected_panels,
                        std::vector<std::string>& rejection_reasons);
    
    // Create placeholder record in search_opinioncluster for missing cluster ID
    bool createPlaceholderCluster(int cluster_id);
    
    // Test connection
    bool testConnection();

private:
    std::string connection_string_;
    std::set<int> valid_cluster_ids_; // Cache of valid cluster IDs
};

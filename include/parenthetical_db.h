#pragma once

#include "parenthetical.h"
#include <pqxx/pqxx>
#include <string>
#include <vector>
#include <set>

class ParentheticalDatabase {
public:
    // Constructor with connection parameters
    ParentheticalDatabase(const std::string& host, int port, 
                         const std::string& dbname, const std::string& user, 
                         const std::string& password);
    
    // Load all valid group IDs from search_parentheticalgroup table
    void loadValidGroupIds();
    
    // Check if a group ID exists (foreign key validation)
    bool isValidGroupId(int group_id) const;
    
    // Insert parenthetical records with FK handling
    // Returns pair: (number of records successfully inserted, number of placeholders created)
    std::pair<size_t, size_t> insertParentheticals(const std::vector<Parenthetical>& records,
                          std::vector<Parenthetical>& rejected_records,
                          std::vector<std::string>& rejection_reasons,
                          std::vector<int>& search_parentheticalgroup_placeholders);
    
    // Create placeholder record in search_parentheticalgroup for missing group ID
    bool createPlaceholderGroup(int group_id, std::vector<int>& search_parentheticalgroup_placeholders);
    
    // Create placeholder record in search_opinion for missing opinion ID
    bool createPlaceholderOpinion(int opinion_id);
    
    // Test connection
    bool testConnection();

private:
    std::string connection_string_;
    std::set<int> valid_group_ids_; // Cache of valid group IDs
};

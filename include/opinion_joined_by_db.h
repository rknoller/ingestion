#pragma once

#include "opinion_joined_by.h"
#include <pqxx/pqxx>
#include <string>
#include <vector>
#include <set>

class OpinionJoinedByDatabase {
public:
    // Constructor with connection parameters
    OpinionJoinedByDatabase(const std::string& host, int port, 
                           const std::string& dbname, const std::string& user, 
                           const std::string& password);
    
    // Load all valid opinion IDs from search_opinion table
    void loadValidOpinionIds();
    
    // Check if an opinion ID exists (foreign key validation)
    bool isValidOpinionId(int opinion_id) const;
    
    // Insert joined_by records with FK handling
    // Returns number of records successfully inserted
    size_t insertJoinedBy(const std::vector<OpinionJoinedBy>& records,
                         std::vector<OpinionJoinedBy>& rejected_records,
                         std::vector<std::string>& rejection_reasons);
    
    // Create placeholder record in search_opinion for missing opinion ID
    bool createPlaceholderOpinion(int opinion_id);
    
    // Test connection
    bool testConnection();

private:
    std::string connection_string_;
    std::set<int> valid_opinion_ids_; // Cache of valid opinion IDs
};

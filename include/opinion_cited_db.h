#pragma once

#include "opinion_cited.h"
#include <pqxx/pqxx>
#include <string>
#include <vector>
#include <set>

class OpinionCitedDatabase {
public:
    // Constructor with connection parameters
    OpinionCitedDatabase(const std::string& host, int port, 
                        const std::string& dbname, const std::string& user, 
                        const std::string& password);
    
    // Load all valid opinion IDs from search_opinion table
    void loadValidOpinionIds();
    
    // Check if an opinion ID exists (foreign key validation)
    bool isValidOpinionId(int opinion_id) const;
    
    // Insert citation records with FK handling
    // Returns number of records successfully inserted
    size_t insertCitations(const std::vector<OpinionCited>& records,
                          std::vector<OpinionCited>& rejected_records,
                          std::vector<std::string>& rejection_reasons);
    
    // Create placeholder record in search_opinion for missing opinion ID
    bool createPlaceholderOpinion(int opinion_id);
    
    // Test connection
    bool testConnection();

private:
    std::string connection_string_;
    std::set<int> valid_opinion_ids_; // Cache of valid opinion IDs
};

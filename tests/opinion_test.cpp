#include <gtest/gtest.h>
#include "opinion.h"
#include <fstream>
#include <string>

TEST(OpinionReaderTest, ParsesCsvLineCorrectly) {
    // Create a temp file with header and one data row
    std::string temp_path = "/tmp/test_single_opinion.csv";
    std::ofstream out(temp_path);
    out << "id,date_created,date_modified,type,sha1,download_url,local_path,plain_text,html,"
        << "html_lawbox,html_columbia,html_with_citations,extracted_by_ocr,author_id,cluster_id,"
        << "per_curiam,page_count,author_str,joined_by_str,xml_harvard,html_anon_2020,ordering_key,main_version_id\n";
    out << "1717410,2013-10-30 07:13:39.78111+00,2025-06-07 04:51:43.344465+00,010combined,"
        << "b2d54f5be925e013ab3586f4d9e305ba396e3887,,"
        << "\"\",\"\",\"<div>test</div>\",\"\",\"\",\"\",false,,2147483646,false,,"
        << "\"\",\"\",\"\",\"\",-2055328520,100\n";
    out.close();
    
    OpinionReader reader(temp_path);
    auto opinions = reader.readOpinions(1);
    
    ASSERT_EQ(opinions.size(), 1);
    Opinion op = opinions[0];
    
    EXPECT_EQ(op.id, 1717410);
    EXPECT_EQ(op.type, "010combined");
    EXPECT_EQ(op.sha1, "b2d54f5be925e013ab3586f4d9e305ba396e3887");
    EXPECT_FALSE(op.extracted_by_ocr);
    EXPECT_FALSE(op.per_curiam);
    EXPECT_EQ(op.cluster_id, 2147483646);
    EXPECT_TRUE(op.ordering_key.has_value());
    EXPECT_EQ(*op.ordering_key, -2055328520);
    EXPECT_TRUE(op.main_version_id.has_value());
    EXPECT_EQ(*op.main_version_id, 100);
}

TEST(OpinionReaderTest, SplitsCsvWithQuotes) {
    OpinionReader reader("");
    
    std::string line = "a,\"b,c\",d";
    auto cols = reader.splitCsvLine(line);
    
    ASSERT_EQ(cols.size(), 3);
    EXPECT_EQ(cols[0], "a");
    EXPECT_EQ(cols[1], "b,c");
    EXPECT_EQ(cols[2], "d");
}

TEST(OpinionReaderTest, HandlesEscapedQuotes) {
    OpinionReader reader("");
    
    std::string line = "a,\"b\"\"c\",d";
    auto cols = reader.splitCsvLine(line);
    
    ASSERT_EQ(cols.size(), 3);
    EXPECT_EQ(cols[0], "a");
    EXPECT_EQ(cols[1], "b\"c");
    EXPECT_EQ(cols[2], "d");
}

TEST(OpinionReaderTest, ReadsMultipleRecords) {
    // Create temp CSV file with proper column alignment
    std::string temp_path = "/tmp/test_opinions_unit.csv";
    std::ofstream out(temp_path);
    out << "id,date_created,date_modified,type,sha1,download_url,local_path,plain_text,html,"
        << "html_lawbox,html_columbia,html_with_citations,extracted_by_ocr,author_id,cluster_id,"
        << "per_curiam,page_count,author_str,joined_by_str,xml_harvard,html_anon_2020,ordering_key,main_version_id\n";
    out << "1,2013-10-30,2025-06-07,type1,sha1,,,,,,,,false,,100,false,,,,,10,\n";
    out << "2,2013-10-31,2025-06-08,type2,sha2,,,,,,,,true,,200,true,,,,,20,1\n";
    out.close();
    
    OpinionReader reader(temp_path);
    auto opinions = reader.readOpinions(10);
    
    ASSERT_GE(opinions.size(), 2);
    EXPECT_EQ(opinions[0].id, 1);
    EXPECT_EQ(opinions[0].type, "type1");
    EXPECT_EQ(opinions[1].id, 2);
    EXPECT_EQ(opinions[1].type, "type2");
}
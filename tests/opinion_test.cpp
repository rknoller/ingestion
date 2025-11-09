// Minimal unit test harness (no external frameworks)
#include "opinion.h"
#include <fstream>
#include <iostream>
#include <string>

static int failures = 0;

#define EXPECT_TRUE(cond) do { \
    if (!(cond)) { \
        std::cerr << __FILE__ << ":" << __LINE__ << ": EXPECT_TRUE failed: " #cond "\n"; \
        ++failures; \
    } \
} while(0)

#define EXPECT_FALSE(cond) EXPECT_TRUE(!(cond))

#define EXPECT_EQ(a, b) do { \
    auto _va = (a); auto _vb = (b); \
    if (!((_va) == (_vb))) { \
        std::cerr << __FILE__ << ":" << __LINE__ << ": EXPECT_EQ failed: " #a " == " #b \
                  << " (" << _va << " != " << _vb << ")\n"; \
        ++failures; \
    } \
} while(0)

#define EXPECT_GE(a, b) do { \
    auto _va = (a); auto _vb = (b); \
    if (!((_va) >= (_vb))) { \
        std::cerr << __FILE__ << ":" << __LINE__ << ": EXPECT_GE failed: " #a " >= " #b \
                  << " (" << _va << " < " << _vb << ")\n"; \
        ++failures; \
    } \
} while(0)

void Test_ParsesCsvLineCorrectly() {
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

    EXPECT_EQ(opinions.size(), 1u);
    Opinion op = opinions[0];

    EXPECT_EQ(op.id, 1717410);
    EXPECT_EQ(op.type, std::string("010combined"));
    EXPECT_EQ(op.sha1, std::string("b2d54f5be925e013ab3586f4d9e305ba396e3887"));
    EXPECT_FALSE(op.extracted_by_ocr);
    EXPECT_FALSE(op.per_curiam);
    EXPECT_EQ(op.cluster_id, 2147483646);
    EXPECT_TRUE(op.ordering_key.has_value());
    EXPECT_EQ(*op.ordering_key, -2055328520);
    EXPECT_TRUE(op.main_version_id.has_value());
    EXPECT_EQ(*op.main_version_id, 100);
}

void Test_SplitsCsvWithQuotes() {
    OpinionReader reader("");

    std::string line = "a,\"b,c\",d";
    auto cols = reader.splitCsvLine(line);

    EXPECT_EQ(cols.size(), 3u);
    EXPECT_EQ(cols[0], std::string("a"));
    EXPECT_EQ(cols[1], std::string("b,c"));
    EXPECT_EQ(cols[2], std::string("d"));
}

void Test_HandlesEscapedQuotes() {
    OpinionReader reader("");

    std::string line = "a,\"b\"\"c\",d";
    auto cols = reader.splitCsvLine(line);

    EXPECT_EQ(cols.size(), 3u);
    EXPECT_EQ(cols[0], std::string("a"));
    EXPECT_EQ(cols[1], std::string("b\"c"));
    EXPECT_EQ(cols[2], std::string("d"));
}

void Test_ReadsMultipleRecords() {
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

    EXPECT_GE(opinions.size(), 2u);
    EXPECT_EQ(opinions[0].id, 1);
    EXPECT_EQ(opinions[0].type, std::string("type1"));
    EXPECT_EQ(opinions[1].id, 2);
    EXPECT_EQ(opinions[1].type, std::string("type2"));
}

int main() {
    Test_ParsesCsvLineCorrectly();
    Test_SplitsCsvWithQuotes();
    Test_HandlesEscapedQuotes();
    Test_ReadsMultipleRecords();
    if (failures) {
        std::cerr << failures << " test(s) failed\n";
        return 1;
    }
    std::cout << "All tests passed\n";
    return 0;
}
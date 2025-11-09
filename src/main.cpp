#include <iostream>
#include <vector>
#include <string>
#include <exception>
#include "hello.h"
#include "opinion.h"

int main(int argc, char** argv) {
    std::cout << hello("World") << "\n";
    std::cout << "2 + 3 = " << add(2, 3) << "\n";

    if (argc < 2) {
        std::cout << "(Pass path to opinions CSV as first argument to read first 100 lines)\n";
        return 0;
    }
    std::string csvPath = argv[1];
    try {
        OpinionReader reader(csvPath);
        auto opinions = reader.readOpinions(100);
        std::cout << "Loaded " << opinions.size() << " opinion records (showing up to 5)\n";
        for (size_t i = 0; i < opinions.size() && i < 5; ++i) {
            std::cout << i << ": " << opinions[i].toString() << "\n";
        }
    } catch (const std::exception& ex) {
        std::cerr << "Error reading CSV: " << ex.what() << "\n";
        return 1;
    }
    return 0;
}

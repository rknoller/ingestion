#include <iostream>
#include <string>
#include "opinion.h"

int main(int argc, char** argv) {
    std::cout << "Opinion Parser Example\n";
    std::cout << "----------------------\n";

    if (argc < 2) {
        std::cout << "Usage: calculator_example <path-to-csv>\n";
        return 0;
    }

    std::string path = argv[1];
    try {
        OpinionReader reader(path);
        auto opinions = reader.readOpinions(5);
        std::cout << "Loaded " << opinions.size() << " opinions (showing all loaded)\n";
        for (size_t i = 0; i < opinions.size(); ++i) {
            std::cout << i << ": " << opinions[i].toString() << "\n";
        }
    } catch (const std::exception& ex) {
        std::cerr << "Error: " << ex.what() << "\n";
        return 1;
    }
    return 0;
}
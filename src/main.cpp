#include <iostream>
#include "hello.h"

int main() {
    std::cout << hello("World") << "\n";
    std::cout << "2 + 3 = " << add(2, 3) << "\n";
    return 0;
}

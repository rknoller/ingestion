#include <iostream>
#include "hello.h"

int main() {
    std::cout << "Calculator Example\n";
    std::cout << "-----------------\n\n";
    
    int a = 10, b = 5;
    std::cout << a << " + " << b << " = " << add(a, b) << "\n";
    
    // Show greeting
    std::cout << "\n" << hello("Calculator User") << "\n";
    
    return 0;
}
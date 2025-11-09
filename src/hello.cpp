#include "hello.h"

#include <string>

std::string hello(const std::string& name) {
    return "Hello, " + name + "!";
}

int add(int a, int b) {
    return a + b;
}

#include <gtest/gtest.h>
#include "hello.h"

TEST(HelloTest, ReturnsCorrectGreeting) {
    std::string result = hello("World");
    EXPECT_EQ(result, "Hello, World!");
}

TEST(HelloTest, HandlesEmptyString) {
    std::string result = hello("");
    EXPECT_EQ(result, "Hello, !");
}

TEST(AddTest, AddsPositiveNumbers) {
    EXPECT_EQ(add(2, 3), 5);
    EXPECT_EQ(add(0, 0), 0);
    EXPECT_EQ(add(100, 200), 300);
}

TEST(AddTest, AddsNegativeNumbers) {
    EXPECT_EQ(add(-2, -3), -5);
    EXPECT_EQ(add(-10, 5), -5);
    EXPECT_EQ(add(5, -10), -5);
}
add_executable(calculator_example
    calculator.cpp
)

target_link_libraries(calculator_example
    PRIVATE
        ingestion_lib
)
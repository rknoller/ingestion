# Ingestion

A C++ library for data ingestion and processing, with examples and tests.

## Project Structure

```
.
├── include/          # Public headers
├── src/             # Library implementation
├── examples/        # Example programs
├── tests/           # Unit tests
└── third_party/     # Dependencies (GoogleTest)
```

## Building

```bash
# Clone with submodules
git clone --recursive https://github.com/rknoller/ingestion.git
cd ingestion

# Configure & build
mkdir -p build
cd build
cmake ..
cmake --build .

# Run tests
ctest --output-on-failure

# Run main app
./ingestion_app

# Run example
./examples/calculator_example
```

## Development

- Main library target: `ingestion_lib`
- Main executable: `ingestion_app`
- Tests executable: `ingestion_tests`
- Example programs in `examples/`

VS Code tasks are configured for building and debugging.

## Testing

Tests use Google Test framework. Run tests with:
```bash
cd build
ctest --output-on-failure
```

Or run the test executable directly for more detailed output:
```bash
./build/ingestion_tests
```

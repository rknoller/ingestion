# Ingestion

A modern C++17 library and demo application for resilient CSV-based opinion data ingestion.
It showcases:

- Header-driven CSV parsing with robust handling of quoted fields, escaped quotes, and multi-line records.
- Safe conversion of integers and booleans with sensible defaults; optional fields via `std::optional`.
- Clean separation between a reusable library (`ingestion_lib`) and executables (`ingestion_app`, examples).
- Lightweight custom test harness (no external testing frameworks or Google dependencies).
- Ready-to-debug VS Code setup (build, run, test tasks + `launch.json`).

## Project Structure

```
.
├── include/          # Public headers
├── src/             # Library implementation
├── examples/        # Example programs
├── tests/           # Unit tests
└── third_party/     # (Currently unused)
```

## Building

```bash
git clone https://github.com/rknoller/ingestion.git
cd ingestion

# Configure & build (Debug by default)
mkdir -p build && cd build
cmake ..
cmake --build .

# Run tests
ctest --output-on-failure

# Run main app (pass a CSV path)
./ingestion_app ../examples/sample.csv

# Run example
./examples/calculator_example
```

## Development

Targets:
- `ingestion_lib`: opinion ingestion logic (`opinion.h` / `opinion.cpp`).
- `ingestion_app`: CLI demo reading up to the first N (default 100) valid opinion rows.
- `ingestion_tests`: Custom minimal unit test harness (no third-party frameworks).
- `calculator_example`: Minimal `OpinionReader` example printing a few parsed rows.

Key features of the CSV parser (`OpinionReader`):
- Dynamically maps columns by header names (order-independent).
- Merges multi-line quoted records before parsing.
- Skips malformed or insufficient rows safely (never throws on content issues—only I/O).
- Provides `toString()` for quick inspection of parsed rows.

Extending Parsing:
- Add new columns by updating `Opinion` and mapping logic in `parseCsvLine()`.
- Consider performance tuning (e.g., reserve vector capacity, streaming iterator) for very large files.

## Testing

The project uses a tiny self-contained test harness (see `tests/opinion_test.cpp`). Assertions are implemented with simple macros; failures print file, line, and expression.

Run tests:
```bash
cd build
ctest --output-on-failure
```

Or invoke directly:
```bash
./ingestion_tests
```

Adding Tests:
1. Add new `Test_*` functions to `tests/opinion_test.cpp` (or create another file and list it in `CMakeLists.txt`).
2. Call your new function from `main()` inside the test file.
3. Rebuild; CTest will rerun the executable.

## Debugging (VS Code)

Launch configs let you supply a CSV path argument. Set breakpoints in `opinion.cpp` to inspect parsing state. The project builds with debug symbols.

## Next Ideas

- Add code coverage (gcov + lcov) and publish as a GitHub Actions artifact.
- Sanitizers (address/undefined) for fuzzing malformed CSV inputs.
- Performance benchmark for very large CSVs.

## License

MIT (add a LICENSE file if distributing publicly).

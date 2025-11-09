.PHONY: all build clean
all: build

build:
	@mkdir -p build
	@cd build && cmake ..
	@cd build && cmake --build .

clean:
	rm -rf build

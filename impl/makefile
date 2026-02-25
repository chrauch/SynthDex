CC      = g++
CFLAGS  = -O3 -mavx -march=native -std=c++23 -w -fopenmp
LDFLAGS = -fopenmp

SOURCES := $(wildcard */*.cpp) $(wildcard *.cpp)
OBJECTS := $(patsubst %.cpp,../output/compilation/%.o,$(SOURCES))

TARGET := nis

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(OBJECTS) -o $(TARGET) $(LDADD)

../output/compilation/%.o: %.cpp
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -rf ../output/compilation
	rm -f $(TARGET)

# pkg_config to find the libraries and include paths for the rabbitmq-c library
# PKG_CONFIG_PATH ?= /usr/local/lib/pkgconfig

CXX ?= g++

# includes
CPPFLAGS += `pkg-config --cflags librabbitmq`

# libraries
LDFLAGS += `pkg-config --libs librabbitmq`

all: send receive

send: send.cpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

receive: receive.cpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -f send receive
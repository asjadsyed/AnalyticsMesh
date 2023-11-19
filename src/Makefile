.PHONY: all
all: $(patsubst %.thrift,gen_py/%,$(wildcard *.thrift))

gen_py/%: %.thrift
	mkdir -p gen_py
	thrift --recurse --gen py --out gen_py "$<"

gen-java/%: %.thrift
	thrift --recurse --gen java "$<"

gen-cpp/%: %.thrift
	thrift --recurse --gen cpp "$<"

.PHONY: clean
clean:
	rm -rf gen-* gen_py

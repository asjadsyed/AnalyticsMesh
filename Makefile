THRIFT_SRC_DIR ?= src
THRIFT_GEN_DIR ?= src/gen

.PHONY: all
all: $(patsubst $(THRIFT_SRC_DIR)/%.thrift,$(THRIFT_GEN_DIR)/py/%,$(wildcard $(THRIFT_SRC_DIR)/*.thrift))

$(THRIFT_GEN_DIR)/py/%: $(THRIFT_SRC_DIR)/%.thrift
	mkdir -p "$(THRIFT_GEN_DIR)/py"
	thrift --recurse --gen "py" --out "$(THRIFT_GEN_DIR)/py" "$<"

$(THRIFT_GEN_DIR)/java/%: $(THRIFT_SRC_DIR)/%.thrift
	mkdir -p "$(THRIFT_GEN_DIR)/java"
	thrift --recurse --gen "java" --out "$(THRIFT_GEN_DIR)/java" "$<"

$(THRIFT_GEN_DIR)/cpp/%: $(THRIFT_SRC_DIR)/%.thrift
	mkdir -p "$(THRIFT_GEN_DIR)/cpp"
	thrift --recurse --gen "cpp" --out "$(THRIFT_GEN_DIR)/cpp" "$<"

.PHONY: clean
clean:
	rm -rf "$(THRIFT_GEN_DIR)"

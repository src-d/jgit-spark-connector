# Package configuration
PROJECT = engine-playground
COMMANDS = engine-playground
DEPENDENCIES = \
	github.com/jteeuwen/go-bindata \
	github.com/src-d/go-compose-installer

# Assets configuration
ASSETS_PATH ?= $(WORKDIR)/etc
ASSETS_PACKAGE := main
BINDATA_FILE := engine-playground/bindata.go
BINDATA_CMD := go-bindata

LD_FLAGS = -extldflags "-static" -s -w

# Including ci Makefile
MAKEFILE = Makefile.main
CI_REPOSITORY = https://github.com/src-d/ci.git
CI_FOLDER = .ci

PACKAGES :=

$(MAKEFILE):
	@git clone --quiet $(CI_REPOSITORY) $(CI_FOLDER); \
	cp $(CI_FOLDER)/$(MAKEFILE) .;

-include $(MAKEFILE)

docker-build: prepare

prepare:
	mkdir -p $(BUILD_PATH)/opt/lib/; \
	mkdir -p $(BUILD_PATH)/opt/src/; \
	cp ../target/engine-uber.jar $(BUILD_PATH)/opt/lib/; \
	cp -rf ../python $(BUILD_PATH)/opt/src/; \

assets:
	$(BINDATA_CMD) \
		-pkg $(ASSETS_PACKAGE) \
		-modtime 1 \
		-nocompress \
		-prefix $(ASSETS_PATH) \
		-o $(BINDATA_FILE) \
		$(ASSETS_PATH)

.PHONY: assets

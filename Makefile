# Docker
DOCKER_CMD = docker
DOCKER_BUILD = $(DOCKER_CMD) build
DOCKER_TAG ?= $(DOCKER_CMD) tag
DOCKER_PUSH ?= $(DOCKER_CMD) push
DOCKER_RUN = $(DOCKER_CMD) run
DOCKER_RMI = $(DOCKER_CMD) rmi -f

# Docker run bblfsh server container
BBLFSH_CONTAINER_NAME = bblfsh
BBLFSH_HOST_PORT = 9432
BBLFSH_CONTAINER_PORT = 9432
BBLFSH_IMAGE = bblfsh/server
BBLFSH_VERSION = latest
BBLFSH_CMD = bblfsh server --log-level debug
BBLFSH_RUN_FLAGS := --rm --privileged \
	-p $(BBLFSH_HOST_PORT):$(BBLFSH_CONTAINER_PORT) \
	--name $(BBLFSH_CONTAINER_NAME) \
	$(BBLFSH_IMAGE):$(BBLFSH_VERSION) \
	$(BBLFSH_CMD)

# escape_docker_tag escape colon char to allow use a docker tag as rule
define escape_docker_tag
$(subst :,--,$(1))
endef

# unescape_docker_tag an escaped docker tag to be use in a docker command
define unescape_docker_tag
$(subst --,:,$(1))
endef

# Docker jupyter image tag
GIT_COMMIT=$(shell git rev-parse HEAD | cut -c1-7)
GIT_DIRTY=$(shell test -n "`git status --porcelain`" && echo "-dirty" || true)
DEV_PREFIX := dev
VERSION ?= $(DEV_PREFIX)-$(GIT_COMMIT)$(GIT_DIRTY)

# Docker jupyter image
JUPYTER_IMAGE ?= srcd/spark-api-jupyter
JUPYTER_IMAGE_VERSIONED ?= $(call escape_docker_tag,$(JUPYTER_IMAGE):$(VERSION))

# Docker run jupyter container
JUPYTER_CONTAINER_NAME = spark-api-jupyter
JUPYTER_HOST_PORT = 8888
JUPYTER_CONTAINER_PORT = 8888
REPOSITORIES_HOST_DIR := $(PWD)/examples/siva-files
REPOSITORIES_CONTAINER_DIR = /repositories
JUPYTER_RUN_FLAGS := --name $(JUPYTER_CONTAINER_NAME) --rm -it \
	-p $(JUPYTER_HOST_PORT):$(JUPYTER_CONTAINER_PORT) \
	-v $(REPOSITORIES_HOST_DIR):$(REPOSITORIES_CONTAINER_DIR) \
	--link bblfsh:bblfsh \
	$(call unescape_docker_tag,$(JUPYTER_IMAGE_VERSIONED))

# Scala version
SCALA_VERSION ?= 2.11.11

# if TRAVIS_SCALA_VERSION defined SCALA_VERSION is overrided
ifneq ($(TRAVIS_SCALA_VERSION), )
	SCALA_VERSION := $(TRAVIS_SCALA_VERSION)
endif

# if TRAVIS_TAG defined VERSION is overrided
ifneq ($(TRAVIS_TAG), )
	VERSION := $(TRAVIS_TAG)
endif

# if we are not in master, and it's not a tag the push is disabled
ifneq ($(TRAVIS_BRANCH), master)
	ifeq ($(TRAVIS_TAG), )
        pushdisabled = "push disabled for non-master branches"
	endif
endif

# if this is a pull request, the push is disabled
ifneq ($(TRAVIS_PULL_REQUEST), false)
        pushdisabled = "push disabled for pull-requests"
endif

#SBT
SBT = ./sbt ++$(SCALA_VERSION)

# Rules
all: clean build

clean:
	$(SBT) clean

test:
	$(SBT) test

build:
	$(SBT) assembly

travis-test:
	$(SBT) scalastyle test test:scalastyle coverage coverageReport

docker-bblfsh:
	$(DOCKER_RUN) $(BBLFSH_RUN_FLAGS)

docker-build: build
	$(DOCKER_BUILD) -t $(call unescape_docker_tag,$(JUPYTER_IMAGE_VERSIONED)) .

docker-run: docker-build
	$(DOCKER_RUN) $(JUPYTER_RUN_FLAGS)

docker-clean:
	$(DOCKER_RMI) $(call unescape_docker_tag,$(JUPYTER_IMAGE_VERSIONED))

docker-push: docker-build
	$(if $(pushdisabled),$(error $(pushdisabled)))

	@if [ "$$DOCKER_USERNAME" != "" ]; then \
		$(DOCKER_CMD) login -u="$$DOCKER_USERNAME" -p="$$DOCKER_PASSWORD"; \
	fi;

	$(DOCKER_PUSH) $(call unescape_docker_tag,$(JUPYTER_IMAGE_VERSIONED))
	@if [ "$$TRAVIS_TAG" != "" ]; then \
		$(DOCKER_TAG) $(call unescape_docker_tag,$(JUPYTER_IMAGE_VERSIONED)) \
			$(call unescape_docker_tag,$(JUPYTER_IMAGE)):latest; \
		$(DOCKER_PUSH) $(call unescape_docker_tag,$(JUPYTER_IMAGE):latest); \
	fi;


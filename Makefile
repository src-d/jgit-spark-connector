# Docsrv: configure the languages whose api-doc can be auto generated
LANGUAGES = "go scala python"
# Docsrv: configure the directory containing the python sources
PYTHON_MAIN_DIR ?= ./python
# Docs: do not edit this
DOCS_REPOSITORY := https://github.com/src-d/docs
SHARED_PATH ?= $(shell pwd)/.docsrv-resources
DOCS_PATH ?= $(SHARED_PATH)/.docs
$(DOCS_PATH)/Makefile.inc:
	git clone --quiet --depth 1 $(DOCS_REPOSITORY) $(DOCS_PATH);
-include $(DOCS_PATH)/Makefile.inc

# Docker
DOCKER_CMD = docker
DOCKER_RUN = $(DOCKER_CMD) run
DOCKER_EXEC = $(DOCKER_CMD) exec

# Docker run bblfsh server container
BBLFSH_CONTAINER_NAME = bblfshd
BBLFSH_HOST_PORT = 9432
BBLFSH_CONTAINER_PORT = 9432
BBLFSH_HOST_VOLUME = /var/lib/bblfshd
BBLFSH_CONTAINER_VOLUME = /var/lib/bblfshd
BBLFSH_IMAGE = bblfsh/bblfshd
BBLFSH_VERSION = v2.1.2

BBLFSH_RUN_FLAGS := --detach --name $(BBLFSH_CONTAINER_NAME) --privileged \
	-p $(BBLFSH_HOST_PORT):$(BBLFSH_CONTAINER_PORT) \
	-v $(BBLFSH_HOST_VOLUME):$(BBLFSH_CONTAINER_VOLUME) \
	$(BBLFSH_IMAGE):$(BBLFSH_VERSION)

BBLFSH_EXEC_FLAGS = -it
BBLFSH_CTL = bblfshctl
BBLFSH_CTL_DRIVER := $(BBLFSH_CTL) driver

BBLFSH_CTL_INSTALL_DRIVERS := $(BBLFSH_CTL_DRIVER) install --all
BBLFSH_EXEC_INSTALL_COMMAND := $(BBLFSH_CONTAINER_NAME) $(BBLFSH_CTL_INSTALL_DRIVERS)
BBLFSH_INSTALL_DRIVERS := $(BBLFSH_EXEC_FLAGS) $(BBLFSH_EXEC_INSTALL_COMMAND)

BBLFSH_CTL_LIST_DRIVERS := $(BBLFSH_CTL_DRIVER) list
BBLFSH_EXEC_LIST_COMMAND := $(BBLFSH_CONTAINER_NAME) bblfshctl driver list
BBLFSH_LIST_DRIVERS := $(BBLFSH_EXEC_FLAGS) $(BBLFSH_EXEC_LIST_COMMAND)

# Docker jupyter image tag
GIT_COMMIT=$(shell git rev-parse HEAD | cut -c1-7)
GIT_DIRTY=$(shell test -n "`git status --porcelain`" && echo "-dirty" || true)
DEV_PREFIX := dev
VERSION ?= $(DEV_PREFIX)-$(GIT_COMMIT)$(GIT_DIRTY)

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
	$(SBT) clean coverage test coverageReport scalastyle test:scalastyle

docker-bblfsh:
	$(DOCKER_RUN) $(BBLFSH_RUN_FLAGS)

docker-bblfsh-install-drivers:
	$(DOCKER_EXEC) $(BBLFSH_INSTALL_DRIVERS)

docker-bblfsh-list-drivers:
	$(DOCKER_EXEC) $(BBLFSH_LIST_DRIVERS)

maven-release:
	$(SBT) clean publishSigned && \
	$(SBT) sonatypeRelease

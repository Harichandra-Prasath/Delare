SHELL := /bin/bash

BINARY_NAME := delared
OUT_DIR := bin

GO_CMD := go
GO_BUILD := $(GO_CMD) build

LDFLAGS := -s -w

.PHONY: all build

all: build ## Default target, builds the delared binary

build: ## Build the delared binary
	@echo "==> Building $(BINARY_NAME)..."
	@mkdir -p $(OUT_DIR)
	$(GO_BUILD) -ldflags="$(LDFLAGS)" -o $(OUT_DIR)/$(BINARY_NAME) ./cmd/
	@echo "==> Binary compiled to $(OUT_DIR)/$(BINARY_NAME)"

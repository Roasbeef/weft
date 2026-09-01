# Weft — common developer commands.
#
# Every target is a thin wrapper over gleam and the scripts, so what CI
# runs and what you run locally are the same commands.

.DEFAULT_GOAL := help

# ---------------------------------------------------------------- checking

.PHONY: check
check: ## Full gate: format, warning-free build, tests, lint, doc-check
	@$(MAKE) --no-print-directory fmt-check build test lint doc-check
	@echo "check clean"

.PHONY: test
test: ## Run tests only (skips format check)
	@gleam test

.PHONY: build
build: ## Warning-free build
	@gleam build --warnings-as-errors

# --------------------------------------------------------------- formatting

.PHONY: fmt
fmt: ## Format all Gleam sources in place
	@gleam format src test
	@echo "formatted"

.PHONY: fmt-check
fmt-check: ## Verify formatting without writing (what CI enforces)
	@gleam format --check src test
	@echo "formatting clean"

# -------------------------------------------------------------------- lint

.PHONY: lint
lint: ## Run the house lint (borrowed from loom; skips if no checkout)
	@scripts/lint.sh

# -------------------------------------------------------------------- docs

.PHONY: doc-check
doc-check: ## Check the doc graph (AGENTS.md mirror, doc comment coverage)
	@scripts/doc_check.sh

.PHONY: docs
docs: ## Render the /// doc comments to HTML under build/dev/docs
	@gleam docs build

# -------------------------------------------------------------------- misc

.PHONY: deps
deps: ## Download dependencies
	@gleam deps download

.PHONY: clean
clean: ## Remove build artifacts
	@rm -rf build
	@echo "cleaned"

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_%-]+:.*## ' $(MAKEFILE_LIST) | \
		awk -F':.*## ' '{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

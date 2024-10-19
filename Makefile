.PHONY: dep test lint lint-fix typecheck check-pip

dep:
	@pip install -r requirements.txt

dep-dev:
	@pip install -r requirements-dev.txt

test: ## Run tests
	python3 -m unittest discover -s tests

lint:
	@python3 -m black . --check --diff

lint-fix:
	@python3 -m black .

typecheck:
	@python3 -m pyright

check-pip:
ifeq ($(shell pip --version 2>/dev/null),)
	$(error "pip not found. Make sure it is installed before running this.")
endif
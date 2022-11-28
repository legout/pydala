.ONESHELL:
POETRY="${HOME}/.local/bin/poetry"

.PHONY: help
help:             ## Show the help.
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@fgrep "##" Makefile | fgrep -v fgrep


.PHONY: dev-setup
dev-setup:        ## Setting up dev environment.
	@echo "Setting up dev environment ..."
	@if [ "$(shell which ${POETRY})" = "" ]; then \
		$(MAKE) install-poetry; \
  fi
	@$(MAKE) setup-poetry
	@$(MAKE) setup-precommit
	@$(MAKE) install


.PHONY: install-poetry
install-poetry:   ## Install poetry.
	@echo "Installing poetry..."
	@curl -sSL https://install.python-poetry.org | python3 - --preview
	@$(eval include ${HOME}/.poetry/env)

.PHONY: setup-poetry
setup-poetry:     ## Setup poetry.
	@echo "Setting up poetry..."
	@$(POETRY) install

.PHONY: setup-precommit
setup-precommit:  ## Setup precommit.
	@echo "Setting up precommit..."
	@$(POETRY) run pre-commit install

.PHONY: show
show:             ## Show the current environment.
	@echo "Current environment:"
	@$(POETRY) env info && exit

.PHONY: install
install:          ## Install the project in dev mode.
	@echo "Installing the project in dev mode"
	@$(POETRY) install

.PHONY: fmt
fmt:              ## Format code using black & isort.
	@$(POETRY) run isort src/pydala
	@$(POETRY) run black src/pydala
	@$(POETRY) run black tests/

.PHONY: lint 
lint:             ## Run pep8, black, mypy linters.
	@$(POETRY) run flake8 src/pydala
	@$(POETRY) run black --check src/pydala
	@$(POETRY) run black --check tests/
	@$(POETRY) run mypy --ignore-missing-imports src/*

.PHONY: test
test: lint        ## Run tests and generate coverage report.
	@poetry run pytest -v --cov-config .coveragerc --cov=src/pydala -l --tb=short --maxfail=1 tests/
	@poetry run coverage xml
	@poetry run coverage html

.PHONY: watch
watch:            ## Run tests on every change.
	ls **/**.py | entr poetry run pytest -s -vvv -l --tb=long --maxfail=1 tests/

.PHONY: clean
clean:            ## Clean unused files.
	@find ./ -name '*.pyc' -exec rm -f {} \;
	@find ./ -name '__pycache__' -exec rm -rf {} \;
	@find ./ -name 'Thumbs.db' -exec rm -f {} \;
	@find ./ -name '*~' -exec rm -f {} \;
	@rm -rf .cache
	@rm -rf .pytest_cache
	@rm -rf .mypy_cache
	@rm -rf build
	@rm -rf dist
	@rm -rf *.egg-info
	@rm -rf htmlcov
	@rm -rf .tox/
	@rm -rf docs/_build

.PHONY: release
release:          ## Create a new tag for release.
	@echo "WARNING: This operation will create s version tag and push to github"
	@read -p "Version? (provide the next x.y.z semver) : " TAG
	@echo "$${TAG}" > src/pydala/VERSION
	@poetry run gitchangelog > HISTORY.md
	@git add src/pydala/VERSION HISTORY.md
	@git commit -m "release: version $${TAG} 🚀"
	@echo "creating git tag : $${TAG}"
	@git tag $${TAG}
	@git push -u origin HEAD --tags
	@echo "Github Actions will detect the new tag and release the new version."

.PHONY: release
build:            ## Build a new source and wheels archives.
	@echo "Building new package source and wheels archives"
	@$(POETRY) build

.PHONY: docs
docs:             ## Build the documentation.
	@echo "building documentation ..."
	@$(POETRY) run mkdocs build
	URL="site/index.html"; open $$URL

.PHONY: serve-docs
serve-docs:       ## Serving the documentation.
	@echo "serving documentation ..."
	@$(POETRY) run mkdocs serve

PYTHON ?= python
TEST_WORKERS ?= 4
COVERAGE_UNIT := .coverage.unit
COVERAGE_INTEGRATION := .coverage.integration

.PHONY: lint type unit integration coverage quality-gate clean-coverage install-hooks

lint:
	ruff format --check .
	ruff check .
	$(PYTHON) -m scripts.validate_git_contract --event "$${GITHUB_EVENT_NAME:-local}" --branch "$${GITHUB_HEAD_REF:-$${GITHUB_REF_NAME:-$$(git branch --show-current)}}" --base "$${GIT_CONTRACT_BASE:-origin/main}"

type:
	mypy application ingestion api scripts

unit:
	COVERAGE_FILE=$(COVERAGE_UNIT) pytest -n $(TEST_WORKERS) tests/unit -m "not network" --cov=application --cov=ingestion --cov=api --cov=scripts --cov-report=

integration:
	COVERAGE_FILE=$(COVERAGE_INTEGRATION) pytest -n $(TEST_WORKERS) --dist=loadgroup tests/integration -m "integration and not network" --cov=application --cov=ingestion --cov=api --cov=scripts --cov-report=

coverage:
	@test -f $(COVERAGE_UNIT)
	@test -f $(COVERAGE_INTEGRATION)
	COVERAGE_FILE=.coverage coverage combine $(COVERAGE_UNIT) $(COVERAGE_INTEGRATION)
	COVERAGE_FILE=.coverage coverage report --fail-under=90

clean-coverage:
	rm -f .coverage .coverage.* coverage.xml

quality-gate: clean-coverage
	$(MAKE) -j4 lint type unit integration
	$(MAKE) coverage

install-hooks:
	bash scripts/install_hooks.sh

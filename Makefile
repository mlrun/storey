# Copyright 2020 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
.NOTPARALLEL:

.PHONY: all
all:
	$(error please pick a target)

# We only want to format and lint checked in python files
CHECKED_IN_PYTHON_FILES := $(shell git ls-files | grep '\.py$$')

# Fallback
ifeq ($(CHECKED_IN_PYTHON_FILES),)
CHECKED_IN_PYTHON_FILES := .
endif

FLAKE8_OPTIONS := --max-line-length 120 --extend-ignore E203,W503
BLACK_OPTIONS := --line-length 120
ISORT_OPTIONS := --profile black

.PHONY: fmt
fmt:
	@echo "Running black fmt..."
	@python -m black $(BLACK_OPTIONS) $(CHECKED_IN_PYTHON_FILES)
	@echo "Running isort..."
	@python -m isort $(ISORT_OPTIONS) $(CHECKED_IN_PYTHON_FILES)

.PHONY: lint
lint: flake8 fmt-check


.PHONY: fmt-check
fmt-check:
	@echo "Running black check..."
	@python -m black $(BLACK_OPTIONS) --check --diff $(CHECKED_IN_PYTHON_FILES)
	@echo "Running isort check..."
	@python -m isort --check --diff $(ISORT_OPTIONS) $(CHECKED_IN_PYTHON_FILES)

.PHONY: flake8
flake8:
	@echo "Running flake8 lint..."
	@python -m flake8 $(FLAKE8_OPTIONS) $(CHECKED_IN_PYTHON_FILES)

.PHONY: clean
clean:
	find storey tests integration -name '*.pyc' -exec rm {} \;

.PHONY: test
test: clean
	python -m pytest --ignore=integration -rf -v .

.PHONY: test-coverage
test-coverage: clean
	rm -f coverage_reports/unit_tests.coverage
	COVERAGE_FILE=coverage_reports/unit_tests.coverage coverage run --rcfile=tests.coveragerc -m pytest --ignore=integration -rf -v .
	@echo "Unit test coverage report:"
	COVERAGE_FILE=coverage_reports/unit_tests.coverage coverage report --rcfile=tests.coveragerc

.PHONY: bench
bench:
	find bench -name '*.pyc' -exec rm {} \;
	python -m pytest --benchmark-json bench-results.json -rf -v bench/*.py

.PHONY: integration
integration: clean
	python -m pytest -rf -v integration

.PHONY: integration-coverage
integration-coverage: clean
	rm -f coverage_reports/integration.coverage
	COVERAGE_FILE=coverage_reports/integration.coverage coverage run --rcfile=tests.coveragerc -m pytest -rf -v integration
	@echo "Integration test coverage report:"
	COVERAGE_FILE=coverage_reports/integration.coverage coverage report --rcfile=tests.coveragerc

.PHONY: env
env:
	python -m pip install -r requirements.txt

.PHONY: dev-env
dev-env: env
	python -m pip install -r dev-requirements.txt

.PHONY: docs-env
docs-env:
	python -m pip install -r docs/requirements.txt

.PHONY: dist
dist: dev-env
	python -m build --sdist --wheel --outdir dist/ .

.PHONY: set-version
set-version:
	python set-version.py

.PHONY: docs
docs: # Build html docs
	rm -f docs/external/*.md
	cd docs && make html

.PHONY: coverage-combine
coverage-combine:
	rm -f coverage_reports/combined.coverage
	COVERAGE_FILE=coverage_reports/combined.coverage coverage combine --keep coverage_reports/integration.coverage coverage_reports/unit_tests.coverage
	@echo "Full coverage report:"
	COVERAGE_FILE=coverage_reports/combined.coverage coverage report --rcfile=tests.coveragerc -i

.PHONY: coverage
coverage: test-coverage integration-coverage coverage-combine

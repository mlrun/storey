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

.PHONY: all
all:
	$(error please pick a target)

.PHONY: lint
lint:
	pipenv run python -m flake8 storey

.PHONY: test
test:
	find storey -name '*.pyc' -exec rm {} \;
	find tests -name '*.pyc' -exec rm {} \;
	pipenv run python -m pytest --ignore=integration -rf -v .

.PHONY: bench
bench:
	find bench -name '*.pyc' -exec rm {} \;
	pipenv run python -m pytest --benchmark-json bench-results.json -rf -v bench/*.py

.PHONY: integration
integration:
	find integration -name '*.pyc' -exec rm {} \;
	pipenv run python -m pytest -rf -v integration

.PHONY: env
env:
	pipenv install -r requirements.txt

.PHONY: dev-env
dev-env: env
	pipenv install -r dev-requirements.txt

.PHONY: docs-env
docs-env:
	pipenv install -r docs/requirements.txt

.PHONY: dist
dist: dev-env
	pipenv run python -m build --sdist --wheel --outdir dist/ .

.PHONY: set-version
set-version:
	python set-version.py

.PHONY: docs
docs: # Build html docs
	rm -f docs/external/*.md
	cd docs && pipenv run make html

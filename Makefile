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

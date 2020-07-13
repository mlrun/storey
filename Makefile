.PHONY: all
all:
	$(error please pick a target)

.PHONY: lint
lint:
	./venv/bin/python -m flake8 storey

.PHONY: test
test:
	find storey -name '*.pyc' -exec rm {} \;
	find tests -name '*.pyc' -exec rm {} \;
	./venv/bin/python -m pytest --ignore=integration -rf -v .

.PHONY: integration
integration:
	find integration -name '*.pyc' -exec rm {} \;
	./venv/bin/python -m pytest -rf -v integration

.PHONY: env
env:
	python3 -m venv venv
	./venv/bin/python -m pip install -r requirements.txt

.PHONY: dev-env
dev-env: env
	./venv/bin/python -m pip install -r dev-requirements.txt

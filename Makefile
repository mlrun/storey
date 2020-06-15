all:
	$(error please pick a target)

test:
	find storey -name '*.pyc' -exec rm {} \;
	find tests -name '*.pyc' -exec rm {} \;
	./venv/bin/python -m pytest -rf -v tests

env:
	python3 -m venv venv
	./venv/bin/python -m pip install -r requirements.txt

dev-env: env
	./venv/bin/python -m pip install -r dev-requirements.txt

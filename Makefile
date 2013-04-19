noop:
	@true

.PHONY: noop

develop:
	python setup.py develop
	pip install -r test_requirements.txt

pytest:
	py.test --cov kaiso test

flake8:
	flake8 --ignore=E128 kaiso test

test: pytest flake8

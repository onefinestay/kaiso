noop:
	@true

.PHONY: noop

develop:
	python setup.py develop
	pip install -r test_requirements.txt

pytest:
	py.test --cov kaiso test --cov-report term-missing

flake8:
	flake8 --ignore=E128 kaiso test

coverage_check:
	coverage report --fail-under=100

test: pytest flake8 coverage_check

all: clean
	rm -rf build/ dist; ./setup.py sdist

upload: all
	python setup.py sdist upload

clean:
	rm -rf *.egg memsql.egg-info dist build
	find . -type f -name "*.pyc" -exec rm {} \;
	find . -name "__pycache__" -print0 | xargs -0 rmdir
	python setup.py clean --all

test:
	python setup.py test

test-watch:
	python setup.py test -w

.PHONY: flake8
flake8:
	flake8 --config=.flake8 .

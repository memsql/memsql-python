all: clean
	rm -rf build/ dist; ./setup.py sdist bdist bdist_egg

clean:
	rm -rf *.egg memsql.egg-info dist build
	./setup.py clean --all

test:
	./setup.py test

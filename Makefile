all:
	rm -rf build/ dist; ./setup.py clean --all sdist bdist bdist_egg

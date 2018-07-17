all:
	rm -rf build/ dist; ./setup.py sdist

upload: all
	python setup.py sdist
	twine upload dist/*

clean:
	rm -rf *.egg memsql.egg-info dist build
	python setup.py clean --all
	for _kill_path in $$(find . -type f -name "*.pyc"); do rm -f $$_kill_path; done
	for _kill_path in $$(find . -name "__pycache__"); do rm -rf $$_kill_path; done

test:
	python setup.py test

test-watch:
	python setup.py test -w

.PHONY: flake8
flake8:
	flake8 --config=.flake8 .

.PHONY: shell-py2
shell-py2:
	nix-shell -A memsqlPython2Env

.PHONY: shell-py3
shell-py3:
	nix-shell -A memsqlPython3Env

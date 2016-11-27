clean-local:
	@find . -type f -name '*.pyc' -delete
clean:
	clean-local

all-local: python -m compileall .

all:
	all-local

EXTRA_DIST = .gitignore .pylintrc Makefile.am __init__.py

clean-local:
	@find . -type f -name '*.pyc' -delete
clean:
	clean-local

all-local: python -m compileall .

all:
	all-local

EXTRA_DIST = .gitignore .pylintrc Makefile.am generator.py validator.py job_spec.py parser.py __init__.py util/cmd_line_parser.py util/s3lib.py util/mulib.py

lint:
	flake8 kafkian/

check:
	mypy --ignore-missing-imports kafkian/

unittest:
	pytest -v --ff -x tests/unit/

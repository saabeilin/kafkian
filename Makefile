lint:
	flake8 kafkian/

check:
	mypy --ignore-missing-imports kafkian/

unittest:
	PYTHONPATH=. pytest -v --ff -x tests/unit/

kafka:
	docker-compose up -d

systemtest: kafka
	pytest -v --ff tests/system

black:
	black kafkian/ tests/

isort:
	isort kafkian/ tests/

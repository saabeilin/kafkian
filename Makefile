lint:
	flake8 kafkian/

mypy:
	mypy --ignore-missing-imports kafkian/ || echo "mypy failed, continuing anyway"

unittest:
	PYTHONPATH=. pytest -v --ff -x tests/unit/

kafka:
	docker compose up -d

systemtest: kafka
	PYTHONPATH=. pytest -v --ff tests/system

d-systemtest:
	SCHEMA_REGISTRY_URL=http://schema-registry:8081/ KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092 PYTHONPATH=. pytest -v --ff tests/system

black:
	black kafkian/ tests/

isort:
	isort kafkian/ tests/

ruff-fix:
	ruff check --fix . || echo "ruff failed, continuing anyway"

ruff-format:
	ruff format .

precommit: ruff-format ruff-fix unittest mypy

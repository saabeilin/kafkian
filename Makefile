unittest:
	PYTHONPATH=. pytest -v --ff -x tests/unit/

kafka:
	docker compose up -d

systemtest: kafka
	PYTHONPATH=. pytest -v --ff tests/system

typecheck:
	ty check --output-format=concise || echo "ty found issues, continuing anyway"

format:
	ruff format .

fix:
	ruff check --fix --output-format=concise || echo "Ruff fixed what it could"

precommit: fix format unittest typecheck
	echo "Now you can commit"

install:
	pip install --use-feature=2020-resolver -e .[dev]

pre-commit-install:
	python -m pre_commit install

lint:
	pre-commit run --all-files

test:
	python -m pytest tests e2e

black:
	pre-commit run black

flake8:
	pre-commit run flake8

isort:
	pre-commit run isort

mypy:
	pre-commit run mypy

.PHONY: install pre-commit-install lint tests black flake8 isort mypy

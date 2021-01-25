install:
	pip install --use-feature=2020-resolver -e .[dev]

pre-commit-install:
	python -m pre_commit install

lint:
	pre-commit run --all-files

test:
	python -m pytest tests e2e -vv

e2e:
	python -m pytest e2e

unit_test:
	python -m pytest tests

black:
	pre-commit run black

flake8:
	pre-commit run flake8

isort:
	pre-commit run isort

mypy:
	pre-commit run mypy

clean:
	git clean -f -d -x

build-tel:
	docker run \
		-v $(PWD):/workdir \
		--workdir /workdir \
		--rm ryohji/antlr4:latest \
		antlr4 -visitor -Dlanguage=Python3 -o src/panoramic/cli/tel_grammar -Xexact-output-dir grammar/Tel.g4

docs:
	python -m docs.generate_tel_docs

.PHONY: install pre-commit-install lint tests black flake8 isort mypy e2e docs

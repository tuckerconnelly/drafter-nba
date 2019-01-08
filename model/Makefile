install:
	pipenv install

fit-debug:
	DEBUG=1 pipenv run python3 drafter/model.py

fit:
	pipenv run python3 drafter/model.py

fit-final:
	FINAL=1 pipenv run python3 drafter/model.py

test:
	pytest drafter

test-watch:
	pipenv run pytest-watch drafter

clean:
	find . -type f -name "*.py[co]" -delete -or -type d -name "__pycache__" -delete

.PHONY: init test test-watch clean
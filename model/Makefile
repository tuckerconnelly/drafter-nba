install:
	pipenv install

test:
	pytest drafter

test-watch:
	pipenv run pytest-watch drafter

clean:
	find . -type f -name "*.py[co]" -delete -or -type d -name "__pycache__" -delete

clear-cache:
	rm -rf tmp


# Scraping

scrape-lineups:
	pipenv run python3 drafter/scraping.py lineups


scrape-teams:
	pipenv run python3 drafter/scraping.py teams


scrape-games-debug:
	DEBUG=1 pipenv run python3 drafter/scraping.py games


scrape-games:
	pipenv run python3 drafter/scraping.py games


# Data

cache-data-debug:
	DEBUG=1 pipenv run python3 drafter/data.py cache-data

cache-data:
	pipenv run python3 drafter/data.py cache-data


# Fitting

fit-debug:
	DEBUG=1 pipenv run python3 drafter/model.py

fit:
	pipenv run python3 drafter/model.py

fit-final:
	FINAL=1 pipenv run python3 drafter/model.py


fit-debug-xg:
	DEBUG=1 pipenv run python3 drafter/model_xgboost.py

fit-xg:
	pipenv run python3 drafter/model_xgboost.py

fit-final-xg:
	FINAL=1 pipenv run python3 drafter/model_xgboost.py


# Drafting


draft:
	pipenv run python3 drafter/drafter.py

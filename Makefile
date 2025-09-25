.PHONY: venv install run12 run3 run4 clean freeze

# venv name
VENV = .venv
PYTHON = $(VENV)/bin/python
PIP = $(VENV)/bin/pip

venv:
	python3 -m venv $(VENV)

install: venv
	$(PIP) install -U pip wheel
	$(PIP) install -r requirements.txt

run_task_1_2:
	$(PYTHON) tasks_1-2.py

run_task_3:
	$(PYTHON) task_3.py

run_task_4:
	$(PYTHON) task_4.py

clean:
	rm -rf $(VENV) *.egg-info __pycache__ .pytest_cache

# save packages names
freeze:
	$(PIP) freeze > requirements.txt

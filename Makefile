.PHONY: up down test lint health fix

up:
	docker compose -f docker/docker-compose.yml up -d

down:
	docker compose -f docker/docker-compose.yml down

test:
	pytest tests/ -v

lint:
	flake8 . --max-line-length=100 --exclude=.git,__pycache__,target,dbt

health:
	python project_health_check.py

fix:
	python auto_fix.py

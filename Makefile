# Makefile for Apache IoTDB Write-Cost Performance Study
# Supports Linux/macOS/WSL/MSYS2 GNU Make. Python binary auto-detects .venv if present.

ifneq ("$(wildcard .venv/Scripts/python.exe)","")
    PYTHON := .venv/Scripts/python.exe
else ifneq ("$(wildcard .venv/bin/python)","")
    PYTHON := .venv/bin/python
else
    PYTHON := python
endif

.PHONY: help up down smoke test demo pilot experiment analyze report clean-data

help:
	@echo "Available targets:"
	@echo "  make up          - Start IoTDB, Prometheus, Grafana, and cAdvisor via Docker Compose"
	@echo "  make down        - Stop and remove infrastructure containers"
	@echo "  make smoke       - Run end-to-end infrastructure & monitoring smoke test"
	@echo "  make test        - Run unit tests for workload generator and metrics schema"
	@echo "  make demo        - Run live side-by-side demo (0% OOO vs 50% OOO)"
	@echo "  make pilot       - Run small pilot benchmark to estimate runtime and variance"
	@echo "  make experiment  - Run full randomized experimental matrix (resumable)"
	@echo "  make analyze     - Execute Jupyter notebook analysis and regenerate all figures/tables"
	@echo "  make report      - Export report/report.md to report/report.docx"

up:
	docker compose up -d iotdb-service cadvisor prometheus grafana

down:
	docker compose down

smoke:
	"$(PYTHON)" -m scripts.smoke_test

test:
	"$(PYTHON)" -m pytest tests/ -v

demo:
	"$(PYTHON)" -m scripts.demo_progress

pilot:
	"$(PYTHON)" -m scripts.run_matrix --mode pilot

experiment:
	"$(PYTHON)" -m scripts.run_matrix --mode full

analyze:
	"$(PYTHON)" -m jupyter nbconvert --to notebook --execute --inplace notebooks/analysis.ipynb

report:
	"$(PYTHON)" -m scripts.build_report_docx

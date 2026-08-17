ifeq ($(OS),Windows_NT)
    VENV        := .venv
    PY          := $(VENV)/Scripts/python.exe
    PIP         := $(VENV)/Scripts/pip.exe
    DBT         := $(CURDIR)/$(VENV)/Scripts/dbt.exe
    PYTHON      := python
    CREATE_VENV := $(PYTHON) -c "import os, sys, subprocess; os.path.exists('$(VENV)') or subprocess.run([sys.executable, '-m', 'venv', '$(VENV)'])"
    RESET_CMD   := $(PYTHON) -c "import os; [os.remove(f) for f in ['warehouse.duckdb', 'warehouse.duckdb.wal'] if os.path.exists(f)]"
    CLEAN_CMD   := $(PYTHON) -c "import os, shutil; [os.remove(f) if os.path.isfile(f) else shutil.rmtree(f, ignore_errors=True) for f in ['warehouse.duckdb', 'warehouse.duckdb.wal', 'dbt/target', 'dbt/logs', 'data/crash'] if os.path.exists(f)]"
    HELP_CMD    := $(PYTHON) -c "import sys, re; sys.stdout.reconfigure(encoding='utf-8'); [print(f'    \033[36m{m.group(1):<14}\033[0m {m.group(2)}') for line in open('Makefile', encoding='utf-8') for m in [re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)] if m]"
else
    SHELL       := /bin/bash
    VENV        := .venv
    PY          := $(VENV)/bin/python
    PIP         := $(VENV)/bin/pip
    DBT         := $(VENV)/bin/dbt
    PYTHON      := python3
    CREATE_VENV := test -d $(VENV) || python3 -m venv $(VENV)
    RESET_CMD   := rm -f warehouse.duckdb warehouse.duckdb.wal
    CLEAN_CMD   := rm -rf warehouse.duckdb warehouse.duckdb.wal dbt/target dbt/logs data/crash
    HELP_CMD    := grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "    \033[36m%-14s\033[0m %s\n", $$1, $$2}'
endif

export LAB17_DB := $(CURDIR)/warehouse.duckdb
export DBT_PROFILES_DIR := $(CURDIR)/dbt
export PYTHONUTF8 := 1

.DEFAULT_GOAL := help
.PHONY: help setup seed seed-extra pipeline verify quick explain plan dbt-test \
        dbt-docs crash-test compact reset clean

help:  ## danh sách lệnh
	@echo ""
	@echo "  LAB 17 — Data Pipeline Engineering"
	@echo ""
	@$(HELP_CMD)
	@echo ""

setup:  ## venv + thư viện + sinh dữ liệu (chạy một lần)
	@$(CREATE_VENV)
	@$(PY) -m pip install -q --upgrade pip
	@$(PY) -m pip install -q -r requirements.txt
	@$(PY) seed/generate.py
	@echo ""
	@echo "  xong. Bước tiếp theo:  make pipeline  rồi  make verify"

seed:  ## sinh lại dữ liệu seed
	@$(PY) seed/generate.py

seed-extra:  ## sinh thêm dữ liệu cho bài mở rộng trong EXTRA.md (~30 giây)
	@$(PY) seed/generate.py --extra
	@$(PY) tools/explain.py --save-baseline

pipeline:  ## chạy đường ống một lượt (14 ngày vận hành)
	@$(PY) tools/run_pipeline.py

verify:  ## ⭐ xoá kho, chạy 3 lượt, in bảng chấm — dùng lệnh này liên tục
	@$(PY) tools/verify.py

quick:  ## như verify nhưng chỉ 1 lượt (nhanh, không kiểm tra tính ổn định)
	@$(PY) tools/verify.py --runs 1

explain:  ## [mở rộng] đo rows scanned của queries/dashboard.sql
	@$(PY) tools/explain.py

plan:  ## [mở rộng] explain + in cây EXPLAIN ANALYZE
	@$(PY) tools/explain.py --plan

compact:  ## [mở rộng] chạy tools/compact.py
	@$(PY) tools/compact.py

dbt-test:  ## chạy dbt test
	@cd dbt && $(DBT) test --profiles-dir . --target-path target --log-path logs

dbt-docs:  ## dựng và mở tài liệu dbt (tuỳ chọn)
	@cd dbt && $(DBT) docs generate --profiles-dir . --target-path target --log-path logs \
	  && $(DBT) docs serve --profiles-dir . --target-path target

crash-test:  ## [mở rộng] kịch bản consumer bị giết giữa batch
	@$(PY) tools/crash_test.py

reset:  ## xoá kho DuckDB (giữ nguyên seed và data/)
	@$(RESET_CMD)
	@echo "  kho đã xoá."

clean:  ## xoá kho + target dbt + thư mục làm việc của crash-test
	@$(CLEAN_CMD)
	@echo "  đã dọn."


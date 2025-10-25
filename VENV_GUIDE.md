# Virtual Environment Guide

This project uses **two separate virtual environments**:

## 1. Baseline Environment (`baseline/venv`)
- **Location**: `baseline/venv/`
- **Purpose**: Run the DuckDB baseline solution
- **Dependencies**: Minimal (just DuckDB and pandas)

## 2. Main Project Environment (`venv`)
- **Location**: `venv/` (project root)
- **Purpose**: Your optimized solution development
- **Dependencies**: Full toolkit (DuckDB, pandas, polars, pyarrow, performance monitoring, etc.)

---

## Quick Setup (Automated)

Run the setup script from the project root:

```bash
./setup_venvs.sh
```

This will create and configure both virtual environments automatically.

---

## Manual Setup

If you prefer to set up manually:

### Baseline Environment
```bash
cd baseline
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
deactivate
cd ..
```

### Main Project Environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
deactivate
```

---

## Usage

### Running the Baseline
```bash
cd baseline
source venv/bin/activate
python main.py
deactivate
```

### Working on Your Optimized Solution
```bash
# From project root
source venv/bin/activate
python src/baseline_runner.py --data-dir data/data
# ... do your development work
deactivate
```

### Quick Tips
- Always activate the appropriate venv before running code
- Use `deactivate` to exit a virtual environment
- Your shell prompt will show `(venv)` when a venv is active
- The two venvs are independent - changes in one don't affect the other

---

## Why Separate Virtual Environments?

1. **Isolation**: Baseline stays clean and lightweight
2. **Dependency Management**: Avoid conflicts between baseline and your solution
3. **Fair Comparison**: Baseline runs in its original environment
4. **Flexibility**: You can experiment with different libraries without affecting the baseline

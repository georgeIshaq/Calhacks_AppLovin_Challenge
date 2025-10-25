# Data Setup Guide

## Quick Start: Moving Data from Google Drive

### Step 1: Download Files from Google Drive

Download these files from the Google Drive folder to your **Downloads** folder (or anywhere convenient):
- `data.zip` (~20 GB compressed, the full dataset)
- `data-lite.zip` (~1 GB compressed, for prototyping)
- `baseline.zip` (baseline DuckDB code)
- Any `results/*.csv` files (expected query outputs)

### Step 2: Move Files to Project Directory

Open Terminal and navigate to this project:

```bash
cd /Users/george/Documents/Code_Projects/Calhacks_AppLovin_Challenge
```

Move the downloaded files here:

```bash
# Move from Downloads (adjust path if you saved elsewhere)
mv ~/Downloads/data.zip .
mv ~/Downloads/data-lite.zip .
mv ~/Downloads/baseline.zip .

# If there are result CSV files, move them too
mv ~/Downloads/*.csv results/ 2>/dev/null || true
```

### Step 3: Extract the Files

```bash
# Extract baseline code
unzip baseline.zip

# For prototyping (1 GB data):
unzip data-lite.zip -d data-lite/

# For final testing (20 GB data - only when needed):
unzip data.zip -d data/
```

**Note**: Start with `data-lite.zip` to save space and time during development. Only extract the full `data.zip` when you're ready for final testing.

### Step 4: Verify Data Structure

Check that your data is in place:

```bash
# Check data-lite
ls -lh data-lite/*.csv

# Or check full data (if extracted)
ls -lh data/*.csv
```

You should see CSV file(s) containing the event data.

### Step 5: Check Baseline Code

After extracting `baseline.zip`, check what's inside:

```bash
# List baseline files
ls -la

# Look for Python files or directories created by baseline.zip
find . -name "*.py" -type f
```

---

## What NOT to Commit to Git

The `.gitignore` file is already configured to exclude:
- `*.zip` files (data archives)
- `data/` and `data-lite/` directories
- Virtual environments
- Database files

This means your large data files won't accidentally get committed to Git.

---

## Next Steps After Data is in Place

1. **Set up Python environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Run the baseline** to see how DuckDB performs:
   ```bash
   python src/baseline_runner.py --data-dir data-lite
   ```

3. **Explore the data** to understand what you're working with

4. **Start building your optimized solution**!

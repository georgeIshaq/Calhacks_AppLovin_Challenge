#!/bin/bash
# Setup script for CalHacks AppLovin Challenge
# Creates separate virtual environments for baseline and optimized solution

set -e  # Exit on error

echo "=========================================="
echo "Setting up virtual environments"
echo "=========================================="
echo ""

# Get the project root directory
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

# Setup baseline venv
echo "📦 Setting up baseline virtual environment..."
cd baseline
if [ -d "venv" ]; then
    echo "  ⚠️  baseline/venv already exists, skipping creation"
else
    python3 -m venv venv
    echo "  ✅ Created baseline/venv"
fi

echo "  📥 Installing baseline dependencies..."
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
deactivate
echo "  ✅ Baseline dependencies installed"
echo ""

# Setup main project venv
cd "$PROJECT_ROOT"
echo "📦 Setting up main project virtual environment..."
if [ -d "venv" ]; then
    echo "  ⚠️  venv already exists, skipping creation"
else
    python3 -m venv venv
    echo "  ✅ Created venv"
fi

echo "  📥 Installing main project dependencies..."
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
deactivate
echo "  ✅ Main project dependencies installed"
echo ""

echo "=========================================="
echo "✅ Setup complete!"
echo "=========================================="
echo ""
echo "To activate virtual environments:"
echo ""
echo "  For baseline:"
echo "    cd baseline && source venv/bin/activate"
echo ""
echo "  For main project:"
echo "    source venv/bin/activate"
echo ""
echo "To deactivate:"
echo "    deactivate"
echo ""

# yahooDataManager
Use yfinance and sqlite to first download available historical data, then automatically update daily
## Available Data Types:
* Equity EOD data
* Equity Minute data
* Equity Analyst Upgrades/Downgrades data
* Equity Options EOD data

## Usage:
1) Clone the repo and run pip install requirements.txt to ensure all packages are installed
2) Specify your database path and ticker source path in config.json file
3) Run setup.py file to setup databases
4) Manually run update.py file to update daily/ set up task scheduler with .bat file to run update.py automatically

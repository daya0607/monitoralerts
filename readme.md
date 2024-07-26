# Fraud Monitoring using yaml and python

This python script runs fraud monitoring checks 

## Prerequisites

- Python version 3
- pip (python package installer)

## Installation

Install the required packages using pip :

pip3 install pyyaml
pip3 install datetime
pip3 install argparse
pip3 install db-sqlite3


## Usage

Run the script from the command line with a date parameter:

Example : python mointor.py --input_run_date=2023-01-04

Command to run the test case:

Example : python -m unittest test_monitor.py

## Date Format

The input date must be in the format YYYY-MM-DD. If an incorrect format is entered, the script will return this error:
Enter input_run_date only in this format(YYYY-MM-DD)

## Monitors - 

1. **JIRA** (Daily) 
    - runs every day 
    - includes all transactions above 300$ in the previous day
2. **Email** (Monthly) 
    - runs every month 
    - includes a list of all accounts that deviated from their previous spending pattern
3. **Slack** 
    - runs every day 
    - list of all distinct customers that reach 500$ cummulative spending
    - sends list of customers that reached their limit the day before parameter date
    - does not return their names again

## Configuration

The monitors are implemented in the 'monitors.yaml' file. Modify this file to adjust the SQL queries, alert channels, recepients


## Troubleshooting

- ensure all the required packages are installed
- verify the 'monitors.yaml', 'sample.db' exists and are in the same directory

                    

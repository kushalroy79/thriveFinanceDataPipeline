# Project Structure

The Thrive Cash FIFO Matching project has been organized into a clean directory structure.

## Directory Layout

```
thrive-cash-fifo-matching/
├── src/                                  # Source code
│   ├── __init__.py                       # Package initialization
│   ├── thrive_cash_processing_dag.py     # Main Airflow DAG (834 lines)
│   └── sample_queries.sql                # SQL queries for analytics (12 queries)
│
├── test/                                 # Test suite
│   ├── __init__.py                       # Test package initialization
│   ├── test_fifo_matching.py             # Unit tests (10 test cases)
│   ├── test_examples.py                  # Visual test examples (6 examples)
│   └── run_tests.py                      # Test runner script
│
├── doc/                                  # Documentation
│   ├── 00_START_HERE.md                  # Project navigation guide
│   ├── QUICKSTART.md                     # 5-minute quick start guide
│   ├── TESTING.md                        # Comprehensive testing guide
│   ├── FIFO_MATCHING_FLOW.md            # Flow diagrams and algorithm
│   └── PROJECT_STRUCTURE.md              # This file
│
├── __pycache__/                          # Python cache (gitignored)
│
├── README.md                             # ⭐ Main technical documentation
└── .gitignore                            # Git ignore rules
```

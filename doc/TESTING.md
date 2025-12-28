# Testing Guide for Thrive Cash FIFO Matching

## Overview

This document describes the testing approach for the Thrive Cash FIFO matching logic.

## Test Files

### 1. `test_fifo_matching.py`
Main test suite with 10 comprehensive test cases.

**Test Cases:**
1. Simple one-to-one matching
2. 1:1 matching (no partial redemption)
3. FIFO order with multiple earned
4. One earned, multiple spent
5. Multiple customers
6. Expired transactions
7. No earned transactions
8. Chronological order validation
9. Balance validation
10. Each TRANS_ID used only once

**Run:**
```bash
python test_fifo_matching.py
```

**Expected Output:**
```
============================================================
Running Thrive Cash FIFO Matching Test Suite
============================================================

✓ Test 1 passed: Simple one-to-one matching
✓ Test 2 passed: 1:1 matching (no partial redemption)
✓ Test 3 passed: FIFO order with multiple earned
...
============================================================
Test Results: 10 passed, 0 failed out of 10 tests
============================================================
```

### 2. `test_examples.py`
Visual examples showing input and output for each scenario.

**Examples:**
1. Simple one-to-one matching
2. 1:1 matching (no partial redemption)
3. FIFO order - oldest earned first
4. One earned, multiple spent
5. Multiple customers (independent matching)
6. Expired transactions

**Run:**
```bash
python test_examples.py
```

**Expected Output:**
Shows detailed tables with input transactions and output with REDEEMID assignments.

### 3. `run_tests.py`
Simple test runner script.

**Run:**
```bash
python test/run_tests.py
```

**Exit Codes:**
- 0: All tests passed
- 1: One or more tests failed

## Test Scenarios Explained

### Scenario 1: Simple Matching
**Input:**
- Earned: $100 on 2024-01-01
- Spent: $100 on 2024-01-05

**Expected Output:**
- Earned REDEEMID → Spent TRANS_ID (redeemed)
- Spent REDEEMID → NULL (always blank)

### Scenario 2: 1:1 Matching (No Partial Redemption)
**Input:**
- Earned: $100 on 2024-01-01
- Spent: $60 on 2024-01-05

**Expected Output:**
- Earned REDEEMID → Spent TRANS_ID (redeemed)
- Spent REDEEMID → NULL (always blank)
- Remaining $40 is "untracked" (not split into separate row)
- Each TRANS_ID used only once

### Scenario 3: FIFO Order
**Input:**
- Earned: $50 (Jan 1), $30 (Jan 2), $20 (Jan 3)
- Spent: $70 (Jan 10)

**Expected Output:**
- Spent matches oldest earned (E001):
  - E001 ($50) REDEEMID → S001
  - E002 ($30) REDEEMID → NULL (not matched)
  - E003 ($20) REDEEMID → NULL (not matched)
  - S001 REDEEMID → NULL (always blank)

### Scenario 4: Multiple Spent
**Input:**
- Earned: $100 (E001), $50 (E002), $30 (E003)
- Spent: $30 (S001), $40 (S002), $20 (S003)

**Expected Output:**
- E001 REDEEMID → S001 (oldest earned to oldest spent)
- E002 REDEEMID → S002 (second earned to second spent)
- E003 REDEEMID → S003 (third earned to third spent)
- All spent REDEEMID → NULL (always blank)
- Each transaction used only once (1:1 matching)

### Scenario 5: Multiple Customers
**Input:**
- C001: Earned $100, Spent $50
- C002: Earned $50, Spent $30

**Expected Output:**
- C001 and C002 matched independently
- No cross-customer matching

### Scenario 6: Expired Transactions
**Input:**
- Earned: $100
- Expired: $50

**Expected Output:**
- Expired treated same as spent
- Earned REDEEMID → Expired TRANS_ID
- Expired REDEEMID → NULL (always blank)
- Each transaction used only once (1:1 matching)

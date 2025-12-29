"""
Test cases for Thrive Cash FIFO Matching Logic

Tests cover various scenarios including:
- Simple one-to-one matching
- 1:1 matching (no partial redemptions or transaction splitting)
- FIFO order (oldest unmatched earned first by CREATEDAT)
- Each TRANS_ID used only once
- Multiple customers
- Chronological order validation
- Edge cases (zero balance, no earned, etc.)

Key Rules:
- 1:1 matching: Each TRANS_ID can only be used once
- No partial redemptions or transaction splitting
- If no unmatched earned exists, REDEEMID is blank/NULL
- Oldest unmatched earned matched first (by CREATEDAT)
"""

import pandas as pd
import pytest
from datetime import datetime
import sys
import os

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.fifo_matching import perform_fifo_matching_logic


# Test Case 1: Simple One-to-One Matching
def test_simple_one_to_one_matching():
    """Test basic FIFO matching: one earned, one spent"""
    earned = pd.DataFrame({
        'transaction_id': ['E001'],
        'customer_id': ['C001'],
        'amount': [100.0],
        'timestamp': pd.to_datetime(['2024-01-01']),
        'transaction_type': ['earned']
    })
    
    spent = pd.DataFrame({
        'transaction_id': ['S001'],
        'customer_id': ['C001'],
        'amount': [-100.0],
        'timestamp': pd.to_datetime(['2024-01-05']),
        'transaction_type': ['spent']
    })
    
    expired = pd.DataFrame(columns=['transaction_id', 'customer_id', 'amount', 'timestamp', 'transaction_type'])
    
    result = perform_fifo_matching_logic(earned, spent, expired)
    
    # Assertions
    assert len(result) == 2, "Should have 2 rows (1 earned, 1 spent)"
    
    spent_row = result[result['TCTYPE'] == 'spent'].iloc[0]
    assert spent_row['REDEEMID'] == 'E001', "Spent should point to earned E001"
    
    earned_row = result[result['TCTYPE'] == 'earned'].iloc[0]
    assert earned_row['REDEEMID'] == 'S001', "Earned should point to spent S001"
    
    print("✓ Test 1 passed: Simple one-to-one matching")


# Test Case 2: 1:1 Matching (No Partial Redemption)
def test_one_to_one_matching():
    """Test 1:1 matching: earned $100, spent $60 - no splitting, earned remains unmatched"""
    earned = pd.DataFrame({
        'transaction_id': ['E001'],
        'customer_id': ['C001'],
        'amount': [100.0],
        'timestamp': pd.to_datetime(['2024-01-01']),
        'transaction_type': ['earned']
    })
    
    spent = pd.DataFrame({
        'transaction_id': ['S001'],
        'customer_id': ['C001'],
        'amount': [-60.0],
        'timestamp': pd.to_datetime(['2024-01-05']),
        'transaction_type': ['spent']
    })
    
    expired = pd.DataFrame(columns=['transaction_id', 'customer_id', 'amount', 'timestamp', 'transaction_type'])
    
    result = perform_fifo_matching_logic(earned, spent, expired)
    
    # Assertions - 1:1 matching means no splitting
    assert len(result) == 2, "Should have 2 rows (1 earned, 1 spent)"
    
    spent_row = result[result['TCTYPE'] == 'spent'].iloc[0]
    assert spent_row['REDEEMID'] == 'E001', "Spent should point to earned E001"
    
    earned_row = result[result['TCTYPE'] == 'earned'].iloc[0]
    assert earned_row['REDEEMID'] == 'S001', "Earned should point to spent S001"
    assert earned_row['AMOUNT'] == 100.0, "Earned amount should remain $100 (no splitting)"
    
    print("✓ Test 2 passed: 1:1 matching (no partial redemption)")


# Test Case 3: Multiple Earned, One Spent (FIFO Order - 1:1 Matching)
def test_fifo_order_multiple_earned():
    """Test FIFO: oldest earned is matched first (1:1, no splitting)"""
    earned = pd.DataFrame({
        'transaction_id': ['E001', 'E002', 'E003'],
        'customer_id': ['C001', 'C001', 'C001'],
        'amount': [50.0, 30.0, 20.0],
        'timestamp': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']),
        'transaction_type': ['earned', 'earned', 'earned']
    })
    
    spent = pd.DataFrame({
        'transaction_id': ['S001'],
        'customer_id': ['C001'],
        'amount': [-70.0],
        'timestamp': pd.to_datetime(['2024-01-10']),
        'transaction_type': ['spent']
    })
    
    expired = pd.DataFrame(columns=['transaction_id', 'customer_id', 'amount', 'timestamp', 'transaction_type'])
    
    result = perform_fifo_matching_logic(earned, spent, expired)
    
    # Assertions - 1:1 matching
    assert len(result) == 4, "Should have 4 rows (3 earned, 1 spent)"
    
    spent_row = result[result['TCTYPE'] == 'spent'].iloc[0]
    assert spent_row['REDEEMID'] == 'E001', "Spent should match oldest earned E001 (FIFO)"
    
    # E001 should be matched to S001
    e001_row = result[result['TRANS_ID'] == 'E001'].iloc[0]
    assert e001_row['REDEEMID'] == 'S001', "E001 matched to S001"
    assert e001_row['AMOUNT'] == 50.0, "E001 amount unchanged (no splitting)"
    
    # E002 and E003 should be unmatched
    e002_row = result[result['TRANS_ID'] == 'E002'].iloc[0]
    assert pd.isna(e002_row['REDEEMID']), "E002 should be unmatched"
    
    e003_row = result[result['TRANS_ID'] == 'E003'].iloc[0]
    assert pd.isna(e003_row['REDEEMID']), "E003 should be unmatched"
    
    print("✓ Test 3 passed: FIFO order with multiple earned (1:1 matching)")


# Test Case 4: One Earned, Multiple Spent (1:1 Matching)
def test_one_earned_multiple_spent():
    """Test one earned with multiple spent - only first spent matches (1:1)"""
    earned = pd.DataFrame({
        'transaction_id': ['E001'],
        'customer_id': ['C001'],
        'amount': [100.0],
        'timestamp': pd.to_datetime(['2024-01-01']),
        'transaction_type': ['earned']
    })
    
    spent = pd.DataFrame({
        'transaction_id': ['S001', 'S002', 'S003'],
        'customer_id': ['C001', 'C001', 'C001'],
        'amount': [-30.0, -40.0, -20.0],
        'timestamp': pd.to_datetime(['2024-01-05', '2024-01-06', '2024-01-07']),
        'transaction_type': ['spent', 'spent', 'spent']
    })
    
    expired = pd.DataFrame(columns=['transaction_id', 'customer_id', 'amount', 'timestamp', 'transaction_type'])
    
    result = perform_fifo_matching_logic(earned, spent, expired)
    
    # Assertions - 1:1 matching means only first spent matches
    assert len(result) == 4, "Should have 4 rows (1 earned, 3 spent)"
    
    # Only S001 should match E001 (first spent in chronological order)
    s001_row = result[result['TRANS_ID'] == 'S001'].iloc[0]
    assert s001_row['REDEEMID'] == 'E001', "S001 should match E001"
    
    # S002 and S003 should have NULL REDEEMID (no more earned available)
    s002_row = result[result['TRANS_ID'] == 'S002'].iloc[0]
    assert pd.isna(s002_row['REDEEMID']), "S002 should have NULL REDEEMID"
    
    s003_row = result[result['TRANS_ID'] == 'S003'].iloc[0]
    assert pd.isna(s003_row['REDEEMID']), "S003 should have NULL REDEEMID"
    
    # E001 should point to S001
    e001_row = result[result['TRANS_ID'] == 'E001'].iloc[0]
    assert e001_row['REDEEMID'] == 'S001', "E001 should point to S001"
    assert e001_row['AMOUNT'] == 100.0, "E001 amount unchanged (no splitting)"
    
    print("✓ Test 4 passed: One earned, multiple spent (1:1 matching)")


# Test Case 5: Multiple Customers
def test_multiple_customers():
    """Test that matching is done per customer independently"""
    earned = pd.DataFrame({
        'transaction_id': ['E001', 'E002'],
        'customer_id': ['C001', 'C002'],
        'amount': [100.0, 50.0],
        'timestamp': pd.to_datetime(['2024-01-01', '2024-01-01']),
        'transaction_type': ['earned', 'earned']
    })
    
    spent = pd.DataFrame({
        'transaction_id': ['S001', 'S002'],
        'customer_id': ['C001', 'C002'],
        'amount': [-50.0, -30.0],
        'timestamp': pd.to_datetime(['2024-01-05', '2024-01-05']),
        'transaction_type': ['spent', 'spent']
    })
    
    expired = pd.DataFrame(columns=['transaction_id', 'customer_id', 'amount', 'timestamp', 'transaction_type'])
    
    result = perform_fifo_matching_logic(earned, spent, expired)
    
    # Assertions for C001
    c001_spent = result[(result['CUSTOMERID'] == 'C001') & (result['TRANS_ID'] == 'S001')].iloc[0]
    assert c001_spent['REDEEMID'] == 'E001', "C001 spent should match C001 earned"
    
    # Assertions for C002
    c002_spent = result[(result['CUSTOMERID'] == 'C002') & (result['TRANS_ID'] == 'S002')].iloc[0]
    assert c002_spent['REDEEMID'] == 'E002', "C002 spent should match C002 earned"
    
    # C001 and C002 should not cross-match
    assert c001_spent['REDEEMID'] != 'E002', "Should not match across customers"
    assert c002_spent['REDEEMID'] != 'E001', "Should not match across customers"
    
    print("✓ Test 5 passed: Multiple customers")


# Test Case 6: Expired Transactions
def test_expired_transactions():
    """Test that expired transactions are matched like spent"""
    earned = pd.DataFrame({
        'transaction_id': ['E001'],
        'customer_id': ['C001'],
        'amount': [100.0],
        'timestamp': pd.to_datetime(['2024-01-01']),
        'transaction_type': ['earned']
    })
    
    spent = pd.DataFrame(columns=['transaction_id', 'customer_id', 'amount', 'timestamp', 'transaction_type'])
    
    expired = pd.DataFrame({
        'transaction_id': ['X001'],
        'customer_id': ['C001'],
        'amount': [-50.0],
        'timestamp': pd.to_datetime(['2024-06-01']),
        'transaction_type': ['expired']
    })
    
    result = perform_fifo_matching_logic(earned, spent, expired)
    
    # Assertions
    expired_row = result[result['TCTYPE'] == 'expired'].iloc[0]
    assert expired_row['REDEEMID'] == 'E001', "Expired should point to earned E001"
    
    earned_rows = result[result['TCTYPE'] == 'earned']
    redeemed = earned_rows[earned_rows['REDEEMID'].notna()].iloc[0]
    assert redeemed['REDEEMID'] == 'X001', "Earned should point to expired X001"
    assert redeemed['AMOUNT'] == 50.0, "Redeemed amount should be $50"
    
    print("✓ Test 6 passed: Expired transactions")


# Test Case 7: No Earned Transactions
def test_no_earned_transactions():
    """Test spent with no earned available"""
    earned = pd.DataFrame(columns=['transaction_id', 'customer_id', 'amount', 'timestamp', 'transaction_type'])
    
    spent = pd.DataFrame({
        'transaction_id': ['S001'],
        'customer_id': ['C001'],
        'amount': [-50.0],
        'timestamp': pd.to_datetime(['2024-01-05']),
        'transaction_type': ['spent']
    })
    
    expired = pd.DataFrame(columns=['transaction_id', 'customer_id', 'amount', 'timestamp', 'transaction_type'])
    
    result = perform_fifo_matching_logic(earned, spent, expired)
    
    # Assertions
    spent_row = result[result['TCTYPE'] == 'spent'].iloc[0]
    assert pd.isna(spent_row['REDEEMID']), "Spent should have NULL REDEEMID when no earned available"
    
    print("✓ Test 7 passed: No earned transactions")


# Test Case 8: Chronological Order Validation
def test_chronological_order():
    """Test that earned date is before spent date"""
    earned = pd.DataFrame({
        'transaction_id': ['E001'],
        'customer_id': ['C001'],
        'amount': [100.0],
        'timestamp': pd.to_datetime(['2024-01-10']),
        'transaction_type': ['earned']
    })
    
    spent = pd.DataFrame({
        'transaction_id': ['S001'],
        'customer_id': ['C001'],
        'amount': [-50.0],
        'timestamp': pd.to_datetime(['2024-01-05']),  # Before earned!
        'transaction_type': ['spent']
    })
    
    expired = pd.DataFrame(columns=['transaction_id', 'customer_id', 'amount', 'timestamp', 'transaction_type'])
    
    result = perform_fifo_matching_logic(earned, spent, expired)
    
    # In this case, spent happens before earned, so it shouldn't match
    spent_row = result[result['TCTYPE'] == 'spent'].iloc[0]
    assert pd.isna(spent_row['REDEEMID']), "Spent before earned should not match"
    
    print("✓ Test 8 passed: Chronological order validation")


# Test Case 9: Each TRANS_ID Used Only Once
def test_each_trans_id_used_once():
    """Test that each TRANS_ID can only be used once as REDEEMID"""
    earned = pd.DataFrame({
        'transaction_id': ['E001', 'E002'],
        'customer_id': ['C001', 'C001'],
        'amount': [100.0, 50.0],
        'timestamp': pd.to_datetime(['2024-01-01', '2024-01-02']),
        'transaction_type': ['earned', 'earned']
    })
    
    spent = pd.DataFrame({
        'transaction_id': ['S001', 'S002'],
        'customer_id': ['C001', 'C001'],
        'amount': [-80.0, -30.0],
        'timestamp': pd.to_datetime(['2024-01-05', '2024-01-06']),
        'transaction_type': ['spent']
    })
    
    expired = pd.DataFrame(columns=['transaction_id', 'customer_id', 'amount', 'timestamp', 'transaction_type'])
    
    result = perform_fifo_matching_logic(earned, spent, expired)
    
    # Assertions - each earned should be used only once
    # S001 should match E001
    s001_row = result[result['TRANS_ID'] == 'S001'].iloc[0]
    assert s001_row['REDEEMID'] == 'E001', "S001 should match E001"
    
    # S002 should match E002 (not E001 again)
    s002_row = result[result['TRANS_ID'] == 'S002'].iloc[0]
    assert s002_row['REDEEMID'] == 'E002', "S002 should match E002 (E001 already used)"
    
    # Verify E001 is not used twice
    spent_with_e001 = result[(result['TCTYPE'].isin(['spent', 'expired'])) & (result['REDEEMID'] == 'E001')]
    assert len(spent_with_e001) == 1, "E001 should only be used once as REDEEMID"
    
    print("✓ Test 9 passed: Each TRANS_ID used only once")


# Test Case 10: Oldest Unmatched Earned by CREATEDAT
def test_oldest_unmatched_earned_by_createdat():
    """Test that spent/expired matches to oldest unmatched earned by CREATEDAT"""
    earned = pd.DataFrame({
        'transaction_id': ['E001', 'E002', 'E003'],
        'customer_id': ['C001', 'C001', 'C001'],
        'amount': [100.0, 50.0, 75.0],
        'timestamp': pd.to_datetime(['2024-01-03', '2024-01-01', '2024-01-02']),  # E002 is oldest
        'transaction_type': ['earned', 'earned', 'earned']
    })
    
    spent = pd.DataFrame({
        'transaction_id': ['S001', 'S002'],
        'customer_id': ['C001', 'C001'],
        'amount': [-30.0, -40.0],
        'timestamp': pd.to_datetime(['2024-01-10', '2024-01-11']),
        'transaction_type': ['spent', 'spent']
    })
    
    expired = pd.DataFrame(columns=['transaction_id', 'customer_id', 'amount', 'timestamp', 'transaction_type'])
    
    result = perform_fifo_matching_logic(earned, spent, expired)
    
    # Assertions - should match by CREATEDAT order (E002 -> E003 -> E001)
    # S001 should match E002 (oldest by CREATEDAT: 2024-01-01)
    s001_row = result[result['TRANS_ID'] == 'S001'].iloc[0]
    assert s001_row['REDEEMID'] == 'E002', "S001 should match E002 (oldest by CREATEDAT)"
    
    # S002 should match E003 (second oldest by CREATEDAT: 2024-01-02)
    s002_row = result[result['TRANS_ID'] == 'S002'].iloc[0]
    assert s002_row['REDEEMID'] == 'E003', "S002 should match E003 (second oldest by CREATEDAT)"
    
    # E001 should remain unmatched
    e001_row = result[result['TRANS_ID'] == 'E001'].iloc[0]
    assert pd.isna(e001_row['REDEEMID']), "E001 should remain unmatched"
    
    print("✓ Test 10 passed: Oldest unmatched earned by CREATEDAT")


# Run all tests
def run_all_tests():
    """Run all test cases"""
    print("\n" + "="*60)
    print("Running Thrive Cash FIFO Matching Test Suite")
    print("="*60 + "\n")
    
    tests = [
        test_simple_one_to_one_matching,
        test_one_to_one_matching,
        test_fifo_order_multiple_earned,
        test_one_earned_multiple_spent,
        test_multiple_customers,
        test_expired_transactions,
        test_no_earned_transactions,
        test_chronological_order,
        test_each_trans_id_used_once,
        test_oldest_unmatched_earned_by_createdat
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"✗ {test.__name__} failed: {str(e)}")
            failed += 1
        except Exception as e:
            print(f"✗ {test.__name__} error: {str(e)}")
            failed += 1
    
    print("\n" + "="*60)
    print(f"Test Results: {passed} passed, {failed} failed out of {len(tests)} tests")
    print("="*60 + "\n")
    
    return passed, failed


if __name__ == "__main__":
    run_all_tests()

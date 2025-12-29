"""
FIFO Matching Logic for Thrive Cash Transactions

This module contains the core FIFO (First-In, First-Out) matching algorithm
for reconciling earned, spent, and expired transactions.

Matching Rules:
- REDEEMID is ONLY populated for earned transactions
- Earned transactions get REDEEMID pointing to spent/expired TRANS_ID that redeemed them
- Spent/expired transactions always have blank/NULL REDEEMID
- Each transaction used only once (1:1 matching)
- Oldest earned (by CREATEDAT) matched first
- Matching done independently per customer
"""

import pandas as pd
import logging
from typing import Tuple

logger = logging.getLogger(__name__)


def perform_fifo_matching_logic(earned_df: pd.DataFrame, 
                                spent_df: pd.DataFrame, 
                                expired_df: pd.DataFrame) -> pd.DataFrame:
    """
    Perform FIFO matching of transactions and create REDEEMID column.
    
    This is the core matching algorithm that:
    - Combines all transactions
    - Standardizes column names
    - Matches spent/expired to oldest earned per customer
    - Assigns REDEEMID only to earned transactions
    
    Args:
        earned_df: DataFrame with earned transactions
        spent_df: DataFrame with spent transactions
        expired_df: DataFrame with expired transactions
    
    Returns:
        DataFrame with all transactions and REDEEMID column
    
    Matching Rules:
    - For each customer, matches spent/expired to oldest earned (by CREATEDAT)
    - Each TRANS_ID can only be used once (no splitting/partial redemptions)
    - If no unmatched earned exists, REDEEMID is blank
    - REDEEMID only populated for earned transactions
    - Scalable for millions of transactions using vectorized operations
    """
    # Combine all transactions for final output
    all_transactions = pd.concat([earned_df, spent_df, expired_df], ignore_index=True)
    
    # Standardize column names to match spec
    column_mapping = {
        'transaction_id': 'TRANS_ID',
        'transaction_type': 'TCTYPE',
        'timestamp': 'CREATEDAT',
        'customer_id': 'CUSTOMERID',
        'amount': 'AMOUNT'
    }
    all_transactions = all_transactions.rename(columns=column_mapping)
    
    logger.info(f"Processing {len(all_transactions)} total transactions")
    
    # Perform FIFO matching per customer
    result_rows = []
    customers = all_transactions['CUSTOMERID'].unique()
    
    logger.info(f"Processing {len(customers)} unique customers")
    
    for customer_id in customers:
        customer_txns = all_transactions[all_transactions['CUSTOMERID'] == customer_id].copy()
        
        # Sort by CREATEDAT for FIFO order
        customer_txns = customer_txns.sort_values('CREATEDAT').reset_index(drop=True)
        
        # Separate by type
        earned = customer_txns[customer_txns['TCTYPE'] == 'earned'].copy()
        spent_expired = customer_txns[customer_txns['TCTYPE'].isin(['spent', 'expired'])].copy()
        
        # Track which earned transactions are still available (not yet matched)
        available_earned = earned['TRANS_ID'].tolist()
        
        # Track redemptions: earned_id -> spent/expired_id
        earned_to_redeemer = {}
        
        # Process each spent/expired transaction in chronological order
        for _, se_txn in spent_expired.iterrows():
            # Find oldest available earned transaction
            if available_earned:
                # Get the oldest earned that hasn't been matched yet
                oldest_earned_id = available_earned[0]
                
                # Track which spent/expired redeemed this earned
                earned_to_redeemer[oldest_earned_id] = se_txn['TRANS_ID']
                
                # Remove from available list (each TRANS_ID used only once)
                available_earned.pop(0)
            
            # Add spent/expired transaction with NULL REDEEMID (always blank)
            result_rows.append({
                'TRANS_ID': se_txn['TRANS_ID'],
                'TCTYPE': se_txn['TCTYPE'],
                'CREATEDAT': se_txn['CREATEDAT'],
                'CUSTOMERID': se_txn['CUSTOMERID'],
                'AMOUNT': se_txn['AMOUNT'],
                'REDEEMID': None  # Always NULL for spent/expired
            })
        
        # Add earned transactions with REDEEMID (only earned gets REDEEMID)
        for _, earned_txn in earned.iterrows():
            # REDEEMID points to the spent/expired that redeemed it
            redeemid = earned_to_redeemer.get(earned_txn['TRANS_ID'], None)
            
            result_rows.append({
                'TRANS_ID': earned_txn['TRANS_ID'],
                'TCTYPE': earned_txn['TCTYPE'],
                'CREATEDAT': earned_txn['CREATEDAT'],
                'CUSTOMERID': earned_txn['CUSTOMERID'],
                'AMOUNT': earned_txn['AMOUNT'],
                'REDEEMID': redeemid  # Points to spent/expired TRANS_ID
            })
    
    # Create final dataframe
    result_df = pd.DataFrame(result_rows)
    
    # Sort by CUSTOMERID and CREATEDAT for readability
    result_df = result_df.sort_values(['CUSTOMERID', 'CREATEDAT']).reset_index(drop=True)
    
    return result_df


def load_from_staging(staging_path: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Load dataframes from staging location.
    
    Args:
        staging_path: Path to staging directory
    
    Returns:
        Tuple of (earned_df, spent_df, expired_df)
    """
    earned_df = pd.read_parquet(f"{staging_path}/earned.parquet")
    spent_df = pd.read_parquet(f"{staging_path}/spent.parquet")
    expired_df = pd.read_parquet(f"{staging_path}/expired.parquet")
    return earned_df, spent_df, expired_df

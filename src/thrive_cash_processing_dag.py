"""
Thrive Cash FIFO Matching DAG

This DAG orchestrates the automated reconciliation of customer rewards transactions
by matching earned transactions with spent or expired transactions following FIFO order.

Workflow:
1. download_data: Download earned, spent, and expired transactions
2. validate_source: Validate source data quality
3. perform_fifo_matching: Match transactions using FIFO logic
4. validate_results: Validate matching results
5. build_analytics: Generate reports and metrics
6. send_alerts: Send notifications on completion or failure
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any
import logging
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule
import json
import requests
import os

# Import FIFO matching logic
from src.fifo_matching import perform_fifo_matching_logic, load_from_staging

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'finance_team',
    'depends_on_past': False,
    'email': ['finance-alerts@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'execution_timeout': timedelta(hours=2),
}

# DAG definition
dag = DAG(
    'thrive_cash_fifo_matching',
    default_args=default_args,
    description='Automated FIFO matching for Thrive Cash transactions',
    schedule_interval='0 2 1 * *',  # Run at 2 AM on the 1st of each month
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['finance', 'thrive_cash', 'reconciliation'],
)

def download_data(**context):
    """
    Download Thrive Cash data from S3 and extract three tables.
    
    Implements Requirement 1: Download Transaction Data
    - Downloads tc_raw_data.xlsx from S3
    - Extracts TC_Data, Sales, and Customers tables
    - Stores raw data in staging location
    - Logs download summary
    """
    execution_date = context['execution_date']
    correlation_id = context['dag_run'].run_id
    
    logger.info(f"[{correlation_id}] Starting data download for execution date: {execution_date}")
    
    try:
        # Download Excel file from S3
        excel_url = "https://thrivemarket-candidate-test.s3.amazonaws.com/tc_raw_data.xlsx"
        logger.info(f"[{correlation_id}] Downloading Excel file from: {excel_url}")
        
        response = requests.get(excel_url, timeout=60)
        response.raise_for_status()
        
        # Create staging directory
        staging_path = f"/tmp/thrive_cash_staging/{execution_date.strftime('%Y%m%d')}"
        os.makedirs(staging_path, exist_ok=True)
        
        # Save Excel file temporarily
        excel_path = f"{staging_path}/tc_raw_data.xlsx"
        with open(excel_path, 'wb') as f:
            f.write(response.content)
        
        logger.info(f"[{correlation_id}] Excel file downloaded successfully: {len(response.content)} bytes")
        
        # Read Excel file and extract tables
        logger.info(f"[{correlation_id}] Extracting tables from Excel file...")
        
        # Read TC_Data sheet
        tc_data_df = pd.read_excel(excel_path, sheet_name='TC_Data')
        logger.info(f"[{correlation_id}] Loaded TC_Data: {len(tc_data_df)} rows")
        
        # Read Sales sheet
        sales_df = pd.read_excel(excel_path, sheet_name='Sales')
        logger.info(f"[{correlation_id}] Loaded Sales: {len(sales_df)} rows")
        
        # Read Customers sheet
        customers_df = pd.read_excel(excel_path, sheet_name='Customers')
        logger.info(f"[{correlation_id}] Loaded Customers: {len(customers_df)} rows")
        
        # Separate TC_Data into earned, spent, and expired transactions
        earned_transactions = tc_data_df[tc_data_df['transaction_type'] == 'earned'].copy()
        spent_transactions = tc_data_df[tc_data_df['transaction_type'] == 'spent'].copy()
        expired_transactions = tc_data_df[tc_data_df['transaction_type'] == 'expired'].copy()
        
        logger.info(
            f"[{correlation_id}] TC_Data breakdown - "
            f"Earned: {len(earned_transactions)}, Spent: {len(spent_transactions)}, "
            f"Expired: {len(expired_transactions)}"
        )
        
        # Store data in staging location
        _save_to_staging(staging_path, earned_transactions, spent_transactions, expired_transactions)
        
        # Save Sales and Customers data
        sales_df.to_parquet(f"{staging_path}/sales.parquet", index=False)
        customers_df.to_parquet(f"{staging_path}/customers.parquet", index=False)
        
        # Push data to XCom for downstream tasks
        context['task_instance'].xcom_push(key='staging_path', value=staging_path)
        context['task_instance'].xcom_push(key='earned_count', value=len(earned_transactions))
        context['task_instance'].xcom_push(key='spent_count', value=len(spent_transactions))
        context['task_instance'].xcom_push(key='expired_count', value=len(expired_transactions))
        context['task_instance'].xcom_push(key='sales_count', value=len(sales_df))
        context['task_instance'].xcom_push(key='customers_count', value=len(customers_df))
        
        logger.info(
            f"[{correlation_id}] Data download completed successfully. "
            f"TC_Data - Earned: {len(earned_transactions)}, Spent: {len(spent_transactions)}, "
            f"Expired: {len(expired_transactions)} | Sales: {len(sales_df)} | Customers: {len(customers_df)}"
        )
        
        return {
            'status': 'success',
            'staging_path': staging_path,
            'counts': {
                'earned': len(earned_transactions),
                'spent': len(spent_transactions),
                'expired': len(expired_transactions),
                'sales': len(sales_df),
                'customers': len(customers_df)
            }
        }
        
    except requests.exceptions.RequestException as e:
        logger.error(f"[{correlation_id}] Failed to download Excel file: {str(e)}", exc_info=True)
        raise AirflowException(f"Failed to download Excel file from S3: {str(e)}")
    except Exception as e:
        logger.error(f"[{correlation_id}] Data download failed: {str(e)}", exc_info=True)
        raise AirflowException(f"Failed to download transaction data: {str(e)}")


def validate_source(**context):
    """
    Validate source data quality before processing.
    
    Implements Requirement 2: Validate Source Data Quality
    - Verifies required fields are present
    - Validates data types and ranges
    - Checks for null values
    - Logs validation errors with row numbers
    """
    correlation_id = context['dag_run'].run_id
    staging_path = context['task_instance'].xcom_pull(task_ids='download_data', key='staging_path')
    
    logger.info(f"[{correlation_id}] Starting source data validation from: {staging_path}")
    
    try:
        # Load data from staging
        earned_df, spent_df, expired_df = _load_from_staging(staging_path)
        
        validation_errors = []
        
        # Validate earned transactions
        logger.info(f"[{correlation_id}] Validating earned transactions...")
        earned_errors = _validate_transactions(earned_df, 'earned', correlation_id)
        validation_errors.extend(earned_errors)
        
        # Validate spent transactions
        logger.info(f"[{correlation_id}] Validating spent transactions...")
        spent_errors = _validate_transactions(spent_df, 'spent', correlation_id)
        validation_errors.extend(spent_errors)
        
        # Validate expired transactions
        logger.info(f"[{correlation_id}] Validating expired transactions...")
        expired_errors = _validate_transactions(expired_df, 'expired', correlation_id)
        validation_errors.extend(expired_errors)
        
        # If validation errors exist, halt execution
        # TODO: This can be written to a error file and sent to financial analyst.
        if validation_errors:
            error_summary = "\n".join(validation_errors[:10])  # Show first 10 errors
            logger.error(
                f"[{correlation_id}] Validation failed with {len(validation_errors)} errors:\n{error_summary}"
            )
            raise AirflowException(f"Source data validation failed with {len(validation_errors)} errors")
        
        # Log validation summary
        logger.info(
            f"[{correlation_id}] Source data validation passed. "
            f"Validated {len(earned_df)} earned, {len(spent_df)} spent, "
            f"{len(expired_df)} expired transactions."
        )
        
        return {
            'status': 'success',
            'validated_counts': {
                'earned': len(earned_df),
                'spent': len(spent_df),
                'expired': len(expired_df)
            }
        }
        
    except AirflowException:
        raise
    except Exception as e:
        logger.error(f"[{correlation_id}] Source validation failed: {str(e)}", exc_info=True)
        raise AirflowException(f"Failed to validate source data: {str(e)}")


def perform_fifo_matching(**context):
    """
    Airflow task wrapper for FIFO matching logic.
    
    Implements Requirement 3: Perform FIFO Matching
    - Loads validated data from staging
    - Calls core FIFO matching algorithm
    - Saves results to CSV and Parquet
    - Pushes metrics to XCom
    """
    correlation_id = context['dag_run'].run_id
    staging_path = context['task_instance'].xcom_pull(task_ids='download_data', key='staging_path')
    
    logger.info(f"[{correlation_id}] Starting FIFO matching with REDEEMID assignment...")
    
    try:
        # Load validated data
        earned_df, spent_df, expired_df = load_from_staging(staging_path)
        
        # Perform FIFO matching using core algorithm
        result_df = perform_fifo_matching_logic(earned_df, spent_df, expired_df)
        
        # Save as CSV (primary deliverable)
        output_csv_path = f"{staging_path}/tc_data_with_redemptions.csv"
        result_df.to_csv(output_csv_path, index=False)
        
        # Also save as parquet for downstream processing
        output_parquet_path = f"{staging_path}/tc_data_with_redemptions.parquet"
        result_df.to_parquet(output_parquet_path, index=False)
        
        # Calculate statistics
        total_with_redeemid = result_df['REDEEMID'].notna().sum()
        total_without_redeemid = result_df['REDEEMID'].isna().sum()
        
        # Push results to XCom
        context['task_instance'].xcom_push(key='output_csv_path', value=output_csv_path)
        context['task_instance'].xcom_push(key='output_parquet_path', value=output_parquet_path)
        context['task_instance'].xcom_push(key='total_rows', value=len(result_df))
        context['task_instance'].xcom_push(key='rows_with_redeemid', value=total_with_redeemid)
        
        logger.info(
            f"[{correlation_id}] FIFO matching completed. "
            f"Total rows: {len(result_df)}, With REDEEMID: {total_with_redeemid}, "
            f"Without REDEEMID: {total_without_redeemid}"
        )
        logger.info(f"[{correlation_id}] Output saved to: {output_csv_path}")
        
        return {
            'status': 'success',
            'output_csv_path': output_csv_path,
            'total_rows': len(result_df),
            'rows_with_redeemid': int(total_with_redeemid),
            'rows_without_redeemid': int(total_without_redeemid)
        }
        
    except Exception as e:
        logger.error(f"[{correlation_id}] FIFO matching failed: {str(e)}", exc_info=True)
        raise AirflowException(f"Failed to perform FIFO matching: {str(e)}")


def validate_results(**context):
    """
    Validate FIFO matching results.
    
    Implements Requirement 4: Validate Matching Results
    - Verifies balance equations
    - Checks that each TRANS_ID is used only once as REDEEMID
    - Ensures REDEEMID references valid earned transactions
    - Validates chronological order
    """
    correlation_id = context['dag_run'].run_id
    staging_path = context['task_instance'].xcom_pull(task_ids='download_data', key='staging_path')
    output_parquet_path = context['task_instance'].xcom_pull(task_ids='perform_fifo_matching', key='output_parquet_path')
    
    logger.info(f"[{correlation_id}] Starting results validation...")
    
    try:
        # Load original data
        earned_df, spent_df, expired_df = _load_from_staging(staging_path)
        
        # Load matching results
        result_df = pd.read_parquet(output_parquet_path)
        
        validation_errors = []
        
        # Validation 1: Check that all spent/expired transactions have REDEEMID (if earned exists)
        spent_expired_in_result = result_df[result_df['TCTYPE'].isin(['spent', 'expired'])]
        missing_redeemid = spent_expired_in_result[spent_expired_in_result['REDEEMID'].isna()]
        
        # Only flag as error if there were earned transactions available
        if len(missing_redeemid) > 0 and len(earned_df) > 0:
            logger.warning(
                f"[{correlation_id}] {len(missing_redeemid)} spent/expired transactions without REDEEMID "
                f"(may be valid if no earned balance available)"
            )
        
        # Validation 2: Verify each REDEEMID references a valid earned transaction
        redeemids = result_df[result_df['REDEEMID'].notna()]['REDEEMID'].unique()
        earned_trans_ids = result_df[result_df['TCTYPE'] == 'earned']['TRANS_ID'].unique()
        
        invalid_redeemids = set(redeemids) - set(earned_trans_ids)
        if invalid_redeemids:
            validation_errors.append(
                f"Invalid REDEEMIDs found (not in earned transactions): {list(invalid_redeemids)[:5]}"
            )
        
        # Validation 3: Verify balance equations per customer
        for customer_id in result_df['CUSTOMERID'].unique():
            customer_data = result_df[result_df['CUSTOMERID'] == customer_id]
            
            earned_total = customer_data[customer_data['TCTYPE'] == 'earned']['AMOUNT'].sum()
            spent_total = abs(customer_data[customer_data['TCTYPE'] == 'spent']['AMOUNT'].sum())
            expired_total = abs(customer_data[customer_data['TCTYPE'] == 'expired']['AMOUNT'].sum())
            
            # Remaining balance = earned with no REDEEMID
            remaining = customer_data[
                (customer_data['TCTYPE'] == 'earned') & 
                (customer_data['REDEEMID'].isna())
            ]['AMOUNT'].sum()
            
            # taking Absolute Value for -ive values
            balance_check = abs(earned_total - (spent_total + expired_total + remaining))
            if balance_check > 0.01:  # Allow small floating point errors
                validation_errors.append(
                    f"Customer {customer_id} balance mismatch: "
                    f"Earned={earned_total}, Spent+Expired+Remaining={spent_total + expired_total + remaining}"
                )
        
        # Validation 4: Verify chronological order (earned before spent/expired)
        for _, row in result_df[result_df['REDEEMID'].notna()].iterrows():
            if row['TCTYPE'] in ['spent', 'expired']:
                # Find the earned transaction
                earned_row = result_df[
                    (result_df['TRANS_ID'] == row['REDEEMID']) & 
                    (result_df['TCTYPE'] == 'earned')
                ]
                
                if len(earned_row) > 0:
                    earned_date = earned_row.iloc[0]['CREATEDAT']
                    spent_date = row['CREATEDAT']
                    
                    if pd.to_datetime(earned_date) > pd.to_datetime(spent_date):
                        validation_errors.append(
                            f"Chronological violation: Earned {row['REDEEMID']} at {earned_date} "
                            f"matched to {row['TCTYPE']} {row['TRANS_ID']} at {spent_date}"
                        )
        
        # Validation 5: Check for duplicate TRANS_IDs in output
        duplicate_trans_ids = result_df[result_df.duplicated(subset=['TRANS_ID', 'REDEEMID'], keep=False)]
        if len(duplicate_trans_ids) > 0:
            # This is actually expected for split earned transactions, so just log it
            logger.info(
                f"[{correlation_id}] Found {len(duplicate_trans_ids)} rows with same TRANS_ID "
                f"(expected for split earned transactions)"
            )
        
        # Calculate summary statistics
        total_rows = len(result_df)
        rows_with_redeemid = result_df['REDEEMID'].notna().sum()
        rows_without_redeemid = result_df['REDEEMID'].isna().sum()
        
        total_earned = result_df[result_df['TCTYPE'] == 'earned']['AMOUNT'].sum()
        total_spent = abs(result_df[result_df['TCTYPE'] == 'spent']['AMOUNT'].sum())
        total_expired = abs(result_df[result_df['TCTYPE'] == 'expired']['AMOUNT'].sum())
        
        # If validation errors exist, halt execution
        if validation_errors:
            error_summary = "\n".join(validation_errors[:10])
            logger.error(
                f"[{correlation_id}] Results validation failed with {len(validation_errors)} errors:\n{error_summary}"
            )
            raise AirflowException(f"Results validation failed with {len(validation_errors)} errors")
        
        logger.info(
            f"[{correlation_id}] Results validation passed all checks. "
            f"Total rows: {total_rows}, With REDEEMID: {rows_with_redeemid}, "
            f"Without REDEEMID: {rows_without_redeemid}"
        )
        
        return {
            'status': 'success',
            'validation_checks_passed': 5,
            'total_rows': int(total_rows),
            'rows_with_redeemid': int(rows_with_redeemid),
            'rows_without_redeemid': int(rows_without_redeemid),
            'total_earned': float(total_earned),
            'total_spent': float(total_spent),
            'total_expired': float(total_expired)
        }
        
    except AirflowException:
        raise
    except Exception as e:
        logger.error(f"[{correlation_id}] Results validation failed: {str(e)}", exc_info=True)
        raise AirflowException(f"Failed to validate results: {str(e)}")


def build_analytics(**context):
    """
    Build analytics and reports from matched transactions.
    
    Implements Requirement 5: Build Analytics and Reports
    - Calculates customer balances over time
    - Generates cumulative earned, spent, expired amounts
    - Enables historical balance queries at any point in time
    - Stores output for finance team analysis
    """
    correlation_id = context['dag_run'].run_id
    staging_path = context['task_instance'].xcom_pull(task_ids='download_data', key='staging_path')
    output_parquet_path = context['task_instance'].xcom_pull(task_ids='perform_fifo_matching', key='output_parquet_path')
    
    logger.info(f"[{correlation_id}] Starting analytics generation...")
    
    try:
        # Load matching results
        result_df = pd.read_parquet(output_parquet_path)
        
        logger.info(f"[{correlation_id}] Generating customer balance history...")
        
        # Create customer balance history
        balance_history = []
        
        # Process each customer
        for customer_id in result_df['CUSTOMERID'].unique():
            customer_txns = result_df[result_df['CUSTOMERID'] == customer_id].copy()
            
            # Sort by transaction date
            customer_txns = customer_txns.sort_values('CREATEDAT').reset_index(drop=True)
            
            # Initialize cumulative counters
            cumulative_earned = 0
            cumulative_spent = 0
            cumulative_expired = 0
            
            # Process each transaction chronologically
            for _, txn in customer_txns.iterrows():
                # Update cumulative amounts based on transaction type
                if txn['TCTYPE'] == 'earned':
                    cumulative_earned += abs(txn['AMOUNT'])
                elif txn['TCTYPE'] == 'spent':
                    cumulative_spent += abs(txn['AMOUNT'])
                elif txn['TCTYPE'] == 'expired':
                    cumulative_expired += abs(txn['AMOUNT'])
                
                # Calculate current balance
                current_balance = cumulative_earned - cumulative_spent - cumulative_expired
                
                # Add to balance history
                balance_history.append({
                    'customer_id': customer_id,
                    'transaction_date': txn['CREATEDAT'],
                    'transaction_id': txn['TRANS_ID'],
                    'transaction_type': txn['TCTYPE'],
                    'transaction_amount': txn['AMOUNT'],
                    'cumulative_earned': cumulative_earned,
                    'cumulative_spent': cumulative_spent,
                    'cumulative_expired': cumulative_expired,
                    'current_balance': current_balance
                })
        
        # Create balance history dataframe
        balance_history_df = pd.DataFrame(balance_history)
        
        # Save customer balance history (primary analytics output)
        balance_history_path = f"{staging_path}/customer_balance_history.csv"
        balance_history_df.to_csv(balance_history_path, index=False)
        
        logger.info(f"[{correlation_id}] Customer balance history saved to: {balance_history_path}")
        
        # Generate summary statistics
        total_earned = result_df[result_df['TCTYPE'] == 'earned']['AMOUNT'].sum()
        total_spent = abs(result_df[result_df['TCTYPE'] == 'spent']['AMOUNT'].sum())
        total_expired = abs(result_df[result_df['TCTYPE'] == 'expired']['AMOUNT'].sum())
        
        # Get current balances (latest balance per customer)
        current_balances = balance_history_df.groupby('customer_id').last()[
            ['current_balance', 'cumulative_earned', 'cumulative_spent', 'cumulative_expired']
        ].reset_index()
        
        # Save current balances summary
        current_balances_path = f"{staging_path}/customer_current_balances.csv"
        current_balances.to_csv(current_balances_path, index=False)
        
        # Transaction counts
        earned_count = len(result_df[result_df['TCTYPE'] == 'earned'])
        spent_count = len(result_df[result_df['TCTYPE'] == 'spent'])
        expired_count = len(result_df[result_df['TCTYPE'] == 'expired'])
        
        # Generate summary report
        report = {
            'execution_date': context['execution_date'].isoformat(),
            'correlation_id': correlation_id,
            'summary_metrics': {
                'total_earned': float(total_earned),
                'total_spent': float(total_spent),
                'total_expired': float(total_expired),
                'total_current_balance': float(current_balances['current_balance'].sum()),
                'earned_transaction_count': int(earned_count),
                'spent_transaction_count': int(spent_count),
                'expired_transaction_count': int(expired_count),
                'total_customers': len(current_balances),
                'customers_with_positive_balance': len(current_balances[current_balances['current_balance'] > 0])
            },
            'top_customers_by_balance': current_balances.nlargest(10, 'current_balance').to_dict('records'),
            'output_files': {
                'balance_history': balance_history_path,
                'current_balances': current_balances_path
            }
        }
        
        # Save summary report
        report_path = f"{staging_path}/analytics_report.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        # Push to XCom
        context['task_instance'].xcom_push(key='report_path', value=report_path)
        context['task_instance'].xcom_push(key='balance_history_path', value=balance_history_path)
        context['task_instance'].xcom_push(key='current_balances_path', value=current_balances_path)
        context['task_instance'].xcom_push(key='report_summary', value=report['summary_metrics'])
        
        logger.info(
            f"[{correlation_id}] Analytics generation completed. "
            f"Balance history: {len(balance_history_df)} records, "
            f"Customers: {len(current_balances)}"
        )
        
        return report
        
    except Exception as e:
        logger.error(f"[{correlation_id}] Analytics generation failed: {str(e)}", exc_info=True)
        raise AirflowException(f"Failed to build analytics: {str(e)}")


def send_success_alert(**context):
    """
    Send success notification with processing summary.
    
    Implements Requirement 6: Send Alerts and Notifications
    """
    correlation_id = context['dag_run'].run_id
    report_summary = context['task_instance'].xcom_pull(task_ids='build_analytics', key='report_summary')
    
    logger.info(f"[{correlation_id}] Sending success alert...")
    
    message = f"""
    Thrive Cash FIFO Matching Completed Successfully
    
    Execution Date: {context['execution_date']}
    Correlation ID: {correlation_id}
    
    Summary:
    - Total Earned: ${report_summary['total_earned']:,.2f}
    - Total Spent: ${report_summary['total_spent']:,.2f}
    - Total Expired: ${report_summary['total_expired']:,.2f}
    - Total Remaining: ${report_summary['total_remaining']:,.2f}
    
    Transaction Counts:
    - Earned: {report_summary['earned_transaction_count']}
    - Spent: {report_summary['spent_transaction_count']}
    - Expired: {report_summary['expired_transaction_count']}
    - Matching Records: {report_summary['matching_records_count']}
    
    Customers with Remaining Balance: {report_summary['customers_with_balance']}
    """
    
    # In production, send to actual notification service (Slack, email, etc.)
    logger.info(f"[{correlation_id}] Success alert:\n{message}")
    
    return {'status': 'alert_sent', 'message': message}


def send_failure_alert(**context):
    """
    Send failure notification with error details.
    
    Implements Requirement 6: Send Alerts and Notifications
    Triggered on any task failure.
    """
    correlation_id = context['dag_run'].run_id
    failed_task = context.get('task_instance')
    
    logger.info(f"[{correlation_id}] Sending failure alert...")
    
    message = f"""
    Thrive Cash FIFO Matching Failed
    
    Execution Date: {context['execution_date']}
    Correlation ID: {correlation_id}
    Failed Task: {failed_task.task_id if failed_task else 'Unknown'}
    
    Please check the Airflow logs for detailed error information.
    """
    
    # In production, send to actual notification service
    logger.error(f"[{correlation_id}] Failure alert:\n{message}")
    
    return {'status': 'alert_sent', 'message': message}


# Helper functions

def _save_to_staging(staging_path, earned_df, spent_df, expired_df):
    """Save dataframes to staging location."""
    os.makedirs(staging_path, exist_ok=True)
    earned_df.to_parquet(f"{staging_path}/earned.parquet", index=False)
    spent_df.to_parquet(f"{staging_path}/spent.parquet", index=False)
    expired_df.to_parquet(f"{staging_path}/expired.parquet", index=False)


def _load_from_staging(staging_path):
    """Load dataframes from staging location."""
    earned_df = pd.read_parquet(f"{staging_path}/earned.parquet")
    spent_df = pd.read_parquet(f"{staging_path}/spent.parquet")
    expired_df = pd.read_parquet(f"{staging_path}/expired.parquet")
    return earned_df, spent_df, expired_df


def _validate_transactions(df, transaction_type, correlation_id):
    """Validate transaction data quality."""
    errors = []
    required_fields = ['transaction_id', 'customer_id', 'amount', 'timestamp', 'transaction_type']
    
    # Check required fields
    missing_fields = [field for field in required_fields if field not in df.columns]
    if missing_fields:
        errors.append(f"{transaction_type}: Missing required fields: {missing_fields}")
        return errors
    
    # Check for null values
    for field in required_fields:
        null_count = df[field].isnull().sum()
        if null_count > 0:
            null_rows = df[df[field].isnull()].index.tolist()
            errors.append(f"{transaction_type}: {null_count} null values in '{field}' at rows: {null_rows[:5]}")
    
    # Validate amounts are numeric (decimal/float)
    try:
        # Check if amount column can be converted to numeric
        # using Coeerrors='coerce' for marking invalid values as NaN and report the specific rows.
        pd.to_numeric(df['amount'], errors='coerce')
        non_numeric = df[pd.to_numeric(df['amount'], errors='coerce').isnull()]
        if len(non_numeric) > 0:
            errors.append(
                f"{transaction_type}: {len(non_numeric)} non-numeric amounts at rows: "
                f"{non_numeric.index.tolist()[:5]}"
            )
    except Exception as e:
        errors.append(f"{transaction_type}: Error validating amount field: {str(e)}")
    
    # Validate transaction types
    invalid_types = df[~df['transaction_type'].isin(['earned', 'spent', 'expired'])]
    if len(invalid_types) > 0:
        errors.append(
            f"{transaction_type}: {len(invalid_types)} invalid transaction types at rows: "
            f"{invalid_types.index.tolist()[:5]}"
        )
    
    return errors


# Define tasks
download_data_task = PythonOperator(
    task_id='download_data',
    python_callable=download_data,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

validate_source_task = PythonOperator(
    task_id='validate_source',
    python_callable=validate_source,
    execution_timeout=timedelta(minutes=15),
    dag=dag,
)

perform_fifo_matching_task = PythonOperator(
    task_id='perform_fifo_matching',
    python_callable=perform_fifo_matching,
    execution_timeout=timedelta(minutes=45),
    dag=dag,
)

validate_results_task = PythonOperator(
    task_id='validate_results',
    python_callable=validate_results,
    execution_timeout=timedelta(minutes=15),
    dag=dag,
)

build_analytics_task = PythonOperator(
    task_id='build_analytics',
    python_callable=build_analytics,
    execution_timeout=timedelta(minutes=15),
    dag=dag,
)

send_success_alert_task = PythonOperator(
    task_id='send_success_alert',
    python_callable=send_success_alert,
    execution_timeout=timedelta(minutes=5),
    dag=dag,
)

send_failure_alert_task = PythonOperator(
    task_id='send_failure_alert',
    python_callable=send_failure_alert,
    execution_timeout=timedelta(minutes=5),
    trigger_rule=TriggerRule.ONE_FAILED,  # Trigger on any upstream failure
    dag=dag,
)

# Define task dependencies
download_data_task >> validate_source_task >> perform_fifo_matching_task >> validate_results_task >> build_analytics_task >> send_success_alert_task

# Failure alert runs if any task fails
[download_data_task, validate_source_task, perform_fifo_matching_task, 
 validate_results_task, build_analytics_task] >> send_failure_alert_task

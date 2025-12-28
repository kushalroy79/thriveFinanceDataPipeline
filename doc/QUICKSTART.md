# Quick Start Guide

Get the Thrive Cash FIFO Matching system running in 5 minutes.

## Prerequisites

- Python 3.8+
- Apache Airflow 2.x
- Basic understanding of Airflow DAGs

## Step 1: Install Dependencies

```bash
pip install pandas requests openpyxl pyarrow apache-airflow
```

## Step 2: Test the Logic

Before deploying to Airflow, test the FIFO matching logic:

```bash
# Run all tests
python test/run_tests.py

# See visual examples
python test_examples.py
```

Expected output:
```
============================================================
Running Thrive Cash FIFO Matching Test Suite
============================================================

✓ Test 1 passed: Simple one-to-one matching
✓ Test 2 passed: Partial redemption
...
Test Results: 10 passed, 0 failed out of 10 tests
============================================================
```

## Step 3: Configure the DAG

Edit `thrive_cash_processing_dag.py`:

```python
# Update email for alerts
default_args = {
    'email': ['your-email@company.com'],  # ← Change this
    ...
}

# Optional: Change schedule
schedule_interval='0 2 1 * *',  # Monthly on 1st at 2 AM
```

## Step 4: Deploy to Airflow

```bash
# Copy DAG to Airflow folder
cp thrive_cash_processing_dag.py $AIRFLOW_HOME/dags/

# Verify DAG appears
airflow dags list | grep thrive_cash

# Enable the DAG
airflow dags unpause thrive_cash_fifo_matching
```

## Step 5: Run the DAG

### Option A: Manual Trigger (Recommended for First Run)

```bash
# Trigger from CLI
airflow dags trigger thrive_cash_fifo_matching

# Or use Airflow UI
# Navigate to: http://localhost:8080
# Find: thrive_cash_fifo_matching
# Click: Trigger DAG button
```

### Option B: Wait for Scheduled Run

The DAG runs automatically on the 1st of each month at 2 AM.

## Step 6: Monitor Execution

### View in Airflow UI

1. Open: `http://localhost:8080`
2. Click on: `thrive_cash_fifo_matching`
3. View: Graph, Tree, or Logs

### Check Task Status

```bash
# List recent DAG runs
airflow dags list-runs -d thrive_cash_fifo_matching

# View task logs
airflow tasks logs thrive_cash_fifo_matching perform_fifo_matching <execution_date>
```

## Step 7: Review Output

Output files are in the staging directory:

```bash
# Default location
cd /tmp/thrive_cash_staging/<YYYYMMDD>/

# View main output
cat tc_data_with_redemptions.csv

# View analytics
cat analytics_report.json

# View customer balances
cat customer_balances.csv
```

## Understanding the Output

### tc_data_with_redemptions.csv

Main deliverable with REDEEMID column:

```csv
TRANS_ID,TCTYPE,CREATEDAT,CUSTOMERID,AMOUNT,REDEEMID
E001,earned,2024-01-01,C001,100.0,S001
S001,spent,2024-01-05,C001,-100.0,E001
```

**REDEEMID Meaning:**
- **Spent/Expired**: Points to earned transaction that was redeemed
- **Earned (redeemed)**: Points to spent/expired that redeemed it
- **Earned (unredeemed)**: NULL (not yet used)

### analytics_report.json

Summary metrics:

```json
{
  "summary_metrics": {
    "total_earned": 1000.00,
    "total_spent": 600.00,
    "total_expired": 100.00,
    "total_remaining": 300.00,
    "redemption_rate": {
      "amount_redeemed_pct": 70.0
    }
  }
}
```

## Common Tasks

### Change Staging Directory

For production, use persistent storage:

```python
# In thrive_cash_processing_dag.py
staging_path = f"/data/thrive_cash/{execution_date}"  # ← Change this
```

### Add Slack Notifications

Replace email alerts with Slack:

```python
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

send_slack_alert = SlackWebhookOperator(
    task_id='send_slack_alert',
    http_conn_id='slack_webhook',
    message='Thrive Cash processing completed!',
    dag=dag,
)
```

### Adjust Timeouts

For larger datasets:

```python
perform_fifo_matching_task = PythonOperator(
    task_id='perform_fifo_matching',
    python_callable=perform_fifo_matching,
    execution_timeout=timedelta(hours=2),  # ← Increase this
    dag=dag,
)
```

## Troubleshooting

### DAG Not Appearing

```bash
# Check for syntax errors
python thrive_cash_processing_dag.py

# Refresh DAGs
airflow dags list-import-errors
```

### Download Fails

```bash
# Test S3 connectivity
curl -I https://thrivemarket-candidate-test.s3.amazonaws.com/tc_raw_data.xlsx

# Check Airflow logs
airflow tasks logs thrive_cash_fifo_matching download_data <execution_date>
```

### Validation Errors

```bash
# View validation errors in logs
airflow tasks logs thrive_cash_fifo_matching validate_source <execution_date>

# Check source data quality
python -c "import pandas as pd; df = pd.read_excel('tc_raw_data.xlsx', sheet_name='TC_Data'); print(df.info())"
```

### Memory Issues

For large datasets:

```bash
# Increase Airflow worker memory
# In airflow.cfg:
[celery]
worker_concurrency = 4
```

## Next Steps

1. **Review Documentation**
   - Read: `README.md` for full documentation
   - Read: `TESTING.md` for testing guide
   - Read: `.kiro/specs/thrive-cash-fifo-matching/design.md` for architecture

2. **Customize for Production**
   - Update email/Slack notifications
   - Configure persistent storage
   - Set up monitoring and alerting
   - Schedule backups of output files

3. **Optimize Performance**
   - Test with production data volumes
   - Adjust timeouts and retries
   - Consider parallel processing for large datasets

4. **Set Up Monitoring**
   - Configure Airflow alerts
   - Set up log aggregation
   - Create dashboards for key metrics

## Quick Reference

### File Structure

```
.
├── src/
│   ├── thrive_cash_processing_dag.py    # Main DAG file
│   └── sample_queries.sql               # SQL queries
├── test/
│   ├── test_fifo_matching.py            # Test suite
│   ├── test_examples.py                 # Visual examples
│   └── run_tests.py                     # Test runner
├── README.md                            # Full documentation
├── TESTING.md                           # Testing guide
├── QUICKSTART.md                        # This file
└── .kiro/specs/
    └── thrive-cash-fifo-matching/
        ├── requirements.md              # Requirements
        └── design.md                    # Design doc
```

### Key Commands

```bash
# Test logic
python test/run_tests.py

# Deploy DAG
cp thrive_cash_processing_dag.py $AIRFLOW_HOME/dags/

# Trigger DAG
airflow dags trigger thrive_cash_fifo_matching

# View logs
airflow tasks logs thrive_cash_fifo_matching <task_id> <execution_date>

# Check output
cat /tmp/thrive_cash_staging/<YYYYMMDD>/tc_data_with_redemptions.csv
```

### Important URLs

- **Airflow UI**: http://localhost:8080
- **Data Source**: https://thrivemarket-candidate-test.s3.amazonaws.com/tc_raw_data.xlsx
- **Support**: finance-team@company.com

## Success Checklist

- [ ] Dependencies installed
- [ ] Tests passing (10/10)
- [ ] DAG deployed to Airflow
- [ ] DAG enabled and visible in UI
- [ ] First run completed successfully
- [ ] Output files generated
- [ ] Email alerts received
- [ ] Analytics report reviewed

## Getting Help

- **Documentation**: See `README.md`
- **Testing Issues**: See `TESTING.md`
- **Architecture Questions**: See `.kiro/specs/thrive-cash-fifo-matching/design.md`
- **Email**: finance-team@company.com
- **Slack**: #thrive-cash-automation

---

**Congratulations!** You now have the Thrive Cash FIFO Matching system running. 🎉

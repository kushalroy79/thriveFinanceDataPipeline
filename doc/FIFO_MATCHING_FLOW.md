# FIFO Matching Flow Diagram

## High-Level Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    FIFO Matching Process                         │
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │  Load Transaction Data │
                    │  (earned, spent,       │
                    │   expired)             │
                    └────────────────────────┘
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │  Combine & Standardize │
                    │  Column Names          │
                    │  (TRANS_ID, TCTYPE,    │
                    │   CREATEDAT, etc.)     │
                    └────────────────────────┘
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │  Group by CUSTOMERID   │
                    └────────────────────────┘
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │  For Each Customer:    │
                    │  Process Independently │
                    └────────────────────────┘
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │  Sort by CREATEDAT     │
                    │  (Oldest First)        │
                    └────────────────────────┘
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │  Separate Transactions │
                    │  - Earned              │
                    │  - Spent/Expired       │
                    └────────────────────────┘
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │  Match Spent/Expired   │
                    │  to Earned (FIFO)      │
                    └────────────────────────┘
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │  Assign REDEEMID       │
                    │  - Earned → Redeemer   │
                    │    TRANS_ID            │
                    │  - Spent/Expired →     │
                    │    Always NULL         │
                    └────────────────────────┘
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │  Combine All Results   │
                    └────────────────────────┘
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │  Save Output CSV       │
                    │  tc_data_with_         │
                    │  redemptions.csv       │
                    └────────────────────────┘
```

## Detailed Per-Customer Matching Flow

```
┌─────────────────────────────────────────────────────────────────┐
│              Customer C001 Transaction Processing                │
└─────────────────────────────────────────────────────────────────┘

INPUT TRANSACTIONS (Sorted by CREATEDAT):
┌──────────────────────────────────────────────────────────────┐
│ TRANS_ID │ TCTYPE  │ CREATEDAT   │ AMOUNT │                 │
├──────────┼─────────┼─────────────┼────────┤                 │
│ E001     │ earned  │ 2024-01-01  │  100   │ ◄─┐             │
│ E002     │ earned  │ 2024-01-02  │   50   │   │ Earned      │
│ E003     │ earned  │ 2024-01-03  │   75   │ ◄─┘ Queue       │
│ S001     │ spent   │ 2024-01-05  │  -80   │ ◄─┐             │
│ S002     │ spent   │ 2024-01-06  │  -40   │   │ Spent/      │
│ X001     │ expired │ 2024-06-01  │  -30   │ ◄─┘ Expired     │
└──────────┴─────────┴─────────────┴────────┘                 │

STEP 1: Create Available Earned List
┌─────────────────────────────────┐
│ Available Earned (FIFO Order): │
│ [E001, E002, E003]              │
└─────────────────────────────────┘

STEP 2: Process S001 (spent)
┌─────────────────────────────────┐
│ S001 needs matching             │
│ ↓                               │
│ Pop oldest: E001                │
│ ↓                               │
│ Track: E001 redeemed by S001    │
│ S001.REDEEMID = NULL (always)   │
│ ↓                               │
│ Available: [E002, E003]         │
└─────────────────────────────────┘

STEP 3: Process S002 (spent)
┌─────────────────────────────────┐
│ S002 needs matching             │
│ ↓                               │
│ Pop oldest: E002                │
│ ↓                               │
│ Track: E002 redeemed by S002    │
│ S002.REDEEMID = NULL (always)   │
│ ↓                               │
│ Available: [E003]               │
└─────────────────────────────────┘

STEP 4: Process X001 (expired)
┌─────────────────────────────────┐
│ X001 needs matching             │
│ ↓                               │
│ Pop oldest: E003                │
│ ↓                               │
│ Track: E003 redeemed by X001    │
│ X001.REDEEMID = NULL (always)   │
│ ↓                               │
│ Available: []                   │
└─────────────────────────────────┘

OUTPUT WITH REDEEMID:
┌──────────────────────────────────────────────────────────────┐
│ TRANS_ID │ TCTYPE  │ CREATEDAT   │ AMOUNT │ REDEEMID        │
├──────────┼─────────┼─────────────┼────────┼─────────────────┤
│ E001     │ earned  │ 2024-01-01  │  100   │ S001 ◄─┐        │
│ E002     │ earned  │ 2024-01-02  │   50   │ S002   │ Earned │
│ E003     │ earned  │ 2024-01-03  │   75   │ X001   │ Only   │
│ S001     │ spent   │ 2024-01-05  │  -80   │ NULL ◄─┘        │
│ S002     │ spent   │ 2024-01-06  │  -40   │ NULL            │
│ X001     │ expired │ 2024-06-01  │  -30   │ NULL            │
└──────────┴─────────┴─────────────┴────────┴─────────────────┘
```

## Edge Case: No Earned Available

```
┌─────────────────────────────────────────────────────────────────┐
│              Customer C002 - Insufficient Earned                 │
└─────────────────────────────────────────────────────────────────┘

INPUT:
┌──────────────────────────────────────────────────────────────┐
│ TRANS_ID │ TCTYPE  │ CREATEDAT   │ AMOUNT │                 │
├──────────┼─────────┼─────────────┼────────┤                 │
│ E010     │ earned  │ 2024-01-01  │   50   │ ◄─ Only 1 earned│
│ S010     │ spent   │ 2024-01-05  │  -30   │                 │
│ S011     │ spent   │ 2024-01-06  │  -40   │                 │
└──────────┴─────────┴─────────────┴────────┘                 │

MATCHING PROCESS:
┌─────────────────────────────────┐
│ Available: [E010]               │
│                                 │
│ S010 → Match E010               │
│ Track: E010 redeemed by S010    │
│ S010.REDEEMID = NULL (always)   │
│ Available: []                   │
│                                 │
│ S011 → No earned available!     │
│ S011.REDEEMID = NULL (always)   │
└─────────────────────────────────┘

OUTPUT:
┌──────────────────────────────────────────────────────────────┐
│ TRANS_ID │ TCTYPE  │ CREATEDAT   │ AMOUNT │ REDEEMID        │
├──────────┼─────────┼─────────────┼────────┼─────────────────┤
│ E010     │ earned  │ 2024-01-01  │   50   │ S010 ◄─ Redeemed│
│ S010     │ spent   │ 2024-01-05  │  -30   │ NULL            │
│ S011     │ spent   │ 2024-01-06  │  -40   │ NULL ◄─ No match│
└──────────┴─────────┴─────────────┴────────┴─────────────────┘

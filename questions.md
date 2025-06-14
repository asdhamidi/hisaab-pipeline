# Expense Analytics Pipelines

## 1️⃣ Financial Trends Pipeline
**DAG Name**: `financial_trends_analysis`
**Execution Order**: Runs daily after data ingestion

|Question | Table Name | Columns | Description |
|----------|------------|---------|-------------|
|Monthly spending per user | `gold.gold_spending_per_user_monthly` | `username, year_month, total_spending` | Tracks spending habits over time |
|User entry frequency | `gold.user_entry_frequency` | `username, month, entry_count, avg_price` | Measures user engagement via entries |
| Expense statistics | `gold.expense_stats` | `expense_type, avg_amount, max_amount, avg_users_involved` | Analyzes expense patterns |
| Expense Share | `gold.gold_user_monthly_share` | `username, year_month, user_monthly_spending, monthly_total_combined, user_monthly_share`  | Analyzes expense patterns |

**PySpark Execution**:
1. `monthly_spending.py` → `user_entry_frequency.py` → `expense_stats.py`

---

## 2️⃣ Debt Reconciliation Pipeline
**DAG Name**: `debt_analysis`
**Execution Order**: Runs after financial trends

| Question | Table Name | Columns | Description |
|----------|------------|---------|-------------|
| Who owes whom | `gold.debt_ledger` | `debtor, creditor, amount, last_updated` | Complete debt network |
| User debt positions | `gold.user_net_balances` | `username, net_balance, is_creditor` | Shows who's overall in credit/debt |
| User Spend, Debt, Credit trends | `gold.gold_user_trends` | `username, year_month, mon_net_spend_change, mom_net_credited_change, mon_net_debted_change, total_paid` | Identifies frequent payers |

**PySpark Execution**:
1. `debt_calculation.py` → `net_balances.py` → `payer_frequency.py`

---

## 3️⃣ Behavioral Analytics Pipeline
**DAG Name**: `user_behavior_analysis`
**Execution Order**: Runs in parallel with financial trends

| # | Question | Table Name | Columns | Description |
|---|----------|------------|---------|-------------|
| 6 | Peak activity days | `gold.activity_peaks` | `date, activity_count, unique_users` | Identifies busiest days |
| 7 | User engagement (MAU/WAU) | `gold.user_engagement` | `username, week, month, active_days` | Tracks user participation |
| 9 | Modification patterns | `gold.modification_analysis` | `username, edit_count, delete_count` | Shows edit/delete behavior |

**PySpark Execution**:
1. `activity_peaks.py` → `engagement_metrics.py` → `modification_patterns.py`

---

## 4️⃣ Expense Patterns Pipeline
**DAG Name**: `expense_patterns_analysis`
**Execution Order**: Runs after data validation

| # | Question | Table Name | Columns | Description |
|---|----------|------------|---------|-------------|
| 3 | Frequent items | `gold.top_items` | `item_name, frequency, total_spent` | Most logged expenses |
| 10 | Split behavior | `gold.split_analysis` | `split_type, count, avg_users` | Analyzes expense sharing patterns |
| 13 | Expense categories | `gold.expense_categories` | `category, subcategory, avg_price` | Auto-categorizes expenses |

**PySpark Execution**:
1. `item_frequency.py` → `split_behavior.py` → `category_classification.py`

---

## 5️⃣ Data Quality Pipeline
**DAG Name**: `data_validation`
**Execution Order**: Runs first before other pipelines

| # | Question | Table Name | Columns | Description |
|---|----------|------------|---------|-------------|
| 8 | Invalid references | `gold.invalid_references` | `reference_type, invalid_count, sample_ids` | Data integrity checks |
| 14 | Logging delays | `gold.logging_delays` | `username, avg_delay_hours, max_delay` | Tracks entry timeliness |
| 15 | Group expense ratio | `gold.group_expense_metrics` | `month, pct_group_expenses` | Measures shared expense frequency |

**PySpark Execution**:
1. `reference_validation.py` → `delay_analysis.py` → `group_expense_ratio.py`

---

## Execution Architecture

```mermaid
graph TD
    A[Data Validation Pipeline] --> B[Financial Trends]
    A --> C[Behavioral Analytics]
    B --> D[Debt Reconciliation]
    C --> D
    A --> E[Expense Patterns]

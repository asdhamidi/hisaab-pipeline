# Expense Analytics Pipelines

## 1️⃣ Financial Trends Pipeline
**DAG Name**: `financial_trends_analysis`
**Execution Order**: Runs daily after data ingestion

|Question | Table Name | Columns | Description |
|----------|------------|---------|-------------|
|Monthly spending per user | `gold.gold_spending_per_user_monthly` | `username, year_month, total_spending` | Tracks spending habits over time |
|User entry frequency | `gold.user_entry_frequency` | `username, month, entry_count, avg_price` | Measures user engagement via entries |
| Expense Share | `gold.gold_user_monthly_share` | `username, year_month, user_monthly_spending, monthly_total_combined, user_monthly_share`  | Analyzes expense patterns |

**PySpark Execution**:
1. `monthly_spending.py` → `user_entry_frequency.py` → `user_monthly_share.py`

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

| Question | Table Name | Columns | Description |
|----------|------------|---------|-------------|
| Peak activity days | `gold.gold_activity_peaks` | `date, activity_count, unique_users` | Identifies busiest days |
| User engagement | `gold.gold_user_engagement` | `username, week, month, year, active_days, activity_count, visit_count, modification_count, create_count` | Tracks user participation |
| User activities on daily basis | `gold.gold_user_activities` | `username, is_weekend, time_of_the_day edit_count, activity_count, visit_count, modification_count, create_count, modification_ratio` | Magnifies user's activities across the day |

**PySpark Execution**:
1. `activity_peaks.py` → `user_engagement.py` → `user_activities.py`

---

## 4️⃣ Expense Patterns Pipeline
**DAG Name**: `expense_patterns_analysis`
**Execution Order**: Runs after Behavioral Analytics

| Question | Table Name | Columns | Description |
|----------|------------|---------|-------------|
| Frequent items | `gold.top_items` | `item_name, frequency, total_spent` | Most logged expenses |
| Expense categories | `gold.expense_categories` | `category, subcategory, avg_price` | Auto-categorizes expenses |
| Expense statistics | `gold.expense_stats` | `expense_type, avg_amount, max_amount, avg_users_involved` | Analyzes expense patterns on sharing type |

**PySpark Execution**:
1. `item_frequency.py` → `split_behavior.py` → `category_classification.py`

---

## Execution Architecture

```mermaid
graph TD
    A[Data Validation Pipeline] --> B[Financial Trends]
    A --> C[Behavioral Analytics]
    B --> D[Debt Reconciliation]
    C --> D
    A --> E[Expense Patterns]

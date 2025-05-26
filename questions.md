## 🧠 Analytical Questions (Gold Layer)

| #   | Analytical Question                                          | Gold Table Name           | Required Fields                                                               |
| --- | ------------------------------------------------------------ | ------------------------- | ----------------------------------------------------------------------------- |
| 1   | What’s the total spending per user per month?                | `monthly_user_spending`   | `username`, `price`, `date`, `created_by` (from Entries)                      |
| 2   | Who owes whom and how much?                                  | `debt_relationships`      | `owed_by`, `paid_by`, `owed_all`, `price`, `date` (from Entries)              |
| 3   | What items are most commonly purchased?                      | `frequent_items`          | `items[].name`, `created_by`, `price`, `date`                                 |
| 4   | How many entries does each user create per month?            | `user_entry_trends`       | `created_by`, `date` (from Entries)                                           |
| 5   | Which users are most often in debt or credit overall?        | `user_debt_summary`       | `owed_by`, `paid_by`, `owed_all`, `price`                                     |
| 6   | What are the most active days for expense logging?           | `daily_entry_activity`    | `created_at`, `created_by`                                                    |
| 7   | How active are users over time (MAU/WAU)?                    | `user_activity_metrics`   | `created_by`, `date` (Entries), `user`, `activity`, `created_at` (Activities) |
| 8   | Are there any invalid references (e.g., non-existent users)? | `dq_invalid_references`   | `created_by`, `owed_by`, `paid_by`, `user` (cross-check with Users)           |
| 9   | How often do users edit or delete entries?                   | `entry_modification_log`  | `activity`, `user`, `date`, `created_at` (from Activities)                    |
| 10  | How frequently do shared expenses involve unequal splits?    | `split_behavior_analysis` | `owed_all`, `owed_by`, `price`, `created_by`                                  |

---

### ➕ Bonus Questions

| #   | Analytical Question                                         | Gold Table Name            | Required Fields                                                       |
| --- | ----------------------------------------------------------- | -------------------------- | --------------------------------------------------------------------- |
| 11  | What’s the average expense size (value and split count)?    | `expense_statistics`       | `price`, `owed_by`, `owed_all`                                        |
| 12  | Do some users predominantly act as payers?                  | `payer_frequency_analysis` | `paid_by`, `created_by`, `price`                                      |
| 13  | What are common expense categories (inferred via items)?    | `expense_category_summary` | `items[].name`, `created_by`, `price` (Category via keyword grouping) |
| 14  | What’s the average time delay between expense date and log? | `logging_delay_analysis`   | `date`, `created_at`, `created_by`                                    |
| 15  | What percentage of expenses involve all group members?      | `groupwide_expenses`       | `owed_by`, `owed_all`, `price`                                        |

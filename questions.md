| Category              | Question                                                                 | Table Name         | Layer  | Columns Involved                      |
|-----------------------|--------------------------------------------------------------------------|--------------------|--------|----------------------------------------|
| User Activity Analysis | How many times did each user log in or open the app in a given month?    | activities         | Silver | user, activity, created_at             |
| Expense Distribution  | What is the total expense per user (paid by) and owed by others?         | entries            | Gold   | paid_by, owed_by, price                |
| User Engagement       | Which users are most active in creating/updating/deleting entries?       | activities         | Silver | user, activity, created_at             |
| Expense Trends        | What is the monthly trend of expenses (total amount spent)?              | entries            | Gold   | date, price                            |
| Debt Calculation      | How much does each user owe others (or is owed by others)?               | entries            | Gold   | paid_by, owed_by, price                |
| Entry Modification    | How many entries were updated/deleted, and by whom?                      | activities         | Silver | user, activity                         |
| User Retention        | How many users were active in consecutive months?                        | activities         | Gold   | user, date                             |
| Common Expenses       | What are the most frequently logged items (e.g., Maggie, Zepto)?         | entries            | Silver | items, price                           |
| Admin Activity        | Do admin users (from users table) log more activities than non-admins?   | users, activities  | Gold   | user, admin, activity                  |
| Data Quality          | Are there entries with invalid owed_by users (not in users table)?       | entries, users     | Silver | owed_by, username                      |

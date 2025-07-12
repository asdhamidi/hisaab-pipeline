# WIP

import re
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.window import Window

sys.path.append(str(Path(__file__).parent.parent))
from utils.utils import get_table_for_spark, write_data_to_table

# Improved regex for splitting and cleaning
SPLIT_REGEX = re.compile(r'[,&+]| and | with |/|\n', re.IGNORECASE)
QUANTITY_REGEX = re.compile(r'\b\d+\s*[xX]?\b|\b\d+\s*(?:ml|g|kg|x|l|litre|ltr?|pcs?)\b', re.IGNORECASE)

# Expanded and tuned category mapping (add more as needed)
CATEGORY_KEYWORDS = [
    # Groceries
    (r'\bmaggi\b', 'groceries'),
    (r'\bmaggie\b', 'groceries'),
    (r'\bmilk\b', 'groceries'),
    (r'\bbread\b', 'groceries'),
    (r'\begg\b', 'groceries'),
    (r'\beggs\b', 'groceries'),
    (r'\baloo\b', 'groceries'),
    (r'\bpyaaz\b', 'groceries'),
    (r'\bonion\b', 'groceries'),
    (r'\bpyaz\b', 'groceries'),
    (r'\bmushroom\b', 'groceries'),
    (r'\boil\b', 'groceries'),
    (r'\bginger\b', 'groceries'),
    (r'\bcurd\b', 'groceries'),
    (r'\bdahi\b', 'groceries'),
    (r'\bpepper\b', 'groceries'),
    (r'\btea\b', 'groceries'),
    (r'\bpaneer\b', 'groceries'),
    (r'\bsalt\b', 'groceries'),
    (r'\bwater\b', 'groceries'),
    (r'\bcoffee\b', 'groceries'),
    (r'\bgrocery\b', 'groceries'),
    (r'\bgro\b', 'groceries'),

    # Online services
    (r'\bzepto\b', 'online_groceries'),
    (r'\binstamart\b', 'online_groceries'),
    (r'\bblinkit\b', 'online_groceries'),
    (r'\bzomato\b', 'food_delivery'),
    (r'\bswiggy\b', 'food_delivery'),
    (r'\beatclub\b', 'food_delivery'),

    # Food items
    (r'\bkatyayni\b', 'food'),
    (r'\bkatiyani\b', 'food'),
    (r'\bbiryani\b', 'food_delivery'),
    (r'\bbriyani\b', 'food_delivery'),
    (r'\bsandwich\b', 'food_delivery'),
    (r'\bdosa\b', 'food_delivery'),
    (r'\bpizza\b', 'food_delivery'),
    (r'\bparota\b', 'food_delivery'),

    # Fruits
    (r'\bbanana\b', 'fruits'),

    # Transport
    (r'\bauto\b', 'transport'),
    (r'\bcab\b', 'transport'),
    (r'\btaxi\b', 'transport'),
]

def get_expense_category(item):
    """Categorizes an expense item after removing quantities/units."""
    if not item or not isinstance(item, str):
        return "other"
    cleaned_item = QUANTITY_REGEX.sub('', item).strip().lower()
    if not cleaned_item:
        return "other"
    for pattern, category in CATEGORY_KEYWORDS:
        if re.search(pattern, cleaned_item):
            return category
    return "other"

def extract_items(items_str):
    if not items_str:
        return []
    # Split on common delimiters and clean
    items = [i.strip().lower() for i in SPLIT_REGEX.split(items_str) if i.strip()]
    # Remove stopwords and very short items
    GENERAL_WORDS = {"and", "or", "for", "etc", "is", "on", "it", "the", "a", "an", "to", "with", "without", "of", ""}
    return [item for item in items if item not in GENERAL_WORDS and len(item) >= 3]

def top_items():
    spark = None
    try:
        spark = SparkSession.builder.master("local").appName("silver_activities").getOrCreate()
        extract_items_udf = F.udf(extract_items, ArrayType(StringType()))
        expense_category_udf = F.udf(get_expense_category, StringType())
        entries_df = get_table_for_spark(spark, "silver.silver_hisaab_denorm")

        # Step 1: Extract and categorize items
        exploded_df = (
            entries_df
            .filter(F.col("items").isNotNull())
            .withColumn("extracted_items", extract_items_udf(F.col("items")))
            .withColumn("item", F.explode("extracted_items"))
            .filter(F.length(F.trim(F.col("item"))) > 2)
            .withColumn("expense_category", expense_category_udf(F.col("item")))
        )

        # Step 2: Group and count categories
        group_cols = ["items", "username", "activity_created_at"]
        item_counts = (
            exploded_df
            .groupBy(*group_cols, "expense_category")
            .agg(F.count("*").alias("norm_count"))
        )

        # Step 3: Calculate totals
        total_counts = (
            item_counts
            .groupBy(*group_cols)
            .agg(F.sum("norm_count").alias("total"))
        )

        # Step 4: Identify entries where ALL items are "other"
        other_count = (
            item_counts
            .filter(F.col("expense_category") == "other")
            .groupBy(*group_cols)
            .agg(F.sum("norm_count").alias("other_count"))
        )

        all_other_entries = (
            other_count
            .join(total_counts, group_cols)
            .filter(F.col("other_count") == F.col("total"))
            .select(*group_cols, F.lit("other").alias("expense_best_category"))
        )

        # Step 5: For non-all-"other" entries, find most frequent non-"other"
        non_other_counts = (
            item_counts
            .filter(F.col("expense_category") != "other")
            .groupBy(*group_cols, "expense_category")
            .agg(F.sum("norm_count").alias("non_other_count"))
        )

        non_other_window = Window.partitionBy(*group_cols).orderBy(F.desc("non_other_count"))
        best_non_other = (
            non_other_counts
            .withColumn("rn", F.row_number().over(non_other_window))
            .filter(F.col("rn") == 1)
            .select(*group_cols, "expense_category")
        )

        # Step 6: Combine both sets of entries
        best_norm = (
            all_other_entries
            .union(best_non_other.select(*group_cols, F.col("expense_category").alias("expense_best_category")))
        )

        # Step 7: Ensure all groups are covered
        all_groups = exploded_df.select(group_cols).distinct()
        full_coverage = (
            all_groups
            .join(best_norm, group_cols, "left")
            .withColumn("expense_best_category",
                        F.coalesce(F.col("expense_best_category"), F.lit("other")))
        )

        # Step 8: Join back to assign final category
        result_df = (
            exploded_df
            .join(full_coverage, group_cols, "left")
            .withColumnRenamed("expense_best_category", "expense_final_category")
            .withColumn("expense_final_category", F.coalesce(F.col("expense_final_category"), F.lit("other")))
            .drop("expense_category")
            .dropDuplicates(["items", "item", "username", "activity_created_at"])
        )

        # Step 9: Final aggregation
        final_items = (
            result_df
            .groupBy("expense_final_category")
            .agg(
                F.count("*").alias("frequency"),
                F.collect_set("item").alias("original_phrasings")
            )
            .orderBy(F.desc("frequency"))
        )

        # Write to Gold layer
        write_data_to_table(final_items, "gold.gold_top_items_test")

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    top_items()

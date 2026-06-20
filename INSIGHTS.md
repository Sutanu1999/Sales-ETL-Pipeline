# Key Insights

Findings from running `sql/analysis_queries.sql` against the gold-layer MySQL warehouse.

- **Revenue growth**: steady growth through 2017–2018, peaking ~Nov 2017 (~₣1.18M), consistent with Black Friday seasonality.
- **Geographic concentration**: São Paulo (SP) alone accounts for ~₣5.9M revenue and 41K orders, far ahead of any other state.
- **Payments**: credit card dominates (76.8K payments, ~₣12.5M total, avg 3.5 installments) vs boleto (~₣2.9M, single installment).
- **Top categories by revenue**: health_beauty, watches_gifts, bed_bath_table.
- **Freight cost**: for some categories (e.g. home_comfort_2, flowers) freight averages 40-55% of item price — a potential pricing/logistics insight.
- **Data quality gap**: several low-rated, high-volume products have `category_name_english = NULL`, meaning their `product_category_name` didn't match any row in `category_translation` (typos/missing mappings in source data).
- **Average review score**: 4.09 / 5 across 98,410 valid reviews.

## Data Quality Incident: CSV Multi-line Field Corruption

While building the Streamlit dashboard, the "Average Review Score" KPI showed an impossible value (46.18/5). Root-cause investigation traced this back to the bronze ingestion layer:

- The raw `olist_order_reviews_dataset.csv` contains free-text review comments, some of which include embedded newlines (legitimate multi-line CSV fields per RFC 4180).
- The original bronze ingestion script read CSVs without `multiLine=true`, causing Spark to treat each physical line as a new row — silently shifting column values for any review spanning multiple lines (557 of ~104K lines were affected).
- This corruption passed silently through every layer (bronze → silver → gold → MySQL) because the original data quality checks only validated row counts, null keys, key uniqueness, and non-negative numeric values — none of which were violated by the corrupted data (text values sitting in a numeric-looking `review_score` field didn't break any structural constraint).

**Fix:**
- Enabled `.option("multiLine", "true")` and `.option("escape", '"')` on the bronze CSV reader.
- Added `.option("overwriteSchema", "true")` to bronze/silver/gold Delta writes, since the corrected schema differed from the previously (incorrectly) inferred one.
- Added `check_value_range()` to the data quality suite, validating `review_score` is within `[1, 5]` at both silver and gold layers, plus added the previously-missing `order_reviews` checks (row count, null keys, key uniqueness) to silver.

**Result:** `review_score` average corrected from a nonsensical 46.18 to 4.09 (the genuinely correct value), and ~814 true duplicate reviews were additionally caught and removed by existing deduplication logic once the schema was fixed.

**Lesson:** structural data quality checks (nulls, uniqueness, non-negativity) are necessary but not sufficient — value-range and type-conformance checks are essential for catching silent content corruption that doesn't violate any structural constraint.
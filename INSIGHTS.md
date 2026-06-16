# Key Insights

Findings from running `sql/analysis_queries.sql` against the gold-layer MySQL warehouse.

- **Revenue growth**: steady growth through 2017–2018, peaking ~Nov 2017 (₣1.18M), consistent with Black Friday seasonality.
- **Geographic concentration**: São Paulo (SP) alone accounts for ~₣5.9M revenue and 41K orders, far ahead of any other state.
- **Payments**: credit card dominates (76.8K payments, ~₣12.5M total, avg 3.5 installments) vs boleto (~₣2.9M, single installment).
- **Top categories by revenue**: health_beauty, watches_gifts, bed_bath_table.
- **Freight cost**: for some categories (e.g. home_comfort_2, flowers) freight averages 40-55% of item price — a potential pricing/logistics insight.
- **Data quality gap**: several low-rated, high-volume products have `category_name_english = NULL`, meaning their `product_category_name` didn't match any row in `category_translation` (typos/missing mappings in source data).
.. _data_tag_homogenization:

Data Tag Homogenization
=======================

One of the objectives of the Data Curator is to homogenize the names of the concepts represented by different data provider tags. Below are the prefixes we assign to each “block” or category of data rows, followed by the rules used to construct the final tag names.

Prefix Mapping
--------------

- **fbs_** – Fundamental Balance-Sheet data rows
- **fcf_** – Fundamental Cash-Flow data rows
- **fis_**  – Fundamental Income Statement data rows
- **f_**   – Fundamental Common data rows (e.g., identifiers shared across statements)
- **m_**   – Market data (daily, including unadjusted, split-adjusted, and dividend-and-split-adjusted)
- **d_**   – Dividend data rows
- **s_**   – Split data rows

Tag Construction Rules
----------------------

1. **Single Snake_Case Identifier**
   All tags must be a single “word” in `snake_case` (no spaces or hyphens), so they can be used directly as Python parameter or variable names.

2. **Full Concept Spelled Out**
   The tag spells out the full concept in underscore-joined words.
   - Avoid acronyms and abbreviations unless they are industry standards.
   - Examples:

     - `fis_revenue` (not `fis_rev`)

     - `fbs_shareholders_equity` (not `fbs_sq_eq`)

3. **Natural Word Order**
   Words appear in the order they would naturally be spoken: modifiers (adjectives) come before the main noun.
   - Example:

     - `nonoperating_income` (instead of `income_nonoperating`)

4. **Hyphenated Prefixes**
   Treat any hyphenated phrase as a non-hyphenated sequence.

   - Example:
     - Original label: “Non-Operating Income”
     - Tag: `nonoperating_income`

5. **Avoiding Ambiguity with Opposing Subconcepts**

   If a concept can represent two opposing subconcepts depending on sign (e.g., “Income (Loss)”), only the primary subconcept is used.
   - Example:

     - Use `fis_income` rather than `fis_income_loss`.


6. **Restricted Use of “net”**
   Use “net” only when the subtraction is unambiguous (i.e., net after subtracting a directly opposing concept).

   - Examples:
     - `fis_net_proceeds` implies proceeds minus direct costs.
     - `fis_net_cash_from_operating_activities` implies operating cash inflows minus operating cash outflows.

   If “net” would be ambiguous, spell out the offset using `_after_` when practical.

   - Example: use `after_tax` instead of `net_of_tax`.


7. **Reordering to Remove “of”**
   Remove the conjunction “of” unless it is part of a widely recognized concept name.
   - Example:

     - Keep `cost_of_revenue` because “Cost of Revenue” is standard.
     - Convert “Depreciation of Assets” to `asset_depreciation`.


8. **Aligning with US GAAP Taxonomy**
   When a tag matches a US GAAP taxonomy concept, name it as closely as possible while enforcing the above rules.
   If the official taxonomy label omits a key word, include it.

   - Example:
     - Taxonomy concept “Interest Income (Expense), Operating”
     - Tag: `fi_interest_income_operating` (rather than the shorter `fis_interest_income_expense`).

9. **Singular vs. Plural**
   Use the singular form for a specific subtype (e.g., `fis_interest_expense`).
   Use the plural form for general aggregates (e.g., `fis_operating_expenses`).

10. **Preferred Terminology**
    Use `stockholder` instead of “shareholder.”
    Use `noncontrolling_interest` instead of “minority_interest.”
    Only use `total` when the concept is explicitly the sum of multiple subcomponents (e.g., `fbs_total_debt_including_capital_lease_obligations`).


11. **Disambiguating Cash-Flow Terminology**
    For cash-flow concepts involving net “Increase/Decrease,” use the term `change`:
    - Example: `fcf_inventory_change` (rather than “Inventory Increase/Decrease”).

    For cash inflows, use `proceeds`; for outflows, use `payments`:

    - Examples:
      - `fcf_common_stock_issuance_proceeds`
      - `fcf_preferred_stock_dividend_payments`

By following these rules and using the prefixes listed above, Data Curator achieves a consistent, unambiguous naming convention across all data-provider tags.

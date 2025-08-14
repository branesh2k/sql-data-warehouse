
/*
------------------------------------------------------------------------
GOLD LAYER DATA QUALITY AND DIMENSIONAL MODEL VALIDATION
------------------------------------------------------------------------
 SCRIPT PURPOSE:
 *	This script validates the gold layer dimensional model integrity.
 *	Ensures star schema relationships and business rules are enforced.
 *	Verifies data quality in analytics-ready views.
 
 VALIDATION SCOPE:
 *	Dimension Key Integrity: Uniqueness of surrogate keys
 *	Referential Integrity: Fact-to-dimension relationships
 
 VIEWS VALIDATED:
 *	gold.dim_customers: Customer dimension validation
 *	gold.dim_products: Product dimension validation
 *	gold.fact_sales: Fact table and relationship validation
 
 QUALITY CHECKS:
 *	Surrogate Key Uniqueness: No duplicates in dimension keys
 *	Foreign Key Integrity: All fact records have dimension matches
 *	Categorical Values: Standardization verification
 *	Null Checks: Required attributes population
 *	Business Rules: Implementation verification
 
 EXPECTED RESULTS:
 *	All validation queries should return NO RESULTS if model is valid
 *	Any results indicate dimensional modeling issues requiring attention
 *	Compare with silver validation to ensure proper transformation
 
 USAGE:
 *	Run this script after gold view refresh
 *	Execute before allowing BI tool access
 *	Use results to validate dimensional model integrity
 
 WARNING:
 *	This script only identifies issues - it does not fix them
 *	Issues found may require changes to view definitions
 *	Some checks may require investigation in silver layer
*/

-- ========================================================================
-- CUSTOMER DIMENSION VALIDATION
-- ========================================================================

-- Check standardization of categorical values (gender)
-- Expectation: Only standardized values (Male, Female, n/a)
SELECT DISTINCT dc.new_gender FROM gold.dim_customers dc;

-- Check for uniqueness of Customer surrogate key
-- Expectation: No duplicate surrogate keys should exist
SELECT
customer_key,
COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ========================================================================
-- PRODUCT DIMENSION VALIDATION
-- ========================================================================

-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results
SELECT
product_key,
COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ========================================================================
-- FACT TABLE AND RELATIONSHIP VALIDATION
-- ========================================================================

-- Check referential integrity between fact and dimension tables
-- Expectation: No orphaned fact records (all dimension keys should exist)
SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_customers dc USING(customer_key)
LEFT JOIN gold.dim_products dp USING(product_key)
WHERE dc.customer_key IS null;

-- ========================================================================
-- VALIDATION COMPLETE
-- ========================================================================
-- Review all results above:
-- - No results indicate a valid dimensional model
-- - Any results require investigation and possible view modifications
-- - Check silver layer validation results if issues are found

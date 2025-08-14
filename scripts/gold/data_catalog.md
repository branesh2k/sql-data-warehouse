# Gold Layer Data Catalog

This catalog documents the analytics-ready views in the gold layer of our data warehouse. The gold layer implements a star schema dimensional model optimized for business intelligence and reporting.It consists of two dimension views (customers and products) and one fact view (sales) arranged in a star schema pattern.

## Dimensional Model Views

### 1. gold.dim_customers

**Purpose**: Primary customer dimension containing integrated customer attributes from both CRM and ERP systems. Provides a unified, deduplicated view of customer information with standardized categorical values.

| Column Name     | Data Type   | Description                         | Example         |
| --------------- | ----------- | ----------------------------------- | --------------- |
| customer_key    | INTEGER     | Surrogate key for dimensional joins | 1001            |
| customer_id     | INTEGER     | Natural key from CRM (cst_id)       | 5432            |
| customer_number | VARCHAR(50) | Business identifier (cst_key)       | "CUST_789"      |
| first_name      | VARCHAR(50) | Customer's first name               | "John"          |
| last_name       | VARCHAR(50) | Customer's last name                | "Smith"         |
| country         | VARCHAR(50) | Customer's country from ERP         | "United States" |
| marital_status  | VARCHAR(50) | Standardized marital status         | "Married"       |
| new_gender      | VARCHAR(50) | Standardized gender, CRM preferred  | "Male"          |
| birthdate       | DATE        | Customer's birth date from ERP      | "1990-05-15"    |
| create_date     | DATE        | Customer record creation date       | "2023-01-01"    |

### 2. gold.dim_products

**Purpose**: Product dimension combining CRM product details with ERP categorization. Implements SCD Type 2 with historical tracking through start/end dates and filters to current products only.

| Column Name    | Data Type   | Description                         | Example             |
| -------------- | ----------- | ----------------------------------- | ------------------- |
| product_key    | INTEGER     | Surrogate key for dimensional joins | 2001                |
| product_id     | INTEGER     | Natural key from CRM (prd_id)       | 789                 |
| product_number | VARCHAR(50) | Business identifier (prd_key)       | "PRD-2023-456"      |
| product_name   | VARCHAR(50) | Product's display name              | "Mountain Bike Pro" |
| category_id    | VARCHAR(50) | Category identifier                 | "BIKE_1"            |
| category       | VARCHAR(50) | Main product category               | "Bikes"             |
| subcategory    | VARCHAR(50) | Product subcategory                 | "Mountain"          |
| maintenance    | VARCHAR(50) | Maintenance level required          | "Medium"            |
| cost           | INTEGER     | Product cost                        | 599                 |
| product_line   | VARCHAR(50) | Standardized product line           | "Mountain"          |
| start_date     | DATE        | Product version start date          | "2023-01-01"        |

### 3. gold.fact_sales

**Purpose**: Primary fact table containing sales transaction measures with surrogate keys linking to dimension views. Provides the central metrics for sales analysis and reporting.

| Column Name   | Data Type   | Description                | Example         |
| ------------- | ----------- | -------------------------- | --------------- |
| order_number  | VARCHAR(50) | Natural key for order      | "ORD-2023-1234" |
| product_key   | INTEGER     | FK to dim_products         | 2001            |
| customer_key  | INTEGER     | FK to dim_customers        | 1001            |
| order_date    | DATE        | Date order was placed      | "2023-06-15"    |
| shipping_date | DATE        | Date order was shipped     | "2023-06-17"    |
| due_date      | DATE        | Order due date             | "2023-06-20"    |
| sales_amount  | INTEGER     | Total sale amount          | 1299            |
| quantity      | INTEGER     | Number of units sold       | 2               |
| price         | INTEGER     | Unit price at time of sale | 649             |

## Usage Notes

1. Always join fact_sales to dimensions using the surrogate keys (product_key, customer_key)
2. The dim_products view only contains current products (prd_end_dt IS NULL)
3. Gender values are standardized to 'Male', 'Female', 'n/a'
4. Country codes are standardized to full country names
5. All date fields are properly formatted DATE type for easy analysis

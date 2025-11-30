# Data Catalog — Gold Layer

The Gold Layer represents the semantic business model of the data warehouse.  
All objects are modeled as views and optimized for analytical workloads, reporting, and BI consumption.

It follows a classic star schema design consisting of dimension views and a fact view.

---

## 1. gold.dim_customers

**Purpose**  
Business dimension storing customer master data enriched with demographic and geographic attributes sourced from CRM and ERP systems.

**Columns**

| Column Name     | Data Type     | Description                                                                 |
|-----------------|---------------|-----------------------------------------------------------------------------|
| customer_key    | INT           | Surrogate key used for joining fact records to the customer dimension.      |
| customer_id     | INT           | Natural key of the customer from the operational system.                    |
| customer_number | NVARCHAR(50)  | Business identifier used by CRM for customer tracking.                      |
| first_name      | NVARCHAR(50)  | Customer's given name.                                                       |
| last_name       | NVARCHAR(50)  | Customer's family name.                                                      |
| country         | NVARCHAR(50)  | Standardized country of residence.                                           |
| marital_status  | NVARCHAR(50)  | Customer’s marital status as provided by CRM.                               |
| gender          | NVARCHAR(50)  | Standardized gender attribute sourced from ERP.                              |
| birthdate       | DATE          | Customer's date of birth.                                                    |
| create_date     | DATE          | Record creation date from the CRM system.                                   |

---

## 2. gold.dim_products

**Purpose**  
Dimension providing standardized product attributes including category, subcategory, maintenance flags, and commercial classification.

**Columns**

| Column Name          | Data Type     | Description                                                               |
|----------------------|---------------|---------------------------------------------------------------------------|
| product_key          | INT           | Surrogate key used for analytical joins.                                  |
| product_id           | INT           | Natural product identifier from CRM.                                      |
| product_number       | NVARCHAR(50)  | Business product code used by operational systems.                        |
| product_name         | NVARCHAR(50)  | Human-readable product name.                                              |
| category_id          | NVARCHAR(50)  | High-level classification identifier derived from ERP.                    |
| category             | NVARCHAR(50)  | Standardized product category.                                            |
| subcategory          | NVARCHAR(50)  | Detailed subcategory for finer segmentation.                              |
| maintenance_required | NVARCHAR(50)  | Indicates whether the product requires maintenance.                       |
| cost                 | INT           | Product cost value provided by CRM.                                       |
| product_line         | NVARCHAR(50)  | Commercial grouping or product line.                                      |
| start_date           | DATE          | Availability start date in the sales catalog.                             |

---

## 3. gold.fact_sales

**Purpose**  
Fact view representing individual sales transactions.  
Stores numeric metrics and foreign keys linking to dimension views.

**Columns**

| Column Name   | Data Type     | Description                                                                |
|---------------|---------------|----------------------------------------------------------------------------|
| order_number  | NVARCHAR(50)  | Unique order identifier.                                                    |
| product_key   | INT           | Foreign key referencing dim_products.                                      |
| customer_key  | INT           | Foreign key referencing dim_customers.                                     |
| order_date    | DATE          | Date when the order was placed.                                            |
| shipping_date | DATE          | Date when the ordered item was shipped.                                    |
| due_date      | DATE          | Payment due date for the order.                                            |
| sales_amount  | INT           | Calculated sales amount (quantity × price).                                |
| quantity      | INT           | Units sold for the line item.                                              |
| price         | INT           | Unit price of the product for the transaction.                             |

---

# Notes

- All Gold Layer objects are views, not physical tables.  
- Surrogate keys (product_key, customer_key) are generated in the Silver Layer.  
- Gold Layer fields are standardized and business-friendly to support BI and reporting tools.


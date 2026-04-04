
===============================================================================
-- Create Gold Views
-- Create Dimension: gold.dim_customers



IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
select
	ROW_Number() OVER (Order by cst_id) AS customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.cntry as country,
	ci.cst_marital_status as marital_status,
	case when ci.cst_gndr != 'n/a' Then ci.cst_gndr   ----- CRM is master for gender info
		else COALESCE (ca.gen, 'n/a')
	END As gender,
	ca.bdate as birthdate,
	ci.cst_create_date as create_date	

from Silver.Crm_cust_info ci
Left join silver.erp_cust_az12 ca
--on cast(ci.cst_key as nvarchar(50)) = Cast(ca.cid as nvarchar(50))
on ci.cst_key = ca.cid
Left join silver.erp_loc_a101 la
ON CAST(ci.cst_key AS NVARCHAR(50)) = cast(la.cid as nvarchar(50))

GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS

select
		Row_number() Over(order by pn.prd_start, pn.prd_key) As product_key,
		pn.prd_id As product_id,
		pn.prd_key as product_number,
		pn.prd_nm product_name,
		pn.cat_id category_id,
		pc.cat category,
		pc.subcat subcategory,
		pn.prd_cost cost,
		pc.maintenance maintenance,
		pn.prd_line product_line,
		pn.prd_start start_date
		--pn.prd_end_date

	from silver.Crm_prd_info pn

	Left join silver.erp_px_cat_g1v2 pc
	on pn.cat_id = pc.id
	where prd_end_date IS NUll

GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS

Select
	sd.sls_ord_num as order_number,
	pr.product_key as product_key,
	cu.customer_key as customer_key,
	sd.sls_order_dt as order_date,
	sd.sls_ship_dt as shipping_date,
	sd.sls_due_dt as due_date,
	sd.sls_sales as sales_amount,
	sd.sls_quantity as quantity,
	sd.sls_price as price
from silver.Crm_sales_details sd
left join gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id;

GO

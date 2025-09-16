drop procedure if exists bronze.load_bronze();
create procedure bronze.load_bronze()
language plpgsql
as $$
begin
	raise notice '==============================';
	raise warning 'Loading Bronze Layer';
	raise notice '==============================';

	raise notice '==============================';
	raise warning 'Loading CRM Tables';
	raise notice '==============================';
	
	truncate table bronze.crm_cust_info;
	copy bronze.crm_cust_info
	from '/Users/hongrmuckaev/Desktop/studying/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
	with (format csv, header, delimiter ',');
	
	truncate table bronze.crm_prd_info;
	copy bronze.crm_prd_info
	from '/Users/hongrmuckaev/Desktop/studying/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
	with (format csv, header, delimiter ',');
	
	truncate table bronze.crm_sales_details;
	copy bronze.crm_sales_details
	from '/Users/hongrmuckaev/Desktop/studying/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
	with (format csv, header, delimiter ',');

	raise notice '==============================';
	raise warning 'Loading ERP Tables';
	raise notice '==============================';
	
	truncate table bronze.erp_cust_az12;
	copy bronze.erp_cust_az12
	from '/Users/hongrmuckaev/Desktop/studying/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
	with (format csv, header, delimiter ',');
	
	truncate table bronze.erp_loc_a101;
	copy bronze.erp_loc_a101
	from '/Users/hongrmuckaev/Desktop/studying/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
	with (format csv, header, delimiter ',');
	
	truncate table bronze.erp_px_cat_g1v2;
	copy bronze.erp_px_cat_g1v2
	from '/Users/hongrmuckaev/Desktop/studying/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
	with (format csv, header, delimiter ',');
end;
$$;

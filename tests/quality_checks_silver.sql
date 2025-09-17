/*
===============================================================================
Проверка качества данных
===============================================================================
Назначение скрипта:
    Этот скрипт выполняет различные проверки качества для обеспечения 
    согласованности, точности и стандартизации данных в слое 'silver'. 
    Проверки включают:
    - Null или дубли в первичных ключах.
    - Нежелательные пробелы в строковых полях.
    - Стандартизацию и согласованность данных.
    - Неверные диапазоны и порядок дат.
    - Согласованность данных между связанными полями.

Примечания по использованию:
    - Запускайте эти проверки после загрузки данных в слой silver.
    - При обнаружении несоответствий необходимо их проанализировать и устранить.
===============================================================================
*/

-- ====================================================================
-- Проверка 'silver.crm_cust_info'
-- ====================================================================
-- Проверка на NULL или дубликаты в первичном ключе
-- Ожидание: результатов быть не должно
select 
    cst_id,
    count(*) 
from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null;

-- Проверка на лишние пробелы
-- Ожидание: результатов быть не должно
select 
    cst_key 
from silver.crm_cust_info
where cst_key != trim(cst_key);

-- Стандартизация и согласованность данных
select distinct 
    cst_marital_status 
from silver.crm_cust_info;

-- ====================================================================
-- Проверка 'silver.crm_prd_info'
-- ====================================================================
-- Проверка на NULL или дубликаты в первичном ключе
-- Ожидание: результатов быть не должно
select 
    prd_id,
    count(*) 
from silver.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null;

-- Проверка на лишние пробелы
-- Ожидание: результатов быть не должно
select 
    prd_nm 
from silver.crm_prd_info
where prd_nm != trim(prd_nm);

-- Проверка на NULL или отрицательные значения стоимости
-- Ожидание: результатов быть не должно
select 
    prd_cost 
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null;

-- Стандартизация и согласованность данных
select distinct 
    prd_line 
from silver.crm_prd_info;

-- Проверка на некорректный порядок дат (начало > конец)
-- Ожидание: результатов быть не должно
select 
    * 
from silver.crm_prd_info
where prd_end_dt < prd_start_dt;

-- ====================================================================
-- Проверка 'silver.crm_sales_details'
-- ====================================================================
-- Проверка на некорректные даты
-- Ожидание: некорректных дат быть не должно
select 
    nullif(sls_due_dt, 0) as sls_due_dt 
from bronze.crm_sales_details
where sls_due_dt <= 0 
    or length(sls_due_dt::text) != 8 
    or sls_due_dt > 20500101 
    or sls_due_dt < 19000101;

-- Проверка на порядок дат (order_dt > ship/due)
-- Ожидание: результатов быть не должно
select 
    * 
from silver.crm_sales_details
where sls_order_dt > sls_ship_dt 
   or sls_order_dt > sls_due_dt;

-- Проверка согласованности данных: sales = quantity * price
-- Ожидание: результатов быть не должно
select distinct 
    sls_sales,
    sls_quantity,
    sls_price 
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
   or sls_sales is null 
   or sls_quantity is null 
   or sls_price is null
   or sls_sales <= 0 
   or sls_quantity <= 0 
   or sls_price <= 0
order by sls_sales, sls_quantity, sls_price;

-- ====================================================================
-- Проверка 'silver.erp_cust_az12'
-- ====================================================================
-- Проверка дат рождения вне диапазона
-- Ожидание: даты рождения между 1924-01-01 и текущей датой
select distinct 
    bdate 
from silver.erp_cust_az12
where bdate < '1924-01-01' 
   or bdate > current_date;

-- Стандартизация и согласованность данных
select distinct 
    gen 
from silver.erp_cust_az12;

-- ====================================================================
-- Проверка 'silver.erp_loc_a101'
-- ====================================================================
-- Стандартизация и согласованность данных
select distinct 
    cntry 
from silver.erp_loc_a101
order by cntry;

-- ====================================================================
-- Проверка 'silver.erp_px_cat_g1v2'
-- ====================================================================
-- Проверка на лишние пробелы
-- Ожидание: результатов быть не должно
select 
    * 
from silver.erp_px_cat_g1v2
where cat != trim(cat) 
   or subcat != trim(subcat) 
   or maintenance != trim(maintenance);

-- Стандартизация и согласованность данных
select distinct 
    maintenance 
from silver.erp_px_cat_g1v2;

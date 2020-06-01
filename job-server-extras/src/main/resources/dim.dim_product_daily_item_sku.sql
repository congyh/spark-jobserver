SELECT
    max(division_id_new) as division_id_new,
    max(division_name_new) as division_name_new,
    max(pur_first_dept_cd) as ad_first_dept_id,
    max(pur_first_dept_name) as ad_first_dept_name,
    max(pur_second_dept_cd) as ad_second_dept_id,
    max(pur_second_dept_name) as ad_second_dept_name,
    max(pur_third_dept_cd) as ad_third_dept_id,
    max(pur_third_dept_name) as ad_third_dept_name,
    max(item_first_cate_cd) as item_first_cate_cd,
    max(item_first_cate_name) as item_first_cate_name,
    max(brand_code) as ad_sku_brand_code,
    max(barndname_full) as ad_sku_brandname_full,
    max(type) as type_raw,
    item_sku_id AS sku_id
FROM
    dim.dim_product_daily_item_sku
WHERE
    dt = '2020-05-20' and dp = 'active'
group BY
    item_sku_id
CLUSTER BY
    sku_id
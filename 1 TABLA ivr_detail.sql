-- TABLA ivr_detail
CREATE OR REPLACE TABLE keepcoding.ivr_detail AS

SELECT steps.ivr_id AS calls_ivr_id 
     , phone_number AS calls_phone_number
     , ivr_result AS calls_ivr_result    
     , vdn_label AS calls_vdn_label  
     , start_date AS calls_start_date 
     , safe_cast (FORMAT_DATE('%Y%m%d', start_date) as string) AS calls_start_date_id
     , end_date   AS calls_end_date  
     , SAFE_CAST (FORMAT_DATE('%Y%m%d', end_date) AS STRING) AS end_date_id
     , total_duration AS calls_total_duration	
     , customer_segment   AS calls_customer_segment  
     , ivr_language     AS calls_ivr_language
     , steps_module AS calls_steps_module    
     , module_aggregation AS calls_module_aggregation
     , steps.module_sequece  
     , module_name
     , module_duration
     , module_result
     , step_sequence
     , step_name
     , step_result
     , step_description_error
     , document_type
     , document_identification
     , customer_phone
     , billing_account_id

  FROM `keepcodingdayana.keepcoding.ivr_steps` steps
  left 
  JOIN `keepcodingdayana.keepcoding.ivr_modules`
    ON steps.ivr_id = `keepcodingdayana.keepcoding.ivr_modules`.ivr_id
    and steps.module_sequece = `keepcodingdayana.keepcoding.ivr_modules`.module_sequece


 LEFT 
 JOIN   `keepcodingdayana.keepcoding.ivr_calls`
    ON steps.ivr_id =`keepcodingdayana.keepcoding.ivr_calls`.ivr_id


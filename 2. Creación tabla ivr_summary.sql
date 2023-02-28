CREATE OR REPLACE TABLE keepcoding.ivr_summary AS
 SELECT d.calls_ivr_id
        , d.calls_phone_number
        ,d.calls_ivr_result ,
            CASE WHEN d.calls_vdn_label LIKE 'ATC%' THEN 'FRONT'
                WHEN d.calls_vdn_label LIKE 'TECH%' THEN 'TECH'
                WHEN d.calls_vdn_label = 'ABSORPTION' THEN 'Absorption'
            ELSE 'RESTO'
        END AS vdn_aggregation
        , d.calls_start_date 
        , d.calls_total_duration
        , d.calls_customer_segment  
        , d.calls_ivr_language
        , d.calls_steps_module 
        , d.calls_module_aggregation
        , IFNULL(NULLIF(d.document_type,'NULL'),"DESCONOCIDO") AS document_type
        , IFNULL(NULLIF(d.document_identification,'NULL'),"DESCONOCIDO") AS document_identification
        , IFNULL(NULLIF(d.customer_phone,'NULL'),"DESCONOCIDO") AS customer_phone
        , IFNULL(NULLIF(d.billing_account_id,'NULL'),"DESCONOCIDO") AS  billing_account_id 
        , case 
            when d.calls_module_aggregation like '%Averia_masiva%' THEN 1 
            ELSE 0
            END AS masiva_lg
            
        , if (d.step_name = 'CUSTOMERINFOBYPHONE.TX' AND d.step_description_error = 'NULL',1,0) AS INFO_BY_PHONE_LG
        , if (d.step_name = 'CUSTOMERINFOBYDNI.TX' AND d.step_description_error = 'NULL',1,0) AS INFO_BY_DNI_LG
        , if (TIMESTAMP_DIFF (d.calls_start_date, LAG(d.calls_start_date) OVER (ORDER BY d.calls_phone_number),SECOND)<86400
              AND d.calls_phone_number = LAG(d.calls_phone_number) OVER  (ORDER BY d.calls_phone_number),1,0) AS repeated_phone_24H
        , if (TIMESTAMP_DIFF (LEAD (d.calls_start_date) OVER (ORDER BY d.calls_phone_number),d.calls_start_date,SECOND)<86400
              AND d.calls_phone_number = LEAD(d.calls_phone_number) OVER  (ORDER BY d.calls_phone_number),1,0) AS cause_recall_phone_24h

        FROM `keepcodingdayana.keepcoding.ivr_detail` d
        LEFT 
        JOIN `keepcodingdayana.keepcoding.ivr_detail` dBIS
        ON d.calls_ivr_id= dBIS.calls_ivr_id
        group by  d.calls_ivr_id
                , d.calls_phone_number
                ,d.calls_ivr_result
                ,vdn_aggregation 
                , d.calls_start_date 
                , d.calls_end_date  
                , d.calls_total_duration
                , d.calls_customer_segment   
                , d.calls_ivr_language
                , d.calls_steps_module 
                , d.calls_module_aggregation                
                , DOCUMENT_TYPE
                , document_identification
                , customer_phone
                , billing_account_id
                , masiva_lg
                , INFO_BY_PHONE_LG
                , INFO_BY_DNI_LG

                   QUALIFY ROW_NUMBER() OVER(PARTITION BY CAST(d.calls_ivr_id AS string) ORDER BY document_type NULLS LAST, 
                document_identification NULLS LAST, customer_phone NULLS LAST, billing_account_id NULLS LAST) = 1

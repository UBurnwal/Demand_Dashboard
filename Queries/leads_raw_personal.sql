-- Leads
WITH raw_data AS (

SELECT
    _lead.id,
    date_trunc('month',_lead.sf_created_at )::DATE AS sf_created_at,
    _lead.status,
    lead_det.converted,
    max_customer_created_at,
    lead_det.first_form_source,
    lead_det.geo_region_curated,
    lead_det.cluster_id,
    phones.customer_id,
    lead_attr.platform,
    lead_attr.source,
    phones.number AS mobile,
    CASE WHEN lead_attr.attribution = 5  THEN 'FOS' 
    WHEN lead_attr.attribution = 3 THEN 'attribution_paid'
    WHEN lead_attr.attribution = 7 THEN 'attribution_referrer'
    ELSE 'attribution_organic' END AS attribution
    FROM 
    sf_leads _lead
    INNER JOIN sf_lead_spot_details lead_det
        ON lead_det.lead_spot_id = _lead.id
    LEFT JOIN sf_lead_spot_detail_phones phones
    ON phones.detail_id = lead_det.id
    LEFT JOIN sf_lead_marketing_attributions lead_attr
    ON LEAD_attr.lead_id = _lead.id
    WHERE (_lead.sf_created_at )::DATE >=  '{start_date}'
    AND (_lead.sf_created_at )::DATE <  '{end_date}'
    AND type = 'Sf::Lead::Spot'
    AND object_type = 1
    AND lead_det.lead_source <> 'Secondary Data' and lower(lead_det.frequency) = 'personal'
    
) 

SELECT 
sf_created_at AS MONTH, 
geo_region_curated AS name,
attribution, 
count(id) AS leads
FROM raw_data 
GROUP BY 1,2,3
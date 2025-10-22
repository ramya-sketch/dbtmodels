-- dbt model for PS_Demo_Dataset.Pharma_Data.Patients table
-- This model provides a clean, transformed view of patient data

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['patient_id'], 'unique': True},
        {'columns': ['date_registered']},
        {'columns': ['insurance_provider']},
        {'columns': ['gender']}
    ]
) }}

with source_data as (
    select 
        patient_id,
        first_name,
        last_name,
        email,
        phone_number,
        date_of_birth,
        address,
        gender,
        insurance_provider,
        insurance_policy_number,
        date_registered,
        primary_doctor_id,
        -- Add calculated fields
        case 
            when gender = 'Male' then 'M'
            when gender = 'Female' then 'F'
            when gender = 'Other' then 'O'
            else 'U'
        end as gender_code,
        
        -- Extract age from date_of_birth
        case 
            when date_of_birth is not null then
                datediff(year, date_of_birth, getdate()) - 
                case 
                    when dateadd(year, datediff(year, date_of_birth, getdate()), date_of_birth) > getdate() 
                    then 1 
                    else 0 
                end
            else null
        end as age,
        
        -- Extract registration year and month for analysis
        year(date_registered) as registration_year,
        month(date_registered) as registration_month,
        datename(month, date_registered) as registration_month_name,
        
        -- Create full name
        concat(first_name, ' ', last_name) as full_name,
        
        -- Extract domain from email
        case 
            when email like '%@%' then
                substring(email, charindex('@', email) + 1, len(email))
            else null
        end as email_domain,
        
        -- Categorize insurance providers
        case 
            when insurance_provider in ('Blue Cross Blue Shield', 'Anthem', 'Independence Blue Cross') then 'Blue Cross Family'
            when insurance_provider in ('Medicare', 'Medicaid') then 'Government'
            when insurance_provider in ('Aetna', 'Cigna', 'UnitedHealth', 'Humana') then 'Commercial'
            else 'Other'
        end as insurance_category,
        
        -- Age group categorization
        case 
            when date_of_birth is null then 'Unknown'
            when datediff(year, date_of_birth, getdate()) < 18 then 'Under 18'
            when datediff(year, date_of_birth, getdate()) between 18 and 30 then '18-30'
            when datediff(year, date_of_birth, getdate()) between 31 and 50 then '31-50'
            when datediff(year, date_of_birth, getdate()) between 51 and 65 then '51-65'
            when datediff(year, date_of_birth, getdate()) > 65 then 'Over 65'
            else 'Unknown'
        end as age_group,
        
        -- Registration recency
        case 
            when date_registered >= dateadd(day, -30, getdate()) then 'Last 30 days'
            when date_registered >= dateadd(day, -90, getdate()) then 'Last 90 days'
            when date_registered >= dateadd(day, -365, getdate()) then 'Last year'
            else 'Older than 1 year'
        end as registration_recency,
        
        -- Current timestamp for data freshness
        getdate() as dbt_updated_at
        
    from dqlabs.airbyte.Patients
),

-- Add data quality checks
quality_checks as (
    select 
        *,
        -- Data quality flags
        case when first_name is null or first_name = '' then 1 else 0 end as missing_first_name,
        case when last_name is null or last_name = '' then 1 else 0 end as missing_last_name,
        case when email is null or email = '' or email not like '%@%' then 1 else 0 end as invalid_email,
        case when phone_number is null or phone_number = '' then 1 else 0 end as missing_phone,
        case when date_of_birth is null then 1 else 0 end as missing_dob,
        case when insurance_provider is null or insurance_provider = '' then 1 else 0 end as missing_insurance,
        case when date_registered is null then 1 else 0 end as missing_registration_date
    from source_data
)

select 
    patient_id,
    first_name,
    last_name,
    full_name,
    email,
    email_domain,
    phone_number,
    date_of_birth,
    age,
    age_group,
    address,
    gender,
    gender_code,
    insurance_provider,
    insurance_category,
    insurance_policy_number,
    date_registered,
    registration_year,
    registration_month,
    registration_month_name,
    registration_recency,
    primary_doctor_id,
    
    -- Data quality indicators
    case when (missing_first_name + missing_last_name + invalid_email + missing_phone + missing_dob + missing_insurance + missing_registration_date) = 0 
         then 'Complete' 
         else 'Incomplete' 
    end as data_quality_status,
    
    (missing_first_name + missing_last_name + invalid_email + missing_phone + missing_dob + missing_insurance + missing_registration_date) as data_quality_score,
    
    dbt_updated_at

from quality_checks

-- Add row-level security if needed
-- where {{ var('patient_data_filter', '1=1') }}

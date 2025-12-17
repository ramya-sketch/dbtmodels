-- dbt model: Patient Insights using ref() function
-- This model demonstrates how to use ref() to reference the patients table
{{
    config(
        enabled=false
        materialized="view",
        indexes=[
            {"columns": ["age_group"]},
            {"columns": ["insurance_category"]},
            {"columns": ["registration_year"]},
        ],
    )
}}

-- Patient demographics analysis using ref()
with
    patient_demographics as (
        select
            age_group,
            gender,
            insurance_category,
            count(*) as patient_count,
            avg(age) as avg_age,
            min(age) as min_age,
            max(age) as max_age,
            sum(
                case when data_quality_status = 'Complete' then 1 else 0 end
            ) as complete_records,
            avg(data_quality_score) as avg_quality_score
        from {{ ref("patients") }}
        group by age_group, gender, insurance_category
    ),

    -- Registration trends using ref()
    registration_trends as (
        select
            registration_year,
            registration_month,
            registration_month_name,
            count(*) as monthly_registrations,
            avg(data_quality_score) as avg_quality_score,
            sum(
                case when data_quality_status = 'Complete' then 1 else 0 end
            ) as complete_records,
            count(distinct insurance_provider) as unique_insurance_providers
        from {{ ref("patients") }}
        group by registration_year, registration_month, registration_month_name
    ),

    -- Insurance analysis using ref()
    insurance_analysis as (
        select
            insurance_provider,
            insurance_category,
            count(*) as patient_count,
            count(distinct email_domain) as unique_email_domains,
            avg(age) as avg_age,
            sum(
                case when data_quality_status = 'Complete' then 1 else 0 end
            ) as complete_records,
            avg(data_quality_score) as avg_quality_score,
            count(distinct primary_doctor_id) as unique_doctors
        from {{ ref("patients") }}
        group by insurance_provider, insurance_category
    ),

    -- Data quality analysis using ref()
    data_quality_analysis as (
        select
            data_quality_status,
            count(*) as record_count,
            avg(data_quality_score) as avg_quality_score,
            min(data_quality_score) as min_quality_score,
            max(data_quality_score) as max_quality_score,
            avg(age) as avg_age,
            count(distinct insurance_provider) as unique_insurance_providers
        from {{ ref("patients") }}
        group by data_quality_status
    ),

    -- Recent activity analysis using ref()
    recent_activity as (
        select
            case
                when date_registered >= dateadd(day, -7, getdate())
                then 'Last 7 days'
                when date_registered >= dateadd(day, -30, getdate())
                then 'Last 30 days'
                when date_registered >= dateadd(day, -90, getdate())
                then 'Last 90 days'
                else 'Older than 90 days'
            end as registration_period,
            count(*) as patient_count,
            avg(data_quality_score) as avg_quality_score,
            sum(
                case when data_quality_status = 'Complete' then 1 else 0 end
            ) as complete_records
        from {{ ref("patients") }}
        group by
            case
                when date_registered >= dateadd(day, -7, getdate())
                then 'Last 7 days'
                when date_registered >= dateadd(day, -30, getdate())
                then 'Last 30 days'
                when date_registered >= dateadd(day, -90, getdate())
                then 'Last 90 days'
                else 'Older than 90 days'
            end
    ),

    -- Email domain analysis using ref()
    email_domain_analysis as (
        select
            email_domain,
            count(*) as patient_count,
            avg(age) as avg_age,
            count(distinct insurance_provider) as unique_insurance_providers,
            sum(
                case when data_quality_status = 'Complete' then 1 else 0 end
            ) as complete_records
        from {{ ref("patients") }}
        where email_domain is not null
        group by email_domain
    ),

    -- Doctor workload analysis using ref()
    doctor_workload as (
        select
            primary_doctor_id,
            count(*) as patient_count,
            avg(age) as avg_age,
            count(distinct insurance_provider) as unique_insurance_providers,
            sum(
                case when data_quality_status = 'Complete' then 1 else 0 end
            ) as complete_records,
            avg(data_quality_score) as avg_quality_score
        from {{ ref("patients") }}
        group by primary_doctor_id
    )

-- Final insights combining all analyses
select
    'demographics' as insight_type,
    age_group as category_1,
    gender as category_2,
    insurance_category as category_3,
    patient_count as metric_value,
    avg_age as secondary_metric,
    complete_records as quality_metric,
    avg_quality_score as quality_score,
    getdate() as analysis_timestamp
from patient_demographics

union all

select
    'registration_trends' as insight_type,
    cast(registration_year as varchar) as category_1,
    registration_month_name as category_2,
    cast(registration_month as varchar) as category_3,
    monthly_registrations as metric_value,
    avg_quality_score as secondary_metric,
    complete_records as quality_metric,
    unique_insurance_providers as quality_score,
    getdate() as analysis_timestamp
from registration_trends

union all

select
    'insurance_analysis' as insight_type,
    insurance_provider as category_1,
    insurance_category as category_2,
    cast(unique_email_domains as varchar) as category_3,
    patient_count as metric_value,
    avg_age as secondary_metric,
    complete_records as quality_metric,
    unique_doctors as quality_score,
    getdate() as analysis_timestamp
from insurance_analysis

union all

select
    'data_quality' as insight_type,
    data_quality_status as category_1,
    cast(avg_quality_score as varchar) as category_2,
    cast(unique_insurance_providers as varchar) as category_3,
    record_count as metric_value,
    avg_age as secondary_metric,
    min_quality_score as quality_metric,
    max_quality_score as quality_score,
    getdate() as analysis_timestamp
from data_quality_analysis

union all

select
    'recent_activity' as insight_type,
    registration_period as category_1,
    cast(avg_quality_score as varchar) as category_2,
    cast(complete_records as varchar) as category_3,
    patient_count as metric_value,
    avg_quality_score as secondary_metric,
    complete_records as quality_metric,
    0 as quality_score,
    getdate() as analysis_timestamp
from recent_activity

union all

select
    'email_domains' as insight_type,
    email_domain as category_1,
    cast(avg_age as varchar) as category_2,
    cast(unique_insurance_providers as varchar) as category_3,
    patient_count as metric_value,
    avg_age as secondary_metric,
    complete_records as quality_metric,
    0 as quality_score,
    getdate() as analysis_timestamp
from email_domain_analysis

union all

select
    'doctor_workload' as insight_type,
    cast(primary_doctor_id as varchar) as category_1,
    cast(avg_age as varchar) as category_2,
    cast(unique_insurance_providers as varchar) as category_3,
    patient_count as metric_value,
    avg_age as secondary_metric,
    complete_records as quality_metric,
    avg_quality_score as quality_score,
    getdate() as analysis_timestamp
from doctor_workload

order by insight_type, metric_value desc

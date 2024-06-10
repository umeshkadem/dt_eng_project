-- Configuration
{% if target.name == 'dev' %}
    {{ config(
        materialized='table',
        schema='DB_DEV_SCHEMA',
        database='DB_DEV'
    ) }}
{% elif target.name == 'qa' %}
    {{ config(
        materialized='table',
        schema='DB_QA_SCHEMA',
        database='DB_QA'
    ) }}
{% endif %}


-- first
with s_s as (
    select ss_hdemo_sk, ss_sold_time_sk, ss_store_sk 
    from {{ source('snowflake_sample_data', 'store_sales') }}
),
h_d as (
    select hd_demo_sk, hd_dep_count, hd_vehicle_count 
    from {{ source('snowflake_sample_data', 'household_demographics') }}
),
td as (
    select t_time_sk, t_minute, t_hour 
    from {{ source('snowflake_sample_data', 'time_dim') }}
),
st as (
    select s_store_sk, s_store_name 
    from {{ source('snowflake_sample_data', 'store') }}
),

-- first join
ss_join_hd as (
    select ss.ss_hdemo_sk, ss.ss_sold_time_sk, ss.ss_store_sk, hd.hd_demo_sk, hd.hd_dep_count, hd_vehicle_count 
    from s_s as ss
    join h_d as hd
    on ss.ss_hdemo_sk = hd.hd_demo_sk 
    and (
        (hd.hd_dep_count = 4 and hd.hd_vehicle_count <= 4 + 2) or
        (hd.hd_dep_count = 2 and hd.hd_vehicle_count <= 2 + 2) or
        (hd.hd_dep_count = 0 and hd.hd_vehicle_count <= 0 + 2)
    )
),

-- second join with unique names
ss_hd_join_t1 as (
    select ss_hd.ss_sold_time_sk, ss_hd.ss_store_sk, t.t_time_sk, t.t_hour, t.t_minute
    from ss_join_hd as ss_hd 
    join td as t 
    on ss_hd.ss_sold_time_sk = t.t_time_sk
    where t.t_hour = 8 and t.t_minute >= 30
),
ss_hd_join_t2 as (
    select ss_hd.ss_sold_time_sk, ss_hd.ss_store_sk, t.t_time_sk, t.t_hour, t.t_minute
    from ss_join_hd as ss_hd 
    join td as t 
    on ss_hd.ss_sold_time_sk = t.t_time_sk
    where t.t_hour = 9 and t.t_minute < 30
),
ss_hd_join_t3 as (
    select ss_hd.ss_sold_time_sk, ss_hd.ss_store_sk, t.t_time_sk, t.t_hour, t.t_minute
    from ss_join_hd as ss_hd 
    join td as t 
    on ss_hd.ss_sold_time_sk = t.t_time_sk
    where t.t_hour = 9 and t.t_minute >= 30
),
ss_hd_join_t4 as (
    select ss_hd.ss_sold_time_sk, ss_hd.ss_store_sk, t.t_time_sk, t.t_hour, t.t_minute
    from ss_join_hd as ss_hd 
    join td as t 
    on ss_hd.ss_sold_time_sk = t.t_time_sk
    where t.t_hour = 10 and t.t_minute < 30
),
ss_hd_join_t5 as (
    select ss_hd.ss_sold_time_sk, ss_hd.ss_store_sk, t.t_time_sk, t.t_hour, t.t_minute
    from ss_join_hd as ss_hd 
    join td as t 
    on ss_hd.ss_sold_time_sk = t.t_time_sk
    where t.t_hour = 10 and t.t_minute >= 30
),
ss_hd_join_t6 as (
    select ss_hd.ss_sold_time_sk, ss_hd.ss_store_sk, t.t_time_sk, t.t_hour, t.t_minute
    from ss_join_hd as ss_hd 
    join td as t 
    on ss_hd.ss_sold_time_sk = t.t_time_sk
    where t.t_hour = 11 and t.t_minute < 30
),
ss_hd_join_t7 as (
    select ss_hd.ss_sold_time_sk, ss_hd.ss_store_sk, t.t_time_sk, t.t_hour, t.t_minute
    from ss_join_hd as ss_hd 
    join td as t 
    on ss_hd.ss_sold_time_sk = t.t_time_sk
    where t.t_hour = 11 and t.t_minute >= 30
),
ss_hd_join_t8 as (
    select ss_hd.ss_sold_time_sk, ss_hd.ss_store_sk, t.t_time_sk, t.t_hour, t.t_minute
    from ss_join_hd as ss_hd 
    join td as t 
    on ss_hd.ss_sold_time_sk = t.t_time_sk
    where t.t_hour = 12 and t.t_minute < 30
),

-- third join with unique names
ss_hd_t_join_s1 as (
    select ss_hd_t.ss_store_sk, s.s_store_sk
    from ss_hd_join_t1 as ss_hd_t
    join st as s
    on ss_hd_t.ss_store_sk = s.s_store_sk
    where s.s_store_name = 'ese'
),
ss_hd_t_join_s2 as (
    select ss_hd_t.ss_store_sk, s.s_store_sk
    from ss_hd_join_t2 as ss_hd_t
    join st as s
    on ss_hd_t.ss_store_sk = s.s_store_sk
    where s.s_store_name = 'ese'
),
ss_hd_t_join_s3 as (
    select ss_hd_t.ss_store_sk, s.s_store_sk
    from ss_hd_join_t3 as ss_hd_t
    join st as s
    on ss_hd_t.ss_store_sk = s.s_store_sk
    where s.s_store_name = 'ese'
),
ss_hd_t_join_s4 as (
    select ss_hd_t.ss_store_sk, s.s_store_sk
    from ss_hd_join_t4 as ss_hd_t
    join st as s
    on ss_hd_t.ss_store_sk = s.s_store_sk
    where s.s_store_name = 'ese'
),
ss_hd_t_join_s5 as (
    select ss_hd_t.ss_store_sk, s.s_store_sk
    from ss_hd_join_t5 as ss_hd_t
    join st as s
    on ss_hd_t.ss_store_sk = s.s_store_sk
    where s.s_store_name = 'ese'
),
ss_hd_t_join_s6 as (
    select ss_hd_t.ss_store_sk, s.s_store_sk
    from ss_hd_join_t6 as ss_hd_t
    join st as s
    on ss_hd_t.ss_store_sk = s.s_store_sk
    where s.s_store_name = 'ese'
),
ss_hd_t_join_s7 as (
    select ss_hd_t.ss_store_sk, s.s_store_sk
    from ss_hd_join_t7 as ss_hd_t
    join st as s
    on ss_hd_t.ss_store_sk = s.s_store_sk
    where s.s_store_name = 'ese'
),
ss_hd_t_join_s8 as (
    select ss_hd_t.ss_store_sk, s.s_store_sk
    from ss_hd_join_t8 as ss_hd_t
    join st as s
    on ss_hd_t.ss_store_sk = s.s_store_sk
    where s.s_store_name = 'ese'
),

-- CTEs for counts
s1 as (
    select count(*) as h8_30_to_9
    from ss_hd_t_join_s1
),
s2 as (
    select count(*) as h9_to_9_30
    from ss_hd_t_join_s2
),
s3 as (
    select count(*) as h9_30_to_10
    from ss_hd_t_join_s3
),
s4 as (
    select count(*) as h10_to_10_30
    from ss_hd_t_join_s4
),
s5 as (
    select count(*) as h10_30_to_11
    from ss_hd_t_join_s5
),
s6 as (
    select count(*) as h11_to_11_30
    from ss_hd_t_join_s6
),
s7 as (
    select count(*) as h11_30_to_12
    from ss_hd_t_join_s7
),
s8 as (
    select count(*) as h12_to_12_30
    from ss_hd_t_join_s8
),

-- final CTE
fc as (
    select 
        s1.h8_30_to_9,
        s2.h9_to_9_30,
        s3.h9_30_to_10,
        s4.h10_to_10_30,
        s5.h10_30_to_11,
        s6.h11_to_11_30,
        s7.h11_30_to_12,
        s8.h12_to_12_30
    from s1, s2, s3, s4, s5, s6, s7, s8
)

select * from fc

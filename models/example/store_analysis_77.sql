
-- models/sales_analysis.sql
-- {{ config(enabled = true, materialized='table') }}

- Configuration
{% if target.name == 'dev' %}
    {{ config(
        materialized='table',
        schema='DEV',
        database='DBT_DEV'
    ) }}
{% elif target.name == 'qa' %}
    {{ config(
        materialized='table',
        schema='QA',
        database='DBT_QA'
    ) }}
{% endif %}

-- first cte:
with store_sales_1 as (
    select ss_store_sk, 
    ss_ext_sales_price, 
    ss_net_profit, 
    ss_sold_time_sk, 
    ss_sold_date_sk
     from {{ source('snowflake_sample_data', 'store_sales') }}
), 

date_dim_1 as (
    select d_date_sk, 
    d_date 
    from {{ source('snowflake_sample_data', 'date_dim') }}
),
store_returns_1 as (
    select 
        sr_store_sk, 
        sr_return_amt, 
        sr_net_loss, 
        sr_returned_date_sk
    from 
        {{ source('snowflake_sample_data', 'store_returns') }}
), 
catalog_sales_1 as (
    select 
        cs_call_center_sk, 
        cs_ext_sales_price, 
        cs_net_profit, 
        cs_sold_date_sk
    from 
        {{ source('snowflake_sample_data', 'catalog_sales') }}
),
catalog_returns_1 as (
    select 
        cr_call_center_sk, 
        cr_return_amount, 
        cr_net_loss, 
        cr_returned_date_sk
    from 
        {{ source('snowflake_sample_data', 'catalog_returns') }}
), 
web_sales_1 as (
    select 
        ws_web_page_sk, 
        ws_ext_sales_price, 
        ws_net_profit, 
        ws_sold_date_sk
    from 
        {{ source('snowflake_sample_data', 'web_sales') }}
),
web_returns_1 as (
    select 
        wr_web_page_sk, 
        wr_return_amt, 
        wr_net_loss, 
        wr_returned_date_sk
    from 
        {{ source('snowflake_sample_data', 'web_returns') }}
),
-- first cte
ss_join_d as (
    select ss1.ss_sold_date_sk,
            ss1.ss_store_sk,
            d1.d_date_sk,
            ss1.ss_ext_sales_price,
            ss1.ss_net_profit

    from 
        store_sales_1 as ss1
    join 
        date_dim_1 as d1 on ss1.ss_sold_date_sk = d1.d_date_sk
        
    where d1.d_date between cast('2000-08-23' as date) and (cast('2000-08-23' as date) + 30)
),
ss as (
    select 
        ss_join_d.ss_store_sk,
        sum(ss_join_d.ss_ext_sales_price) as sales,
        sum(ss_join_d.ss_net_profit) as profit
    from 
        ss_join_d
    group by 
        ss_join_d.ss_store_sk
), 
-- second cte
sr_join_d as (
    select 
        sr1.sr_returned_date_sk,
        sr1.sr_store_sk,
        d1.d_date_sk,
        sr1.sr_return_amt,
        sr1.sr_net_loss
    from 
        store_returns_1 as sr1
    join 
        date_dim_1 as d1 on sr1.sr_returned_date_sk = d1.d_date_sk
    where 
        d1.d_date between cast('2000-08-23' as date) and (cast('2000-08-23' as date) + 30)
),
sr as (
    select 
        sr_join_d.sr_store_sk,
        sum(sr_join_d.sr_return_amt) as returns,
        sum(sr_join_d.sr_net_loss) as profit_loss
    from 
        sr_join_d
    group by 
        sr_join_d.sr_store_sk
),

-- third cte 

cs_join_d as (
    select 
        cs1.cs_sold_date_sk,
        cs1.cs_call_center_sk,
        d1.d_date_sk,
        cs1.cs_ext_sales_price,
        cs1.cs_net_profit
    from 
        catalog_sales_1 as cs1
    join 
        date_dim_1 as d1 on cs1.cs_sold_date_sk = d1.d_date_sk
    where 
        d1.d_date between cast('2000-08-23' as date) and (cast('2000-08-23' as date) + 30)
),
cs as (
    select 
        cs_join_d.cs_call_center_sk,
        sum(cs_join_d.cs_ext_sales_price) as sales,
        sum(cs_join_d.cs_net_profit) as profit
    from 
        cs_join_d
    group by 
        cs_join_d.cs_call_center_sk
),

-- fourth cte

cr_join_d as (
    select 
        cr1.cr_returned_date_sk,
        cr1.cr_call_center_sk,
        d1.d_date_sk,
        cr1.cr_return_amount,
        cr1.cr_net_loss
    from 
        catalog_returns_1 as cr1
    join 
        date_dim_1 as d1 on cr1.cr_returned_date_sk = d1.d_date_sk
    where 
        d1.d_date between cast('2000-08-23' as date) and (cast('2000-08-23' as date) + 30)
),
cr as (
    select 
        cr_join_d.cr_call_center_sk,
        sum(cr_join_d.cr_return_amount) as returns,
        sum(cr_join_d.cr_net_loss) as profit_loss
    from 
        cr_join_d
    group by 
        cr_join_d.cr_call_center_sk
),
-- fifth cte
ws_join_d as (
    select 
        ws1.ws_sold_date_sk,
        ws1.ws_web_page_sk,
        d1.d_date_sk,
        ws1.ws_ext_sales_price,
        ws1.ws_net_profit
    from 
        web_sales_1 as ws1
    join 
        date_dim_1 as d1 on ws1.ws_sold_date_sk = d1.d_date_sk
    where 
        d1.d_date between cast('2000-08-23' as date) and (cast('2000-08-23' as date) + 30)
),
ws as (
    select 
        ws_join_d.ws_web_page_sk,
        sum(ws_join_d.ws_ext_sales_price) as sales,
        sum(ws_join_d.ws_net_profit) as profit
    from 
        ws_join_d
    group by 
        ws_join_d.ws_web_page_sk
),

-- six cte
 
wr_join_d as (
    select 
        wr1.wr_returned_date_sk,
        wr1.wr_web_page_sk,
        d1.d_date_sk,
        wr1.wr_return_amt,
        wr1.wr_net_loss
    from 
        web_returns_1 as wr1
    join 
        date_dim_1 as d1 on wr1.wr_returned_date_sk = d1.d_date_sk
    where 
        d1.d_date between cast('2000-08-23' as date) and (cast('2000-08-23' as date) + 30)
),
wr as (
    select 
        wr_join_d.wr_web_page_sk,
        sum(wr_join_d.wr_return_amt) as returns,
        sum(wr_join_d.wr_net_loss) as profit_loss
    from 
        wr_join_d
    group by 
        wr_join_d.wr_web_page_sk
),

-- final_cte

fc as (
    select 
        'store channel' as channel,
        ss.ss_store_sk as id,
        sales,
        coalesce(returns, 0) as returns,
        (profit - coalesce(profit_loss, 0)) as profit
    from 
        ss 
    left join 
        sr on ss.ss_store_sk = sr.sr_store_sk

    union all

    select 
        'catalog channel' as channel,
        cs.cs_call_center_sk as id,
        cs.sales,
        coalesce(cr.returns, 0) as returns,
        (cs.profit - coalesce(cr.profit_loss, 0)) as profit
    from 
        cs 
    left join 
        cr on cs.cs_call_center_sk = cr.cr_call_center_sk

    union all

    select 
        'web channel' as channel,
        ws.ws_web_page_sk as id,
        ws.sales,
        coalesce(wr.returns, 0) as returns,
        (ws.profit - coalesce(wr.profit_loss, 0)) as profit
    from 
        ws 
    left join 
        wr on ws.ws_web_page_sk = wr.wr_web_page_sk
)

select 
    channel,
    id,
    sum(sales) as sales,
    sum(returns) as returns,
    sum(profit) as profit
from fc
group by 
    rollup(channel, id)
order by 
    channel,
    id

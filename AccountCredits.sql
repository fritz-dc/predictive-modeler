with ledger as (
    select donorid, transactiondate::date as txn_date, amount as signed_amount
    from accountcredit
    where coalesce(_fivetran_deleted, false) = false
    union all
    select donorid, transactiondate::date, -amount
    from accountdebit
    where coalesce(_fivetran_deleted, false) = false
),
daily as (
    select donorid, txn_date,
           sum(case when signed_amount > 0 then signed_amount else 0 end)  as credits,
           sum(case when signed_amount < 0 then -signed_amount else 0 end) as debits,
           sum(signed_amount)                                              as net_change
    from ledger
    group by 1, 2
)
select donorid, txn_date, credits, debits, net_change,
       sum(net_change) over (partition by donorid order by txn_date
                             rows between unbounded preceding and current row) as balance_eod
from daily
order by donorid, txn_date;
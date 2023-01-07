select
    c_count,
    count(*) as custdist
from
    (
        select
            c_custkey,
            count(o_orderkey) as c_count
        from
            data.tpch.customer left outer join data.tpch.orders on
                c_custkey = o_custkey
                and o_comment not like '%unusual%accounts%'
        group by
            c_custkey
    ) c_orders
group by
    c_count
order by
    custdist desc,
    c_count desc;
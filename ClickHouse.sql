-- std10_44.data_mart_total определение

CREATE TABLE std10_44.data_mart_total
(

    `plant` String,

    `turnover` Float32,

    `total_discount` Float32,

    `net_turnover` Float32,

    `quantity` Float64,

    `total_bills` Int32,

    `total_traffic` Int32,

    `total_coupons` Int32,

    `Share of discounted items` Float32,

    `Average number of items in a receipt` Float64,

    `Store conversion rate` Float32,

    `Average check` Float32,

    `turnover per visitor` Float32
)
ENGINE = PostgreSQL('host:port',
 'schemaname',
 'dbename',
 'login',
 'pass',
 'schemaname');

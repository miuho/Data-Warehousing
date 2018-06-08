-- I checked the data file sizes and realized that lineorder has the largest size.
-- Since each impala select query has to traverse each line in lineorder, I realized
-- that I should parition lineorder table for each of the 3 queries to improve
-- runtime

-- first, create a optimized table to query 1
CREATE TABLE lineorder_opt_1(
	lo_orderkey INT,
	lo_linenumber INT,
	lo_custkey INT,
	lo_partkey INT,
	lo_suppkey INT,
	lo_orderdate INT,
	lo_orderpriority STRING,
	lo_shippriority STRING,
	lo_quantity INT,
	lo_extendedprice INT,
	lo_ordertotalprice INT,
	lo_discount INT,
	lo_revenue INT,
	lo_supplycost INT,
	lo_tax INT,
	lo_commitdate INT,
	lo_shipmode STRING);

-- parition the lines for query 1 to the optimized table
insert into table lineorder_opt_1 select lo_orderkey, lo_linenumber, lo_custkey, lo_partkey, lo_suppkey, lo_orderdate, lo_orderpriority, lo_shippriority, lo_quantity, lo_extendedprice, lo_ordertotalprice, lo_discount, lo_revenue, lo_supplycost, lo_tax, lo_commitdate, lo_shipmode from lineorder, dwdate where lo_orderdate=d_datekey and d_year=1997 and lo_discount between 1 and 3 and lo_quantity < 24;

-- second, create a optimized table to query 2
CREATE TABLE lineorder_opt_2(
	lo_orderkey INT,
	lo_linenumber INT,
	lo_custkey INT,
	lo_partkey INT,
	lo_suppkey INT,
	lo_orderdate INT,
	lo_orderpriority STRING,
	lo_shippriority STRING,
	lo_quantity INT,
	lo_extendedprice INT,
	lo_ordertotalprice INT,
	lo_discount INT,
	lo_revenue INT,
	lo_supplycost INT,
	lo_tax INT,
	lo_commitdate INT,
	lo_shipmode STRING);

-- parition the lines for query 2 to the optimized table
insert into table lineorder_opt_2 select lo_orderkey, lo_linenumber, lo_custkey, lo_partkey, lo_suppkey, lo_orderdate, lo_orderpriority, lo_shippriority, lo_quantity, lo_extendedprice, lo_ordertotalprice, lo_discount, lo_revenue, lo_supplycost, lo_tax, lo_commitdate, lo_shipmode from lineorder, dwdate, part, supplier where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and p_category = 'MFGR#12' and s_region = 'AMERICA';

-- lastly, create a optimized table to query 3
CREATE TABLE lineorder_opt_3(
	lo_orderkey INT,
	lo_linenumber INT,
	lo_custkey INT,
	lo_partkey INT,
	lo_suppkey INT,
	lo_orderdate INT,
	lo_orderpriority STRING,
	lo_shippriority STRING,
	lo_quantity INT,
	lo_extendedprice INT,
	lo_ordertotalprice INT,
	lo_discount INT,
	lo_revenue INT,
	lo_supplycost INT,
	lo_tax INT,
	lo_commitdate INT,
	lo_shipmode STRING);

-- I attempted to partition the lines for query 3 as previous queries, but I got a Memory
-- Limit error, and so I have to parition the lines for query 3 in 3 smaller steps

-- create 2 temporary tables
CREATE TABLE lineorder_tmp_1(
	lo_orderkey INT,
	lo_linenumber INT,
	lo_custkey INT,
	lo_partkey INT,
	lo_suppkey INT,
	lo_orderdate INT,
	lo_orderpriority STRING,
	lo_shippriority STRING,
	lo_quantity INT,
	lo_extendedprice INT,
	lo_ordertotalprice INT,
	lo_discount INT,
	lo_revenue INT,
	lo_supplycost INT,
	lo_tax INT,
	lo_commitdate INT,
	lo_shipmode STRING);

CREATE TABLE lineorder_tmp_2(
	lo_orderkey INT,
	lo_linenumber INT,
	lo_custkey INT,
	lo_partkey INT,
	lo_suppkey INT,
	lo_orderdate INT,
	lo_orderpriority STRING,
	lo_shippriority STRING,
	lo_quantity INT,
	lo_extendedprice INT,
	lo_ordertotalprice INT,
	lo_discount INT,
	lo_revenue INT,
	lo_supplycost INT,
	lo_tax INT,
	lo_commitdate INT,
	lo_shipmode STRING);


-- parition the lines for query 3 to the optimized table (take take of supplier table)
insert into table lineorder_tmp_1 select lo_orderkey, lo_linenumber, lo_custkey, lo_partkey, lo_suppkey, lo_orderdate, lo_orderpriority, lo_shippriority, lo_quantity, lo_extendedprice, lo_ordertotalprice, lo_discount, lo_revenue, lo_supplycost, lo_tax, lo_commitdate, lo_shipmode from lineorder, supplier where lo_suppkey = s_suppkey and (s_city='UNITED KI1' or s_city='UNITED KI5');

-- parition the lines for query 3 to the optimized table (take take of customer table)
insert into table lineorder_tmp_2 select lo_orderkey, lo_linenumber, lo_custkey, lo_partkey, lo_suppkey, lo_orderdate, lo_orderpriority, lo_shippriority, lo_quantity, lo_extendedprice, lo_ordertotalprice, lo_discount, lo_revenue, lo_supplycost, lo_tax, lo_commitdate, lo_shipmode from customer, lineorder_tmp_1 where lo_custkey = c_custkey and (c_city='UNITED KI1' or c_city='UNITED KI5');

-- parition the lines for query 3 to the optimized table (take take of dwdate table)
insert into table lineorder_opt_3 select lo_orderkey, lo_linenumber, lo_custkey, lo_partkey, lo_suppkey, _orderdate, lo_orderpriority, lo_shippriority, lo_quantity, lo_extendedprice, lo_ordertotalprice, lo_discount, lo_revenue, lo_supplycost, lo_tax, lo_commitdate, lo_shipmode from lineorder_tmp_2, dwdate where lo_orderdate = d_datekey and d_yearmonth = 'Dec1997';


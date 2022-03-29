-- TODO 设置 hive 引擎
set hive.execution.engine=mr;
set hive.execution.engine=tez;
set hive.execution.engine=spark;
hive on spark rdd

select shop_name
from dmp_order_ex_users_buytimes
group by shop_name;

-- TODO 本地跑 hivesql的时候最好设置上，否则内存不够
set hive.auto.convert.join = false;
set hive.ignore.mapjoin.hint = false;
set hive.exec.parallel = true;
set hive.mapjoin.localtask.max.memory.usage=0.99;
set hive.execution.engine = mr;
set hive.auto.convert.join = false;
-- 在hive连接时开启智能本地模型
SET hive.exec.mode.local.auto=true;

-- TODO 查看某个函数的用法
desc function extended nvl;

-- TODO 拉链表
DROP TABLE IF EXISTS ods_user_info;
CREATE EXTERNAL TABLE ods_user_info
(
    `id`         STRING COMMENT '用户id',
    `name`       STRING COMMENT '用户姓名',
    `phone_num`  STRING COMMENT '手机号码',
    `start_date` STRING COMMENT '开始日期',
    `end_date`   STRING COMMENT '结束日期'
) COMMENT '用户表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/ods/ods_user_info/'
    TBLPROPERTIES ("parquet.compression" = "lzo");
-- 首次进入到 ods 的数据，此次数据进入到 '2022-03-17' 分区，就是说数据进入到今天的分区
insert overwrite table ods_user_info partition (dt = '2022-03-17')
values ('1', 'zs', '111', '2022-03-17', '9999-99-99'),
       ('2', 'ls', '222', '2022-03-17', '9999-99-99'),
       ('3', 'ww', '333', '2022-03-17', '9999-99-99')
;

DROP TABLE IF EXISTS dim_user_info;
CREATE EXTERNAL TABLE dim_user_info
(
    `id`         STRING COMMENT '用户id',
    `name`       STRING COMMENT '用户姓名',
    `phone_num`  STRING COMMENT '手机号码',
    `start_date` STRING COMMENT '开始日期',
    `end_date`   STRING COMMENT '结束日期'
) COMMENT '用户表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dim/dim_user_info/'
    TBLPROPERTIES ("parquet.compression" = "lzo");

-- 首次将 ods 层的数据导入到 dim 中的 '9999-99-99' 分区中
insert overwrite table dim_user_info partition (dt = '9999-99-99')
select id, name, phone_num, start_date, end_date
from ods_user_info
where dt = '2022-03-17';

-- 往 ods 中生成新的数据，第2天了
insert into ods_user_info partition (dt = '2022-03-18')
values ('2', 'ls', '223', '2022-03-18', '9999-99-99'),
       ('4', 'zl', '444', '2022-03-18', '9999-99-99');

-- 开始拉链，做 2022-03-18 的拉链
with t1 as (
    -- 拿到 ods 中 2022-03-18 的数据
    select id, name, phone_num, start_date, end_date from ods_user_info where dt = '2022-03-18'
),
     t2 as (
         -- 将 dim 中最新的数据拿到
         select id, name, phone_num, start_date, end_date, dt
         from dim_user_info
         where dt = '9999-99-99'
     ),
     t3 as (
         -- 将 ods 中的数据与 dim 中 '9999-99-99' 分区中的数据进行比对，将 dim 中的数据修改
         select t2.id,
                t2.name,
                t2.phone_num                     as phone_num,
                t2.start_date,
                nvl(t1.start_date, '9999-99-99') as end_date, -- 要修改的数据
                nvl(t1.start_date, '9999-99-99') as dt -- 与更新后的 end_date 保持一致
         from t2
                  left join t1 on t1.id = t2.id and t1.name = t2.name
     ),
     t4 as (
         select id, name, phone_num, start_date, end_date, dt
         from t3
         union all
         select id, name, phone_num, start_date, end_date, '9999-99-99' -- 最新的数据
         from t1
     )
insert
overwrite
table
dim_user_info
partition
(
dt
)
select id, name, phone_num, start_date, end_date, dt
from t4
;

-- TODO 测试数据
insert into business(name, orderdate, cost)
values ('jack', '2017-01-01', 10),
       ('tony', '2017-01-02', 15),
       ('jack', '2017-02-03', 23),
       ('tony', '2017-01-04', 29),
       ('jack', '2017-01-05', 46),
       ('jack', '2017-04-06', 42),
       ('tony', '2017-01-07', 50),
       ('jack', '2017-01-08', 55),
       ('mart', '2017-04-08', 62),
       ('mart', '2017-04-09', 68),
       ('neil', '2017-05-10', 12),
       ('mart', '2017-04-11', 75),
       ('neil', '2017-06-12', 80),
       ('mart', '2017-04-13', 94);

SELECT NAME,
       orderdate,
       cost,
       SUM(cost)
           over (ORDER BY orderdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) sum_cost -- 从一开始到当前行累加
FROM business; -- 不能再窗口外面排序，这样的结果是乱的

SELECT NAME,
       orderdate,
       cost,                                                                                     -- 购买明细
       SUM(cost) over (ORDER BY orderdate)                                            sum_cost,  -- 按照日期累加
       SUM(cost)
           over (ORDER BY orderdate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_cost1, -- 从一开始到当前行累加 和 sum_cost一样
       SUM(cost) over (ORDER BY orderdate ROWS BETWEEN 1 PRECEDING AND CURRENT ROW )  sum_cost2, -- 上一行与当前行的累加
       SUM(cost) over (ORDER BY orderdate ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING)   sum_cost3, -- 当前行与下一行的累加
       SUM(cost)
           over (ORDER BY orderdate ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING )        sum_cost4, -- 上一行 与 当前行 与 下一行的累加
       sum(cost)
           over (order by orderdate ROWS BETWEEN UNBOUNDED PRECEDING and 1 PRECEDING) sum_cost5  -- 从一开始到上一行累加，也就是说第一个为null
FROM business;

-- TODO 连续问题
-- 如下数据为蚂蚁森林中用户领取的减少碳排放量
DROP TABLE IF EXISTS continuous_problem;

insert into continuous_problem(id, dt, lowcarbon)
values (1001, '2021-12-12', 123),
       (1002, '2021-12-12', 45),
       (1001, '2021-12-13', 43),
       (1001, '2021-12-13', 45),
       (1001, '2021-12-13', 23),
       (1002, '2021-12-14', 45),
       (1001, '2021-12-14', 230),
       (1002, '2021-12-15', 45),
       (1001, '2021-12-15', 23);

with t1 as (
    -- 找到每个用户单日减少的碳排放量在100以上的用户
    select id, dt, sum(lowcarbon) lowcarbon
    from continuous_problem
    group by id, dt
    having lowcarbon > 100
),
     t2 as (
         -- 使用用户分组，排序
         select id, dt, row_number() over (partition by id order by dt) rn
         from t1
     ),
     t3 as (
         -- 使用日期与排序值相减
         select id, dt, date_sub(dt, rn) diff
         from t2
     ),
     t4 as (
         -- 找到连续3天
         select id, diff, count(1) flag
         from t3
         group by id, diff
         having flag >= 3
     )
select *
from t4;


-- TODO 分组问题
-- 如下为电商公司用户访问时间数据

insert into group_problem(id, ts)
values (1001, 17523641234),
       (1001, 17523641256),
       (1002, 17523641278),
       (1001, 17523641334),
       (1002, 17523641434),
       (1001, 17523641534),
       (1001, 17523641544),
       (1002, 17523641634),
       (1001, 17523641638),
       (1001, 17523641654);

with t1 as (
    -- 按照用户分组，时间从小到大排序，且数据向后错一位
    select id, ts, lag(ts, 1, 0) over (partition by id order by ts) lagts
    from group_problem
),
     t2 as (
         -- 获取时间差
         select id, ts, ts - lagts diff
         from t1
     ),
     t3 as (
         -- 如果小于60则加1
         select id, ts, sum(if(diff >= 60, 1, 0)) over (partition by id order by ts) from t2
     )
select *
from t3;


-- TODO 间隔连续问题
-- 某游戏公司记录的用户每日登录数据
insert into interval_continuity_problem(id, dt)
values (1001, '2021-12-12'),
       (1002, '2021-12-12'),
       (1001, '2021-12-13'),
       (1001, '2021-12-14'),
       (1001, '2021-12-16'),
       (1002, '2021-12-16'),
       (1001, '2021-12-19'),
       (1002, '2021-12-17'),
       (1001, '2021-12-20');

t2
1001	2021-12-12	2021-12-11
1001	2021-12-13	2021-12-11
1001	2021-12-14	2021-12-11
1001	2021-12-16	2021-12-12
1001	2021-12-19	2021-12-14
1001	2021-12-20	2021-12-14
1002	2021-12-12	2021-12-11
1002	2021-12-16	2021-12-14
1002	2021-12-17	2021-12-14
t3
1001	2021-12-11	3
1001	2021-12-12	1
1001	2021-12-14	2
1002	2021-12-11	1
1002	2021-12-14	2
t4
1001	2021-12-11	1
1001	2021-12-12	2
1001	2021-12-14	3
1002	2021-12-11	1
1002	2021-12-14	2
t5
1001	2021-12-11	2021-12-10
1001	2021-12-12	2021-12-10
1001	2021-12-14	2021-12-11
1002	2021-12-11	2021-12-10
1002	2021-12-14	2021-12-12
t6
1001	2021-12-10	2
1001	2021-12-11	1
1002	2021-12-10	1
1002	2021-12-12	1
with t1 as (
    select id, dt, row_number() over (partition by id order by dt) as rn from interval_continuity_problem
),
     t2 as (
         -- 计算一天的差值
         select id, dt, date_sub(dt, rn) as diff
         from t1
     ),
     t3 as (
         -- 统计连续一天的天数
         select id, diff, count(1) as cont_1
         from t2
         group by id, diff
     ),
     t4 as (
         select id, diff, row_number() over (partition by id order by diff) as rn4
         from t3
     ),
     t5 as (
         -- 计算2天的差值
         select id, diff, date_sub(diff, rn4) as diff5
         from t4
     ),
     t6 as (
         -- 统计连续2天的天数
         select id, diff5, count(1) as cont_6
         from t5
         group by id, diff5
     ),
     t7 as (
         -- 将连续1天的和连续2天的加起来
         select id, t3.diff, sum(t3.cont_1 + t6.cont_6) as sn
         from t3
                  join t6 on t3.id = t6.id and t3.diff = date_add(t6.diff5, 1)
         group by t3.id, t3.diff
     ),
     t8 as (
         -- 取出每组最大的值
         select id, max(sn)
         from t7
         group by id
     )
select *
from t8;

-- TODO 打折日期交叉问题
-- 某游戏公司记录的用户每日登录数据

insert into discount_date_cross_problem(brand, stt, edt)
values ('oppo', '2021-06-05', '2021-06-09'),
       ('oppo', '2021-06-11', '2021-06-21'),
       ('vivo', '2021-06-05', '2021-06-15'),
       ('vivo', '2021-06-09', '2021-06-21'),
       ('redmi', '2021-06-05', '2021-06-21'),
       ('redmi', '2021-06-09', '2021-06-15'),
       ('redmi', '2021-06-17', '2021-06-26'),
       ('huawei', '2021-06-05', '2021-06-26'),
       ('huawei', '2021-06-09', '2021-06-15'),
       ('huawei', '2021-06-17', '2021-06-21');


with t1 as (
    select brand, min(stt) as stt, max(edt) edt
    from discount_date_cross_problem
    group by brand
),
     t2 as (
         select brand, datediff(edt, stt)
         from t1
     )
select *
from t2
;
set hive.execution.engine=spark;

select brand,
       if(maxEdt is null, stt, if(stt > maxEdt, stt, date_add(maxEdt, 1))) stt,
       edt
from (select brand,
             stt,
             edt,
             max(edt) over (partition by brand order by stt rows between UNBOUNDED PRECEDING and 1 PRECEDING) maxEdt
      from discount_date_cross_problem)
;



select brand,
       sum(if(days >= 0, days + 1, 0)) days
from (select brand,
             datediff(edt, stt) days
      from (select brand,
                   if(maxEdt is null, stt, if(stt > maxEdt, stt, date_add(maxEdt, 1))) stt,
                   edt
            from (select brand,
                         stt,
                         edt,
                         max(edt)
                             over (partition by brand order by stt rows between UNBOUNDED PRECEDING and 1 PRECEDING) maxEdt
                  from discount_date_cross_problem) t1) t2) t3
group by brand;


-- TODO 同时在线问题
-- 如下为某直播平台主播开播及关播时间，根据该数据计算出平台最高峰同时在线的主播人数。

set hive.execution.engine=spark;

insert into simu_online_problem(id, stt, edt)
values (1001, '2021-06-14 12:12:12', '2021-06-14 18:12:12'),
       (1003, '2021-06-14 13:12:12', '2021-06-14 16:12:12'),
       (1004, '2021-06-14 13:15:12', '2021-06-14 20:12:12'),
       (1002, '2021-06-14 15:12:12', '2021-06-14 16:12:12'),
       (1005, '2021-06-14 15:18:12', '2021-06-14 20:12:12'),
       (1001, '2021-06-14 20:12:12', '2021-06-14 23:12:12'),
       (1006, '2021-06-14 21:12:12', '2021-06-14 23:15:12'),
       (1007, '2021-06-14 22:12:12', '2021-06-14 23:10:12');


select id, stt dt, 1 p
from simu_online_problem
union
select id, edt dt, -1 p
from simu_online_problem;
t1

select id,
       dt,
       sum(p) over (order by dt) sum_p
from (select id, stt dt, 1 p
      from simu_online_problem
      union
      select id, edt dt, -1 p
      from simu_online_problem) t1;
t2

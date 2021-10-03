-- TODO postgresql查询某个表 pid，SQL、开始时间、执行SQL的ip地址
-- 一般用这个
SELECT distinct pg_stat_get_backend_pid(S.backendid)            AS pid,
                pg_stat_get_backend_activity_start(S.backendid) AS start_time,
                pg_stat_get_backend_activity(S.backendid)       AS query_sql,
                m.client_addr                                   as ip_addr
FROM (SELECT pg_stat_get_backend_idset() AS backendid) s,
     pg_locks t,
     pg_class p,
     pg_stat_activity m
where pg_stat_get_backend_pid(S.backendid) = t.pid
  and t.relation = p.oid
  and m.pid = t.pid
  and p.relname = 'table_name';

-- TODO 查看PostgreSQL正在执行的SQL
SELECT procpid,
       start,
       now() - start AS lap,
       current_query
FROM (SELECT backendid,
             pg_stat_get_backend_pid(S.backendid)            AS procpid,
             pg_stat_get_backend_activity_start(S.backendid) AS start,
             pg_stat_get_backend_activity(S.backendid)       AS current_query
      FROM (SELECT pg_stat_get_backend_idset() AS backendid) AS S
     ) AS S
WHERE current_query <> '<IDLE>'
ORDER BY lap DESC
;

-- 或者
SELECT datname, pid, state, query
FROM pg_stat_activity where state='active';
-- TODO 查找所有活动的被锁的表

select pid, state, usename, query, query_start
from pg_stat_activity
where pid in (
    select pid
    from pg_locks l
             join pg_class t on l.relation = t.oid
        and t.relkind = 'r'
);

-- TODO 解锁被锁的表
SELECT pg_cancel_backend(pid);


-- TODO exists 和 in 的问题
-- exists 速度快，in 速度慢
-- exists 子表中后面要加 where 条件 a.id = id
-- 这个链接有解释 exists 是如何运行的。 https://bbs.csdn.net/topics/80519889
select a.*
from test2 a
where exists(select * from test1 where a.id = id)


-- TODO case when 问题
select case
           when sale_amount is null then 0
           when paid_amount is null then 0
           else paid_amount
from ods.s_jst_sales_report

-- TODO 更新操作
update ods.s_jst_sales_report_status
set merge_status = 1
where report_day = '2021-09-01'

-- TODO postgresql 解析json数组
select o_id, json_array_elements(items::json) ->> 'amount'
from ods.f_order_report_detail


-- TODO delete数据
delete
from ods.f_order_report_detail
where send_date >= '2021-07-27 00:00:00'
  and send_date <= '2021-07-31 23:59:59';


-- TODO 删除重复数据，这两个方法都需要使用唯一键

-- mysql
SELECT
 *
FROM
 student
WHERE
 id NOT IN (
 SELECT
  t.id
 FROM
 ( SELECT MIN( id ) AS id FROM student GROUP BY `name` ) t
 )
;

-- postgresql
delete
from table_name as ta
where ta.唯一键 <> (select max(tb.唯一键) from table_name as tb where ta.判断重复的列 = tb.判断重复的列);



-- TODO PGSql生成随机数，生成8位ID，类UUid，数字id
-- 8位字符串
select substring(md5(random()::varchar), 2, 8);
-- 8位数字
select substring(random()::varchar, 3, 8);


-- 生成自增主键
-- Java里面不要设置 id 的值 默认会自动生成，需要确保 aaa_id 已经创建了
CREATE TABLE "public"."aaa" (
  "id" int4 DEFAULT nextval('aaa_id'::regclass),
  "name" varchar(255) COLLATE "pg_catalog"."default" DEFAULT nextval('comm_seq'::regclass)
)
;

ALTER TABLE "public"."aaa"
  OWNER TO "gpadmin";

-- TODO 开窗过滤
select so_id,num
from (
         select so_id, count(1) over (partition by so_id) as num
         from ods.f_order_report_detail
     ) t1
where t1.num > 1
;

-- TODO topN

2020-10-01 a 100
2020-10-01 b 50
2020-10-01 c 200
2020-10-01 d 300
2020-10-02 a 150
2020-10-02 b 30
2020-10-02 c 50
2020-10-02 d 70

     |
    \|/

2020-10-01,d,300
2020-10-01,c,200
2020-10-02,a,150
2020-10-02,d,70


select a.v_date,a.name,a.bonus
from(select  row_number() over(partition by v_date order by bonus desc) as top,
v_date,name,bonus from aaaa) a
where a.top<=2;


SELECT col1, col2, col3
FROM (
 SELECT col1, col2, col3
   ROW_NUMBER() OVER ([PARTITION BY col1[, col2..]]
   ORDER BY col1 [asc|desc][, col2 [asc|desc]...]) AS rownum
 FROM table_name)
WHERE rownum <= N [AND conditions]


-- TODO insert 插入数据
INSERT INTO 表名 (字段，字段，。。。)
VALUES (字段值，字段值，。。。),(字段值，字段值。。)
;

INSERT INTO 表名
VALUES (字段值，字段值，。。。),(字段值，字段值。。)
;[VALUES 需要将所有字段都补充上]


-- TODO 高效去重方案
-- 保留首行的去重策略（Deduplicate Keep FirstRow）
SELECT *
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY b ORDER BY proctime) as rowNum
  FROM T
)
WHERE rowNum = 1

-- 保留末行的去重策略（Deduplicate Keep LastRow）
SELECT *
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY b, d ORDER BY rowtime DESC) as rowNum
  FROM T
)
WHERE rowNum = 1

-- TODO postgresql 官方文档
http://www.postgres.cn/docs/9.4/performance-tips.html

-- TODO Postgres 释放磁盘存储空间
-- https://www.vicw.com/groups/code_monkey/topics/219
VACUUM full table_name;


-- TODO 查看表的大小
SELECT table_name,
       pg_size_pretty(table_size)   AS table_size,
       pg_size_pretty(indexes_size) AS indexes_size,
       pg_size_pretty(total_size)   AS total_size
FROM (
         SELECT table_name,
                pg_table_size(table_name)          AS table_size,
                pg_indexes_size(table_name)        AS indexes_size,
                pg_total_relation_size(table_name) AS total_size
         FROM (
                  SELECT ('"' || table_schema || '"."' || table_name || '"') AS table_name
                  FROM information_schema.tables
                  where table_name ~ 't_wx_msg'
              ) AS all_tables
         ORDER BY total_size DESC
     ) AS pretty_sizes;

-- TODO 查看支持连接数
show max_connections;
-- TODO 查询当前连接数
select count(1)
from pg_stat_activity;
-- TODO 查询连接的详细数据
select *
from pg_stat_activity;
-- TODO 杀掉idle连接用户
select pg_terminate_backend(pid)
from pg_stat_activity
where state = 'idle';
-- TODO 查看版本号
select version();
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

-- 查看公司ip 数据库执行的状态
SELECT *
FROM pg_stat_activity
where client_addr = '222.128.73.210';

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
FROM pg_stat_activity
where state = 'active';
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


-- TODO exists , in 和 join 的问题
-- exists 速度快，in 速度慢
-- exists 子表中后面要加 where 条件 a.id = id
-- 这个链接有解释 exists 是如何运行的。 https://bbs.csdn.net/topics/80519889
select a.*
from test2 a
where exists(select * from test1 where a.id = id)

-- 能用 exists 就用 exists，join 操作上更灵活，可以形成一张新表，然后在 where 后面进行查询
-- TODO 什么时候用 join ?
-- 使用join的时候可以将2个表的字段都用上。而 exists 和 in 只能使用最外面查询的字段。

-- 如果in后面出来的是一张临时表，很有可能是该临时表没有建立索引导致的，
-- 建议尽量避免使用in而改用join或者是exist.由于没有具体的sql放出，无法进行更实质性的优化。

-- TODO case when 问题
select case
           when sale_amount is null then 0
           when paid_amount is null then 0
           else paid_amount
from ods.s_jst_sales_report
;

SELECT t1.id
     , CASE
           WHEN t2.is_view IS NULL THEN 1
           else t2.is_view
    END AS is_view
     , CASE
           WHEN t2.is_edit IS NULL THEN 1
           else t2.is_edit
    END AS is_edit
     , t1.menu_name
     , t1.parent_id
     , t1.pre_parent_id
     , t1.level
     , t1.link
     , t1.menu_type
     , t1.menu_status
     , t1.menu_desc
     , t1.tenant_id
     , t1.create_time
     , t1.update_time
FROM dfsw_report_import_menu t1
         LEFT JOIN (
    SELECT *
    FROM dfsw_report_import_user_menu
    WHERE user_id = '1356078255257554944'
) t2
                   ON t1.id = t2.menu_id
WHERE t1.menu_status = 0
  AND t1.level = 1
;


-- TODO 更新操作
update ods.s_jst_sales_report_status
set merge_status = 1
where report_day = '2021-09-01';

-- TODO 从一个表中的字段更新另一个字段
update ods.u_tag_config t1
set first_tag = (select tag from ods.u_tag_config t2 where t1.id = t2.id);


-- TODO postgresql 解析json数组
select o_id, json_array_elements(items::json) ->> 'amount'
from ods.f_order_report_detail


-- TODO delete数据
         delete from ods.f_order_report_detail
where send_date >= '2021-07-27 00:00:00'
  and send_date <= '2021-07-31 23:59:59';


-- TODO 删除重复数据，这两个方法都需要使用唯一键

-- mysql
SELECT *
FROM student
WHERE id NOT IN (
    SELECT t.id
    FROM (SELECT MIN(id) AS id FROM student GROUP BY ` name `) t
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
CREATE TABLE "public"."aaa"
(
    "id"   int4                                        DEFAULT nextval('aaa_id'::regclass),
    "name" varchar(255) COLLATE "pg_catalog"."default" DEFAULT nextval('comm_seq'::regclass)
)
;

ALTER TABLE "public"."aaa"
    OWNER TO "gpadmin";

-- TODO 开窗过滤
select so_id, num
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


select a.v_date, a.name, a.bonus
from (select row_number() over (partition by v_date order by bonus desc) as top,
             v_date,
             name,
             bonus
      from aaaa) a
where a.top <= 2;


SELECT col1, col2, col3
FROM (
         SELECT col1,
                col2,
                col3
                    ROW_NUMBER() OVER ([PARTITION BY col1[, col2..]]
   ORDER BY col1 [asc|desc][, col2 [asc|desc]...]) AS rownum
         FROM table_name)
WHERE rownum <= N [AND conditions]


-- TODO insert 插入数据
INSERT INTO 表名 (字段，字段，。。。)
VALUES (字段值，字段值，。。。),
       (字段值，字段值。。)
;

INSERT INTO 表名
VALUES (字段值，字段值，。。。),
       (字段值，字段值。。)
;
[VALUES 需要将所有字段都补充上]

-- TODO insert 后带返回值
insert into tb3(name)
values ('aa')
returning id;

-- TODO 从一个表中导入到另一个表中
INSERT INTO ods.u_user_profile_main(id, tag, wxid, hit_count, batch_id, send_time_day) (
    select id,
           tag,
           wxid,
           hit_count,
           to_char(now() AT TIME ZONE 'PRC', 'yyyy-MM-dd hh24:mi:ss') as batch_id,
           send_time_day
    from ods.u_user_profile
)

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


---
    where pay_date 左右都要加范围，否则子查询会很慢



--- TODO union all 和 union
union
:
2
个表中有相同的值，最后只会展示2个表中的1个
union all
:
最终会展示2个表的所有数据

--- TODO 求时间差
select order_date::timestamp, date_part('day', order_date::timestamp - '2018-01-10 10:12:15'::timestamp)
from t_jst_order_all


-- TODO 如何将多张表的结果合并成一行，也就是一列数据和成一行
select 'a' as name1;
select 'b' as name2;
select 'c' as name3;
select 'd' as name4;

-- 将多张表的结果转成列，要求每行带有一个别名，并且原来数据的名字改成一样的
select *
from (
         select 'a' as word, 'name1' as name
         union all
         select 'b' as word, 'name2' as name
         union all
         select 'c' as word, 'name3' as name
         union all
         select 'd' as word, 'name4' as name
     ) t1;

-- 将列转成行， 使用case when，此时的结果是每行都有数据，并且有地方是null
select case when name = 'name1' then word end,
       case when name = 'name2' then word end,
       case when name = 'name3' then word end,
       case when name = 'name4' then word end
from (
         select 'a' as word, 'name1' as name
         union all
         select 'b' as word, 'name2' as name
         union all
         select 'c' as word, 'name3' as name
         union all
         select 'd' as word, 'name4' as name
     ) t1;

-- 使用max聚合函数将一列函数合在一起，选取合适的聚合函数就可以，例如sum
select MAX(case when name = 'name1' then word end),
       MAX(case when name = 'name2' then word end),
       MAX(case when name = 'name3' then word end),
       MAX(case when name = 'name4' then word end)
from (
         select 'a' as word, 'name1' as name
         union all
         select 'b' as word, 'name2' as name
         union all
         select 'c' as word, 'name3' as name
         union all
         select 'd' as word, 'name4' as name
     ) t1;

-- TODO 分页查询
-- https://www.cnblogs.com/bbgasj/archive/2012/11/06/2756567.html?ivk_sa=1024320u
SELECT *
FROM (
         SELECT ROW_NUMBER() OVER (ORDER BY id) AS RowNum, *
         FROM ods.t_wx_msg
         where send_time >= '2021-08-01 00:00:00'
           and send_time <= '2021-08-30 23:59:59'
     ) as t1
where RowNum between (1 - 1) * 10 + 1 and 1 * 10
order by RowNum
;

-- TODO 匹配手机号
SELECT txt
FROM ods.t_wx_msg
WHERE txt ~ '^[1][35678][0-9]{9}$';


-- TODO SQL查找是否"存在"
SELECT 1
FROM table
WHERE a = 1
  AND b = 2
LIMIT 1
    #### Java写法:
Integer exist = xxDao.existXxxxByXxx(params);
if
    (exist != NULL)
    {
  //当存在时，执行这里的代码
} else {
  //当不存在时，执行这里的代码
}

-- TODO 时间转化
select to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss');
//日期转化为字符串
select to_char(sysdate, 'yyyy');
//获取时间的年
select to_char(sysdate, 'mm');
//获取时间的月
select to_char(sysdate, 'dd');
//获取时间的日
select to_char(sysdate, 'hh24');
//获取时间的时
select to_char(sysdate, 'mi');
//获取时间的分
select to_char(sysdate, 'ss');
//获取时间的秒

-- TODO MD5 加密
select MD5('加密');

-- TODO 关键词模糊匹配
select t1.txt, t2.tag
from (
         select *
         from ods.t_wx_msg
         where send_time > '2022-03-02 00:00:00'
     ) t1
         join (select * from ods.u_tag_config) t2
              on t1.txt ~ t2.tag
limit 100;

-- TODO 按30分钟统计分组
select to_timestamp(
                -- 小于30分钟的按00统计，大于30分钟的按30统计
               concat(to_char(send_time, 'yyyy-mm-dd HH24'), ':', FLOOR(date_part('minute', send_time) / 30) * 30),
               'yyyy-mm-dd HH24:MI') as stat_at,
       count(1)
from ods.t_wx_msg
where send_time > '2021-03-03 00:00:00'
group by stat_at
order by stat_at
limit 100;
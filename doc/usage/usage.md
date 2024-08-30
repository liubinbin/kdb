例子语句如下：

describe database kdb

describe table stu

create table stu1(id int, age int, name varchar(256))

insert into stu1 (id, age, name) VALUES (1, 29, 'Alice')

select count(*) from stu

select id,age,name from stu

select * from stu where id = 1

select * from stu order by age

select * from stu limit 10
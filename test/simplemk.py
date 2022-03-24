name = ['alice', 'bob', 'charlie', 'dave', 'eve', 'issac', 'ivan', 'justin', 'mallory', 'matilda', 'oscar', 'pat']
import random
test_num = 1000
with open("test{}.txt".format(test_num), "w") as f:
    begin = "create table stu(\nsid int unique, uid int ,name char(20)\n);\n"
    f.write(begin)
    val=1
    for i in range(test_num):
        val=val+random.randint(1,4)
        s = "insert into stu values({}, {} ,'{}');\n".format(val, val+2,name[random.randint(0, len(name)-1)])
        f.write(s)
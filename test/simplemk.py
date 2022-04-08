import random
name = ['alice', 'bob', 'charlie', 'dave', 'eve', 'issac',
        'ivan', 'justin', 'mallory', 'matilda', 'oscar', 'pat']
test_num = 100000
with open("testSelectIndex{}.txt".format(test_num), "w") as f:
    val = 1
    for i in range(test_num):
        val = val+random.randint(1, 4)
        s = "insert into stu5 values({}, {} ,'{}');\n".format(
            val, val+2, name[random.randint(0, len(name)-1)])
        f.write(s)

复杂数据类型定义：
~~~
    array：array<string>
    map:map<string,string>
    struct:struct<id:string,name:string>
~~~

构造:
~~~
    array(val1,val2)
    map(key,value1,key2,value2)
    named_struct(name1,val1,name2,val2,...)
~~~

取值：
~~~
    array[1]
    map['key']
    struct.name1
~~~

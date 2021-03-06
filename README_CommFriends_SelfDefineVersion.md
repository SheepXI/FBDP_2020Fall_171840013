## 设计思路

仍然是两个Map和Reduce

自定义数据类型为Friends，其数据成员有person1/person2/commfriend，分别是两个不同用户以及他们的共同好友之一

假设输入为：

```
100, 200 300 400
200, 100 300 400
300, 100
400, 100
```

第一个Map及Reduce用于输出具有共同好友的用户对及其共同好友，其输出为：

```
[200,300]:100
[200,400]:100
[300,400]:100
[100,200]:300
[100,200]:400
```

第一个Map用于统计“被好友”的主体及对象，以输入的第一行为例，第一个Map产生的[key,value]有：[200,100] [300,100] [400,100] ，可得到所有将某个key当做好友的人；再到Reduce中将这些人两两组合成用户对作为key，对应的value是二者的某一共同好友，自定义数据类型Friends就是用于这个阶段。

第二个Map及Reduce用于整合同一好友对下的共同好友们，其输出为：

```
[100,200]:[300,400]
[200,300]:100
[200,400]:100
[300,400]:100
```



## 代码结果
<img src="https://github.com/SheepXI/FBDP_2020Fall_171840013/raw/main/images/image-20201104024149193.png" />


## 运行截图

<img src="https://github.com/SheepXI/FBDP_2020Fall_171840013/raw/main/images/image-20201104024207690.png" alt="image-20201104024207690" style="zoom:67%;" />

# CarCount
各省，市，区间的运行车次统计

mr的Driver类位于/src/main/java/com/bfd/carcount/mr/目录下

此程序主要用于统计各省，市，县间每天往来车次的统计，
例如，一辆车的行驶按时间先后排序，以县级为例：A-A-B-A-C，则统计结果为 A:3,B:2,C:1
车辆所在的第一个位置不计入统计范围，同一辆车进一个区域，后出这个区域，
这个区域的统计次数为2
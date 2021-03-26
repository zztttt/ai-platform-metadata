## 使用规范
该项目实现了一个**数据缓存池**，采用了**write-through**模式来更新**用户表**和**缓存表**。**缓存表**是hardcode的，需要扩展时要手动新增该表的元数据信息。**用户表**是用户新建的，分为实时和非实时两种，实时的每天凌晨一点更新前一千的数据，两种都支持手动更新。  
要支持一张新的表，首先需要在metadata表中有**一行**描述该表的信息，同时要在metadatadetail表中有**若干行**描述该表列的信息。data.sql里面有使用样例

## data.sql
初始化元数据数据库。具体包括metadata，metadatadetail，task三张表

## metadata表
存储有**哪些表**以及表的相关信息

## metadatadetail表
存储每张表的具体信息。一张表有多少column，在这张表里就会有多少行信息。每行具体是对该列的描述。  
windcolumn指的是万得里的万德代码，比如**windcode**指的是证券代码，**lastradeday_s** 指的是交易日期。  
dbcolumn指的是该windcolumn在数据库里面存储的列名。  
usercolumn是对windcolumn的中文描述。

## task表
每次新建一个表，就对应一个task，里面存储了该task的参数信息，以及任务状态。  
如果**type**参数是**实时**，那么就会在后台常驻，并且每次程序重启会自动加载状态为running的task，并将这些task作为一个TaskInstance实例放在线程池内部，每天凌晨一点会自动更新线程池里所有的task，更新的日期为前一天。

# api
## /create
POST请求，用户创建一个新的task。task的调度详见**InitializeBean**，**InitializeTaskPool**和**TaskInstance**文件。
```json
对于wsd类型任务：（usercolumn是没用的，因为它只是对windcolumn的一个中文描述，直接从元数据里面拿取的）
{
    "oldTableName": "wind_AShareDescription_test",
    "newTableName": "new_table29",
    "updateTime": "date",
    "updateUser": "zzt",
    "timeRange": [
        "20190601",
        "20190602"
    ],
    "windCodes":[
        "000001.SZ",
        "000002.SZ",
        "000004.SZ",
        "000005.SZ",
        "000006.SZ"
    ],
    "columns": [
        {
            "windColumn": "windcode",
            "userColumn": ""
        },
        {
            "windColumn": "trade_code",
            "userColumn": ""
        },
        {
            "windColumn": "comp_name",
            "userColumn": ""
        },
        {
            "windColumn": "ipo_date",
            "userColumn": ""
        }
    ],
    type: "实时"
}
对于edb类型任务：（windColumn是没用的，并且windCodes和columns里的数量是一致的，因为usercolumn的作用是显式地描述windCode是什么）
{
    "oldTableName": "wind_GlobalMacrography_test",
    "newTableName": "new_table114",
    "updateTime": "date",
    "updateUser": "zzt",
    "timeRange": [
        "20200201",
        "20201231"
    ],
    "windCodes":[
        "M0048648",
        "M0002021",
        "M0002022"
    ],
    "columns": [
        {
            "windColumn": "",
            "userColumn": "北京GDP累计值"
        },
        {
            "windColumn": "",
            "userColumn": "北京GDP"
        },
        {
            "windColumn": "",
            "userColumn": "北京GDP：第一产业"
        }
    ]
}
```

## /getWindTableName
GET请求。用于返回给前端告诉有哪些表在元数据库里可以用的。是hardcode的

## /getWindTableDetails
POST请求。用于返回给前端告诉指定表的所有列的信息。edb不需要这个，因为它的数据只有一列。
```json
{
    "windTableName": "wind_AShareEODPrices_test"
}
```

## /update
实时类型的任务每天凌晨一点自行更新前一天的数据；同时也支持手动更新指定日期的数据。  
非实时的任务只能手动更新指定日期的数据。
```json
    {
        "oldTableName": "wind_AShareEODPrices_test",
        "newTableName": "new_table34",
        "updateTime": "date",
        "updateUser": "zzt",
        "timeRange": [
            "20190604",
            "20190605"
        ],
        "windCodes":[
            "000001.SZ",
            "000002.SZ",
            "000004.SZ",
            "000005.SZ"
        ],
        "columns": [
            {
                "windColumn": "windcode",
                "userColumn": "u1"
            },
            {
                "windColumn": "lastradeday_s",
                "userColumn": "u2"
            },
            {
                "windColumn": "open",
                "userColumn": "u3"
            },
            {
                "windColumn": "high",
                "userColumn": "u4"
            }
        ]
    }
```
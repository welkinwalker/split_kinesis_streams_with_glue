使用 AWS Glue 从 Kinesis 数据流中分离不同数据库表格
================================================
关系型数据库是数据分析过程中非常普遍的一个数据源。一般我们会通过ETL过程，将数据库中的数据采集并转换为我们需要的格式，再由后端分析工具产生我们需要的结果。

在现代数据仓库架构中，我们推荐基于 Amazon Simple Storage(S3)  的数据湖体系结构，AWS Database Migration Service(DMS) 能帮助我们完成关系型数据库到 S3 的全量和增量数据采集。其操作过程非常简单：
1. 准备 DMS 环境，包括创建 VPC、子网、IAM 角色和安全组，创建 DMS 子网组；
2. 创建 DMS 复制实例，因为 DMS 需要缓存从任务开始时起的数据库变更，所以预留好内存和硬盘应对需要。生产环境下，建议启用 Multi-AZ 保证 DMS 的高可用；
3. 建立指向源数据库和 S3 的 Endpoints；
4. 创建并启动迁移任务，数据库记录就会源源不断的进入S3。

DMS 会按每个表一个目录的方式，把数据库记录存储为 CSV 或 Parquet 格式的 S3 对象。AWS 的 ETL 工具 AWS Glue 可以通过爬虫程序爬取表结构，存储在统一的元数据存储——数据目录中，供各种分析工具调用，比如说，使用 Amazon Athena 或者 Amazon Redshift Spectrum 进行即席查询。

有些时候，我们希望更加迅速的访问到数据库的变更内容，而通过 S3 中转，增加了处理时延，不符合我们的性能需求。这个时候，我们会引入流处理框架。

Amazon Kinesis Data Streams 是在 Amazon 内部和外部都得到广泛使用的流式存储引擎。我们通过 Amazon Kinesis Data Streams，把数据表通过 Kinesis 转化为数据流。不过这种情况下，如果我们想复用这个数据流，进行批式数据处理，会遇到一些问题。当我们通过 Amazon Kinesis Firehose 把数据投递到 S3 后，我们会发现整个流的数据被放置在同一个文件夹下，而且数据是JSON格式，每条记录中包含metadata和data两个一级元素。AWS Glue 的结构爬取程序对记录结构进行解析后，会仅识别为一张只有两个字段的大表。

如果让每个表格使用独立的数据流，可以解决上述问题，但增加了管理难度。如果另起一个 DMS 进程，则会增加源库负担。是否有更其它方法呢？其实我们可以借助 Glue 对 PySpark 语法的扩展，来灵活处理此问题。

具体来讲，就是使用 filter 这个 Transform 方法，基于 metadata 中的 schema name + table name 对记录进行过滤，把不同的表格内容分离出来。

接下来，我们将通过一个 demo 来演示具体操作，假设您已经安装并正确设置了 [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html)，并使用 AWS 东京区域（ap-northeast-1）。

## 1. 新建 Kinesis Data Streams 数据流和 Firehose 投递流
Kinesis Data Streams 的创建非常简单，提供 stream 名称和 shard 数量即可
```
aws kinesis create-stream \
  --stream-name "employees" \
  --shard-count 2 \
  --region ap-northeast-1
```
Kinesis Firehose 支持投递到 Redshift、S3、ElasticSearch 和 Splunk，我们这里以 S3 为例。配置前需要定义好 IAM role 并建好 S3 bucket，ARN 的格式可以参考这个[页面](https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html)，对配置中的 your_account_id、role_name 和 bucket_name 根据实际情况进行替换。
```
echo '''
{
  "RoleARN": "arn:aws:iam::your_acount_id:role/role_name",
  "BucketARN": "arn:aws:s3:::bucket_name",
  "Prefix": "source/employees/!{timestamp:yyyy-MM-dd}",
  "ErrorOutputPrefix": "source/errors/!{firehose:error-output-type}-!{timestamp:yyyy-MM-dd}",
  "BufferingHints": {
    "SizeInMBs": 128,
    "IntervalInSeconds": 600
  },
  "CompressionFormat": "GZIP",
  "CloudWatchLoggingOptions": {
    "Enabled": true,
    "LogGroupName": "deliverystream",
    "LogStreamName": "S3Delivery"
  }
}
''' > s3_settings.json

echo '''
{
  "KinesisStreamARN": "arn:aws:kinesis:ap-northeast-1:your_account_id:stream/employees",
  "RoleARN": "arn:aws:iam::your_account_id:role/role_name"
}
'''> kinesis_settings.json

aws firehose create-delivery-stream \
  --delivery-stream-name "employees" \
  --delivery-stream-type "KinesisStreamAsSource" \
  --kinesis-stream-source-configuration "file://kinesis_settings.json" \
  --s3-destination-configuration "file://s3_settings.json"
```

## 2. 配置 DMS 进行数据采集
DMS 的配置参考[Using Amazon Kinesis Data Streams as a Target for AWS Database Migration Service
](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.Kinesis.html)。要注意的是，DMS 默认使用单线程向 Kinesis 进行投递，因此我们需要对任务进行配置，增加并发度。我们假设您已经正确创建了 Replication instance 和 Endpoints，并经测试可以成功连接。
在示例代码中，我们从一个 MySQL 版本的 RDS 实例，进行全量和增量的数据抽取，通过 MaxFullLoadSubTasks 设置并发处理 8 张表，ParallelLoadThreads 为 32 表示每张表并发 32 线程进行处理。
```
echo '''
{
  "TargetMetadata": {
    "ParallelLoadThreads": 16,
    "ParallelLoadBufferSize":500
  },
  "FullLoadSettings": {
    "MaxFullLoadSubTasks": 8,
    "TransactionConsistencyTimeout": 600,
    "CommitRate": 10000
  },
  "Logging": {
    "EnableLogging": true
  },
  "ControlTablesSettings": {
    "ControlSchema":"dms",
    "HistoryTimeslotInMinutes":5,
    "HistoryTableEnabled": true,
    "SuspendedTablesTableEnabled": true,
    "StatusTableEnabled": true
  },
  "ValidationSettings": {
     "EnableValidation": false,
     "ThreadCount": 5
  }
}
''' > task_settings.json

echo '''
{
  "TableMappings": [
    {
      "Type": "Include",
      "SourceSchema": "employees",
      "SourceTable": "%"
    }
    ]
}
''' > table_mapping.json

aws dms create-replication-task \
  --replication-task-identifier "employees-steams" \
  --source-endpoint-arn arn:aws:dms:ap-northeast-1:your_account_id:endpoint:ARSRJBKL7NLRIWN3NSMQW6OHGY \
  --target-endpoint-arn arn:aws:dms:ap-northeast-1:your_account_id:endpoint:TOTJIZQDMJANNC2CY2CNGUVH74 \
  --replication-instance-arn arn:aws:dms:ap-northeast-1:your_account_id:rep:EIKUGIRSZIHP7TDYCBZ6EUFIFA \
  --migration-type "full-load-and-cdc" \
  --table-mappings 'file://table_mapping.json' \
  --replication-task-settings 'file://task_settings.json' 
```
当看到任务状态转为 ready 后，启动任务：
```
aws dms start-replication-task \
  --replication-task-arn arn:aws:dms:ap-northeast-1:your_account_id:task:5M4LJ567IL3RAM4PSVNZUL6DP4 \
  --start-replication-task-type start-replication
```

## 3. 增加一个 Glue Job 来进行表格分离操作
可以先创建一个 Glue Crawler ，对 Firehose 投递到 S3 中的内容进行爬取，我们可以看到仅有 metadata 和 data 两个字段。
![schema_of_source](https://github.com/Nickbehindgfw/split_kinesis_stream_with_glue/raw/master/image/image1.png)  
实际上， 

### 从 S3 对象创建一个 DDF


### 根据表名进行拆分


### relationize 


### 写入 S3



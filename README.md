# split_kinesis_stream_with_glue
关系型数据库是数据分析过程中非常普遍的一个数据源。一般我们会通过ETL过程，将数据库中的数据采集并转换为我们需要的格式，再由后端分析工具产生我们需要的结果。

在现代数据仓库架构中，我们推荐基于 S3 的数据湖体系结构，AWS Database Migration Service（DMS）能帮助我们完成关系型数据库到 S3 的全量和增量迁移。其操作过程非常简单：
0. 准备 DMS 环境，包括创建 VPC、Subnets、IAM roles 和 Security groups，创建 DMS Subnet groups；
1. 创建好DMS环境；
2. 指定RDBMS数据源；
3. 指定S3存储位置

DMS 会按每个表一个目录的方式，把数据库记录存储为 CSV 或 Parquet 格式的 S3 文件对象。AWS 的 ETL 工具 AWS Glue 可以爬取表结构，存储在统一的元数据存储中，供各种分析工具调用。

不过，有些时候，我们希望更加迅速的访问到数据库的变更内容，通过 S3 中转，增加了处理时延，不符合我们的性能需求。这个时候，我们会引入流处理框架。

Amazon Kinesis Data Streams 是在 Amazon 内部和外部都得到广泛使用的流式存储引擎。我们通过 Amazon Kinesis Data Streams，把数据表通过 Kinesis 转化为数据流。那么，是否我们需要为每一个表建立一个采集任务呢？DMS 可以通过通配符，在一个任务内，采集多个库、表内容，并加载到同一个流内。而 AWS 的 ETL 工具 Glue，则具有从单一流中，分离出单个表格的能力。





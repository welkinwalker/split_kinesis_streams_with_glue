# 使用 AWS Glue 从 Kinesis 数据流中分离不同数据库表格
关系型数据库是数据分析过程中非常普遍的一个数据源。一般我们会通过ETL过程，将数据库中的数据采集并转换为我们需要的格式，再由后端分析工具产生我们需要的结果。

在现代数据仓库架构中，我们推荐基于 Amazon Simple Storage(S3)  的数据湖体系结构，AWS Database Migration Service(DMS) 能帮助我们完成关系型数据库到 S3 的全量和增量数据采集。其操作过程非常简单：
1. 准备 DMS 环境，包括创建 VPC、子网、IAM 角色和安全组，创建 DMS 子网组；
2. 创建 DMS 复制实例，因为 DMS 需要缓存从任务开始时起的数据库变更，所以预留好内存和硬盘应对需要。生产环境下，建议启用 Multi-AZ 保证 DMS 的高可用；
3. 建立指向源数据库和 S3 的 Endpoints；
4. 创建并启动迁移任务，数据库记录就会源源不断的进入S3。

DMS 会按每个表一个目录的方式，把数据库记录存储为 CSV 或 Parquet 格式的 S3 对象。AWS 的 ETL 工具 AWS Glue 可以通过爬虫程序爬取表结构，存储在统一的元数据存储——数据目录中，供各种分析工具调用，比如说，使用 Amazon Athena 或者 Amazon Redshift Spectrum 进行即席查询。

有些时候，我们希望更加迅速的访问到数据库的变更内容，而通过 S3 中转，增加了处理时延，不符合我们的性能需求。这个时候，我们会引入流处理框架。

Amazon Kinesis Data Streams 是在 Amazon 内部和外部都得到广泛使用的流式存储引擎。我们通过 Amazon Kinesis Data Streams，把数据表通过 Kinesis 转化为数据流。不过这种情况下，如果我们想复用这个数据流，进行批式数据处理，会遇到一些问题。当我们通过 Amazon Kinesis Firehose 把数据投递到 S3 后，我们会发现整个流的数据被放置在同一个文件夹下，而且数据是JSON格式，每条记录中包含metadata和data。AWS Glue 的结构爬取程序对记录结构进行解析后，会仅识别一张表。

如果让每个表格使用独立的数据流，可以解决上述问题，但增加了管理难度。是否有更其它方法呢？其实我们可以借助 Glue 对 PySpark 语法的扩展，来灵活处理此问题。

具体来讲，就是使用 SplitRows 这个 Transform 方法，基于 metadata 中的 schema name + table name 对记录进行过滤，把不同的表格内容分离出来。




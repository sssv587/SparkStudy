## Spark的运行环境
- Local模式
  - bin/spark-shell
- standAlone模式
  - master-worker模式
- Yarn模式(国内运用最多的模式)
  - Yarn Client模式
    - Client 模式将用于监控和调度的 Driver 模块在客户端执行，而不是在 Yarn 中，所以一般用于测试
    - Driver 在任务提交的本地机器上运行
    - Driver 启动后会和 ResourceManager通讯申请启动ApplicationMaster
    - ResourceManager分配container，在合适的NodeManager上启动ApplicationMaster，负责向ResourceManager申请Executor内存
    - ResourceManager接到ApplicationMaster的资源申请后会分配container，然后ApplicationMaster在资源分配指定的NodeManager上启动Executor进程
    - Executor 进程启动后会向 Driver 反向注册，Executor 全部注册完成后 Driver 开始执行main函数
    - 之后执行到Action算子时，触发一个Job，并根据宽依赖开始划分stage，每个stage生成对应的TaskSet，之后将task分发到各个Executor上执行
  - Yarn Cluster模式
    -  
- k8s & Mesos

## Spark的运行架构
- Driver => 负责管理整个集群中的作业任务调度
  - 将用户程序转换为作业(job)
  - 在Executor之间调度任务(task)
  - 跟踪Executor的执行情况
  - 通过UI展示查询运行情况
- Executor => 负责实际执行任务
  - 负责运行组成Spark应用的任务，并将结果返回给驱动器进程
  - 它们通过自身的块管理器（Block Manager）为用户程序中要求缓存的 RDD 提供内存式存储。RDD 是直接缓存在 Executor 进程内的，因此任务可以在运行时充分利用缓存数据加速运算。
- Master & Worker
  - Master是一个进程，主要负责资源的调度和分配，并进行集群的监控等职责，类似于Yarn环境中的RM
  - Worker 运行在集群中的一台服务器上，由 Master 分配资源对数据进行并行的处理和计算，类似于Yarn环境中NM
- ApplicationMaster
  - 用于向资源调度器申请执行任务的资源容器Container，运行用户自己的程序任务job，监控整个任务的执行，跟踪整个任务的状态，处理任务失败等异常情况

## 介绍一下RDD


## RDD的内存创建，分区数

## RDD的文件创建，分区数

## map和mapPartitions的区别
- 数据处理角度
  - map算子是分区内一个数据一个数据的执行，类似于串行操作。
  - mapPartitions算子是以分区为单位进行批处理操作。
- 功能的角度
  - map算子主要目的将数据源中的数据进行转换和改变。但是不会减少或增多数据。
  - mapPartitions算子需要传递一个迭代器，返回一个迭代器，没有要求的元素的个数保持不变，所以可以增加或减少数据
- 性能的角度
  - map算子因为类似于串行操作，所以性能比较低
  - mapPartitions算子类似于批处理，所以性能较高。
  - mapPartitions算子会长时间占用内存，那么这样会导致内存可能不够用，出现内存溢出的错误。所以在内存有限的情况下，不推荐使用
  
  
## cache(persist)与checkpoint的区别
- cache只是将数据缓存起来，不切断血缘依赖；checkpoint检查点切断血缘依赖
- cache缓存的数据通常存储在磁盘、内存等地方，job执行完成后会删除，可靠性低；checkpoint的数据通常保存在HDFS等容错、高可用的文件系统，job执行完成后不会删除，可靠性靠
- 建议对checkpoint()的RDD使用cache缓存，这样checkpoint的job只需从cache缓存读取数据即可，否则需要再重头计算依次RDD
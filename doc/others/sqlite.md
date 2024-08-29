这个文档用于记录查看 SQLite 文档所记录下来的内容，便于此项目做设计和开发。
链接地址为 https://www.sqlite.org/docs.html。

从 SQLIte 的架构图看，大致分为 core 和 backend 模块。

在 core 模块中，分为 interface、Sql command processor 和 visual machine 三个模块。其中 sql command processor 模块是核心模块，负责解析 sql 语句，生成 sql 命令。而 visual machine 模块是 sql 命令的执行模块，负责执行 sql 命令。

interface 包含了整个项目的对外接口。

tokenizer 负责分析 sql 语句，生成 token，然后调用 parser 模块进行语法分析，SQLite 使用了 Lemon，最后时生成计划，类似 where 和 select 会包含各种复杂的与机器有关的计划。

在执行时，有一个字节码引擎，将任务以字节码形式在虚拟机中运行。

执行模块包含 b树、page cache 和 os 接口。其中 page cache 缓存负责了所有对 page cache 的处理，包括锁定、提交、回滚等，os 接口里提供了一个名为 vfs 的接口抽象，支持了 linux 和 windows 。

其他包括 util 和 测试代码。
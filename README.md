# 电商大数据综合实践项目

> 
> 大数据离线分析 + Flask Web 可视化综合实训项目
> 使用 Spark RDD / MapReduce 双版本实现电商数据分析，结合 ECharts 完成结果可视化展示

## 📖 项目简介

本项目基于电商原始数据集，实现从原始文本数据清洗、多表关联、多维指标统计、数据入库到 Web 可视化展示的完整大数据离线分析流程。
分别采用 **MapReduce(Java)** 和 **Spark RDD(Scala)** 两套技术实现相同业务逻辑，部署于 Hadoop 集群运行；后端使用 Flask 开发 Web 服务，LayUiMini 做后台管理框架，ECharts 渲染统计图表，直观展示商户、用户消费相关分析指标。

**业务分析任务：**

1. 数据清洗：过滤空字段脏数据，输出干净数据集
2. 商户画像构建：按店铺聚合服装风格，去重拼接生成店铺风格画像
3. 商铺订单数量统计：统计每个商铺订单总量
4. 用户穿衣画像：聚合用户历史购买服装风格，生成用户画像
5. 用户消费习惯分析：时间戳转换星期，区分工作日 / 周末统计用户下单偏好

## 🛠 技术栈

### 大数据离线计算

- Hadoop：HDFS 分布式存储、MapReduce 计算框架、YARN 资源调度
- Spark：Spark RDD（Scala 开发），本地 本地 模式 + YARN 集群模式运行
- MySQL：存储清洗后数据与业务统计结果，作为可视化数据源
- Java：MapReduce 业务代码开发
- Scala：Spark RDD 业务代码开发

### Web 可视化模块

- Python Flask：Web 后端服务，路由接口开发、MySQL 数据查询
- pymysql：Python 操作 MySQL 数据库
- LayUiMini：前端后台管理 UI 框架，菜单与页面布局
- ECharts：图表渲染（柱状图、饼图、折线图）
- HTML / JavaScript：前端页面开发
## 数据库表说明

表格

| 表名 | 用途 |
| --- | --- |
| cleared_order | 清洗后的订单商品数据表 |
| cleared_products | 清洗后的商品数据表 |
| price_order_count | 商铺订单数量统计结果表 |
| style_group_output | 商户画像（店铺服装风格）结果表 |
| user_style_profile | 用户穿衣画像结果表 |
| weekday_profile | 用户工作日 / 周末消费习惯统计表 |

## 🚀 运行步骤

### 1. 环境准备

1. 启动 Hadoop 集群 (HDFS + YARN)
2. MySQL 服务启动，执行 sql 文件夹中建表语句，创建业务数据表
3. 本地环境准备：JDK、Scala、Maven、Python3，安装 flask、pymysql 依赖包

### 2. MapReduce 运行

1. 将原始数据集上传至 HDFS
2. Maven 编译项目，生成可执行 Jar 包
3. **本地测试**：本地运行 Driver 类调试逻辑
4. **集群运行**：使用 yarn 命令提交 jar 包至集群执行任务
5. 将 HDFS 输出目录结果导入 MySQL 对应数据表

### 3. Spark RDD 运行

1. 本地模式：`local[*]`模式调试代码，验证处理逻辑
2. 集群模式：打包提交 Spark 任务到 YARN 执行
3. 将分析输出结果批量写入 MySQL

> 
> 任务执行完成后，HDFS 会生成 5 个业务输出目录，分别对应 5 个分析任务结果。

### 4. Flask Web 可视化启动

```
# 进入flask项目目录
cd flask_wcq
# 安装依赖
pip install flask pymysql
# 启动web服务
python app.py
```

浏览器访问：`http://127.0.0.1:5000`
侧边菜单【电商数据统计分析】下包含 5 个子页面，分别查看各项分析图表：

- 商户订单统计（柱状图）
- 商户风格分布（饼图）
- 用户穿衣画像（柱状图）
- 用户消费时段趋势（折线图）

## ✨ 核心实现要点

1. **数据清洗**：校验全部字段非空，过滤空字符串脏数据，避免空 key 引发统计异常。
2. **性能优化**：Spark 使用`reduceByKey`替代`groupByKey`减少 Shuffle 网络 IO；中间 RDD 使用`cache()`缓存复用；MapReduce 使用 Combiner 做局部聚合。
3. **多表关联**：MapReduce 通过 Mapper 标记数据源，Reducer 端实现表关联；Spark 使用`join`算子完成商品 ID 多表 JOIN，打通用户‑商品‑店铺关联关系。
4. **时间处理**：订单毫秒时间戳转换为东八区星期，区分工作日（1‑5）、周末（6‑7）统计下单频次。
5. **前后端联调**：后端查询 MySQL 数据，使用`tojson`过滤器序列化，前端 ECharts 解析渲染图表；统一 static 路径解决 LayUiMini 静态资源 404 问题。

## ⚠️ 常见问题与解决方案

1. **脏数据导致统计结果错误**

> 
> 解决：遍历一行所有字段，全部非空才保留，过滤掉存在空值的行。

2. **Spark 集群运行慢，Shuffle 数据量大**

> 
> 解决：替换 groupByKey 为 reduceByKey；对多次复用 RDD 添加 cache 缓存。

3. **MapReduce 多表关联产生冗余笛卡尔积**

> 
> 解决：Mapper 标记数据来源，Reducer 内做左连接，过滤无效匹配数据。

4. **时间戳转换星期结果错误，时区偏移**

> 
> 解决：封装统一日期转换工具类，指定东八区时区，提取星期数字。

5. **ECharts 图表无数据展示**

> 
> 解决：后端元组列表使用 jinja2 `tojson`序列化，前端 JS 拆分 x 轴、y 轴数组。

6. **LayUiMini 页面样式丢失，资源 404**

> 
> 解决：静态资源引用统一以`static/`为前缀，菜单配置文件路由与 Flask 后端路由保持一致。

## 📊 项目输出成果

1. 两套分布式代码（MapReduce + Spark RDD）完成 5 项电商数据分析，可本地、Hadoop 集群双环境运行；
2. 清洗得到有效订单 154 条、商品 40 条，全部统计指标落地 MySQL；
3. Web 可视化平台，支持柱状图、饼图、折线图多图表展示商户、用户多维度分析结果；
4. 完整实现：原始文本 → 清洗过滤 → 多表关联聚合 → 入库存储 → Web 可视化完整大数据业务闭环。

## 📌 后续改进方向

1. 使用 Spark SQL / DataFrame 改写 RDD 代码，简化多表关联逻辑；
2. 引入数据仓库分层 ODS/DWD/DWS 对数据分层管理，提高数据复用；
3. 增加 Spark 数据倾斜处理优化：广播变量、自定义分区；
4. Web 端增加下拉筛选、时间范围选择等交互式图表功能。

## 工具
用go实现的实用工具，目前有以下功能
1. ES索引数据对比
### ES索引数据对比
#### 特色
1. 支持同集群和不同集群索引对比，支持es5跟7对比
2. 默认使用scroll获取参照索引数据，批量获取需要对比索引的数据
3. 获取参照索引数据时支持Slice并发，通过-S指定slice数，建议设置为索引分片数
4. 获取参照索引数据时支持支持根据指定字段拆分请求并发，通过-f指定字段，-c指定分段数，例如：-f order_time -c 10 按照order_time字段分10段并发请求
5. 支持只对比指定条件的数据，通过-q参数指定，这种情况不支持slice并发
6. 对比不一致的_doc.id会存在当前目录的diffIds文件
7. 对比过程中支持自动reindex不一致的数据，通过-r true设置

> 因为scroll使用的是snapshot，调用scroll期间感知不到新数据
> 所以使用scroll对比发现不一致数据时，会使用search获取数据再次对比，确保对比结果的准确性
#### 示例
```
同集群不同索引全量对比:
	./tool compare \
	-a http://127.0.0.1:9200 -u username -p password -i index \
	-I index1 \
	-s 4000 \
	-S 8
不同集群索引全量对比，对比过程中自动reindex不一致数据:
	./tool compare \
	-a http://127.0.0.1:9200 -u username -p password -i index \
	-H http://0.0.0.0:9200 -U username1 -P password1 -I index1 \
	-s 4000 \
	-S 8 \
	-r true
对比指定条件数据:
	./tool compare \
	-a http://127.0.0.1:9200 -u username -p password -i index \
	-H http://0.0.0.0:9200 -U username1 -P password1 -I index1 \
	-s 4000 \
	-q '{"query":{"terms":{"id":["1715478400352693852"]}}}'
根据order_time分10段并发对比:
	./tool compare \
	-a http://127.0.0.1:9200 -u username -p password -i index \
	-H http://0.0.0.0:9200 -U username1 -P password1 -I index1 \
	-s 4000 \
	-f order_time \
	-c 10
```
#### 帮助
```
tool compare -h 查看帮助

Flags:
  -a, --srcHost string       Source ES host: http//ip:port
  -i, --srcIndex string      Source ES index
  -p, --srcPassword string   Source ES password
  -u, --srcUserName string   Source ES userName
  -A, --dstHost string       Destination ES host: http//ip:port, default same as srcHost.
  -I, --dstIndex string      Destination ES index, It should be different from srcIndex when dstHost is same as srcHost.
  -P, --dstPassword string   Destination ES password, default same as srcPassword.
  -U, --dstUserName string   Destination ES userName, default same as srcUserName.
  -s, --size int             Batch size (default 1000)
  -S, --slices int           Scroll slices, suggest to use the same number of shards (default 1)
  -f, --field string         Custom concurrency field (default "order_time")
  -c, --concurNum int        Number of custom concurrency queries (default 1)
  -q, --query string         Query body
  -r, --repair               Auto repair
  -h, --help                 help for compare
```

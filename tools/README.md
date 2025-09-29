# MongoDB 授权管理工具

这是一个用于管理 MongoDB 中节点授权状态的命令行工具，支持以下功能：

- 列出所有节点及其授权状态
- 授权单个节点
- 取消授权单个节点
- 批量授权多个节点
- 批量取消授权多个节点

## 编译

```bash
go build -o mongo_auth_tool tools/mongo_auth_tool.go
```

## 使用方法

### 列出所有节点

```bash
./mongo_auth_tool -action list [-output nodes_list.json]
```

- `-output`: 可选参数，指定输出文件名，默认为 `nodes_list.json`

### 授权单个节点

```bash
./mongo_auth_tool -action authorize -node <节点ID>
```

- `-node`: 必须参数，指定要授权的节点 ID

### 取消授权单个节点

```bash
./mongo_auth_tool -action unauthorize -node <节点ID>
```

- `-node`: 必须参数，指定要取消授权的节点 ID

### 批量授权节点

```bash
./mongo_auth_tool -action batch-authorize -file <节点ID列表文件>
```

- `-file`: 必须参数，指定包含节点 ID 列表的文件路径

节点 ID 列表文件格式：每行一个节点 ID，例如：

```
slave-e0c22751
slave-f1d33862
slave-g2e44973
```

### 批量取消授权节点

```bash
./mongo_auth_tool -action batch-unauthorize -file <节点ID列表文件>
```

- `-file`: 必须参数，指定包含节点 ID 列表的文件路径

## 示例

1. 列出所有节点并保存到 `all_nodes.json`:

```bash
./mongo_auth_tool -action list -output all_nodes.json
```

2. 授权单个节点:

```bash
./mongo_auth_tool -action authorize -node slave-e0c22751
```

3. 取消授权单个节点:

```bash
./mongo_auth_tool -action unauthorize -node slave-e0c22751
```

4. 批量授权节点:

```bash
./mongo_auth_tool -action batch-authorize -file nodes_to_authorize.txt
```

5. 批量取消授权节点:

```bash
./mongo_auth_tool -action batch-unauthorize -file nodes_to_unauthorize.txt
```

## 自定义 MongoDB 连接

如果需要使用不同的 MongoDB 连接，可以使用 `-uri` 参数:

```bash
./mongo_auth_tool -uri "mongodb+srv://username:password@cluster.example.net" -action list
```

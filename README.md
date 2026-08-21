# job-neo

持久化任务队列（Node.js），基于 [vkv-neo](https://www.npmjs.com/package/vkv-neo) 存储。开箱即用的延迟任务、定时任务（cron）、重试退避、优先级队列、多 worker 并发与任务依赖 / DAG。无需 Redis、无需数据库，数据落本地磁盘，进程崩溃不丢任务。

## 特性

- **持久化**：任务写入 vkv-neo（WAL + fsync），重启进程后任务仍在；崩溃后遗留的 `running` 任务自动重置为 `pending`
- **延迟执行**：`delay: '5m'` / `'10s'` / 毫秒数 / Date
- **定时任务**：5 段 cron 表达式（支持 `*`、`*/n`、`a-b`、`a,b`、单值）
- **重试机制**：指数退避 / 固定间隔，可配置 base / factor / max
- **优先级队列**：`priority` 越高越先执行，同优先级按创建顺序
- **多 worker**：可配置 `concurrency`，同时消费多个任务
- **依赖 / DAG**：按任务 id 或 name 声明依赖，支持级联失败与阻塞等待
- **事件钩子**：`enqueued` / `started` / `completed` / `failed` / `retrying` / `drained`

## 安装

```bash
npm install vkv-neo
# 或直接 clone 本仓库使用
```

## 快速开始

```js
const job = require('job-neo');

// 消费任务
job.process('send-email', async (payload) => {
  console.log('发邮件:', payload);
});

// 创建任务（5 分钟后执行，最多重试 3 次，高优先级）
const id = await job.enqueue('send-email', {
  to: 'user@example.com',
  subject: 'Hello'
}, {
  delay: '5m',
  retry: 3,
  priority: 10
});

// 定时任务：每天 2 点执行
await job.schedule('backup', {}, '0 2 * * *');

// 查看状态
await job.status(id); // pending / waiting / running / completed / failed / cancelled
```

> 默认存储路径为 `./job-neo.ndb`。用 `init()` 显式配置：

```js
await job.init({ storage: './data/queue.ndb' });   // 磁盘持久化
await job.init({ storage: ':memory:' });           // 仅内存
```

## API

### 创建任务

#### `await job.enqueue(name, payload, options)` → `id`

| 选项 | 类型 | 默认 | 说明 |
| --- | --- | --- | --- |
| `delay` | `string \| number \| Date` | `0` | 延迟执行：`'5m'` / `'10s'` / `'2h'` / `'1d'` / 毫秒 / Date |
| `retry` | `number` | `0` | 失败后最多重试次数 |
| `priority` | `number` | `0` | 优先级，越大越先执行 |
| `dependencies` | `string[]` | `-` | 依赖的任务 id 或 name，全部完成后才执行 |
| `backoff` | `object \| number` | 指数退避 | 见[重试退避](#重试退避) |
| `id` | `string` | uuid | 自定义任务 id |
| `tags` | `string[]` | `-` | 自定义标签 |
| `schedule` / `scheduleId` | - | `-` | 由 `schedule()` 内部使用，标识实例来源 |

```js
const id = await job.enqueue('send-email', { to: 'a@b.c' }, {
  delay: '5m',
  retry: 3,
  priority: 10
});
```

#### `await job.enqueueMany(items)` → `string[]`

批量入队。`items: [{ name, payload, options }]`

```js
const ids = await job.enqueueMany([
  { name: 'send-email', payload: { to: 'a@b.c' } },
  { name: 'send-email', payload: { to: 'd@e.f' }, options: { priority: 5 } }
]);
```

#### `await job.dag(nodes)` → `Record<name, id>`

以图谱形式一次性入队 DAG，自动解析依赖并保证执行顺序。

```js
await job.dag([
  { name: 'fetch',  id: 'fetch',  payload: {} },
  { name: 'parse',  id: 'parse',  payload: {}, dependencies: ['fetch'] },
  { name: 'save',   id: 'save',   payload: {}, dependencies: ['fetch', 'parse'] }
]);
// 执行顺序：fetch → parse → save
```

#### `job.create(name, definition)` → `name`

注册一个可复用任务定义（payload / options），供依赖引用。

### 定时任务

#### `await job.schedule(name, payload, cronExpr, options)` → `id`

| 选项 | 类型 | 默认 | 说明 |
| --- | --- | --- | --- |
| `overlap` | `boolean` | `false` | 允许上一次实例未结束时再次触发 |
| `enabled` | `boolean` | `true` | 是否启用 |
| `payload` | `any` | - | 每次触发传给 handler 的 payload |

```js
await job.schedule('backup', {}, '0 2 * * *');   // 每天 02:00
await job.schedule('report', { type: 'daily' }, '*/30 * * * *'); // 每 30 分钟
```

其他方法：

- `job.cancelCron(name)` / `job.removeCron(name)`：删除定时任务
- `job.getCron(name)`：查看单个定时定义
- `job.crons()`：列出全部定时定义

cron 表达式为 5 段：`分 时 日 月 周`。周 `0` 或 `7` 均表示周日；日与周同时受限时满足其一即可（标准 cron 行为）。

### 消费任务

#### `job.process(name, handler)`

```js
job.process('send-email', async (payload, ctx) => {
  console.log('发邮件:', payload);
  // ctx: { id, name, attempt, payload, queue, retry }
});
```

- handler 为 `async` 函数；抛错 / reject 视为失败，触发重试
- `ctx.retry(delay)`：在 handler 内手动安排重试
- `job.processMany({ name: handler, ... })`：批量注册
- `job.unprocess(name)`：注销处理器

### 查询 / 管理

| 方法 | 说明 |
| --- | --- |
| `await job.status(id)` | 状态：`pending` / `waiting` / `running` / `completed` / `failed` / `cancelled` |
| `await job.get(id)` | 完整任务记录（含 payload / 重试次数 / 时间戳 / 错误） |
| `job.list({ status, name })` | 列出任务，可按状态 / 名称过滤 |
| `await job.cancel(id)` | 取消尚未运行的任务 → `true/false` |
| `await job.retry(id, { delay, resetAttempts })` | 强制重跑 failed / cancelled / completed 任务 |
| `await job.remove(id)` | 删除任务记录（运行中的不可删除） |

### 生命周期

```js
await job.init({ storage, concurrency, pollInterval });
job.start();            // 开始调度（init 后自动开始）
job.stop();             // 停止调度（已认领任务会执行完）
job.pause();            // 暂停领取新任务
job.resume();           // 恢复
job.setConcurrency(n);  // 动态调整并发
job.setPollInterval(ms);// 调整轮询间隔（默认 200ms）
await job.close();      // 停止并刷盘、关闭存储
```

创建独立队列实例（互不干扰）：

```js
const { createQueue } = require('job-neo');
const q = createQueue({ storage: ':memory:', concurrency: 2 });
await q.init();
```

### 事件钩子

```js
job.on('enqueued', (job) => {});
job.on('started',  (job) => {});
job.on('completed',(job) => {});
job.on('failed',   (job) => {});
job.on('retrying', (job) => {}); // job.nextAttemptAt 为下次执行时间
job.on('drained',  ()  => {});   // 所有任务执行完毕
job.on('error',    (err) => {});
```

### 重试退避

默认指数退避：`base * factor^(attempts-1)`，封顶 `max`。

```js
// 默认：base 1s，factor 2，max 5min
await job.enqueue('x', {}, { retry: 3 });

// 自定义指数退避
await job.enqueue('x', {}, {
  retry: 5,
  backoff: { type: 'exponential', base: 1000, factor: 2, max: 60000 }
});

// 固定间隔
await job.enqueue('x', {}, { retry: 3, backoff: { type: 'fixed', base: '10s' } });
```

## 存储设计

存储基于 vkv-neo 的 KV 模型（磁盘模式下数据落 `storage` 指定的路径，WAL + fsync 保证崩溃安全）。任务以 JSON 序列化后存储，键结构如下：

| 键 | 值 | 说明 |
| --- | --- | --- |
| `job:<id>` | JSON | 任务记录（对应你设计的 `jobs` 表） |
| `cron:<name>` | JSON | 定时任务定义 |
| `__ids` | `string[]` | 全部任务 id 索引 |
| `__crons` | `string[]` | 定时任务名索引 |
| `__last:<name>` | `id` | 最近一次创建的某 name 任务（供按 name 依赖） |

任务记录字段（对应 `jobs` 表设计）：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `id` | `string` | 任务唯一 id |
| `name` | `string` | 任务类型（对应 handler） |
| `payload` | `any` | 任务数据 |
| `status` | `string` | `pending` / `waiting` / `running` / `completed` / `failed` / `cancelled` |
| `priority` | `number` | 优先级 |
| `schedule` / `scheduleId` | `string \| null` | 来源 cron 表达式 / 定义 |
| `delay` | `number` | 入队时的延迟毫秒数 |
| `retry` / `attempts` | `number` | 最大重试次数 / 已尝试次数 |
| `runAt` | `number` | 计划执行时间戳 |
| `dependencies` | `string[] \| null` | 依赖的任务 id / name |
| `backoff` | `object` | 退避配置 |
| `createdAt` | `number` | 创建时间 |
| `startedAt` | `number \| null` | 开始时间 |
| `completedAt` | `number \| null` | 完成 / 失败 / 取消时间 |
| `error` | `string \| null` | 最近一次错误信息 |

## 并发与顺序保证

- **优先级**：每次轮询按 `priority` 降序、`runAt` 升序、`createdAt` 升序取任务
- **并发**：最多同时运行 `concurrency` 个任务；超出部分留在队列等待
- **依赖**：`waiting` 状态的依赖任务会阻塞后续任务；依赖失败时级联标记为 `failed`
- **防抖**：同一 cron 的上一个实例未结束时，默认跳过本次触发（除非 `overlap: true`）

## 测试

```bash
npm test
```

覆盖：基本入队消费、延迟执行、重试退避、优先级排序、DAG 串行、cron 解析、多 worker 并发、取消 / 删除、磁盘持久化跨重启。

## License

Apache-2.0

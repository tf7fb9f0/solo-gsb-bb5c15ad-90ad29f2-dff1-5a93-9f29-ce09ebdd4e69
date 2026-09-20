# 🎙️ 发言席位（Speak Queue）

适合**线下讨论、会议投屏**使用的多人发言席位 Web 应用：

- 创建者一键生成 **6 位房间码**，即主持人；
- 参与者用**昵称加入**，自行**申请 / 取消发言**；
- 主持人可**拖动排序**、**开始 / 结束发言**、将成员移出队列、**一键清空**；
- 大屏展示当前发言人、大号**发言计时**、发言队列、在场成员与**操作记录**；
- 弱网/刷新/休眠后**自动断线重连**，恢复一致的队列顺序与当前发言计时；
- 所有队列命令携带客户端所见**版本号**，服务端**原子校验 + 单调递增**，
  过期命令被拒绝并返回最新快照，多端操作不会互相覆盖。

技术栈：Node.js + Express + ws（WebSocket），前端为原生 HTML / CSS / JavaScript，无构建步骤。

---

## 快速开始（Docker）

需要安装 Docker（含 Compose v2）。

```bash
docker compose up --build
```

该命令会自动完成依赖安装、镜像构建并启动服务。启动后浏览器访问：

> **http://localhost:3000**

主持人打开页面 → 「创建房间」→ 把 6 位房间码（或「复制邀请链接」）投给参与者；
参与者打开同一地址输入房间码和昵称即可加入。

停止服务：

```bash
docker compose down
```

> 若希望使用其他宿主机端口（例如 8080）：
>
> ```bash
> PORT=8080 docker compose up --build
> ```
>
> 然后访问 http://localhost:8080

---

## 本地直接运行（不使用 Docker）

```bash
npm install
npm start
```

默认监听 http://localhost:3000 。

运行端到端测试（会自动在 3099 端口拉起一个测试服务实例）：

```bash
npm test
```

---

## 配置项（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PORT` | `3000` | 服务监听端口。compose 中同时用它控制宿主机映射端口（`${PORT:-3000}:3000`） |
| `HOST` | `0.0.0.0` | 监听地址 |
| `WS_PATH` | `/ws` | WebSocket 挂载路径 |
| `ROOM_TTL_MS` | `14400000`（4 小时） | 房间内**所有人离线**后，房间保留多久再回收 |
| `LOG_LIMIT` | `500` | 服务端保留的操作记录最大条数（超出滚动淘汰） |

compose.yaml 的 `environment` 段可直接修改容器内配置；宿主机端口可通过
`docker compose up --build` 前设置 `PORT` 环境变量覆盖。

---

## 使用说明

### 主持人（创建房间者）

- 页面会显示黄色「主持人」标识；创建信息保存在本浏览器，刷新/断线自动恢复主持身份。
- **拖动**队列条目前的 ⠿ 手柄即可调整发言顺序（鼠标与触屏均可）。
- 每条队列可点 **▶ 开始** 请其发言（自动从队列移除并开始计时），舞台区可随时**结束发言**。
- **一键清空**：清空发言队列、结束当前发言并重置操作记录。
- 另有单独的**清空记录**按钮，只清空操作记录。

### 参与者

- 输入房间码 + 昵称加入；断线后自动用原身份重连（刷新页面也会恢复席位与队列位置）。
- 点 **🙋 申请发言** 排入队尾，排队中可随时**取消申请**。
- 正在发言时无法重复申请；离线成员在队列中会标注「离线」，重连后自动归位。

### 投屏建议

页面采用大字号、高对比布局；建议使用全屏浏览器（F11）。窗口较窄时布局自动变为单列。

---

## 协议与一致性设计

所有消息均为 WebSocket 文本帧（JSON）。

**客户端 → 服务端**（除 `join` / `leave` 外均需带 `version`）：

```
{ "type": "request_speak",  "version": 12 }
{ "type": "cancel_speak",   "version": 12 }
{ "type": "start_speaking", "version": 12, "participantId": "p_xxx" }   // 主持人
{ "type": "end_speaking",   "version": 12 }                             // 主持人
{ "type": "reorder",        "version": 12, "order": ["p_a", "p_b"] }    // 主持人
{ "type": "remove",         "version": 12, "participantId": "p_xxx" }   // 主持人
{ "type": "clear",          "version": 12 }                             // 主持人
{ "type": "clear_logs",     "version": 12 }                             // 主持人
```

- 服务端在 Node 单事件循环中**同步**处理每条命令：校验 `version === 当前版本`
  后立即修改状态、`version += 1`，并向房间内所有连接广播快照——整个过程不可被其他命令插入。
- 版本不匹配时返回 `{ type:"error", code:"STALE_VERSION", snapshot:{…最新快照…} }`，
  客户端用其覆盖本地状态后再渲染，杜绝过期覆盖。
- 每次快照附带 `serverTs`，客户端据此修正本机时钟偏差，发言计时基于服务端的
  `speaking.startedAt`，重连后计时连续不重置。
- 断线不会立即删除席位：成员标记为离线、队列位置与当前发言均保留；
  参与者凭 `participantId + secret`、主持人凭 `hostToken` 重连恢复。

完整消息清单见 `server.js` 顶部注释，行为用例见 `test-e2e.js`。

---

## 项目结构

```
.
├── server.js          # Express + WebSocket 服务端（房间、版本化命令、记录、重连）
├── public/
│   ├── index.html     # 首页与房间视图
│   ├── style.css      # 投屏优化样式
│   └── app.js         # WS 客户端、自动重连、渲染、拖拽排序、计时
├── test-e2e.js        # 端到端协议测试（npm test）
├── Dockerfile
├── compose.yaml
└── package.json
```

## 说明 / 限制

- 房间状态保存在**内存**中（适合一次线下会议场景）：进程重启或房间超过
  `ROOM_TTL_MS` 无人在线后数据清除；无外部数据库依赖。
- 主持人能力由服务端通过 `hostToken` 校验，令牌仅保存在创建者浏览器的 localStorage 中。

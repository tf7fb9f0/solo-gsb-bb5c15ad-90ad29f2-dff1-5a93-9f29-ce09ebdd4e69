/**
 * 多人发言席位 —— HTTP + WebSocket 服务
 *
 * 协议（均为 JSON 文本帧）：
 *
 * 客户端 -> 服务端
 *   { type: "join",         room, nick }
 *   { type: "join",         room, nick, participantId, secret, hostToken }  // 断线重连
 *   { type: "request_speak",  version }
 *   { type: "cancel_speak",   version }
 *   { type: "leave" }
 *   { type: "start_speaking", version, participantId }  // 主持人
 *   { type: "end_speaking",   version }                 // 主持人
 *   { type: "reorder",        version, order:[pid...] } // 主持人
 *   { type: "remove",         version, participantId }  // 主持人
 *   { type: "clear",          version }                 // 主持人：一键清空
 *   { type: "clear_logs",     version }                 // 主持人：仅清空操作记录
 *   { type: "swap_request",   version, targetId }       // 排队者：向另一排队者发起换位邀请
 *   { type: "swap_cancel",    version, swapId }         // 发起方：撤回邀请
 *   { type: "swap_accept",    version, swapId }         // 目标方：接受（服务端重校验位置后原子交换）
 *   { type: "swap_decline",   version, swapId }         // 目标方：拒绝
 *
 * 换位邀请（snapshot.swaps[]）：记录双方 participantId、发起时的队列版本
 * 与双方位置；每人同一时刻最多参与一条待处理邀请。任一方取消申请、开始
 * 发言、被移除、主动离开或主持人重排时，相关邀请失效并记录原因。
 *
 * 服务端 -> 客户端
 *   { type: "joined",  snapshot, you:{id,secret,host} }
 *   { type: "snapshot", snapshot }
 *   { type: "error",   error, code, snapshot? }
 *
 * 所有队列命令必须携带客户端所见 version；服务端原子校验，
 * 不匹配则拒绝执行并返回最新快照（code = STALE_VERSION）。
 */

'use strict';

const http = require('http');
const path = require('path');
const crypto = require('crypto');
const express = require('express');
const { WebSocketServer, OPEN } = require('ws');

const PORT = parseInt(process.env.PORT, 10) || 3000;
const HOST = process.env.HOST || '0.0.0.0';
const WS_PATH = process.env.WS_PATH || '/ws';
const ROOM_TTL_MS = parseInt(process.env.ROOM_TTL_MS, 10) || 4 * 60 * 60 * 1000;
const LOG_LIMIT = parseInt(process.env.LOG_LIMIT, 10) || 500;

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: WS_PATH });

app.use(express.json({ limit: '32kb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ---------------------------------------------------------------------------
// 内存数据（房间数据仅在事件循环中被同步读写，命令处理天然原子）
// ---------------------------------------------------------------------------

/**
 * room = {
 *   code, hostToken, hostId,
 *   version,                 // 单调递增的状态版本号
 *   participants: Map<id, {id, nick, joinedAt, online, ws?}>,
 *   order: [id],             // 排队顺序
 *   speaking: { id, startedAt, elapsedMs } | null,
 *   swaps: Map<id, {id, fromId, toId, fromPos, toPos, version, createdAt}>, // 待处理换位邀请
 *   logs: [{id, ts, kind, text}],
 *   createdAt, lastSeen
 * }
 */
const rooms = new Map();

function newRoom(code) {
  return {
    code,
    hostToken: crypto.randomBytes(16).toString('hex'),
    hostId: null,
    version: 1,
    participants: new Map(),
    order: [],
    speaking: null,
    swaps: new Map(),
    logs: [],
    createdAt: Date.now(),
    lastSeen: Date.now()
  };
}

function genId(prefix) {
  return prefix + crypto.randomBytes(6).toString('hex');
}

function genRoomCode() {
  for (let i = 0; i < 20; i++) {
    const code = String(crypto.randomInt(0, 1000000)).padStart(6, '0');
    if (!rooms.has(code)) return code;
  }
  // 理论上不会走到这里
  return crypto.randomBytes(3).toString('hex').toUpperCase();
}

function addLog(room, kind, text) {
  room.logs.push({ id: genId('log_'), ts: Date.now(), kind, text });
  if (room.logs.length > LOG_LIMIT) room.logs.splice(0, room.logs.length - LOG_LIMIT);
}

function uniqueNick(room, nick) {
  let name = nick.trim().slice(0, 20);
  if (!name) name = '参与者';
  const used = new Set([...room.participants.values()].map((p) => p.nick));
  if (!used.has(name)) return name;
  for (let i = 2; i < 100; i++) {
    const candidate = `${name}(${i})`;
    if (!used.has(candidate)) return candidate;
  }
  return name + crypto.randomBytes(2).toString('hex');
}

// ---------------------------------------------------------------------------
// 换位邀请：失效清理
// ---------------------------------------------------------------------------

/** 让某条换位邀请失效并记录原因（原因会随快照广播给全员） */
function invalidateSwap(room, swap, reason) {
  if (!room.swaps.delete(swap.id)) return;
  const fromNick = room.participants.get(swap.fromId)?.nick ?? '已离开';
  const toNick = room.participants.get(swap.toId)?.nick ?? '已离开';
  addLog(room, 'swap', `「${fromNick}」与「${toNick}」的换位邀请失效（${reason}）`);
}

/** 清理与某参与者相关的全部待处理邀请 */
function invalidateSwapsFor(room, pid, reason) {
  for (const s of [...room.swaps.values()]) {
    if (s.fromId === pid || s.toId === pid) invalidateSwap(room, s, reason);
  }
}

/** 清理位置记录已与实际队列不符的邀请（如主持人重排后） */
function sweepStaleSwaps(room, reason) {
  for (const s of [...room.swaps.values()]) {
    if (room.order[s.fromPos] !== s.fromId || room.order[s.toPos] !== s.toId) {
      invalidateSwap(room, s, reason);
    }
  }
}

/** 某人当前参与的待处理邀请（每人同一时刻最多一条） */
function pendingSwapOf(room, pid) {
  for (const s of room.swaps.values()) {
    if (s.fromId === pid || s.toId === pid) return s;
  }
  return null;
}

/**
 * 构建快照。服务端时钟随快照下发（serverTs），
 * 客户端据此修正时钟偏差计算发言时长。
 */
function buildSnapshot(room) {
  const serverTs = Date.now();
  return {
    room: room.code,
    version: room.version,
    hostId: room.hostId,
    serverTs,
    order: room.order.slice(),
    speaking: room.speaking
      ? {
          id: room.speaking.id,
          nick: room.participants.get(room.speaking.id)?.nick ?? '已离开',
          startedAt: room.speaking.startedAt
        }
      : null,
    participants: [...room.participants.values()].map((p) => ({
      id: p.id,
      nick: p.nick,
      online: p.online,
      joinedAt: p.joinedAt
    })),
    swaps: [...room.swaps.values()].map((s) => ({
      id: s.id,
      fromId: s.fromId,
      toId: s.toId,
      fromPos: s.fromPos,
      toPos: s.toPos,
      version: s.version,
      createdAt: s.createdAt
    })),
    logs: room.logs.slice(-200)
  };
}

function send(ws, obj) {
  if (ws && ws.readyState === OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

/** 版本 +1 并向房间内所有在线连接广播最新快照 */
function commit(room) {
  room.version += 1;
  room.lastSeen = Date.now();
  const snap = { type: 'snapshot', snapshot: buildSnapshot(room) };
  const data = JSON.stringify(snap);
  for (const p of room.participants.values()) {
    if (p.ws && p.ws.readyState === OPEN) p.ws.send(data);
  }
}

function sendError(ws, message, code, room) {
  const msg = { type: 'error', error: message, code };
  if (room) msg.snapshot = buildSnapshot(room);
  send(ws, msg);
}

// ---------------------------------------------------------------------------
// HTTP：创建房间
// ---------------------------------------------------------------------------

app.post('/api/rooms', (req, res) => {
  const nick = String(req.body?.nick ?? '').trim().slice(0, 20) || '主持人';
  const code = genRoomCode();
  const room = newRoom(code);
  rooms.set(code, room);

  const pid = genId('p_');
  room.participants.set(pid, {
    id: pid,
    nick: uniqueNick(room, nick),
    joinedAt: Date.now(),
    online: false,
    ws: null
  });
  addLog(room, 'system', `房间已创建，主持人「${room.participants.get(pid).nick}」`);
  room.hostId = pid;

  res.json({
    room: code,
    version: room.version,
    participantId: pid,
    hostToken: room.hostToken
  });
});

app.get('/healthz', (_req, res) => res.json({ ok: true, rooms: rooms.size }));

// ---------------------------------------------------------------------------
// WebSocket
// ---------------------------------------------------------------------------

wss.on('connection', (ws) => {
  let room = null;
  let pid = null;
  let isHost = false;
  ws.isAlive = true;

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return sendError(ws, '消息不是合法的 JSON', 'BAD_MESSAGE');
    }
    if (!msg || typeof msg.type !== 'string') {
      return sendError(ws, '缺少 type 字段', 'BAD_MESSAGE');
    }

    try {
      switch (msg.type) {
        case 'join':
          return handleJoin(ws, msg, (ctx) => {
            room = ctx.room;
            pid = ctx.pid;
            isHost = ctx.isHost;
          });

        case 'leave':
          if (!room) return;
          return handleLeave(room, pid, 'leave');

        case 'request_speak':
        case 'cancel_speak':
        case 'start_speaking':
        case 'end_speaking':
        case 'reorder':
        case 'remove':
        case 'clear':
        case 'clear_logs':
        case 'swap_request':
        case 'swap_cancel':
        case 'swap_accept':
        case 'swap_decline':
          return handleCommand(ws, msg, room, pid, isHost);

        default:
          return sendError(ws, `未知消息类型：${msg.type}`, 'UNKNOWN_TYPE', room);
      }
    } catch (err) {
      console.error('处理消息出错：', err);
      sendError(ws, '服务端内部错误', 'INTERNAL', room);
    }
  });

  ws.on('close', () => {
    if (!room || !pid) return;
    // 若该连接已被重连的新连接顶替，旧连接关闭时不做清理
    const current = room.participants.get(pid);
    if (!current || current.ws !== ws) return;
    handleLeave(room, pid, 'disconnect');
  });

  ws.on('error', () => {
    // close 事件会负责清理
  });
});

// ---------------------------------------------------------------------------
// 加入 / 重连
// ---------------------------------------------------------------------------

function handleJoin(ws, msg, bind) {
  const code = String(msg.room ?? '').trim().toUpperCase();
  const target = rooms.get(code);
  if (!target) {
    return sendError(ws, '房间不存在或已过期', 'NO_ROOM');
  }

  const hostToken = typeof msg.hostToken === 'string' ? msg.hostToken : null;
  const asHost = hostToken && hostToken === target.hostToken;

  // 携带 hostToken：恢复/接管主持人身份
  if (asHost) {
    const existingId = typeof msg.participantId === 'string' ? msg.participantId : null;
    const p = existingId ? target.participants.get(existingId) : null;

    if (p) {
      // 旧连接让位
      if (p.ws && p.ws !== ws && p.ws.readyState === OPEN) {
        send(p.ws, { type: 'kicked', reason: '同一身份在其他标签页/设备打开' });
        p.ws.close(1000, 'replaced');
      }
      if (typeof msg.nick === 'string' && msg.nick.trim()) {
        const nick = uniqueNickExcept(target, msg.nick, p.id);
        if (nick !== p.nick) {
          p.nick = nick;
          addLog(target, 'presence', `主持人改名为「${nick}」`);
        }
      }
      p.online = true;
      p.ws = ws;
      target.hostId = p.id;
      target.lastSeen = Date.now();
      bind({ room: target, pid: p.id, isHost: true });
      send(ws, {
        type: 'joined',
        you: { id: p.id, nick: p.nick, host: true },
        snapshot: buildSnapshot(target)
      });
      return commit(target);
    }

    // id 对不上（例如本地身份丢失）：以新成员身份进入并接管为主持人
    const nick = String(msg.nick ?? '').trim().slice(0, 20) || '主持人';
    const id = genId('p_');
    const participant = {
      id,
      nick: uniqueNick(target, nick),
      joinedAt: Date.now(),
      online: true,
      ws
    };
    target.participants.set(id, participant);
    target.hostId = id;
    addLog(target, 'presence', `主持人「${participant.nick}」已接入`);
    bind({ room: target, pid: id, isHost: true });
    send(ws, {
      type: 'joined',
      you: { id, nick: participant.nick, host: true },
      snapshot: buildSnapshot(target)
    });
    return commit(target);
  }

  // 普通参与者断线重连：id + secret
  const reconnectId = typeof msg.participantId === 'string' ? msg.participantId : null;
  const secret = typeof msg.secret === 'string' ? msg.secret : null;
  const existing = reconnectId ? target.participants.get(reconnectId) : null;
  if (existing && secret && existing.secret === secret) {
    if (existing.ws && existing.ws !== ws && existing.ws.readyState === OPEN) {
      send(existing.ws, { type: 'kicked', reason: '同一身份在其他标签页/设备打开' });
      existing.ws.close(1000, 'replaced');
    }
    existing.online = true;
    existing.ws = ws;
    target.lastSeen = Date.now();
    bind({ room: target, pid: existing.id, isHost: false });
    send(ws, {
      type: 'joined',
      you: { id: existing.id, nick: existing.nick, host: false },
      snapshot: buildSnapshot(target)
    });
    return commit(target);
  }

  // 全新加入
  const nick = String(msg.nick ?? '').trim().slice(0, 20);
  if (!nick) {
    return sendError(ws, '请填写昵称', 'NO_NICK', target);
  }
  const id = genId('p_');
  const participant = {
    id,
    nick: uniqueNick(target, nick),
    secret: crypto.randomBytes(12).toString('hex'),
    joinedAt: Date.now(),
    online: true,
    ws
  };
  target.participants.set(id, participant);
  target.lastSeen = Date.now();
  addLog(target, 'presence', `「${participant.nick}」加入房间`);
  bind({ room: target, pid: id, isHost: false });
  send(ws, {
    type: 'joined',
    you: { id, nick: participant.nick, host: false, secret: participant.secret },
    snapshot: buildSnapshot(target)
  });
  commit(target);
}

function uniqueNickExcept(room, nick, exceptId) {
  const name = nick.trim().slice(0, 20) || '参与者';
  const used = new Set(
    [...room.participants.values()].filter((p) => p.id !== exceptId).map((p) => p.nick)
  );
  if (!used.has(name)) return name;
  for (let i = 2; i < 100; i++) {
    const candidate = `${name}(${i})`;
    if (!used.has(candidate)) return candidate;
  }
  return name + crypto.randomBytes(2).toString('hex');
}

function handleLeave(room, id, reason) {
  const p = room.participants.get(id);
  if (!p) return;

  if (reason === 'disconnect') {
    // 断线：保留席位与队列位置，标记离线，等待重连
    p.online = false;
    p.ws = null;
    return commit(room);
  }

  // 主动离开：移出房间；若正在发言则结束发言（保留计时记录）
  const nick = p.nick;
  const queuedIdx = room.order.indexOf(id);
  const wasSpeaking = room.speaking && room.speaking.id === id;

  if (wasSpeaking) {
    room.speaking = null;
  }
  if (queuedIdx !== -1) room.order.splice(queuedIdx, 1);
  invalidateSwapsFor(room, id, `「${nick}」离开房间`);
  room.participants.delete(id);
  addLog(room, 'presence', `「${nick}」离开房间`);
  commit(room);
}

// ---------------------------------------------------------------------------
// 队列命令：统一版本校验
// ---------------------------------------------------------------------------

function guardVersion(ws, msg, room, requireVersion = true) {
  const ver = Number(msg.version);
  if (!requireVersion || !Number.isInteger(ver)) {
    sendError(ws, '命令缺少 version', 'NO_VERSION', room);
    return false;
  }
  if (ver !== room.version) {
    // 过期命令：拒绝执行，返回最新快照
    sendError(
      ws,
      `版本已过期（你的 v${ver}，当前 v${room.version}）`,
      'STALE_VERSION',
      room
    );
    return false;
  }
  return true;
}

function requireHost(ws, room, isHost) {
  if (!isHost) {
    sendError(ws, '只有主持人可以执行此操作', 'FORBIDDEN', room);
    return false;
  }
  return true;
}

function handleCommand(ws, msg, room, pid, isHost) {
  if (!room) return sendError(ws, '尚未加入房间', 'NOT_JOINED');
  const me = room.participants.get(pid);
  if (!me) return sendError(ws, '参与者不存在，请重新加入', 'NOT_JOINED');

  switch (msg.type) {
    case 'request_speak': {
      if (!guardVersion(ws, msg, room)) return;
      if (room.speaking?.id === pid) {
        return sendError(ws, '你正在发言中', 'ALREADY_SPEAKING', room);
      }
      if (room.order.includes(pid)) {
        return sendError(ws, '你已在发言队列中', 'ALREADY_QUEUED', room);
      }
      room.order.push(pid);
      addLog(room, 'queue', `「${me.nick}」申请发言`);
      return commit(room);
    }

    case 'cancel_speak': {
      if (!guardVersion(ws, msg, room)) return;
      const idx = room.order.indexOf(pid);
      if (idx === -1) {
        return sendError(ws, '你不在发言队列中', 'NOT_QUEUED', room);
      }
      room.order.splice(idx, 1);
      invalidateSwapsFor(room, pid, `「${me.nick}」取消了发言申请`);
      addLog(room, 'queue', `「${me.nick}」取消发言`);
      return commit(room);
    }

    case 'start_speaking': {
      if (!guardVersion(ws, msg, room) || !requireHost(ws, room, isHost)) return;
      const targetId = String(msg.participantId || room.order[0] || '');
      const target = room.participants.get(targetId);
      if (!target) return sendError(ws, '参与者不存在', 'NO_PARTICIPANT', room);

      if (room.speaking) {
        const prev = room.participants.get(room.speaking.id);
        room.speaking = null;
        addLog(room, 'talk', `「${prev ? prev.nick : '已离开'}」发言结束`);
      }
      const idx = room.order.indexOf(targetId);
      if (idx !== -1) room.order.splice(idx, 1);
      invalidateSwapsFor(room, targetId, `「${target.nick}」开始发言`);
      room.speaking = { id: targetId, startedAt: Date.now() };
      addLog(room, 'talk', `主持人请「${target.nick}」开始发言`);
      return commit(room);
    }

    case 'end_speaking': {
      if (!guardVersion(ws, msg, room) || !requireHost(ws, room, isHost)) return;
      if (!room.speaking) return sendError(ws, '当前没有人发言', 'NO_SPEAKER', room);
      const speaker = room.participants.get(room.speaking.id);
      room.speaking = null;
      addLog(room, 'talk', `主持人结束「${speaker ? speaker.nick : '已离开'}」的发言`);
      return commit(room);
    }

    case 'reorder': {
      if (!guardVersion(ws, msg, room) || !requireHost(ws, room, isHost)) return;
      if (!Array.isArray(msg.order)) {
        return sendError(ws, 'order 必须是数组', 'BAD_ORDER', room);
      }
      const incoming = msg.order.map(String);
      // 必须与当前队列是同一批人，仅允许改变顺序
      const current = room.order.slice().sort();
      const proposed = incoming.slice().sort();
      if (current.length !== proposed.length || current.some((id, i) => id !== proposed[i])) {
        return sendError(ws, '排序内容与当前队列不一致，请刷新', 'ORDER_MISMATCH', room);
      }
      if (incoming.some((id) => !room.participants.has(id))) {
        return sendError(ws, '队列中包含不存在的参与者', 'NO_PARTICIPANT', room);
      }
      const same = incoming.length === room.order.length && incoming.every((id, i) => id === room.order[i]);
      if (same) return sendError(ws, '顺序没有变化', 'NO_CHANGE', room);
      room.order = incoming;
      // 重排后位置记录失效的邀请立即清理并同步原因
      sweepStaleSwaps(room, '主持人调整了发言顺序');
      addLog(room, 'queue', '主持人调整了发言顺序');
      return commit(room);
    }

    case 'remove': {
      if (!guardVersion(ws, msg, room) || !requireHost(ws, room, isHost)) return;
      const targetId = String(msg.participantId || '');
      const target = room.participants.get(targetId);
      if (!target) return sendError(ws, '参与者不存在', 'NO_PARTICIPANT', room);
      const idx = room.order.indexOf(targetId);
      if (idx !== -1) room.order.splice(idx, 1);
      if (room.speaking?.id === targetId) room.speaking = null;
      invalidateSwapsFor(room, targetId, `「${target.nick}」被移出队列`);
      addLog(room, 'queue', `主持人将「${target.nick}」移出队列`);
      return commit(room);
    }

    case 'clear': {
      if (!guardVersion(ws, msg, room) || !requireHost(ws, room, isHost)) return;
      room.order = [];
      room.speaking = null;
      room.swaps.clear();
      room.logs = [];
      addLog(room, 'system', '主持人执行了一键清空（队列、当前发言、操作记录已重置）');
      return commit(room);
    }

    case 'clear_logs': {
      if (!guardVersion(ws, msg, room) || !requireHost(ws, room, isHost)) return;
      room.logs = [];
      addLog(room, 'system', '主持人清空了操作记录');
      return commit(room);
    }

    // -----------------------------------------------------------------
    // 协商换位
    // -----------------------------------------------------------------

    case 'swap_request': {
      if (!guardVersion(ws, msg, room)) return;
      const targetId = String(msg.targetId || '');
      if (targetId === pid) {
        return sendError(ws, '不能与自己交换位置', 'BAD_TARGET', room);
      }
      const target = room.participants.get(targetId);
      if (!target) return sendError(ws, '参与者不存在', 'NO_PARTICIPANT', room);
      const fromPos = room.order.indexOf(pid);
      if (fromPos === -1) {
        return sendError(ws, '你不在发言队列中，无法发起换位', 'NOT_QUEUED', room);
      }
      const toPos = room.order.indexOf(targetId);
      if (toPos === -1) {
        return sendError(ws, '对方不在发言队列中', 'TARGET_NOT_QUEUED', room);
      }
      if (pendingSwapOf(room, pid)) {
        return sendError(ws, '你已有一条待处理的换位邀请，请先撤回或等待结果', 'SWAP_BUSY', room);
      }
      if (pendingSwapOf(room, targetId)) {
        return sendError(ws, '对方已有待处理的换位邀请，稍后再试', 'SWAP_BUSY', room);
      }
      const swap = {
        id: genId('sw_'),
        fromId: pid,
        toId: targetId,
        fromPos,
        toPos,
        version: room.version, // 发起时的队列版本
        createdAt: Date.now()
      };
      room.swaps.set(swap.id, swap);
      addLog(
        room,
        'swap',
        `「${me.nick}」邀请「${target.nick}」交换发言位置（第 ${fromPos + 1} ↔ 第 ${toPos + 1} 位）`
      );
      return commit(room);
    }

    case 'swap_cancel': {
      if (!guardVersion(ws, msg, room)) return;
      const swap = room.swaps.get(String(msg.swapId || ''));
      if (!swap) return sendError(ws, '换位邀请不存在或已失效', 'NO_SWAP', room);
      if (swap.fromId !== pid) {
        return sendError(ws, '只有发起人可以撤回换位邀请', 'FORBIDDEN', room);
      }
      room.swaps.delete(swap.id);
      const toNick = room.participants.get(swap.toId)?.nick ?? '已离开';
      addLog(room, 'swap', `「${me.nick}」撤回了对「${toNick}」的换位邀请`);
      return commit(room);
    }

    case 'swap_decline': {
      if (!guardVersion(ws, msg, room)) return;
      const swap = room.swaps.get(String(msg.swapId || ''));
      if (!swap) return sendError(ws, '换位邀请不存在或已失效', 'NO_SWAP', room);
      if (swap.toId !== pid) {
        return sendError(ws, '只有被邀请人可以拒绝换位邀请', 'FORBIDDEN', room);
      }
      room.swaps.delete(swap.id);
      const fromNick = room.participants.get(swap.fromId)?.nick ?? '已离开';
      addLog(room, 'swap', `「${me.nick}」拒绝了「${fromNick}」的换位邀请`);
      return commit(room);
    }

    case 'swap_accept': {
      if (!guardVersion(ws, msg, room)) return;
      const swap = room.swaps.get(String(msg.swapId || ''));
      if (!swap) return sendError(ws, '换位邀请不存在或已失效', 'NO_SWAP', room);
      if (swap.toId !== pid) {
        return sendError(ws, '只有被邀请人可以接受换位邀请', 'FORBIDDEN', room);
      }
      // 接受前重新校验：双方必须仍处在邀请发起时记录的位置
      const stillInPlace =
        room.order[swap.fromPos] === swap.fromId && room.order[swap.toPos] === swap.toId;
      if (!stillInPlace) {
        // 校验失败：邀请标记失效，队列保持不变
        invalidateSwap(room, swap, '队列位置已变化');
        return commit(room);
      }
      // 校验通过与交换在同一同步块内完成，天然原子
      room.order[swap.fromPos] = swap.toId;
      room.order[swap.toPos] = swap.fromId;
      room.swaps.delete(swap.id);
      const fromNick = room.participants.get(swap.fromId)?.nick ?? '已离开';
      addLog(
        room,
        'swap',
        `「${fromNick}」与「${me.nick}」交换了发言位置（第 ${swap.fromPos + 1} ↔ 第 ${swap.toPos + 1} 位）`
      );
      return commit(room);
    }
  }
}

// ---------------------------------------------------------------------------
// 心跳：清理死连接；空闲房间回收
// ---------------------------------------------------------------------------

const heartbeat = setInterval(() => {
  for (const client of wss.clients) {
    if (client.isAlive === false) {
      client.terminate();
      continue;
    }
    client.isAlive = false;
    try {
      client.ping();
    } catch {
      // ignore
    }
  }

  const now = Date.now();
  for (const [code, room] of rooms) {
    const anyOnline = [...room.participants.values()].some(
      (p) => p.ws && p.ws.readyState === OPEN
    );
    if (!anyOnline && now - room.lastSeen > ROOM_TTL_MS) {
      rooms.delete(code);
    }
  }
}, 30000);

wss.on('close', () => clearInterval(heartbeat));

server.listen(PORT, HOST, () => {
  console.log(`发言席位服务已启动: http://localhost:${PORT}  (WebSocket: ${WS_PATH})`);
});

<script setup>
import { ref } from 'vue'

const emit = defineEmits(['report', 'error', 'loading'])

const fileInput = ref(null)
const wsUrl = ref('ws://127.0.0.1:8000/ws/stream')
const connected = ref(false)
const liveBytes = ref(0)
const liveBatches = ref(0)
let ws = null

async function onFile(e) {
  const file = e.target.files[0]
  if (!file) return
  emit('loading', true)
  emit('error', '')
  try {
    const fd = new FormData()
    fd.append('file', file)
    const resp = await fetch('/api/inspect/upload', { method: 'POST', body: fd })
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`)
    emit('report', await resp.json())
  } catch (err) {
    emit('error', `上传解码失败: ${err.message}`)
  } finally {
    emit('loading', false)
    e.target.value = ''
  }
}

function connect() {
  if (connected) return
  emit('error', '')
  ws = new WebSocket(wsUrl.value)
  ws.binaryType = 'arraybuffer'
  connected.value = true

  ws.onmessage = (ev) => {
    const msg = JSON.parse(ev.data)
    liveBytes.value = msg.bytes_received || liveBytes.value
    liveBatches.value = (msg.batches || []).length
    if (msg.event === 'progress' || msg.event === 'final' || msg.event === 'error') {
      emit('report', msg)
    }
    if (msg.event === 'error') emit('error', msg.error || '连接出错')
    if (msg.event === 'final') disconnect()
  }
  ws.onerror = () => emit('error', 'WebSocket 连接错误')
  ws.onclose = () => {
    connected.value = false
  }
}

function disconnect() {
  if (ws) {
    try { ws.send('EOS') } catch (_) { /* already closed */ }
    ws.close()
  }
  ws = null
  connected.value = false
}
</script>

<template>
  <div class="panel">
    <h3>数据源</h3>

    <div class="source-block">
      <p class="label">上传流文件</p>
      <input
        ref="fileInput"
        type="file"
        accept=".arrow,.arrows,application/octet-stream"
        @change="onFile"
        hidden
      />
      <button class="btn wide" @click="fileInput.click()">选择 .arrow 文件</button>
    </div>

    <div class="divider"><span>或</span></div>

    <div class="source-block">
      <p class="label">连接本地发送器</p>
      <input v-model="wsUrl" class="input" type="text" spellcheck="false" />
      <div class="btn-row">
        <button v-if="!connected" class="btn wide" @click="connect">连接</button>
        <button v-else class="btn danger wide" @click="disconnect">断开</button>
      </div>
      <div v-if="connected || liveBytes" class="live-meta">
        <span>已收 {{ liveBytes }} 字节</span>
        <span>{{ liveBatches }} 批次</span>
        <span class="dot" :class="{ on: connected }"></span>
      </div>
    </div>
  </div>
</template>

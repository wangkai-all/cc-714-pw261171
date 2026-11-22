<script setup>
import { ref, computed } from 'vue'
import SourcePanel from './components/SourcePanel.vue'
import SchemaView from './components/SchemaView.vue'
import BatchList from './components/BatchList.vue'
import DataGrid from './components/DataGrid.vue'
import ProbePanel from './components/ProbePanel.vue'

const report = ref(null)
const loading = ref(false)
const errorMsg = ref('')
const selectedBatchOrdinal = ref(null)
const probe = ref(null) // { batchOrdinal, row, fieldPath, value, node }

const selectedBatch = computed(() => {
  if (!report.value) return null
  return report.value.batches.find(
    (b) => b.ordinal === selectedBatchOrdinal.value
  ) || report.value.batches[0] || null
})

function setReport(r) {
  report.value = r
  probe.value = null
  selectedBatchOrdinal.value = r.batches.length ? r.batches[0].ordinal : null
}

function onProbe(payload) {
  probe.value = payload
}

function clearAll() {
  report.value = null
  probe.value = null
  selectedBatchOrdinal.value = null
  errorMsg.value = ''
}

async function exportJson() {
  if (!report.value) return
  const resp = await fetch('/api/export/json', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(report.value),
  })
  const blob = await resp.blob()
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = 'arrow-export.json'
  a.click()
  URL.revokeObjectURL(url)
}
</script>

<template>
  <div class="app">
    <header class="topbar">
      <div class="brand">
        <span class="logo">⬡</span>
        <div>
          <h1>Arrow IPC Stream Inspector</h1>
          <p>RecordBatch · 逻辑值 / 字段类型 / 底层缓冲区</p>
        </div>
      </div>
      <div class="actions">
        <button class="btn" :disabled="!report" @click="exportJson">
          导出结构化 JSON
        </button>
        <button class="btn ghost" :disabled="!report" @click="clearAll">
          清空
        </button>
      </div>
    </header>

    <div class="layout">
      <aside class="sidebar">
        <SourcePanel
          :loading="loading"
          @loading="(v) => (loading = v)"
          @report="setReport"
          @error="(m) => (errorMsg = m)"
        />
        <SchemaView v-if="report" :schema="report.schema" />
        <BatchList
          v-if="report"
          :batches="report.batches"
          :selected="selectedBatchOrdinal"
          @select="(o) => (selectedBatchOrdinal = o)"
        />
      </aside>

      <main class="content">
        <div v-if="errorMsg" class="error-banner">{{ errorMsg }}</div>

        <template v-if="report">
          <section class="status-strip">
            <div class="stat">
              <span class="stat-label">终止状态</span>
              <span class="badge" :class="`term-${report.termination}`">
                {{ report.termination }}
              </span>
            </div>
            <div class="stat">
              <span class="stat-label">消息数</span>
              <span class="stat-value">{{ report.messages_received }}</span>
            </div>
            <div class="stat">
              <span class="stat-label">字节数</span>
              <span class="stat-value">{{ report.bytes_received }}</span>
            </div>
            <div class="stat">
              <span class="stat-label">字典消息</span>
              <span class="stat-value">{{ report.dictionary_log.length }}</span>
            </div>
            <div v-if="report.termination_detail" class="stat wide">
              <span class="stat-label">说明</span>
              <span class="stat-value detail">{{ report.termination_detail }}</span>
            </div>
          </section>

          <DataGrid
            :batch="selectedBatch"
            :schema="report.schema"
            @probe="onProbe"
          />
        </template>

        <div v-else class="placeholder">
          <div class="placeholder-card">
            <h2>等待 Arrow IPC Stream</h2>
            <p>
              上传 <code>.arrow</code> 流文件，或连接本地发送器（WebSocket）。
              选中任意嵌套值即可查看它用到的有效位、偏移和字典项。
            </p>
            <ul>
              <li>空列表 <code>[]</code>、空字符串 <code>""</code> 与
                <code>null</code> 严格区分</li>
              <li>字典消息可先于批次、替换或追加；按批次保留快照</li>
              <li>非法偏移 / 未定义字典 / 流截断时保留已完整接收的批次</li>
            </ul>
          </div>
        </div>
      </main>

      <ProbePanel :probe="probe" :dictionary-log="report?.dictionary_log || []" />
    </div>
  </div>
</template>

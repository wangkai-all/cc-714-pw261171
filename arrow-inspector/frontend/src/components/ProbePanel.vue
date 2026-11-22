<script setup>
import { computed } from 'vue'
import PhysicalNode from './PhysicalNode.vue'

const props = defineProps({
  probe: { type: Object, default: null },
  dictionaryLog: { type: Array, default: () => [] },
})

const dictEntries = computed(() => {
  if (!props.probe || !props.probe.node) return []
  // find the dictionary id referenced by the probed subtree
  const findDictId = (n) => {
    if (n.extra && n.extra.dictionary_id !== null &&
        n.extra.dictionary_id !== undefined) return n.extra.dictionary_id
    for (const c of n.children || []) {
      const r = findDictId(c)
      if (r !== null && r !== undefined) return r
    }
    return null
  }
  const id = findDictId(props.probe.node)
  if (id === null || id === undefined) return []
  const log = props.dictionaryLog.filter((d) => d.dictionary_id === id)
  return log.map((d) => ({
    sequence: d.sequence,
    kind: d.kind,
    values: d.values_preview,
  }))
})

const displayValue = computed(() => {
  if (!props.probe) return ''
  const v = props.probe.value
  if (v === null || v === undefined) return 'null'
  if (typeof v === 'object') return JSON.stringify(v)
  if (v === '') return '"" (空字符串)'
  return String(v)
})
</script>

<template>
  <aside class="probe-panel">
    <h3>值来源核对</h3>

    <div v-if="!probe" class="probe-empty">
      <p>在左侧表格中点击任意逻辑值（包括嵌套列表、结构体成员、字典值），
        这里会展开它实际使用的：</p>
      <ul>
        <li>空值位图（validity）的对应 bit</li>
        <li>列表 / 字符串的偏移区间</li>
        <li>字典索引与对应字典项</li>
        <li>底层缓冲区的原始字节</li>
      </ul>
    </div>

    <template v-else>
      <div class="probe-value">
        <div class="section-title">选中的逻辑值</div>
        <div class="selected-value">{{ displayValue }}</div>
        <div class="probe-loc">
          行 #{{ probe.row }}
          <span v-if="probe.indexPath && probe.indexPath.length">
            · 路径 {{ probe.indexPath.join(' → ') }}
          </span>
          <span v-if="probe.fieldPath"> · 字段 {{ probe.fieldPath }}</span>
        </div>
      </div>

      <div v-if="dictEntries.length" class="dict-section">
        <div class="section-title">关联字典消息快照</div>
        <div v-for="d in dictEntries" :key="d.sequence" class="dict-snap">
          <div class="dict-head">
            消息 #{{ d.sequence }}
            <span class="badge" :class="d.kind">{{ d.kind }}</span>
          </div>
          <ol class="dict-values">
            <li v-for="(val, i) in d.values" :key="i">
              <span class="dict-idx">{{ i }}</span> {{ val === null ? 'null' : JSON.stringify(val) }}
            </li>
          </ol>
        </div>
        <p class="dict-note">
          每个批次按其接收时刻的字典快照解码；后续字典替换不会回溯改变先前批次。
        </p>
      </div>

      <div v-if="probe.node" class="phys-wrap">
        <div class="section-title">物理节点与缓冲区</div>
        <PhysicalNode :node="probe.node" :highlight-row="probe.row" />
      </div>
    </template>
  </aside>
</template>

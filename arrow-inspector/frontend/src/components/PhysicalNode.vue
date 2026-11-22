<script setup>
import { computed } from 'vue'

const props = defineProps({
  node: { type: Object, required: true },
  highlightRow: { type: Number, default: null },
  highlightIndex: { type: Number, default: null },
})

// Dense-union offsets index the selected child array per row (one entry per
// row); list/string offsets are intervals with a terminal length+1 entry.
const isDenseUnion = computed(() =>
  (props.node.arrow_type || '').startsWith('dense_union'))
</script>

<template>
  <div class="phys-node">
    <div class="phys-head">
      <span class="phys-name">{{ node.name }}</span>
      <span class="phys-type">{{ node.arrow_type }}</span>
    </div>
    <div class="phys-meta">
      length {{ node.length }} · null_count {{ node.null_count }}
      <code v-if="node.path" class="phys-path">{{ node.path }}</code>
    </div>

    <div v-if="node.validity_bits" class="phys-section">
      <div class="section-title">有效位 validity (LSB 优先)</div>
      <div class="bits">
        <span
          v-for="(bit, i) in node.validity_bits"
          :key="i"
          :class="['bit', bit ? 'one' : 'zero', { hl: i === highlightRow }]"
          :title="`index ${i}: ${bit ? 'valid' : 'null'}`"
        >{{ bit }}</span>
      </div>
      <div v-if="highlightRow !== null" class="hint">
        选中行 index {{ highlightRow }} →
        <strong>{{ node.validity_bits[highlightRow] ? '有效 (1)' : 'null (0)' }}</strong>
      </div>
    </div>

    <div v-if="node.offsets && node.offsets.length" class="phys-section">
      <div class="section-title">
        偏移 offsets{{ isDenseUnion ? '（dense union：每行一个子数组下标，非区间）' : '' }}
      </div>
      <div class="offsets">
        <span
          v-for="(o, i) in node.offsets"
          :key="i"
          :class="{ hl: highlightRow !== null && (i === highlightRow || (!isDenseUnion && i === highlightRow + 1)) }"
        >[{{ i }}]={{ o }}</span>
      </div>
      <div
        v-if="highlightRow !== null && isDenseUnion && node.offsets[highlightRow] !== undefined"
        class="hint"
      >
        选中行 index {{ highlightRow }} → 子数组元素下标
        <strong>{{ node.offsets[highlightRow] }}</strong>（结合 type_id 决定落在哪个子数组）
      </div>
      <div
        v-else-if="highlightRow !== null && node.offsets[highlightRow + 1] !== undefined"
        class="hint"
      >
        选中区间 [{{ node.offsets[highlightRow] }},
        {{ node.offsets[highlightRow + 1] }}) →
        {{ node.offsets[highlightRow + 1] - node.offsets[highlightRow] }} 个元素
      </div>
    </div>

    <div v-if="node.extra && node.extra.indices" class="phys-section">
      <div class="section-title">
        字典索引 (dict id {{ node.extra.dictionary_id }})
      </div>
      <div class="offsets">
        <span
          v-for="(ix, i) in node.extra.indices"
          :key="i"
          :class="{ hl: i === highlightRow }"
        >[{{ i }}]={{ ix === null ? 'null' : ix }}</span>
      </div>
    </div>

    <div v-if="node.extra && node.extra.logical_values" class="phys-section">
      <div class="section-title">逻辑值（字符串/二进制）</div>
      <div class="logical-list">
        <span v-for="(v, i) in node.extra.logical_values" :key="i"
          :class="{ hl: i === highlightIndex }">
          {{ v === null ? 'null' : JSON.stringify(v) }}
        </span>
      </div>
    </div>

    <div v-if="node.extra && node.extra.physical_values" class="phys-section">
      <div class="section-title">物理数据值</div>
      <div class="logical-list">
        <span v-for="(v, i) in node.extra.physical_values" :key="i"
          :class="{ hl: i === highlightRow }">{{ v }}</span>
      </div>
    </div>

    <div class="phys-section">
      <div class="section-title">缓冲区 buffers ({{ node.buffers.length }})</div>
      <div v-for="(b, i) in node.buffers" :key="i" class="buffer">
        <div class="buffer-head">
          <span class="role">{{ b.role }}</span>
          <span class="blen">{{ b.length }} 字节</span>
        </div>
        <code v-if="b.hex_preview" class="hex">{{ b.hex_preview }}<span
          v-if="b.length > 48" class="more">…</span></code>
        <code v-else class="hex empty">(空 / 省略)</code>
      </div>
    </div>

    <div v-if="node.children && node.children.length" class="phys-children">
      <PhysicalNode
        v-for="c in node.children"
        :key="c.path"
        :node="c"
        :highlight-row="highlightRow"
      />
    </div>
  </div>
</template>

<script>
export default { name: 'PhysicalNode' }
</script>

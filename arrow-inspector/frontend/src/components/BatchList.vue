<script setup>
defineProps({
  batches: { type: Array, required: true },
  selected: { type: Number, default: null },
})
const emit = defineEmits(['select'])
</script>

<template>
  <div class="panel">
    <h3>RecordBatches ({{ batches.length }})</h3>
    <ul class="batch-list">
      <li
        v-for="b in batches"
        :key="b.ordinal"
        :class="['batch-item', `st-${b.status}`, { active: b.ordinal === selected }]"
        @click="emit('select', b.ordinal)"
      >
        <div class="batch-head">
          <span class="batch-ord">#{{ b.ordinal }}</span>
          <span class="badge" :class="`st-${b.status}`">{{ b.status }}</span>
        </div>
        <div class="batch-meta">{{ b.num_rows }} 行 · offset {{ b.message_offset }}</div>
        <div v-if="b.validation_issues.length" class="batch-issues">
          {{ b.validation_issues.length }} 个物理问题
        </div>
        <div v-if="b.error" class="batch-error" :title="b.error">解码失败</div>
      </li>
    </ul>
  </div>
</template>

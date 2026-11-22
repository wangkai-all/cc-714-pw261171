<script setup>
import { computed } from 'vue'
import ValueCell from './ValueCell.vue'

const props = defineProps({
  batch: { type: Object, default: null },
  schema: { type: Object, required: true },
})
const emit = defineEmits(['probe'])

const columns = computed(() => (props.batch ? props.schema.fields : []))

function physicalMap(nodes, acc = {}) {
  for (const n of nodes) {
    acc[n.path] = n
    if (n.children) physicalMap(n.children, acc)
  }
  return acc
}
const nodeByPath = computed(() =>
  props.batch && props.batch.physical ? physicalMap(props.batch.physical) : {}
)

function probeCell(rowIndex, field, value) {
  emit('probe', {
    batchOrdinal: props.batch.ordinal,
    row: rowIndex,
    fieldPath: field.name,
    indexPath: [],
    value,
    node: nodeByPath.value[field.name],
  })
}

function probeNested(payload) {
  emit('probe', { batchOrdinal: props.batch.ordinal, ...payload })
}
</script>

<template>
  <section v-if="batch" class="panel grid-panel">
    <div class="grid-head">
      <h3>逻辑值 — RecordBatch #{{ batch.ordinal }}</h3>
      <span class="badge" :class="`st-${batch.status}`">{{ batch.status }}</span>
    </div>

    <div v-if="batch.error" class="undecodable">
      <strong>该批次无法解释：</strong>{{ batch.error }}
      <p v-if="batch.validation_issues.length" class="issue-list">
        <span v-for="(p, i) in batch.validation_issues" :key="i">⚠ {{ p }}<br /></span>
      </p>
    </div>

    <div v-else class="table-wrap">
      <table>
        <thead>
          <tr>
            <th class="row-idx">#</th>
            <th v-for="c in columns" :key="c.name">
              <div class="col-name">{{ c.name }}</div>
              <div class="col-type">{{ c.type.name }}</div>
            </th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(row, ri) in batch.logical" :key="ri">
            <td class="row-idx">{{ ri }}</td>
            <td v-for="c in columns" :key="c.name" class="cell">
              <ValueCell
                :value="row[c.name]"
                :node="nodeByPath[c.name]"
                :row="ri"
                :index-path="[]"
                @probe="probeNested"
                @probe-self="(v) => probeCell(ri, c, v)"
              />
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <details v-if="batch.validation_issues.length" class="issues-detail">
      <summary>物理校验问题（{{ batch.validation_issues.length }}）</summary>
      <ul>
        <li v-for="(p, i) in batch.validation_issues" :key="i">⚠ {{ p }}</li>
      </ul>
    </details>
  </section>
</template>

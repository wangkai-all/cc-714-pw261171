<script setup>
import { computed } from 'vue'

const props = defineProps({
  value: { default: undefined },
  node: { type: Object, default: null },
  row: { type: Number, required: true },
  indexPath: { type: Array, default: () => [] },
})
const emit = defineEmits(['probe', 'probe-self'])

const kind = computed(() => classify(props.value))

function classify(v) {
  if (v === null || v === undefined) return 'null'
  if (Array.isArray(v)) return v.length === 0 ? 'empty-list' : 'list'
  if (typeof v === 'object') return 'struct'
  if (v === '') return 'empty-string'
  return 'scalar'
}

function childNode(name) {
  if (!props.node || !props.node.children) return null
  return props.node.children.find((c) => c.name === name) || null
}

function childAt(index) {
  if (!props.node || !props.node.children) return null
  // list/map physical child holds all flattened items in order
  return props.node.children[0] || null
}

function clickSelf() {
  emit('probe-self', props.value)
}

function emitProbe(node, indexPath, value) {
  emit('probe', { row: props.row, node, indexPath, value })
}

function scalarDisplay(v) {
  if (typeof v === 'boolean') return v ? 'true' : 'false'
  return String(v)
}
</script>

<template>
  <!-- null -->
  <button v-if="kind === 'null'" class="v null" @click="clickSelf" title="null 值（有效位为 0）">
    null
  </button>

  <!-- empty string, distinct from null -->
  <button
    v-else-if="kind === 'empty-string'"
    class="v empty-string"
    @click="clickSelf"
    title='空字符串 ""（有效位为 1，偏移相等）'
  >
    ""
  </button>

  <!-- empty list, distinct from null -->
  <button
    v-else-if="kind === 'empty-list'"
    class="v empty-list"
    @click="clickSelf"
    title="空列表 []（有效位为 1，相邻偏移相等）"
  >
    []
  </button>

  <!-- scalar -->
  <button v-else-if="kind === 'scalar'" class="v scalar" @click="clickSelf">
    {{ scalarDisplay(value) }}
  </button>

  <!-- list -->
  <div v-else-if="kind === 'list'" class="v list">
    <span class="bracket">[</span>
    <template v-for="(item, i) in value" :key="i">
      <ValueCell
        :value="item"
        :node="childAt(i)"
        :row="row"
        :index-path="[...indexPath, i]"
        @probe="(p) => emitProbe(p.node, p.indexPath, p.value)"
        @probe-self="(v) => emitProbe(childAt(i), [...indexPath, i], v)"
      />
      <span v-if="i < value.length - 1" class="comma">, </span>
    </template>
    <span class="bracket">]</span>
  </div>

  <!-- struct / map entry -->
  <div v-else class="v struct">
    <span class="bracket">{</span>
    <template v-for="(key, i) in Object.keys(value)" :key="key">
      <span class="struct-key">{{ key }}:</span>
      <ValueCell
        :value="value[key]"
        :node="childNode(key)"
        :row="row"
        :index-path="[...indexPath, key]"
        @probe="(p) => emitProbe(p.node, p.indexPath, p.value)"
        @probe-self="(v) => emitProbe(childNode(key), [...indexPath, key], v)"
      />
      <span v-if="i < Object.keys(value).length - 1" class="comma">, </span>
    </template>
    <span class="bracket">}</span>
  </div>
</template>

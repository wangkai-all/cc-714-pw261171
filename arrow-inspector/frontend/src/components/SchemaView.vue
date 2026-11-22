<script setup>
defineProps({ schema: { type: Object, required: true } })
</script>

<template>
  <div class="panel">
    <h3>Schema 字段类型</h3>
    <ul class="type-tree">
      <li v-for="f in schema.fields" :key="f.name">
        <div class="type-node">
          <span class="field-name">{{ f.name }}</span>
          <span class="type-name">{{ f.type.name }}</span>
          <span v-if="f.type.dictionary_id !== undefined" class="dict-tag">
            dict#{{ f.type.dictionary_id }}
          </span>
          <span v-if="!f.nullable" class="req-tag">non-null</span>
        </div>
        <ul v-if="f.type.fields && f.type.fields.length" class="type-children">
          <li v-for="c in f.type.fields" :key="c.name">
            <span class="field-name">{{ c.name }}</span>
            <span class="type-name">{{ c.type.name }}</span>
            <ul v-if="c.type.fields && c.type.fields.length" class="type-children">
              <li v-for="gc in c.type.fields" :key="gc.name">
                <span class="field-name">{{ gc.name }}</span>
                <span class="type-name">{{ gc.type.name }}</span>
              </li>
            </ul>
          </li>
        </ul>
      </li>
    </ul>
  </div>
</template>

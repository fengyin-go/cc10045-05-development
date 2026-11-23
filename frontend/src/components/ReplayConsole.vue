<template>
  <section class="replay-console" :data-testid="`replay-console-${mode}`">
    <template v-if="mode === 'event_tracker'">
      <div class="replay-block">
        <h3 class="replay-title">整组重放 · 多选历史批次一次提交</h3>
        <p class="replay-hint">
          勾选同组的历史事件批次（可跨日期）后提交。样本暂时缺失的批次会标记“样本缺失”，
          恢复后可在下方续办；同一原始事件在同一窗口只贡献一次，重复提交不会多计。
        </p>
        <div class="replay-toolbar">
          <label class="replay-field">
            <span>回放组名称</span>
            <input v-model="groupName" data-testid="replay-group-name" placeholder="如：周一周二跨日补算组" />
          </label>
          <label class="replay-field">
            <span>备注</span>
            <input v-model="groupNote" data-testid="replay-group-note" placeholder="可选" />
          </label>
          <button
            class="btn primary"
            type="button"
            data-testid="submit-replay-group"
            :disabled="submitting || selectedBatches.length === 0"
            @click="submitGroup"
          >
            {{ submitting ? '提交中…' : `提交整组重放（已选 ${selectedBatches.length} 个批次）` }}
          </button>
          <button class="btn ghost" type="button" @click="loadBatches">刷新批次清单</button>
        </div>
        <table class="data-table">
          <thead>
            <tr>
              <th style="width: 42px">选择</th>
              <th>原始批次号</th>
              <th>业务日期</th>
              <th>事件数</th>
              <th>最早发生时间</th>
              <th>最晚发生时间</th>
              <th>批次类型</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="batch in batches" :key="batch.batch_id">
              <td>
                <input
                  v-model="selectedBatches"
                  type="checkbox"
                  :value="batch.batch_id"
                  :data-testid="`select-batch-${batch.batch_id}`"
                  :disabled="batch.is_replay_batch"
                />
              </td>
              <td>{{ batch.batch_id }}</td>
              <td>{{ batch.business_date }}</td>
              <td>{{ batch.event_count }}</td>
              <td>{{ batch.first_occurred_at }}</td>
              <td>{{ batch.last_occurred_at }}</td>
              <td>
                <span :class="batch.is_replay_batch ? 'tag tag-replay' : 'tag tag-live'">
                  {{ batch.is_replay_batch ? '重放批次' : '历史批次' }}
                </span>
              </td>
            </tr>
            <tr v-if="!batches.length">
              <td colspan="7" class="empty-state">暂无历史事件批次</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div class="replay-block">
        <h3 class="replay-title">回放组办理进度</h3>
        <div v-for="group in groups" :key="group.id" class="replay-group-card" :data-testid="`replay-group-${group.id}`">
          <div class="replay-group-head">
            <div>
              <strong>{{ group.group_name }}</strong>
              <span class="replay-code">{{ group.replay_group_code }}</span>
              <span :class="['tag', groupStatusTag(group.status)]">{{ group.status }}</span>
            </div>
            <button
              v-if="group.status !== '已完成'"
              class="btn"
              type="button"
              :data-testid="`resume-group-${group.id}`"
              @click="resumeGroup(group.id)"
            >
              恢复后续办
            </button>
          </div>
          <table class="data-table replay-sub-table">
            <thead>
              <tr><th>原始批次</th><th>业务日期</th><th>办理状态</th><th>样本总数</th><th>实际接受</th><th>重复跳过</th></tr>
            </thead>
            <tbody>
              <tr v-for="src in group.sources" :key="src.id" :data-testid="`group-source-${src.id}`">
                <td>{{ src.source_batch_id }}</td>
                <td>{{ src.business_date || '—' }}</td>
                <td>
                  <span :class="['tag', sourceStatusTag(src.status)]">{{ src.status }}</span>
                </td>
                <td>{{ src.event_total }}</td>
                <td>{{ src.event_accepted }}</td>
                <td>{{ src.event_duplicated }}</td>
              </tr>
            </tbody>
          </table>
          <div v-if="group.affected_windows && group.affected_windows.length" class="replay-windows">
            <span v-for="w in group.affected_windows" :key="`${w.window_id}-${w.window_start}`" class="replay-window-chip">
              {{ w.window_name }} · {{ w.window_start }} · 源记录 {{ w.source_records }} ·
              <span :class="w.status === '已修正' ? 'tag tag-done' : 'tag tag-pending'">{{ w.status }}</span>
            </span>
          </div>
        </div>
        <p v-if="!groups.length" class="empty-state">尚未提交过整组重放</p>
      </div>
    </template>

    <template v-else>
      <div class="replay-block">
        <h3 class="replay-title">窗口补算结果 · 重放接受事件驱动的汇总修正</h3>
        <p class="replay-hint">
          “待验明”表示关联的回放组仍有批次样本缺失，不能当作完成补算；样本补齐续办后自动转为“已修正”。
          源记录数为该窗口时间桶台账内实际贡献的去重事件数，同一原始事件在同一窗口只计一次。
        </p>
        <div class="replay-toolbar">
          <label class="replay-field">
            <span>补算状态</span>
            <select v-model="statusFilter" data-testid="correction-status-filter" @change="loadCorrections">
              <option value="">全部</option>
              <option value="待验明">待验明</option>
              <option value="已修正">已修正</option>
            </select>
          </label>
          <button class="btn ghost" type="button" @click="loadCorrections">刷新补算结果</button>
          <button class="btn ghost" type="button" @click="loadTrace">刷新溯源回查</button>
        </div>
        <table class="data-table">
          <thead>
            <tr>
              <th>聚合窗口</th><th>窗口起始</th><th>维度分组</th><th>修正值</th>
              <th>源记录数</th><th>版本</th><th>补算状态</th><th>收到的原始批次</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="item in corrections" :key="item.id" :data-testid="`correction-${item.id}`">
              <td>{{ item.window_name }}</td>
              <td>{{ item.window_start }}</td>
              <td>{{ item.dimension_key }}</td>
              <td>{{ item.computed_value }}</td>
              <td>{{ item.source_record_count }}</td>
              <td>v{{ item.computation_version }}</td>
              <td>
                <span :class="['tag', item.status === '已修正' ? 'tag-done' : 'tag-pending']">{{ item.status }}</span>
              </td>
              <td>
                <span v-for="src in item.correction_sources"
                      :key="`${src.replay_group_id}-${src.source_batch_id}`"
                      class="replay-batch-chip">
                  {{ src.source_batch_id }}（{{ src.event_count }}）
                </span>
                <span v-if="!item.correction_sources || !item.correction_sources.length">—</span>
              </td>
            </tr>
            <tr v-if="!corrections.length">
              <td colspan="8" class="empty-state">暂无补算结果，可先在事件追踪器提交整组重放</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div class="replay-block">
        <h3 class="replay-title">溯源回查 · 已修正分组收到了哪个原始批次</h3>
        <table class="data-table">
          <thead>
            <tr><th>回放组</th><th>原始批次</th><th>聚合窗口</th><th>窗口起始</th><th>维度分组</th><th>贡献事件数</th><th>补算状态</th></tr>
          </thead>
          <tbody>
            <tr v-for="row in traceRows" :key="`${row.correction_id}-${row.replay_group_id}-${row.source_batch_id}`"
                :data-testid="`trace-${row.correction_id}-${row.source_batch_id}`">
              <td>{{ row.replay_group_code }}<br /><span class="replay-code">{{ row.group_name }}</span></td>
              <td>{{ row.source_batch_id }}</td>
              <td>{{ row.window_name }}</td>
              <td>{{ row.window_start }}</td>
              <td>{{ row.dimension_key }}</td>
              <td>{{ row.event_count }}</td>
              <td>
                <span :class="['tag', row.correction_status === '已修正' ? 'tag-done' : 'tag-pending']">
                  {{ row.correction_status }}
                </span>
              </td>
            </tr>
            <tr v-if="!traceRows.length">
              <td colspan="7" class="empty-state">暂无溯源记录</td>
            </tr>
          </tbody>
        </table>
      </div>
    </template>

    <p v-if="errorMessage" class="error-text replay-error" data-testid="replay-error">{{ errorMessage }}</p>
    <p v-if="infoMessage" class="replay-info" data-testid="replay-info">{{ infoMessage }}</p>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  fetchCorrectionSources,
  fetchCorrections,
  fetchReplayGroups,
  fetchSourceBatches,
  resumeReplayGroup,
  submitReplayGroup,
} from '@/api/replay'
import type { Correction, CorrectionSource, ReplayGroup, SourceBatch } from '@/api/replay'

const props = defineProps<{ mode: 'event_tracker' | 'realtime_agg' }>()

const batches = ref<SourceBatch[]>([])
const selectedBatches = ref<string[]>([])
const groupName = ref('')
const groupNote = ref('')
const groups = ref<ReplayGroup[]>([])
const submitting = ref(false)
const corrections = ref<Correction[]>([])
const traceRows = ref<CorrectionSource[]>([])
const statusFilter = ref('')
const errorMessage = ref('')
const infoMessage = ref('')

function groupStatusTag(status: string): string {
  if (status === '已完成') return 'tag-done'
  if (status === '处理中') return 'tag-pending'
  return 'tag-muted'
}

function sourceStatusTag(status: string): string {
  if (status === '已读取') return 'tag-done'
  if (status === '样本缺失') return 'tag-missing'
  return 'tag-pending'
}

async function loadBatches() {
  errorMessage.value = ''
  try {
    batches.value = await fetchSourceBatches()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '历史批次读取失败'
  }
}

async function loadGroups() {
  try {
    groups.value = await fetchReplayGroups()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回放组读取失败'
  }
}

async function submitGroup() {
  if (!selectedBatches.value.length) return
  submitting.value = true
  errorMessage.value = ''
  infoMessage.value = ''
  try {
    const result = await submitReplayGroup(selectedBatches.value, groupName.value, groupNote.value)
    infoMessage.value = result.idempotent
      ? `重复提交已拦截：${result.message}`
      : result.message ?? '整组重放已提交'
    selectedBatches.value = []
    groupName.value = ''
    groupNote.value = ''
    await Promise.all([loadBatches(), loadGroups()])
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '整组重放提交失败'
  } finally {
    submitting.value = false
  }
}

async function resumeGroup(groupId: number) {
  errorMessage.value = ''
  infoMessage.value = ''
  try {
    const result = await resumeReplayGroup(groupId)
    infoMessage.value = result.message ?? '续办完成'
    await Promise.all([loadBatches(), loadGroups(), loadCorrections(), loadTrace()])
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '续办失败'
  }
}

async function loadCorrections() {
  errorMessage.value = ''
  try {
    corrections.value = await fetchCorrections(statusFilter.value ? { status: statusFilter.value } : {})
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '补算结果读取失败'
  }
}

async function loadTrace() {
  try {
    traceRows.value = await fetchCorrectionSources()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '溯源回查失败'
  }
}

onMounted(async () => {
  if (props.mode === 'event_tracker') {
    await Promise.all([loadBatches(), loadGroups()])
  } else {
    await Promise.all([loadCorrections(), loadTrace()])
  }
})
</script>

<style scoped>
.replay-console {
  margin: 16px 0;
  display: flex;
  flex-direction: column;
  gap: 16px;
}
.replay-block {
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 14px 16px;
}
.replay-title { margin: 0 0 6px; font-size: 15px; }
.replay-hint { margin: 0 0 12px; color: var(--muted); font-size: 12px; line-height: 1.6; }
.replay-toolbar { display: flex; flex-wrap: wrap; gap: 12px; align-items: flex-end; margin-bottom: 12px; }
.replay-field { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: var(--muted); }
.replay-field input,
.replay-field select { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; min-width: 180px; }
.replay-group-card { border: 1px solid var(--border); border-radius: 8px; padding: 10px 12px; margin-bottom: 12px; }
.replay-group-head { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; gap: 8px; }
.replay-code { color: var(--muted); font-size: 12px; margin: 0 8px; }
.replay-sub-table { margin: 8px 0; }
.replay-windows { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; }
.replay-window-chip,
.replay-batch-chip {
  display: inline-block;
  background: #f1f5f9;
  border-radius: 999px;
  padding: 2px 10px;
  font-size: 12px;
  margin: 2px 4px 2px 0;
}
.tag { display: inline-block; border-radius: 999px; padding: 2px 10px; font-size: 12px; }
.tag-done { background: #dcfce7; color: #166534; }
.tag-pending { background: #fef9c3; color: #854d0e; }
.tag-missing { background: #fee2e2; color: #991b1b; }
.tag-muted { background: #e2e8f0; color: #475569; }
.tag-replay { background: #e0e7ff; color: #3730a3; }
.tag-live { background: #f1f5f9; color: #334155; }
.replay-error { margin: 0; }
.replay-info { margin: 0; color: #166534; font-size: 12px; }
</style>

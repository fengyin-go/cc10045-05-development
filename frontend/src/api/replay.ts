/** 整组重放与窗口补算接口封装，供事件追踪器与实时聚合器页面共用。 */
import { fetchJson } from './client'

export type SourceBatch = {
  batch_id: string
  event_count: number
  first_occurred_at: string
  last_occurred_at: string
  ingested_at: string
  business_date: string
  is_replay_batch: boolean
}

export type ReplaySource = {
  id: number
  replay_group_id: number
  source_batch_id: string
  business_date: string
  status: '待读取' | '已读取' | '样本缺失'
  event_total: number
  event_accepted: number
  event_duplicated: number
  message: string
}

export type ReplayEventRow = {
  id: number
  replay_batch_id: string
  source_batch_id: string
  event_name: string
  occurred_at: string
  accept_status: '已接受' | '重复跳过'
}

export type ReplayGroup = {
  id: number
  replay_group_code: string
  group_name: string
  status: '待处理' | '处理中' | '已完成'
  note: string
  created_at: string
  sources: ReplaySource[]
  replay_events?: ReplayEventRow[]
  affected_windows?: AffectedWindow[]
}

export type AffectedWindow = {
  window_id: number
  window_name: string
  window_start: string
  window_end: string
  status: '待验明' | '已修正'
  source_records: number
  dimension_groups: number
}

export type CorrectionSource = {
  correction_id: number
  replay_group_id: number
  source_batch_id: string
  event_count: number
  window_name: string
  window_start: string
  dimension_key: string
  correction_status: '待验明' | '已修正'
  replay_group_code: string
  group_name: string
}

export type Correction = {
  id: number
  window_id: number
  window_name: string
  window_start: string
  window_end: string
  dimension_key: string
  computed_value: number
  event_count: number
  source_record_count: number
  computation_version: number
  status: '待验明' | '已修正'
  correction_sources?: CorrectionSource[]
}

type ListWrap<T> = { ok: boolean; items: T[]; total: number }
type GroupWrap = { ok: boolean; group: ReplayGroup; message?: string; idempotent?: boolean }

export async function fetchSourceBatches(): Promise<SourceBatch[]> {
  const payload = await fetchJson<ListWrap<SourceBatch>>('/api/pipeline/replay/batches')
  return payload.items
}

export async function submitReplayGroup(
  sourceBatchIds: string[],
  groupName: string,
  note: string,
): Promise<GroupWrap> {
  return fetchJson<GroupWrap>('/api/pipeline/replay/groups', {
    method: 'POST',
    body: JSON.stringify({ source_batch_ids: sourceBatchIds, group_name: groupName, note }),
  })
}

export async function fetchReplayGroups(status?: string): Promise<ReplayGroup[]> {
  const query = status ? `?status=${encodeURIComponent(status)}` : ''
  const payload = await fetchJson<ListWrap<ReplayGroup>>(`/api/pipeline/replay/groups${query}`)
  return payload.items
}

export async function resumeReplayGroup(groupId: number): Promise<GroupWrap> {
  return fetchJson<GroupWrap>(`/api/pipeline/replay/groups/${groupId}/resume`, {
    method: 'POST',
  })
}

export async function fetchCorrections(filters: {
  windowId?: number
  status?: string
  replayGroupId?: number
} = {}): Promise<Correction[]> {
  const params = new URLSearchParams()
  if (filters.windowId !== undefined) params.set('window_id', String(filters.windowId))
  if (filters.status) params.set('status', filters.status)
  if (filters.replayGroupId !== undefined) params.set('replay_group_id', String(filters.replayGroupId))
  const query = params.toString()
  const payload = await fetchJson<ListWrap<Correction>>(
    `/api/pipeline/aggregation/corrections${query ? `?${query}` : ''}`,
  )
  return payload.items
}

export async function fetchCorrectionSources(
  replayGroupId?: number,
  sourceBatchId?: string,
): Promise<CorrectionSource[]> {
  const params = new URLSearchParams()
  if (replayGroupId !== undefined) params.set('replay_group_id', String(replayGroupId))
  if (sourceBatchId) params.set('source_batch_id', sourceBatchId)
  const query = params.toString()
  const payload = await fetchJson<ListWrap<CorrectionSource>>(
    `/api/pipeline/aggregation/corrections/sources${query ? `?${query}` : ''}`,
  )
  return payload.items
}

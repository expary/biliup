import { API_BASE } from './api-streamer'

export type YouTubeJobStatus = 'idle' | 'running' | 'paused' | 'error'
export type YouTubeSourceType = 'channel' | 'playlist' | 'shorts'

export type YouTubeItemStatus =
  | 'discovered'
  | 'meta_ready'
  | 'downloaded'
  | 'transcoded'
  | 'ready_upload'
  | 'uploaded'
  | 'skipped_duplicate'
  | 'failed'

export interface YouTubeJobEntity {
  id: number
  name: string
  source_url: string
  source_type: YouTubeSourceType
  upload_streamer_id: number
  enabled: number
  sync_interval_seconds: number
  auto_publish: number
  backfill_mode: string
  status: YouTubeJobStatus
  last_sync_at?: number
  next_sync_at?: number
  last_error?: string
  created_at: number
  updated_at: number
}

export interface YouTubeItemEntity {
  id: number
  job_id: number
  video_id: string
  video_url: string
  source_title?: string
  generated_title?: string
  generated_description?: string
  generated_tags?: string
  local_file_path?: string
  transcoded_file_path?: string
  status: YouTubeItemStatus
  retry_count: number
  last_error?: string
  bili_aid?: number
  bili_bvid?: string
  uploaded_at?: number
}

export interface YouTubeJobListResponse {
  summary: {
    total_jobs: number
    pending_items: number
    failed_items: number
    uploaded_items: number
  }
  jobs: YouTubeJobEntity[]
}

export interface YouTubeItemListResponse {
  items: YouTubeItemEntity[]
  total: number
  page: number
  page_size: number
}

export const YOUTUBE_SOURCE_TYPE_OPTIONS: Array<{ value: YouTubeSourceType; label: string }> = [
  { value: 'channel', label: '频道' },
  { value: 'playlist', label: '播放列表' },
  { value: 'shorts', label: 'Shorts 短视频' },
]

export function getYouTubeSourceTypeLabel(sourceType?: string) {
  return YOUTUBE_SOURCE_TYPE_OPTIONS.find(option => option.value === sourceType)?.label ?? sourceType ?? '-'
}

async function handleResponse(res: Response) {
  if (res.status === 401) {
    const returnTo = encodeURIComponent(window.location.pathname + window.location.search)
    window.location.href = `/login?next=${returnTo}`
    throw new Error('Unauthorized')
  }

  if (!res.ok) {
    const text = await res.text().catch(() => '')
    if (text) {
      try {
        const parsed = JSON.parse(text)
        const message = parsed?.message
        if (typeof message === 'string' && message.trim()) {
          throw new Error(message)
        }
      } catch {
        // ignore json parse error
      }
    }
    throw new Error(text || `HTTP ${res.status}`)
  }
  return res
}

export async function fetcher<T>(input: string) {
  const res = await fetch(API_BASE + input)
  await handleResponse(res)
  return (await res.json()) as T
}

export async function post<T>(url: string, body?: unknown) {
  const res = await fetch(API_BASE + url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: body == null ? undefined : JSON.stringify(body),
  })
  await handleResponse(res)
  return (await res.json()) as T
}

export async function put<T>(url: string, body: unknown) {
  const res = await fetch(API_BASE + url, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  await handleResponse(res)
  return (await res.json()) as T
}

export async function del<T>(url: string) {
  const res = await fetch(API_BASE + url, {
    method: 'DELETE',
  })
  await handleResponse(res)
  return (await res.json()) as T
}

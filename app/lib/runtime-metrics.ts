export interface ControlCenterGlobalMetrics {
  active_downloads: number
  active_uploads: number
  total_download_bytes: number
  total_upload_bytes: number
  avg_download_bps: number
  avg_upload_bps: number
  avg_upload_file_duration_ms: number
}

export interface ControlCenterTaskMetrics {
  key: string
  kind: 'streamer' | 'youtube' | string
  id: number
  name: string
  url: string
  stage?: string | null
  message?: string | null
  download_status: string
  upload_status: string
  cleanup_status: string
  metrics: any
  download_progress?: number | null
  upload_progress?: number | null
  ffmpeg_progress?: number | null
}

export interface ControlCenterMetricsResponse {
  ts_ms: number
  global: ControlCenterGlobalMetrics
  tasks: ControlCenterTaskMetrics[]
}

export interface RuntimePhaseSummary {
  stage: string
  percent: number | null
  detail: string
}

function getPositiveNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) && value > 0 ? value : null
}

export function formatBytes(bytes?: number | null): string {
  if (!bytes || !Number.isFinite(bytes) || bytes <= 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  const base = 1024
  const idx = Math.min(units.length - 1, Math.floor(Math.log(bytes) / Math.log(base)))
  const value = bytes / Math.pow(base, idx)
  return `${value.toFixed(value >= 100 || idx === 0 ? 0 : 2)} ${units[idx]}`
}

export function formatBps(bps?: number | null): string {
  if (!bps || !Number.isFinite(bps) || bps <= 0) return '-'
  return `${formatBytes(bps)}/s`
}

export function formatDurationMs(ms?: number | null): string {
  if (!ms || !Number.isFinite(ms) || ms <= 0) return '-'
  const totalSec = Math.floor(ms / 1000)
  const h = Math.floor(totalSec / 3600)
  const m = Math.floor((totalSec % 3600) / 60)
  const s = totalSec % 60
  if (h > 0) return `${h}h ${m}m ${s}s`
  if (m > 0) return `${m}m ${s}s`
  return `${s}s`
}

export function getNormalizedProgress(progress?: number | null): number | null {
  if (typeof progress !== 'number' || !Number.isFinite(progress)) {
    return null
  }
  return Math.min(1, Math.max(0, progress))
}

export function getPrimaryProgress(task?: ControlCenterTaskMetrics | null): number | null {
  if (!task) return null
  return (
    getNormalizedProgress(task.upload_progress) ??
    getNormalizedProgress(task.ffmpeg_progress) ??
    getNormalizedProgress(task.download_progress)
  )
}

export function getFfmpegElapsedMs(task?: ControlCenterTaskMetrics | null): number | null {
  return getPositiveNumber(task?.metrics?.ffmpeg?.out_time_ms)
}

export function getFfmpegTotalMs(task?: ControlCenterTaskMetrics | null): number | null {
  const elapsedMs = getFfmpegElapsedMs(task)
  const progress = getNormalizedProgress(task?.ffmpeg_progress)
  if (elapsedMs == null || progress == null || progress <= 0) {
    return null
  }
  return Math.round(elapsedMs / progress)
}

export function getFfmpegEtaMs(task?: ControlCenterTaskMetrics | null): number | null {
  const totalMs = getFfmpegTotalMs(task)
  const elapsedMs = getFfmpegElapsedMs(task)
  if (totalMs == null || elapsedMs == null || totalMs <= elapsedMs) {
    return null
  }
  return totalMs - elapsedMs
}

function getUploadEtaMs(task?: ControlCenterTaskMetrics | null): number | null {
  const totalBytes = getPositiveNumber(task?.metrics?.upload?.current_file_total_bytes)
  const sentBytes = getPositiveNumber(task?.metrics?.upload?.current_file_sent_bytes)
  const currentBps = getPositiveNumber(task?.metrics?.upload?.current_bps)
  if (totalBytes == null || currentBps == null) {
    return null
  }
  const remainingBytes = Math.max(totalBytes - (sentBytes ?? 0), 0)
  if (remainingBytes <= 0) {
    return null
  }
  return Math.round((remainingBytes / currentBps) * 1000)
}

export function getRuntimePhaseSummary(task?: ControlCenterTaskMetrics | null): RuntimePhaseSummary | null {
  if (!task) return null

  const stage = task.stage?.trim() || '执行中'
  const ffmpegProgress = getNormalizedProgress(task.ffmpeg_progress)
  const ffmpegActive = task?.metrics?.ffmpeg?.active === true
  if (stage === '转码' || stage === '处理' || ffmpegActive || ffmpegProgress != null) {
    const detailParts: string[] = []
    const percent = ffmpegProgress == null ? null : Math.round(ffmpegProgress * 100)
    const elapsedMs = getFfmpegElapsedMs(task)
    const totalMs = getFfmpegTotalMs(task)
    const etaMs = getFfmpegEtaMs(task)
    const speed = typeof task?.metrics?.ffmpeg?.speed === 'string' ? task.metrics.ffmpeg.speed.trim() : ''
    const outputSizeBytes = getPositiveNumber(task?.metrics?.ffmpeg?.total_size)

    if (percent != null) detailParts.push(`进度 ${percent}%`)
    if (elapsedMs != null && totalMs != null) {
      detailParts.push(`已处理 ${formatDurationMs(elapsedMs)} / ${formatDurationMs(totalMs)}`)
    } else if (elapsedMs != null) {
      detailParts.push(`已处理 ${formatDurationMs(elapsedMs)}`)
    }
    if (speed) detailParts.push(`速度 ${speed}`)
    if (etaMs != null) detailParts.push(`ETA ${formatDurationMs(etaMs)}`)
    if (outputSizeBytes != null) detailParts.push(`输出 ${formatBytes(outputSizeBytes)}`)

    return {
      stage,
      percent,
      detail: detailParts.join(' · ') || task.message?.trim() || 'FFmpeg 处理中',
    }
  }

  const uploadProgress = getNormalizedProgress(task.upload_progress)
  const uploadActive = task?.metrics?.upload?.active === true
  if (stage === '上传' || stage === '投稿' || uploadActive || uploadProgress != null) {
    const detailParts: string[] = []
    const percent = uploadProgress == null ? null : Math.round(uploadProgress * 100)
    const totalBytes = getPositiveNumber(task?.metrics?.upload?.current_file_total_bytes)
    const sentBytes = getPositiveNumber(task?.metrics?.upload?.current_file_sent_bytes) ?? 0
    const currentBps = getPositiveNumber(task?.metrics?.upload?.current_bps)
    const avgBps = getPositiveNumber(task?.metrics?.upload?.avg_bps)
    const etaMs = getUploadEtaMs(task)

    if (percent != null) detailParts.push(`进度 ${percent}%`)
    if (totalBytes != null) {
      detailParts.push(`已发送 ${formatBytes(sentBytes)} / ${formatBytes(totalBytes)}`)
    }
    if (currentBps != null) detailParts.push(`速度 ${formatBps(currentBps)}`)
    if (avgBps != null) detailParts.push(`平均 ${formatBps(avgBps)}`)
    if (etaMs != null) detailParts.push(`ETA ${formatDurationMs(etaMs)}`)

    return {
      stage,
      percent,
      detail: detailParts.join(' · ') || task.message?.trim() || '上传中',
    }
  }

  const downloadProgress = getNormalizedProgress(task.download_progress)
  const downloadActive = task?.metrics?.download?.active === true
  if (stage === '下载' || downloadActive || downloadProgress != null) {
    const detailParts: string[] = []
    const percent = downloadProgress == null ? null : Math.round(downloadProgress * 100)
    const totalBytes = getPositiveNumber(task?.metrics?.download?.total_bytes)
    const currentBps =
      getPositiveNumber(task?.metrics?.download?.last_bps) ??
      getPositiveNumber(task?.metrics?.download?.avg_bps)

    if (percent != null) detailParts.push(`进度 ${percent}%`)
    if (totalBytes != null) detailParts.push(`已下载 ${formatBytes(totalBytes)}`)
    if (currentBps != null) detailParts.push(`速度 ${formatBps(currentBps)}`)

    return {
      stage,
      percent,
      detail: detailParts.join(' · ') || task.message?.trim() || '下载中',
    }
  }

  return {
    stage,
    percent:
      getPrimaryProgress(task) == null ? null : Math.round((getPrimaryProgress(task) ?? 0) * 100),
    detail: task.message?.trim() || '执行中',
  }
}

'use client'

import React, { useMemo } from 'react'
import useSWR from 'swr'
import { fetcher } from '@/app/lib/api-streamer'
import { Card, Layout, List, Nav, Progress, Space, Spin, Table, Tag, Typography } from '@douyinfe/semi-ui'
import { IconHome, IconSetting } from '@douyinfe/semi-icons'

type ControlCenterMetrics = {
  ts_ms: number
  global: {
    active_downloads: number
    active_uploads: number
    total_download_bytes: number
    total_upload_bytes: number
    avg_download_bps: number
    avg_upload_bps: number
    avg_upload_file_duration_ms: number
  }
  tasks: Array<{
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
    download_progress?: number | null
    upload_progress?: number | null
    ffmpeg_progress?: number | null
    metrics: any
  }>
}

function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  const base = 1024
  const idx = Math.min(units.length - 1, Math.floor(Math.log(bytes) / Math.log(base)))
  const value = bytes / Math.pow(base, idx)
  return `${value.toFixed(value >= 100 || idx === 0 ? 0 : 2)} ${units[idx]}`
}

function formatBps(bps?: number | null): string {
  if (!bps || !Number.isFinite(bps) || bps <= 0) return '-'
  return `${formatBytes(bps)}/s`
}

function formatDurationMs(ms?: number | null): string {
  if (!ms || !Number.isFinite(ms) || ms <= 0) return '-'
  const totalSec = Math.floor(ms / 1000)
  const h = Math.floor(totalSec / 3600)
  const m = Math.floor((totalSec % 3600) / 60)
  const s = totalSec % 60
  if (h > 0) return `${h}h ${m}m ${s}s`
  if (m > 0) return `${m}m ${s}s`
  return `${s}s`
}

function statusTag(status: string) {
  const normalized = (status || '').trim()
  const color =
    normalized === 'Working'
      ? 'green'
      : normalized === 'Pending'
        ? 'orange'
        : normalized === 'Pause'
          ? 'grey'
          : 'cyan'
  return (
    <Tag size="small" color={color} type="solid">
      {normalized || '-'}
    </Tag>
  )
}

export default function Home() {
  const { Header, Content } = Layout
  const { Title, Text } = Typography

  const { data, error, isLoading } = useSWR<ControlCenterMetrics>('/v1/metrics', fetcher, {
    refreshInterval: 1000,
  })

  const global = data?.global
  const tasksStable = useMemo(() => data?.tasks ?? [], [data?.tasks])

  const processing = useMemo(() => {
    const result: Array<{
      key: string
      kind: 'streamer' | 'youtube'
      title: string
      subtitle?: string
      tags: React.ReactNode[]
      progress?: number | null
      detail?: string
      href?: string
      updated_at_ms: number
    }> = []

    for (const record of tasksStable) {
      const metrics = record.metrics ?? {}
      const tags: React.ReactNode[] = []
      const isYouTube = record.kind === 'youtube'

      if (isYouTube) {
        tags.push(
          <Tag key="yt" color="cyan" type="solid" size="small">
            YouTube
          </Tag>
        )
        if (record.stage) {
          tags.push(
            <Tag key="stage" color="grey" type="solid" size="small">
              {record.stage}
            </Tag>
          )
        }
      }

      if (record.download_status === 'Working') {
        tags.push(
          <Tag key="dl" color="green" type="solid" size="small">
            下载
          </Tag>
        )
      }
      if (metrics.ffmpeg?.active) {
        tags.push(
          <Tag key="ff" color="blue" type="solid" size="small">
            FFmpeg
          </Tag>
        )
      }
      if (record.upload_status === 'Pending') {
        tags.push(
          <Tag key="ul" color="orange" type="solid" size="small">
            上传/后处理
          </Tag>
        )
      }
      if (record.cleanup_status === 'Pending') {
        tags.push(
          <Tag key="cl" color="grey" type="solid" size="small">
            清理
          </Tag>
        )
      }

      if (tags.length === 0) {
        continue
      }

      const progress =
        (typeof record.upload_progress === 'number' ? record.upload_progress : null) ??
        (typeof record.ffmpeg_progress === 'number' ? record.ffmpeg_progress : null) ??
        (typeof record.download_progress === 'number' ? record.download_progress : null)

      const download = metrics.download ?? {}
      const upload = metrics.upload ?? {}
      const ffmpeg = metrics.ffmpeg ?? {}
      const detailParts: string[] = []
      if (isYouTube && record.message) {
        detailParts.push(record.message)
      }
      if (record.download_status === 'Working') {
        detailParts.push(`下载 ${formatBytes(download.total_bytes ?? 0)} · 平均 ${formatBps(download.avg_bps)}`)
      }
      if (ffmpeg.active) {
        detailParts.push(
          `FFmpeg ${ffmpeg.out_time_ms ? formatDurationMs(ffmpeg.out_time_ms) : '-'} · ${ffmpeg.speed ?? '-'}`
        )
      }
      if (record.upload_status === 'Pending') {
        detailParts.push(
          `上传 ${formatBps(upload.current_bps)} · 平均 ${formatBps(upload.avg_bps)} · 耗时 ${formatDurationMs(
            upload.avg_file_duration_ms
          )}`
        )
      }

      const updated_at_ms_candidates = [
        metrics.ffmpeg?.updated_at_ms,
        metrics.upload?.current_started_at_ms,
        metrics.download?.segment_started_at_ms,
        metrics.download?.started_at_ms,
        metrics.upload?.started_at_ms,
        data?.ts_ms,
      ].filter((v: any) => typeof v === 'number' && Number.isFinite(v)) as number[]
      const updated_at_ms = updated_at_ms_candidates.length ? Math.max(...updated_at_ms_candidates) : Date.now()

      result.push({
        key: record.key ?? `${isYouTube ? 'youtube' : 'streamer'}-${record.id}`,
        kind: isYouTube ? 'youtube' : 'streamer',
        title: record.name || `#${record.id}`,
        subtitle: record.url,
        tags,
        progress,
        detail: detailParts.join(' · '),
        href: isYouTube ? `/youtube/${record.id}` : '/streamers',
        updated_at_ms,
      })
    }

    result.sort((a, b) => b.updated_at_ms - a.updated_at_ms)
    return result.slice(0, 10)
  }, [data?.ts_ms, tasksStable])

  const columns = useMemo(
    () => [
      {
        title: '任务',
        dataIndex: 'name',
        width: 220,
        render: (_: any, record: any) => (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8 }}>
              <Text strong>{record.name || `#${record.id}`}</Text>
              <Space spacing={6}>
                {record.kind === 'youtube' ? (
                  <Tag size="small" color="cyan" type="solid">
                    YouTube
                  </Tag>
                ) : null}
                {record.stage ? (
                  <Tag size="small" color="grey" type="solid">
                    {record.stage}
                  </Tag>
                ) : null}
              </Space>
            </div>
            <Text type="tertiary" style={{ fontSize: 12, wordBreak: 'break-all' }}>
              {record.url}
            </Text>
            {record.message ? (
              <Text type="tertiary" style={{ fontSize: 12, wordBreak: 'break-all' }}>
                {record.message}
              </Text>
            ) : null}
          </div>
        ),
      },
      {
        title: '下载',
        width: 320,
        render: (_: any, record: any) => {
          const metrics = record.metrics?.download ?? {}
          const p = typeof record.download_progress === 'number' ? record.download_progress : null
          const percent = p == null ? 0 : Math.round(p * 100)
          const currentBps = metrics.last_bps
          return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                {statusTag(record.download_status)}
                <Text type="tertiary" style={{ fontSize: 12 }}>
                  总量 {formatBytes(metrics.total_bytes ?? 0)} · 速度 {formatBps(currentBps)} · 平均 {formatBps(metrics.avg_bps)}
                </Text>
              </div>
              <Progress percent={percent} showInfo={false} />
            </div>
          )
        },
      },
      {
        title: 'FFmpeg',
        width: 260,
        render: (_: any, record: any) => {
          const metrics = record.metrics?.ffmpeg ?? {}
          const p = typeof record.ffmpeg_progress === 'number' ? record.ffmpeg_progress : null
          const percent = p == null ? 0 : Math.round(p * 100)
          const speed = metrics.speed ?? '-'
          const outTime = metrics.out_time_ms ? formatDurationMs(metrics.out_time_ms) : '-'
          return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <Tag size="small" color={metrics.active ? 'green' : 'grey'} type="solid">
                  {metrics.active ? 'Running' : 'Idle'}
                </Tag>
                <Text type="tertiary" style={{ fontSize: 12 }}>
                  {outTime} · {speed}
                </Text>
              </div>
              <Progress percent={percent} showInfo={false} />
            </div>
          )
        },
      },
      {
        title: '上传',
        width: 380,
        render: (_: any, record: any) => {
          const metrics = record.metrics?.upload ?? {}
          const p = typeof record.upload_progress === 'number' ? record.upload_progress : null
          const percent = p == null ? 0 : Math.round(p * 100)
          const current = metrics.current_bps
          const avg = metrics.avg_bps
          return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                {statusTag(record.upload_status)}
                <Text type="tertiary" style={{ fontSize: 12 }}>
                  速度 {formatBps(current)} · 平均 {formatBps(avg)} · 平均耗时{' '}
                  {formatDurationMs(metrics.avg_file_duration_ms)}
                </Text>
              </div>
              <Progress percent={percent} showInfo={false} />
              {metrics.current_file ? (
                <Text type="tertiary" style={{ fontSize: 12, wordBreak: 'break-all' }}>
                  {metrics.current_file}
                </Text>
              ) : null}
            </div>
          )
        },
      },
      {
        title: '清理',
        width: 120,
        render: (_: any, record: any) => statusTag(record.cleanup_status),
      },
    ],
    [Text]
  )

  if (isLoading) {
    return <Spin size="large" />
  }
  if (error) {
    return <div style={{ padding: 12 }}>加载失败：{String((error as any)?.message ?? error)}</div>
  }

  return (
    <>
      <Header style={{ backgroundColor: 'var(--semi-color-bg-1)' }}>
        <Nav
          style={{ border: 'none' }}
          header={
            <>
              <div
                style={{
                  backgroundColor: '#ffaa00ff',
                  borderRadius: 'var(--semi-border-radius-large)',
                  color: 'var(--semi-color-bg-0)',
                  display: 'flex',
                  padding: '6px',
                }}
              >
                <IconHome size="large" />
              </div>
              <h4 style={{ marginLeft: '12px' }}>中控面板</h4>
            </>
          }
          mode="horizontal"
        ></Nav>
      </Header>
      <Content
        style={{
          paddingLeft: 12,
          paddingRight: 12,
          paddingTop: 12,
          backgroundColor: 'var(--semi-color-bg-0)',
        }}
      >
        <main>
          <Card style={{ marginBottom: 12 }}>
            <Space align="center">
              <IconSetting />
              <Title heading={6} style={{ margin: 0 }}>
                正在处理（最多 10 个）
              </Title>
            </Space>
            <div style={{ marginTop: 8 }}>
              {processing.length === 0 ? (
                <Text type="tertiary">暂无进行中任务</Text>
              ) : (
                <List
                  size="small"
                  split={false}
                  dataSource={processing}
                  renderItem={item => (
                    <List.Item style={{ padding: 0, marginBottom: 10 }}>
                      <div
                        style={{
                          padding: 12,
                          borderRadius: 8,
                          border: '1px solid var(--semi-color-border)',
                          background: 'var(--semi-color-fill-0)',
                          display: 'flex',
                          flexDirection: 'column',
                          gap: 6,
                        }}
                      >
                        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12 }}>
                          <Text strong ellipsis={{ showTooltip: true }} style={{ maxWidth: '60%' }}>
                            {item.href ? (
                              <a href={item.href} style={{ color: 'inherit', textDecoration: 'none' }}>
                                {item.title}
                              </a>
                            ) : (
                              item.title
                            )}
                          </Text>
                          <Space wrap spacing={6}>
                            {item.tags}
                            {typeof item.progress === 'number' ? (
                              <Tag size="small" color="grey">
                                {Math.round(item.progress * 100)}%
                              </Tag>
                            ) : null}
                          </Space>
                        </div>
                        {item.detail ? (
                          <Text type="tertiary" style={{ fontSize: 12, wordBreak: 'break-all' }}>
                            {item.detail}
                          </Text>
                        ) : null}
                        {item.subtitle ? (
                          <Text type="tertiary" style={{ fontSize: 12, wordBreak: 'break-all' }}>
                            {item.subtitle}
                          </Text>
                        ) : null}
                      </div>
                    </List.Item>
                  )}
                />
              )}
            </div>
          </Card>

          <div
            style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
              gap: 12,
              marginBottom: 12,
            }}
          >
            <Card>
              <Space align="center">
                <IconSetting />
                <Title heading={6} style={{ margin: 0 }}>
                  下载
                </Title>
              </Space>
              <div style={{ marginTop: 8 }}>
                <Text>
                  活跃：{global?.active_downloads ?? 0} · 总量：{formatBytes(global?.total_download_bytes ?? 0)}
                </Text>
                <br />
                <Text type="tertiary">平均速度：{formatBps(global?.avg_download_bps ?? 0)}</Text>
              </div>
            </Card>
            <Card>
              <Space align="center">
                <IconSetting />
                <Title heading={6} style={{ margin: 0 }}>
                  上传
                </Title>
              </Space>
              <div style={{ marginTop: 8 }}>
                <Text>
                  活跃：{global?.active_uploads ?? 0} · 总量：{formatBytes(global?.total_upload_bytes ?? 0)}
                </Text>
                <br />
                <Text type="tertiary">平均速度：{formatBps(global?.avg_upload_bps ?? 0)}</Text>
              </div>
            </Card>
            <Card>
              <Space align="center">
                <IconSetting />
                <Title heading={6} style={{ margin: 0 }}>
                  平均耗时
                </Title>
              </Space>
              <div style={{ marginTop: 8 }}>
                <Text>单文件上传：{formatDurationMs(global?.avg_upload_file_duration_ms ?? 0)}</Text>
              </div>
            </Card>
          </div>

          <Table
            size="small"
            rowKey="key"
            columns={columns as any}
            dataSource={tasksStable as any}
            pagination={false}
          />
        </main>
      </Content>
    </>
  )
}

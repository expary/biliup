'use client'

import React, { useMemo } from 'react'
import useSWR from 'swr'
import { fetcher } from '@/app/lib/api-streamer'
import { Card, Layout, Nav, Progress, Space, Spin, Table, Tag, Typography } from '@douyinfe/semi-ui'
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
    id: number
    name: string
    url: string
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

  const tasks = data?.tasks ?? []
  const global = data?.global

  const columns = useMemo(
    () => [
      {
        title: '任务',
        dataIndex: 'name',
        width: 220,
        render: (_: any, record: any) => (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            <Text strong>{record.name || `#${record.id}`}</Text>
            <Text type="tertiary" style={{ fontSize: 12, wordBreak: 'break-all' }}>
              {record.url}
            </Text>
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
          return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                {statusTag(record.download_status)}
                <Text type="tertiary" style={{ fontSize: 12 }}>
                  总量 {formatBytes(metrics.total_bytes ?? 0)} · 平均 {formatBps(metrics.avg_bps)}
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
            rowKey="id"
            columns={columns as any}
            dataSource={tasks as any}
            pagination={false}
          />
        </main>
      </Content>
    </>
  )
}

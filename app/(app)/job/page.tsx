'use client'

import { useMemo, useState } from 'react'
import { Button, Empty, Layout, Modal, Nav, Notification, Popconfirm, Select, Space, Table, Tag, Typography } from '@douyinfe/semi-ui'
import { IconCloudStroked, IconPause, IconPlay, IconRefresh } from '@douyinfe/semi-icons'
import useSWR from 'swr'
import {
  del as youtubeDelete,
  fetcher as youtubeFetcher,
  YouTubeActiveTasksResponse,
  getYouTubeSourceTypeLabel,
  post as youtubePost,
  YouTubeGlobalItemEntity,
  YouTubeGlobalItemListResponse,
  YouTubeItemLogsResponse,
  YouTubeJobListResponse,
  YouTubeQueueHealth,
} from '@/app/lib/api-youtube'
import { humDate } from '@/app/lib/utils'

function statusDot(color: string) {
  return (
    <span
      style={{
        width: 8,
        height: 8,
        borderRadius: '50%',
        display: 'inline-block',
        backgroundColor: color,
        boxShadow: `0 0 0 2px ${color}22`,
      }}
    />
  )
}

function itemRunState(item: YouTubeGlobalItemEntity, isExecuting: boolean, queuePaused: boolean) {
  if (isExecuting) {
    return {
      label: '正在执行',
      color: '#52c41a',
    }
  }
  if (queuePaused && PENDING_STATUSES.includes(item.status)) {
    return {
      label: '已暂停',
      color: '#ff4d4f',
    }
  }
  if (item.status === 'failed' || item.last_error) {
    return {
      label: '异常',
      color: '#fa8c16',
    }
  }
  if (PENDING_STATUSES.includes(item.status)) {
    return {
      label: '等待中',
      color: '#94a3b8',
    }
  }
  return {
    label: '已完成',
    color: '#94a3b8',
  }
}

function itemStatusTag(status?: string) {
  switch (status) {
    case 'discovered':
      return <Tag color="orange">待采集元数据</Tag>
    case 'meta_ready':
      return <Tag color="lime">待下载</Tag>
    case 'downloaded':
      return <Tag color="blue">待处理</Tag>
    case 'transcoded':
      return <Tag color="violet">待上传</Tag>
    case 'ready_upload':
      return <Tag color="cyan">待投稿</Tag>
    case 'failed':
      return <Tag color="red">失败</Tag>
    case 'uploaded':
      return <Tag color="green">已上传</Tag>
    case 'skipped_duplicate':
      return <Tag color="grey">重复跳过</Tag>
    default:
      return <Tag color="grey">{status || '-'}</Tag>
  }
}

function formatTs(ts?: number) {
  if (!ts || !Number.isFinite(ts) || ts <= 0) return '-'
  return humDate(ts)
}

function formatUploadDate(value?: string) {
  if (!value) return '-'
  if (/^\d{8}$/.test(value)) {
    return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`
  }
  return value
}

function parseTags(value?: string) {
  if (!value) return []
  try {
    const parsed = JSON.parse(value)
    return Array.isArray(parsed) ? parsed.filter(item => typeof item === 'string') : []
  } catch {
    return []
  }
}

const STATUS_OPTIONS = [
  { label: '全部', value: '' },
  { label: '待采集元数据', value: 'discovered' },
  { label: '待下载', value: 'meta_ready' },
  { label: '待处理', value: 'downloaded' },
  { label: '待上传', value: 'transcoded' },
  { label: '待投稿', value: 'ready_upload' },
  { label: '失败', value: 'failed' },
  { label: '已上传', value: 'uploaded' },
  { label: '重复跳过', value: 'skipped_duplicate' },
]

const PENDING_STATUSES = ['discovered', 'meta_ready', 'downloaded', 'transcoded', 'ready_upload']

export default function JobPage() {
  const { Header, Content } = Layout
  const [status, setStatus] = useState<string>('')
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(50)
  const [pendingAction, setPendingAction] = useState<string | null>(null)
  const [detailItem, setDetailItem] = useState<YouTubeGlobalItemEntity | null>(null)

  const query = useMemo(() => {
    const search = new URLSearchParams()
    search.set('page', String(page))
    search.set('page_size', String(pageSize))
    if (status) search.set('status', status)
    return `/v1/youtube/items?${search.toString()}`
  }, [page, pageSize, status])

  const {
    data: itemsResp,
    mutate,
    isLoading,
  } = useSWR<YouTubeGlobalItemListResponse>(query, youtubeFetcher, {
    refreshInterval: 10_000,
  })
  const { data: jobsResp } = useSWR<YouTubeJobListResponse>(
    '/v1/youtube/jobs',
    youtubeFetcher,
    {
      refreshInterval: 10_000,
    }
  )

  const { data: activeResp, mutate: mutateActive } = useSWR<YouTubeActiveTasksResponse>(
    '/v1/youtube/active?limit=20',
    youtubeFetcher,
    {
      refreshInterval: 3_000,
    }
  )
  const { data: healthResp, mutate: mutateHealth } = useSWR<YouTubeQueueHealth>(
    '/v1/youtube/manager/health',
    youtubeFetcher,
    {
      refreshInterval: 3_000,
    }
  )
  const detailLogsUrl = useMemo(
    () => (detailItem ? `/v1/youtube/items/${detailItem.id}/logs` : null),
    [detailItem]
  )
  const { data: detailLogsResp, isLoading: detailLogsLoading } = useSWR<YouTubeItemLogsResponse>(
    detailLogsUrl,
    youtubeFetcher,
    {
      refreshInterval: 3_000,
    }
  )

  const allItems = useMemo(() => itemsResp?.items ?? [], [itemsResp?.items])
  const activeVideoKeys = useMemo(() => {
    const keys = new Set<string>()
    for (const entry of activeResp?.items ?? []) {
      if (entry.video_id) {
        keys.add(`${entry.job_id}:${entry.video_id}`)
      }
    }
    return keys
  }, [activeResp?.items])
  const items = useMemo(() => {
    const list = [...allItems]
    list.sort((a, b) => {
      const aActive = activeVideoKeys.has(`${a.job_id}:${a.video_id}`)
      const bActive = activeVideoKeys.has(`${b.job_id}:${b.video_id}`)
      if (aActive !== bActive) {
        return aActive ? -1 : 1
      }

      const aQueue = typeof a.queue_position === 'number' ? a.queue_position : Number.MAX_SAFE_INTEGER
      const bQueue = typeof b.queue_position === 'number' ? b.queue_position : Number.MAX_SAFE_INTEGER
      if (aQueue !== bQueue) {
        return aQueue - bQueue
      }

      return b.created_at - a.created_at
    })
    return list
  }, [activeVideoKeys, allItems])

  const refreshAll = async () => {
    await Promise.all([mutate(), mutateHealth(), mutateActive()])
  }

  const runQueue = async () => {
    setPendingAction('run')
    try {
      await youtubePost('/v1/youtube/queue/run')
      await refreshAll()
    } catch (error: any) {
      Notification.error({
        title: '启动失败',
        content: error.message,
        position: 'top',
      })
    } finally {
      setPendingAction(null)
    }
  }

  const pauseQueue = async () => {
    setPendingAction('pause')
    try {
      await youtubePost('/v1/youtube/queue/pause')
      await refreshAll()
    } catch (error: any) {
      Notification.error({
        title: '暂停失败',
        content: error.message,
        position: 'top',
      })
    } finally {
      setPendingAction(null)
    }
  }

  const retryFailed = async () => {
    setPendingAction('retry')
    try {
      const result = await youtubePost<{ ok: boolean; retried_count: number }>(
        '/v1/youtube/queue/retry_failed'
      )
      if (!result.retried_count) {
        Notification.info({
          title: '没有失败项',
          content: '当前没有失败视频任务',
          position: 'top',
        })
      } else {
        Notification.success({
          title: '已加入队尾',
          content: `已将 ${result.retried_count} 个失败视频重新排到队尾`,
          position: 'top',
        })
      }
      await refreshAll()
    } catch (error: any) {
      Notification.error({
        title: '重试失败',
        content: error.message,
        position: 'top',
      })
    } finally {
      setPendingAction(null)
    }
  }

  const deleteItem = async (item: YouTubeGlobalItemEntity) => {
    setPendingAction(`delete-${item.id}`)
    try {
      await youtubeDelete(`/v1/youtube/items/${item.id}`)
      if (detailItem?.id === item.id) {
        setDetailItem(null)
      }
      await refreshAll()
      Notification.success({
        title: '删除成功',
        content: `已删除视频任务 ${item.video_id}`,
        position: 'top',
      })
    } catch (error: any) {
      Notification.error({
        title: '删除失败',
        content: error.message,
        position: 'top',
      })
    } finally {
      setPendingAction(null)
    }
  }

  const failedCount = jobsResp?.summary.failed_items ?? 0
  const bugCount = jobsResp?.summary.bug_items ?? failedCount
  const queuePaused = healthResp?.item_worker_paused ?? false
  const pendingTotal = jobsResp?.summary.pending_items ?? 0
  const currentTotal = itemsResp?.total ?? items.length

  const activeExecutionCount = useMemo(
    () => (activeResp?.items ?? []).filter(item => item.video_id).length,
    [activeResp?.items]
  )
  const queueStatus = useMemo(() => {
    if (queuePaused) {
      return {
        label: '已暂停',
        color: '#ff4d4f',
      }
    }
    if (activeExecutionCount > 0) {
      return {
        label: '正在执行',
        color: '#52c41a',
      }
    }
    return {
      label: '空闲',
      color: '#94a3b8',
    }
  }, [activeExecutionCount, queuePaused])

  const columns = [
    {
      title: '顺序',
      dataIndex: 'order',
      width: 80,
      render: (_: unknown, item: YouTubeGlobalItemEntity, index: number) => {
        return item.queue_position ?? (page - 1) * pageSize + index + 1
      },
    },
    {
      title: '状态点',
      dataIndex: 'run_state',
      width: 120,
      render: (_: unknown, item: YouTubeGlobalItemEntity) => {
        const isExecuting = activeVideoKeys.has(`${item.job_id}:${item.video_id}`)
        const runState = itemRunState(item, isExecuting, queuePaused)
        return (
          <span style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
            {statusDot(runState.color)}
            <Typography.Text>{runState.label}</Typography.Text>
          </span>
        )
      },
    },
    {
      title: '视频任务',
      dataIndex: 'title',
      width: 320,
      render: (_: unknown, item: YouTubeGlobalItemEntity) => {
        const isExecuting = activeVideoKeys.has(`${item.job_id}:${item.video_id}`)
        const hasBug = item.status === 'failed' || !!item.last_error
        const runState = itemRunState(item, isExecuting, queuePaused)
        return (
          <div
            style={{
              display: 'flex',
              flexDirection: 'column',
              gap: 3,
              minHeight: 76,
              padding: '8px 10px',
              borderRadius: 8,
              background: isExecuting ? 'rgba(var(--semi-green-0), 1)' : 'var(--semi-color-fill-0)',
              border: `1px solid ${isExecuting ? 'rgba(var(--semi-green-4), 1)' : 'var(--semi-color-border)'}`,
              justifyContent: 'space-between',
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              {statusDot(runState.color)}
              <Typography.Text strong ellipsis={{ showTooltip: true }} style={{ fontSize: 13 }}>
                {item.generated_title || item.source_title || item.video_id}
              </Typography.Text>
            </div>
            <Space wrap spacing={6}>
              {itemStatusTag(item.status)}
              <Tag size="small" color="blue">{getYouTubeSourceTypeLabel(item.job_source_type)}</Tag>
              <Tag
                size="small"
                color={isExecuting ? 'green' : queuePaused && PENDING_STATUSES.includes(item.status) ? 'red' : hasBug ? 'orange' : 'grey'}
              >
                {runState.label}
              </Tag>
              {hasBug ? <Tag size="small" color="red">异常检测</Tag> : null}
            </Space>
            <Typography.Text type="tertiary" size="small" ellipsis={{ showTooltip: true }}>
              来自任务：{item.job_name}
            </Typography.Text>
            <Typography.Text type="tertiary" size="small" ellipsis={{ showTooltip: true }} style={{ fontSize: 12 }}>
              入队：{formatTs(item.created_at)}
            </Typography.Text>
          </div>
        )
      },
    },
    {
      title: '入库时间',
      dataIndex: 'created_at',
      width: 180,
      render: (value: number | undefined) => formatTs(value),
    },
    {
      title: '队列',
      dataIndex: 'queue',
      width: 220,
      render: (_: unknown, item: YouTubeGlobalItemEntity) => {
        const isExecuting = activeVideoKeys.has(`${item.job_id}:${item.video_id}`)
        const queuePosition = item.queue_position
        const queueTotal = item.queue_total ?? pendingTotal
        if (isExecuting) {
          return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
              <Tag color="orange">当前执行</Tag>
              <Typography.Text type="tertiary" size="small">
                后面还有 {Math.max(queueTotal - (queuePosition ?? 1), 0)} 个待处理
              </Typography.Text>
            </div>
          )
        }
        if (typeof queuePosition === 'number') {
          return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
              <Typography.Text>排队第 {queuePosition} 个</Typography.Text>
              <Typography.Text type="tertiary" size="small">
                前方 {Math.max(queuePosition - 1, 0)} 个，后方 {Math.max(queueTotal - queuePosition, 0)} 个
              </Typography.Text>
            </div>
          )
        }
        return <Typography.Text type="tertiary">-</Typography.Text>
      },
    },
    {
      title: '错误',
      dataIndex: 'last_error',
      width: 220,
      render: (value: string | undefined) =>
        value ? (
          <Typography.Text type="danger" ellipsis={{ showTooltip: true }}>
            {value}
          </Typography.Text>
        ) : (
          <Typography.Text type="tertiary">-</Typography.Text>
        ),
    },
    {
      title: '操作',
      dataIndex: 'action',
      width: 120,
      render: (_: unknown, item: YouTubeGlobalItemEntity) => (
        <Button size="small" onClick={() => setDetailItem(item)}>
          详情
        </Button>
      ),
    },
  ]

  return (
    <>
      <Header style={{ backgroundColor: 'var(--semi-color-bg-1)' }}>
        <Nav
          style={{ border: 'none' }}
          header={
            <>
              <div
                style={{
                  backgroundColor: '#228be6',
                  borderRadius: 'var(--semi-border-radius-large)',
                  color: 'var(--semi-color-bg-0)',
                  display: 'flex',
                  padding: '6px',
                }}
              >
                <IconCloudStroked size="large" />
              </div>
              <h4 style={{ marginLeft: '12px' }}>任务列表</h4>
            </>
          }
          footer={
            <Space wrap>
              <Select
                value={status || undefined}
                onChange={value => {
                  setPage(1)
                  setStatus((value as string) || '')
                }}
                style={{ width: 180 }}
                placeholder="状态筛选"
              >
                {STATUS_OPTIONS.map(option => (
                  <Select.Option key={option.value || 'all'} value={option.value}>
                    {option.label}
                  </Select.Option>
                ))}
              </Select>
              <Button
                theme="solid"
                icon={queuePaused ? <IconPlay /> : <IconPause />}
                loading={pendingAction === 'run' || pendingAction === 'pause'}
                onClick={queuePaused ? runQueue : pauseQueue}
              >
                {queuePaused ? '开始执行' : '暂停执行'}
              </Button>
              <Button
                theme="solid"
                loading={pendingAction === 'retry'}
                onClick={retryFailed}
                disabled={failedCount === 0}
              >
                失败重试
              </Button>
              <Button icon={<IconRefresh />} onClick={() => refreshAll()}>
                刷新
              </Button>
            </Space>
          }
          mode="horizontal"
        />
      </Header>
      <Content
        style={{
          paddingLeft: 12,
          paddingRight: 12,
          backgroundColor: 'var(--semi-color-bg-0)',
        }}
      >
        <main>
          <div style={{ marginBottom: 12, display: 'flex', gap: 12, flexWrap: 'wrap' }}>
            <Tag color="grey">
              <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
                {statusDot(queueStatus.color)}
                队列状态：{queueStatus.label}
              </span>
            </Tag>
            <Tag color="green">正在执行 {activeExecutionCount} 个</Tag>
            <Tag color="cyan">待处理总数 {pendingTotal}</Tag>
            <Tag color="red">异常检测 {bugCount} 条</Tag>
            <Tag color="blue">当前列表 {currentTotal} 条</Tag>
          </div>
          {!isLoading && items.length === 0 ? (
            <Empty title="暂无视频任务" description="YouTube 同步任务采集到的视频会按顺序出现在这里" />
          ) : (
            <Table
              size="small"
              rowKey="id"
              loading={isLoading}
              columns={columns}
              dataSource={items}
              pagination={{
                currentPage: page,
                pageSize,
                total: currentTotal,
                pageSizeOpts: [20, 50, 100, 200],
                showSizeChanger: true,
                onPageChange: currentPage => setPage(currentPage),
                onPageSizeChange: nextPageSize => {
                  setPage(1)
                  setPageSize(nextPageSize)
                },
              }}
            />
          )}
        </main>
      </Content>
      <Modal
        title={detailItem ? `视频详情：${detailItem.video_id}` : '视频详情'}
        visible={!!detailItem}
        footer={null}
        onCancel={() => setDetailItem(null)}
        style={{ width: 'min(900px, 96vw)' }}
      >
        {detailItem ? (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
            <Typography.Text strong>
              {detailItem.generated_title || detailItem.source_title || detailItem.video_id}
            </Typography.Text>
            <Space wrap>
              {itemStatusTag(detailItem.status)}
              <Tag color="blue">{getYouTubeSourceTypeLabel(detailItem.job_source_type)}</Tag>
              <Tag color="grey">任务：{detailItem.job_name}</Tag>
              <Tag color="grey">视频ID：{detailItem.video_id}</Tag>
            </Space>
            <Typography.Text type="tertiary" style={{ wordBreak: 'break-all' }}>
              链接：{detailItem.video_url}
            </Typography.Text>
            <Typography.Text type="tertiary">
              发布日期：{formatUploadDate(detailItem.upload_date)} · 排队时间：{formatTs(detailItem.created_at)}
            </Typography.Text>

            <div style={{ padding: 10, borderRadius: 8, background: 'var(--semi-color-fill-0)' }}>
              <Typography.Text strong>源信息</Typography.Text>
              <div style={{ marginTop: 8, display: 'flex', flexDirection: 'column', gap: 6 }}>
                <Typography.Text type="secondary" style={{ wordBreak: 'break-word' }}>
                  源标题：{detailLogsResp?.item?.source_title || detailItem.source_title || '-'}
                </Typography.Text>
                {parseTags(detailLogsResp?.item?.source_tags || detailItem.source_tags).length > 0 ? (
                  <Space wrap>
                    {parseTags(detailLogsResp?.item?.source_tags || detailItem.source_tags).map(tag => (
                      <Tag key={`src-${detailItem.id}-${tag}`} color="grey">
                        {tag}
                      </Tag>
                    ))}
                  </Space>
                ) : (
                  <Typography.Text type="tertiary">源标签：无</Typography.Text>
                )}
              </div>
            </div>

            <div style={{ padding: 10, borderRadius: 8, background: 'var(--semi-color-fill-0)' }}>
              <Typography.Text strong>AI 生成</Typography.Text>
              <div style={{ marginTop: 8, display: 'flex', flexDirection: 'column', gap: 6 }}>
                <Typography.Text type="secondary" style={{ wordBreak: 'break-word' }}>
                  AI 标题：{detailLogsResp?.item?.generated_title || detailItem.generated_title || '-'}
                </Typography.Text>
                {detailLogsResp?.item?.generated_description || detailItem.generated_description ? (
                  <Typography.Paragraph style={{ marginBottom: 0, whiteSpace: 'pre-wrap' }}>
                    {detailLogsResp?.item?.generated_description || detailItem.generated_description}
                  </Typography.Paragraph>
                ) : (
                  <Typography.Text type="tertiary">AI 简介：无</Typography.Text>
                )}
                {parseTags(detailLogsResp?.item?.generated_tags || detailItem.generated_tags).length > 0 ? (
                  <Space wrap>
                    {parseTags(detailLogsResp?.item?.generated_tags || detailItem.generated_tags).map(tag => (
                      <Tag key={`ai-${detailItem.id}-${tag}`} color="cyan">
                        {tag}
                      </Tag>
                    ))}
                  </Space>
                ) : (
                  <Typography.Text type="tertiary">AI 标签：无</Typography.Text>
                )}
              </div>
            </div>

            <div style={{ padding: 10, borderRadius: 8, background: 'var(--semi-color-fill-0)' }}>
              <Space wrap style={{ marginBottom: 8 }}>
                <Typography.Text strong>执行日志</Typography.Text>
                <Typography.Text type="tertiary">
                  {detailLogsResp?.entries?.length ?? 0} 条
                </Typography.Text>
              </Space>
              <div
                style={{
                  maxHeight: 360,
                  overflow: 'auto',
                  display: 'flex',
                  flexDirection: 'column',
                  gap: 10,
                }}
              >
                {detailLogsLoading ? (
                  <Typography.Text type="tertiary">加载中...</Typography.Text>
                ) : (detailLogsResp?.entries?.length ?? 0) === 0 ? (
                  <Typography.Text type="tertiary">暂无日志</Typography.Text>
                ) : (
                  detailLogsResp?.entries?.map((entry, index) => (
                    <div
                      key={`${detailItem.id}-${entry.created_at}-${index}`}
                      style={{
                        padding: 10,
                        borderRadius: 8,
                        background: 'var(--semi-color-bg-1)',
                        border: '1px solid var(--semi-color-border)',
                      }}
                    >
                      <Space wrap style={{ marginBottom: 4 }}>
                        <Typography.Text type="tertiary">{formatTs(entry.created_at)}</Typography.Text>
                        <Tag color="blue">{entry.stage || '日志'}</Tag>
                      </Space>
                      <Typography.Text style={{ display: 'block', wordBreak: 'break-word' }}>
                        {entry.message}
                      </Typography.Text>
                    </div>
                  ))
                )}
              </div>
            </div>

            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
              {detailItem.last_error ? (
                <Typography.Text type="danger" style={{ wordBreak: 'break-word' }}>
                  错误：{detailItem.last_error}
                </Typography.Text>
              ) : (
                <span />
              )}
              <Popconfirm
                title="确定删除这条视频任务？"
                content="只删除当前这条视频任务，不会删除整个同步任务"
                onConfirm={() => deleteItem(detailItem)}
              >
                <Button
                  type="danger"
                  loading={pendingAction === `delete-${detailItem.id}`}
                >
                  删除这条任务
                </Button>
              </Popconfirm>
            </div>
          </div>
        ) : null}
      </Modal>
    </>
  )
}

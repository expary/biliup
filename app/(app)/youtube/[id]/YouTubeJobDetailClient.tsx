'use client'

import { useEffect, useMemo, useState } from 'react'
import Link from 'next/link'
import { usePathname } from 'next/navigation'
import useSWR from 'swr'
import { Button, Empty, Layout, List, Modal, Notification, Select, Space, TabPane, Tabs, Tag, Typography } from '@douyinfe/semi-ui'
import { IconArrowLeft, IconRefresh } from '@douyinfe/semi-icons'
import {
  fetcher,
  getYouTubeSourceTypeLabel,
  post,
  YouTubeItemEntity,
  YouTubeItemListResponse,
  YouTubeItemLogsResponse,
  YouTubeJobEntity,
  YouTubeJobListResponse,
  YouTubeJobLogsResponse,
} from '@/app/lib/api-youtube'

const PENDING_STATUSES = ['discovered', 'meta_ready', 'downloaded', 'transcoded', 'ready_upload']

function itemStatusTag(status: string) {
  switch (status) {
    case 'uploaded':
      return <Tag color="green">已上传</Tag>
    case 'discovered':
      return <Tag color="orange">已发现</Tag>
    case 'failed':
      return <Tag color="red">失败</Tag>
    case 'skipped_duplicate':
      return <Tag color="grey">重复已跳过</Tag>
    case 'ready_upload':
      return <Tag color="cyan">待上传</Tag>
    case 'transcoded':
      return <Tag color="violet">已转码</Tag>
    case 'downloaded':
      return <Tag color="blue">已下载</Tag>
    case 'meta_ready':
      return <Tag color="lime">元数据就绪</Tag>
    default:
      return <Tag color="orange">采集中</Tag>
  }
}

function jobStatusTag(status?: string) {
  switch (status) {
    case 'running':
      return <Tag color="red">运行中</Tag>
    case 'paused':
      return <Tag color="grey">已暂停</Tag>
    case 'error':
      return <Tag color="orange">错误</Tag>
    default:
      return <Tag color="green">空闲</Tag>
  }
}

function itemStatusAccent(status: string) {
  switch (status) {
    case 'uploaded':
      return 'rgba(var(--semi-green-5), 1)'
    case 'failed':
      return 'rgba(var(--semi-red-5), 1)'
    case 'skipped_duplicate':
      return 'rgba(var(--semi-grey-5), 1)'
    case 'ready_upload':
      return 'rgba(var(--semi-cyan-5), 1)'
    case 'transcoded':
      return 'rgba(var(--semi-purple-5), 1)'
    case 'downloaded':
      return 'rgba(var(--semi-blue-5), 1)'
    case 'meta_ready':
      return 'rgba(var(--semi-lime-5), 1)'
    case 'discovered':
      return 'rgba(var(--semi-orange-5), 1)'
    default:
      return 'rgba(var(--semi-grey-5), 1)'
  }
}

function parseJobIdFromPathname(pathname: string): number | null {
  const match = pathname.match(/^\/youtube\/(\d+)(?:\/|$)/)
  if (!match) return null
  const value = Number(match[1])
  if (!Number.isFinite(value) || value <= 0) return null
  return value
}

export default function YouTubeJobDetailClient() {
  const { Header, Content } = Layout
  const pathname = usePathname()
  const [jobId, setJobId] = useState<number | null>(null)
  const [status, setStatus] = useState<string | undefined>(undefined)

  useEffect(() => {
    const path = typeof window !== 'undefined' ? window.location.pathname : pathname
    setJobId(parseJobIdFromPathname(path))
  }, [pathname])

  const itemsUrl = useMemo(() => {
    if (!jobId) {
      return null
    }
    const search = new URLSearchParams()
    if (status) search.set('status', status)
    search.set('page', '1')
    search.set('page_size', '1000')
    return `/v1/youtube/jobs/${jobId}/items?${search.toString()}`
  }, [jobId, status])
  const allItemsUrl = useMemo(() => {
    if (!jobId) {
      return null
    }
    return `/v1/youtube/jobs/${jobId}/items?page=1&page_size=1000`
  }, [jobId])

  const { data: jobsResp, mutate: mutateJobs } = useSWR<YouTubeJobListResponse>('/v1/youtube/jobs', fetcher, {
    refreshInterval: 10_000,
  })
  const { data: itemsResp, mutate: mutateItems, isLoading } = useSWR<YouTubeItemListResponse>(itemsUrl, fetcher, {
    refreshInterval: 10_000,
  })
  const { data: allItemsResp, mutate: mutateAllItems } = useSWR<YouTubeItemListResponse>(allItemsUrl, fetcher, {
    refreshInterval: 10_000,
  })
  const { data: logsResp, mutate: mutateLogs } = useSWR<YouTubeJobLogsResponse>(jobId ? `/v1/youtube/jobs/${jobId}/logs` : null, fetcher, {
    refreshInterval: 10_000,
  })

  const [activeItem, setActiveItem] = useState<YouTubeItemEntity | null>(null)
  const itemLogsUrl = useMemo(() => (activeItem ? `/v1/youtube/items/${activeItem.id}/logs` : null), [activeItem])
  const { data: itemLogsResp, isLoading: isItemLogsLoading } = useSWR<YouTubeItemLogsResponse>(itemLogsUrl, fetcher, {
    refreshInterval: 10_000,
  })
  const itemLogsItem = useMemo(() => {
    if (!activeItem) return null
    if (!itemLogsResp?.item) return null
    return itemLogsResp.item.id === activeItem.id ? itemLogsResp.item : null
  }, [activeItem, itemLogsResp?.item])
  const itemLogEntries = useMemo(() => {
    if (!activeItem) return []
    if (!itemLogsResp) return []
    if (itemLogsResp.item?.id !== activeItem.id) return []
    return itemLogsResp.entries ?? []
  }, [activeItem, itemLogsResp])

  const currentJob = jobsResp?.jobs.find((job: YouTubeJobEntity) => job.id === jobId)
  const allItems = allItemsResp?.items ?? []
  const failedItems = allItems.filter(item => item.status === 'failed')
  const pendingItems = allItems.filter(item => PENDING_STATUSES.includes(item.status))
  const uploadedCount = allItems.filter(item => item.status === 'uploaded').length
  const skippedCount = allItems.filter(item => item.status === 'skipped_duplicate').length
  const failedCount = allItems.filter(item => item.status === 'failed').length
  const unuploadedCount = allItems.length - uploadedCount - skippedCount

  const [logStage, setLogStage] = useState<string | undefined>(undefined)
  const logEntries = useMemo(() => logsResp?.entries ?? [], [logsResp?.entries])
  const logStages = useMemo(() => Array.from(new Set(logEntries.map(entry => entry.stage).filter(Boolean))).sort(), [logEntries])
  const filteredLogs = useMemo(() => (logStage ? logEntries.filter(entry => entry.stage === logStage) : logEntries), [logEntries, logStage])

  const runNow = async () => {
    if (!jobId) return
    try {
      await post(`/v1/youtube/jobs/${jobId}/run`)
      await Promise.all([mutateJobs(), mutateItems(), mutateAllItems(), mutateLogs()])
    } catch (error: any) {
      Notification.error({ title: '触发失败', content: error.message, position: 'top' })
    }
  }

  const retryItem = async (item: YouTubeItemEntity) => {
    try {
      await post(`/v1/youtube/items/${item.id}/retry`)
      await Promise.all([mutateJobs(), mutateItems(), mutateAllItems(), mutateLogs()])
    } catch (error: any) {
      Notification.error({ title: '重试失败', content: error.message, position: 'top' })
    }
  }

  const openItemLogs = (item: YouTubeItemEntity) => {
    setActiveItem(item)
  }

  const retryFailedBatch = async () => {
    if (!jobId) return
    if (failedItems.length === 0) {
      Notification.info({ title: '没有失败项', content: '当前列表里没有失败视频', position: 'top' })
      return
    }
    try {
      const result = await post<{ ok: boolean; retried_count: number }>(`/v1/youtube/jobs/${jobId}/retry_failed`)
      Notification.success({
        title: '批量重试已触发',
        content: `已触发 ${result.retried_count || failedItems.length} 个失败项重试`,
        position: 'top',
      })
      await Promise.all([mutateJobs(), mutateItems(), mutateAllItems(), mutateLogs()])
    } catch (error: any) {
      Notification.error({ title: '批量重试失败', content: error.message, position: 'top' })
    }
  }

  return (
    <>
      <Header
        style={{
          backgroundColor: 'var(--semi-color-bg-1)',
          position: 'sticky',
          top: 0,
          zIndex: 10,
        }}
      >
        <nav
          style={{
            display: 'flex',
            paddingLeft: '25px',
            paddingRight: '25px',
            alignItems: 'center',
            justifyContent: 'space-between',
            flexWrap: 'wrap',
            gap: 10,
            boxShadow: '0 1px 2px 0 rgb(0 0 0 / 0.05)',
          }}
        >
          <Space wrap>
            <Link href="/youtube">
              <Button icon={<IconArrowLeft />}>返回任务列表</Button>
            </Link>
            <Typography.Title heading={5} style={{ margin: 0 }}>
              {currentJob?.name ?? (jobId ? `任务 #${jobId}` : '任务详情')}
            </Typography.Title>
          </Space>
          <Space wrap>
            <Button icon={<IconRefresh />} onClick={() => Promise.all([mutateItems(), mutateAllItems(), mutateLogs()])}>
              刷新
            </Button>
            <Button onClick={retryFailedBatch} disabled={failedItems.length === 0}>
              失败重试（{failedItems.length}）
            </Button>
            <Button theme="solid" onClick={runNow}>
              立即同步
            </Button>
          </Space>
        </nav>
      </Header>

      <Content style={{ padding: 24, backgroundColor: 'var(--semi-color-bg-0)' }}>
        {!jobId ? (
          <Empty title="加载中" />
        ) : (
          <>
            <div className="yt-summary-grid" style={{ marginBottom: 16 }}>
              <div className="yt-panel">
                <div className="yt-panel-header">
                  <Typography.Text strong>任务信息</Typography.Text>
                </div>
                <div className="yt-panel-body">
                  <Typography.Text type="tertiary" style={{ display: 'block', wordBreak: 'break-all' }}>
                    源地址：{currentJob?.source_url ?? '-'}
                  </Typography.Text>
                  <div className="yt-meta-row">
                    <Tag color="blue">{getYouTubeSourceTypeLabel(currentJob?.source_type)}</Tag>
                    <Tag color={currentJob?.enabled === 1 ? 'green' : 'grey'}>
                      {currentJob?.enabled === 1 ? '启用' : '禁用'}
                    </Tag>
                    {jobStatusTag(currentJob?.status)}
                    {typeof currentJob?.sync_interval_seconds === 'number' ? (
                      <Tag color="grey">间隔 {currentJob.sync_interval_seconds}s</Tag>
                    ) : null}
                    {currentJob?.auto_publish === 1 ? <Tag color="grey">自动发布</Tag> : <Tag color="grey">手动发布</Tag>}
                  </div>
                  {currentJob?.last_error ? (
                    <Typography.Text type="danger" style={{ display: 'block', marginTop: 10, wordBreak: 'break-word' }}>
                      最近错误：{currentJob.last_error}
                    </Typography.Text>
                  ) : null}
                </div>
              </div>

              <div className="yt-panel">
                <div className="yt-panel-header">
                  <Typography.Text strong>状态统计</Typography.Text>
                </div>
                <div className="yt-panel-body">
                  <div className="yt-stats-grid">
                    <div className="yt-stat">
                      <div className="yt-stat-label">已上传</div>
                      <div className="yt-stat-value">{uploadedCount}</div>
                    </div>
                    <div className="yt-stat">
                      <div className="yt-stat-label">待处理</div>
                      <div className="yt-stat-value">{pendingItems.length}</div>
                    </div>
                    <div className="yt-stat">
                      <div className="yt-stat-label">失败</div>
                      <div className="yt-stat-value">{failedCount}</div>
                    </div>
                    <div className="yt-stat">
                      <div className="yt-stat-label">未发布</div>
                      <div className="yt-stat-value">{unuploadedCount}</div>
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <Tabs type="line" defaultActiveKey="pending" keepDOM={false}>
              <TabPane itemKey="pending" tab={`待处理（${pendingItems.length}）`}>
                <div className="yt-panel">
                  <div className="yt-panel-header">
                    <Typography.Text strong>待处理视频</Typography.Text>
                    <Typography.Text type="tertiary" style={{ marginLeft: 8 }}>
                      （展示前 100 条）
                    </Typography.Text>
                  </div>
                  <div className="yt-panel-body" style={{ background: 'var(--semi-color-fill-0)' }}>
                    {pendingItems.length === 0 ? (
                      <Typography.Text type="tertiary">当前没有待处理视频</Typography.Text>
                    ) : (
                      <List
                        size="small"
                        split={false}
                        dataSource={pendingItems.slice(0, 100)}
                        renderItem={item => (
                          <List.Item style={{ padding: 0, marginBottom: 10 }}>
                            <div className="yt-card" style={{ padding: 12, borderLeft: `4px solid ${itemStatusAccent(item.status)}` }}>
                              <div className="yt-card-row">
                                <Typography.Text ellipsis={{ showTooltip: true }} className="yt-card-title">
                                  {item.generated_title || item.source_title || item.video_id}
                                </Typography.Text>
                                <Space wrap>
                                  {itemStatusTag(item.status)}
                                  <Button size="small" onClick={() => openItemLogs(item)}>
                                    日志
                                  </Button>
                                </Space>
                              </div>
                              <div className="yt-meta-row">
                                <Tag color="grey">vid={item.video_id}</Tag>
                                {formatUploadDate(item.upload_date) ? <Tag color="grey">日期 {formatUploadDate(item.upload_date)}</Tag> : null}
                                {formatDuration(item.duration_sec) ? <Tag color="grey">时长 {formatDuration(item.duration_sec)}</Tag> : null}
                              </div>
                            </div>
                          </List.Item>
                        )}
                      />
                    )}
                  </div>
                </div>
              </TabPane>

              <TabPane itemKey="items" tab={`全部（${itemsResp?.total ?? allItems.length}）`}>
                <div
                  style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center',
                    flexWrap: 'wrap',
                    gap: 12,
                    marginBottom: 12,
                  }}
                >
                  <Space wrap>
                    <Typography.Text type="tertiary">按状态筛选</Typography.Text>
                    <Select
                      placeholder="全部"
                      style={{ width: '100%', maxWidth: 240, minWidth: 180 }}
                      onChange={value => setStatus((value as string) || undefined)}
                      showClear
                    >
                      <Select.Option value="discovered">已发现</Select.Option>
                      <Select.Option value="meta_ready">元数据就绪</Select.Option>
                      <Select.Option value="downloaded">已下载</Select.Option>
                      <Select.Option value="transcoded">已转码</Select.Option>
                      <Select.Option value="ready_upload">待上传</Select.Option>
                      <Select.Option value="uploaded">已上传</Select.Option>
                      <Select.Option value="skipped_duplicate">重复已跳过</Select.Option>
                      <Select.Option value="failed">失败</Select.Option>
                    </Select>
                  </Space>
                </div>

                {!isLoading && (itemsResp?.items.length ?? 0) === 0 ? (
                  <Empty title="暂无数据" />
                ) : (
                  <List
                    split={false}
                    dataSource={itemsResp?.items ?? []}
                    renderItem={item => (
                      <List.Item style={{ padding: 0, marginBottom: 12 }}>
                        <div className="yt-card" style={{ padding: 14, borderLeft: `4px solid ${itemStatusAccent(item.status)}` }}>
                          <div className="yt-card-row">
                            <div style={{ flex: 1, minWidth: 0 }}>
                              <Typography.Text strong ellipsis={{ showTooltip: true }} className="yt-card-title">
                                {item.generated_title || item.source_title || item.video_id}
                              </Typography.Text>
                              <Typography.Text type="tertiary" style={{ display: 'block' }} ellipsis={{ showTooltip: true }}>
                                {item.video_url}
                              </Typography.Text>
                            </div>
                            <Space wrap style={{ justifyContent: 'flex-end' }}>
                              {itemStatusTag(item.status)}
                              {item.status === 'failed' ? <Button onClick={() => retryItem(item)}>重试</Button> : null}
                              <Button onClick={() => openItemLogs(item)}>日志</Button>
                            </Space>
                          </div>

                          <div className="yt-meta-row">
                            <Tag color="grey">vid={item.video_id}</Tag>
                            {formatUploadDate(item.upload_date) ? <Tag color="grey">日期 {formatUploadDate(item.upload_date)}</Tag> : null}
                            {formatDuration(item.duration_sec) ? <Tag color="grey">时长 {formatDuration(item.duration_sec)}</Tag> : null}
                            <Tag color="grey">标题 {(item.generated_title || item.source_title || '').length}/80</Tag>
                          </div>

                          {item.generated_description ? (
                            <Typography.Paragraph
                              type="secondary"
                              style={{ marginTop: 8, marginBottom: 8 }}
                              ellipsis={{ rows: 3, showTooltip: true }}
                            >
                              {item.generated_description}
                            </Typography.Paragraph>
                          ) : null}

                          {parseTags(item.generated_tags).length > 0 ? (
                            <Space wrap style={{ marginBottom: 8 }}>
                              {parseTags(item.generated_tags).map(tag => (
                                <Tag key={`${item.id}-${tag}`} color="cyan">
                                  {tag}
                                </Tag>
                              ))}
                            </Space>
                          ) : null}

                          {item.bili_bvid ? (
                            <Typography.Text type="success">
                              投稿结果：{item.bili_bvid} / aid={item.bili_aid}
                            </Typography.Text>
                          ) : null}
                          {item.last_error ? (
                            <Typography.Text type="danger" style={{ display: 'block' }}>
                              错误：{item.last_error}
                            </Typography.Text>
                          ) : null}
                        </div>
                      </List.Item>
                    )}
                  />
                )}
              </TabPane>

              <TabPane itemKey="logs" tab={`日志（${(logsResp?.logs ?? []).length}）`}>
                <div
                  style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center',
                    flexWrap: 'wrap',
                    gap: 12,
                    marginBottom: 12,
                  }}
                >
                  <Space wrap>
                    <Typography.Text type="tertiary">按阶段筛选</Typography.Text>
                    <Select
                      placeholder="全部"
                      style={{ width: '100%', maxWidth: 240, minWidth: 180 }}
                      onChange={value => setLogStage((value as string) || undefined)}
                      showClear
                    >
                      {logStages.map(stage => (
                        <Select.Option key={stage} value={stage}>
                          {stage}
                        </Select.Option>
                      ))}
                    </Select>
                    <Typography.Text type="tertiary">共 {filteredLogs.length} 条</Typography.Text>
                  </Space>
                </div>

                <div className="yt-panel" style={{ maxHeight: 520, overflow: 'auto', background: 'var(--semi-color-fill-0)' }}>
                  <div className="yt-panel-body" style={{ background: 'var(--semi-color-fill-0)' }}>
                    {filteredLogs.length === 0 ? (
                      <Typography.Text type="tertiary">暂无日志</Typography.Text>
                    ) : (
                      <List
                        size="small"
                        split={false}
                        dataSource={filteredLogs}
                        renderItem={entry => (
                          <List.Item style={{ padding: 0, marginBottom: 10 }}>
                            <div className="yt-log-entry" style={{ borderLeftColor: stageAccent(entry.stage) }}>
                              <Space wrap style={{ marginBottom: 4 }}>
                                <Typography.Text type="tertiary">{formatTs(entry.created_at)}</Typography.Text>
                                {stageTag(entry.stage)}
                                {entry.video_id ? <Tag color="grey">vid={entry.video_id}</Tag> : null}
                              </Space>
                              <Typography.Text style={{ display: 'block', wordBreak: 'break-word' }}>{entry.message}</Typography.Text>
                            </div>
                          </List.Item>
                        )}
                      />
                    )}
                  </div>
                </div>
              </TabPane>
            </Tabs>
          </>
        )}
      </Content>

      <Modal
        title={activeItem ? `视频日志：${activeItem.video_id}` : '视频日志'}
        visible={!!activeItem}
        onCancel={() => setActiveItem(null)}
        footer={null}
        style={{ width: 960, maxWidth: '96vw' }}
      >
        {activeItem ? (
          <div>
            <Typography.Text type="tertiary" style={{ display: 'block', wordBreak: 'break-all' }}>
              链接：{activeItem.video_url}
            </Typography.Text>

            <div className="yt-panel" style={{ marginTop: 12 }}>
              <div className="yt-panel-header">
                <Typography.Text strong>源信息</Typography.Text>
              </div>
              <div className="yt-panel-body">
                <Typography.Text type="secondary" style={{ display: 'block', marginBottom: 6, wordBreak: 'break-word' }}>
                  源标题：{itemLogsItem?.source_title || activeItem.source_title || '-'}
                </Typography.Text>
                {parseTags(itemLogsItem?.source_tags || activeItem.source_tags).length > 0 ? (
                  <Space wrap>
                    {parseTags(itemLogsItem?.source_tags || activeItem.source_tags).map(tag => (
                      <Tag key={`src-${activeItem.id}-${tag}`} color="grey">
                        {tag}
                      </Tag>
                    ))}
                  </Space>
                ) : (
                  <Typography.Text type="tertiary">源标签：无</Typography.Text>
                )}
              </div>
            </div>

            <div className="yt-panel" style={{ marginTop: 12 }}>
              <div className="yt-panel-header">
                <Typography.Text strong>AI 生成</Typography.Text>
              </div>
              <div className="yt-panel-body">
                <Typography.Text type="secondary" style={{ display: 'block', marginBottom: 6, wordBreak: 'break-word' }}>
                  AI 标题：{itemLogsItem?.generated_title || activeItem.generated_title || '-'}（
                  {(itemLogsItem?.generated_title || activeItem.generated_title || '').length}/80）
                </Typography.Text>
                {parseTags(itemLogsItem?.generated_tags || activeItem.generated_tags).length > 0 ? (
                  <Space wrap>
                    {parseTags(itemLogsItem?.generated_tags || activeItem.generated_tags).map(tag => (
                      <Tag key={`ai-${activeItem.id}-${tag}`} color="cyan">
                        {tag}
                      </Tag>
                    ))}
                  </Space>
                ) : (
                  <Typography.Text type="tertiary">AI 标签：无</Typography.Text>
                )}
              </div>
            </div>

            <div style={{ marginTop: 12 }}>
              <Space wrap style={{ marginBottom: 8 }}>
                <Typography.Text strong>执行日志</Typography.Text>
                <Typography.Text type="tertiary">
                  {itemLogEntries.length} 条
                </Typography.Text>
              </Space>
              <div
                style={{
                  maxHeight: 420,
                  overflow: 'auto',
                  padding: 12,
                  border: '1px solid var(--semi-color-border)',
                  borderRadius: 8,
                  background: 'var(--semi-color-fill-0)',
                }}
              >
                {isItemLogsLoading ? (
                  <Typography.Text type="tertiary">加载中...</Typography.Text>
                ) : itemLogEntries.length === 0 ? (
                  <Typography.Text type="tertiary">暂无日志</Typography.Text>
                ) : (
                  <List
                    size="small"
                    split={false}
                    dataSource={itemLogEntries}
                    renderItem={entry => (
                      <List.Item style={{ padding: 0, marginBottom: 10 }}>
                        <div className="yt-log-entry" style={{ borderLeftColor: stageAccent(entry.stage) }}>
                          <Space wrap style={{ marginBottom: 4 }}>
                            <Typography.Text type="tertiary">{formatTs(entry.created_at)}</Typography.Text>
                            {stageTag(entry.stage)}
                          </Space>
                          <Typography.Text style={{ display: 'block', wordBreak: 'break-word' }}>{entry.message}</Typography.Text>
                        </div>
                      </List.Item>
                    )}
                  />
                )}
              </div>
            </div>
          </div>
        ) : null}
      </Modal>
    </>
  )
}

function parseTags(jsonTags?: string): string[] {
  if (!jsonTags) {
    return []
  }
  try {
    const value = JSON.parse(jsonTags)
    if (!Array.isArray(value)) {
      return []
    }
    return value.filter(item => typeof item === 'string')
  } catch {
    return []
  }
}

function stageTag(stage: string) {
  switch (stage) {
    case '任务':
      return <Tag color="grey">{stage}</Tag>
    case '采集':
      return <Tag color="blue">{stage}</Tag>
    case '元数据':
      return <Tag color="lime">{stage}</Tag>
    case 'AI':
      return <Tag color="cyan">{stage}</Tag>
    case '下载':
      return <Tag color="blue">{stage}</Tag>
    case '探测':
      return <Tag color="grey">{stage}</Tag>
    case '转码':
      return <Tag color="violet">{stage}</Tag>
    case '处理':
      return <Tag color="purple">{stage}</Tag>
    case '封面':
      return <Tag color="orange">{stage}</Tag>
    case '上传':
      return <Tag color="green">{stage}</Tag>
    case '投稿':
      return <Tag color="green">{stage}</Tag>
    case '清理':
      return <Tag color="grey">{stage}</Tag>
    case '错误':
      return <Tag color="red">{stage}</Tag>
    case '跳过':
      return <Tag color="grey">{stage}</Tag>
    default:
      return <Tag color="grey">{stage || '日志'}</Tag>
  }
}

function stageAccent(stage: string) {
  switch (stage) {
    case '错误':
      return 'rgba(var(--semi-red-5), 1)'
    case '上传':
    case '投稿':
      return 'rgba(var(--semi-green-5), 1)'
    case '转码':
      return 'rgba(var(--semi-purple-5), 1)'
    case '处理':
      return 'rgba(var(--semi-purple-5), 1)'
    case '下载':
    case '采集':
      return 'rgba(var(--semi-blue-5), 1)'
    case 'AI':
      return 'rgba(var(--semi-cyan-5), 1)'
    case '元数据':
      return 'rgba(var(--semi-lime-5), 1)'
    case '封面':
      return 'rgba(var(--semi-orange-5), 1)'
    case '清理':
    case '任务':
    case '跳过':
    default:
      return 'rgba(var(--semi-grey-5), 1)'
  }
}

function formatUploadDate(uploadDate?: string) {
  if (!uploadDate) return null
  const value = String(uploadDate).trim()
  if (!/^\d{8}$/.test(value)) return value
  return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`
}

function formatDuration(durationSec?: number) {
  if (!durationSec || !Number.isFinite(durationSec) || durationSec <= 0) return null
  const total = Math.floor(durationSec)
  const h = Math.floor(total / 3600)
  const m = Math.floor((total % 3600) / 60)
  const s = total % 60
  const pad2 = (n: number) => String(n).padStart(2, '0')
  return h > 0 ? `${h}:${pad2(m)}:${pad2(s)}` : `${m}:${pad2(s)}`
}

function formatTs(tsSeconds: number) {
  if (!tsSeconds) return '-'
  try {
    return new Date(tsSeconds * 1000).toLocaleString(undefined, { hour12: false })
  } catch {
    return String(tsSeconds)
  }
}

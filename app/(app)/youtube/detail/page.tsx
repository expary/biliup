'use client'

import { Suspense, useMemo, useState } from 'react'
import Link from 'next/link'
import { useSearchParams } from 'next/navigation'
import useSWR from 'swr'
import {
  Button,
  Empty,
  Layout,
  List,
  Notification,
  Select,
  Space,
  Tag,
  Typography,
} from '@douyinfe/semi-ui'
import { IconArrowLeft, IconRefresh } from '@douyinfe/semi-icons'
import {
  fetcher,
  getYouTubeSourceTypeLabel,
  post,
  YouTubeItemEntity,
  YouTubeItemListResponse,
  YouTubeJobEntity,
  YouTubeJobListResponse,
} from '@/app/lib/api-youtube'

const PENDING_STATUSES = ['discovered', 'meta_ready', 'downloaded', 'transcoded', 'ready_upload']

function itemStatusTag(status: string) {
  switch (status) {
    case 'uploaded':
      return <Tag color="green">已上传</Tag>
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

function YouTubeJobDetailContent() {
  const searchParams = useSearchParams()
  const jobId = Number(searchParams.get('id') || 0)
  const { Header, Content } = Layout
  const [status, setStatus] = useState<string | undefined>(undefined)

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

  const {
    data: jobsResp,
    mutate: mutateJobs,
  } = useSWR<YouTubeJobListResponse>('/v1/youtube/jobs', fetcher, { refreshInterval: 10_000 })
  const {
    data: itemsResp,
    mutate: mutateItems,
    isLoading,
  } = useSWR<YouTubeItemListResponse>(itemsUrl, fetcher, { refreshInterval: 10_000 })
  const { data: allItemsResp, mutate: mutateAllItems } = useSWR<YouTubeItemListResponse>(
    allItemsUrl,
    fetcher,
    { refreshInterval: 10_000 }
  )
  const { data: logsResp, mutate: mutateLogs } = useSWR<{ job_id: number; logs: string[] }>(
    jobId ? `/v1/youtube/jobs/${jobId}/logs` : null,
    fetcher,
    { refreshInterval: 10_000 }
  )

  const currentJob = jobsResp?.jobs.find((job: YouTubeJobEntity) => job.id === jobId)
  const failedItems = (itemsResp?.items ?? []).filter(item => item.status === 'failed')
  const allItems = allItemsResp?.items ?? []
  const pendingItems = allItems.filter(item => PENDING_STATUSES.includes(item.status))
  const uploadedCount = allItems.filter(item => item.status === 'uploaded').length
  const skippedCount = allItems.filter(item => item.status === 'skipped_duplicate').length
  const failedCount = allItems.filter(item => item.status === 'failed').length
  const unuploadedCount = allItems.length - uploadedCount - skippedCount

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

  const retryFailedBatch = async () => {
    if (failedItems.length === 0) {
      Notification.info({ title: '没有失败项', content: '当前列表里没有失败视频', position: 'top' })
      return
    }
    try {
      await Promise.all(failedItems.map(item => post(`/v1/youtube/items/${item.id}/retry`)))
      Notification.success({
        title: '批量重试已触发',
        content: `已触发 ${failedItems.length} 个失败项重试`,
        position: 'top',
      })
      await Promise.all([mutateJobs(), mutateItems(), mutateAllItems(), mutateLogs()])
    } catch (error: any) {
      Notification.error({ title: '批量重试失败', content: error.message, position: 'top' })
    }
  }

  return (
    <>
      <Header style={{ backgroundColor: 'var(--semi-color-bg-1)' }}>
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
            <Button
              icon={<IconRefresh />}
              onClick={() => Promise.all([mutateItems(), mutateAllItems(), mutateLogs()])}
            >
              刷新
            </Button>
            <Button onClick={retryFailedBatch}>批量重试失败项</Button>
            <Button theme="solid" onClick={runNow}>
              立即同步
            </Button>
          </Space>
        </nav>
      </Header>

      <Content style={{ padding: 24, backgroundColor: 'var(--semi-color-bg-0)' }}>
        {!jobId ? (
          <Empty title="缺少任务 ID" />
        ) : (
          <>
            <div
              style={{
                marginBottom: 16,
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'flex-start',
                flexWrap: 'wrap',
                gap: 16,
              }}
            >
              <Space wrap style={{ maxWidth: '100%' }}>
                <Typography.Text type="tertiary" style={{ wordBreak: 'break-all' }}>
                  源地址：{currentJob?.source_url}
                </Typography.Text>
                <Tag color="blue">{getYouTubeSourceTypeLabel(currentJob?.source_type)}</Tag>
                <Tag color={currentJob?.enabled === 1 ? 'green' : 'grey'}>
                  {currentJob?.enabled === 1 ? '启用' : '禁用'}
                </Tag>
              </Space>
              <Space wrap style={{ maxWidth: '100%' }}>
                <Tag color="green">已上传 {uploadedCount}</Tag>
                <Tag color="orange">待处理 {pendingItems.length}</Tag>
                <Tag color="red">失败 {failedCount}</Tag>
                <Tag color="grey">未发布 {unuploadedCount}</Tag>
                <Select
                  placeholder="按状态筛选"
                  style={{ width: '100%', maxWidth: 220, minWidth: 180 }}
                  onChange={value => setStatus((value as string) || undefined)}
                  showClear
                >
                  <Select.Option value="discovered">discovered</Select.Option>
                  <Select.Option value="meta_ready">meta_ready</Select.Option>
                  <Select.Option value="downloaded">downloaded</Select.Option>
                  <Select.Option value="transcoded">transcoded</Select.Option>
                  <Select.Option value="ready_upload">ready_upload</Select.Option>
                  <Select.Option value="uploaded">uploaded</Select.Option>
                  <Select.Option value="skipped_duplicate">skipped_duplicate</Select.Option>
                  <Select.Option value="failed">failed</Select.Option>
                </Select>
              </Space>
            </div>

            <div
              style={{
                marginBottom: 16,
                padding: 12,
                border: '1px solid var(--semi-color-border)',
                borderRadius: 8,
                backgroundColor: 'var(--semi-color-fill-0)',
              }}
            >
              <Typography.Text strong style={{ display: 'block', marginBottom: 8 }}>
                待处理视频列表（最多展示 1000 条）
              </Typography.Text>
              {pendingItems.length === 0 ? (
                <Typography.Text type="tertiary">当前没有待处理视频</Typography.Text>
              ) : (
                <List
                  size="small"
                  dataSource={pendingItems.slice(0, 100)}
                  renderItem={item => (
                    <List.Item style={{ paddingLeft: 0, paddingRight: 0 }}>
                      <Space style={{ display: 'flex', justifyContent: 'space-between', width: '100%' }}>
                        <Typography.Text style={{ wordBreak: 'break-word' }}>
                          {item.generated_title || item.source_title || item.video_id}
                        </Typography.Text>
                        {itemStatusTag(item.status)}
                      </Space>
                    </List.Item>
                  )}
                />
              )}
            </div>

            {!isLoading && (itemsResp?.items.length ?? 0) === 0 ? (
              <Empty title="暂无数据" />
            ) : (
              <List
                dataSource={itemsResp?.items ?? []}
                renderItem={item => (
                  <List.Item
                    style={{
                      border: '1px solid var(--semi-color-border)',
                      borderRadius: 8,
                      marginBottom: 10,
                      padding: 12,
                    }}
                  >
                    <div style={{ width: '100%' }}>
                      <div
                        style={{
                          display: 'flex',
                          justifyContent: 'space-between',
                          alignItems: 'flex-start',
                          flexWrap: 'wrap',
                          gap: 12,
                        }}
                      >
                        <Typography.Text
                          strong
                          style={{
                            flex: 1,
                            minWidth: 0,
                            wordBreak: 'break-word',
                          }}
                        >
                          {item.generated_title || item.source_title || item.video_id}
                        </Typography.Text>
                        <Space wrap>
                          {itemStatusTag(item.status)}
                          {item.status === 'failed' ? (
                            <Button onClick={() => retryItem(item)}>重试</Button>
                          ) : null}
                        </Space>
                      </div>
                      <Typography.Text type="tertiary" style={{ display: 'block', wordBreak: 'break-all' }}>
                        {item.video_url}
                      </Typography.Text>
                      <div style={{ marginTop: 8 }}>
                        <Typography.Text type="secondary" style={{ display: 'block' }}>
                          标题长度：{(item.generated_title || item.source_title || '').length}/80
                        </Typography.Text>
                        {item.generated_description ? (
                          <Typography.Paragraph
                            type="secondary"
                            style={{ marginTop: 6, marginBottom: 6 }}
                            ellipsis={{ rows: 3, showTooltip: true }}
                          >
                            {item.generated_description}
                          </Typography.Paragraph>
                        ) : null}
                        {parseTags(item.generated_tags).length > 0 ? (
                          <Space wrap style={{ marginBottom: 6 }}>
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
                    </div>
                  </List.Item>
                )}
              />
            )}

            <div style={{ marginTop: 24 }}>
              <Typography.Title heading={6}>执行日志</Typography.Title>
              <pre
                style={{
                  maxHeight: 320,
                  overflow: 'auto',
                  padding: 12,
                  border: '1px solid var(--semi-color-border)',
                  borderRadius: 8,
                  background: 'var(--semi-color-fill-0)',
                  whiteSpace: 'pre-wrap',
                }}
              >
                {(logsResp?.logs ?? []).join('\n') || '暂无日志'}
              </pre>
            </div>
          </>
        )}
      </Content>
    </>
  )
}

export default function YouTubeJobDetailPage() {
  return (
    <Suspense>
      <YouTubeJobDetailContent />
    </Suspense>
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

'use client'

import { useMemo, useState } from 'react'
import Link from 'next/link'
import useSWR from 'swr'
import {
  Button,
  Card,
  Empty,
  Form,
  Layout,
  List,
  Modal,
  Notification,
  Popconfirm,
  Select,
  Space,
  Tag,
  Typography,
} from '@douyinfe/semi-ui'
import {
  IconCloudStroked,
  IconDeleteStroked,
  IconEdit2Stroked,
  IconPause,
  IconPlay,
  IconPlusCircle,
  IconRefresh,
} from '@douyinfe/semi-icons'
import {
  fetcher as youtubeFetcher,
  del as youtubeDelete,
  post as youtubePost,
  put as youtubePut,
  getYouTubeSourceTypeLabel,
  YouTubeJobEntity,
  YouTubeJobListResponse,
  YouTubeSourceType,
  YOUTUBE_SOURCE_TYPE_OPTIONS,
} from '@/app/lib/api-youtube'
import { fetcher as commonFetcher, StudioEntity } from '@/app/lib/api-streamer'

type JobFormData = {
  id?: number
  name: string
  source_url: string
  source_type: YouTubeSourceType
  upload_streamer_id: number | null
  sync_interval_seconds: number
  auto_publish: boolean
  enabled: boolean
}

const defaultFormData: JobFormData = {
  name: '',
  source_url: '',
  source_type: 'channel',
  upload_streamer_id: null,
  sync_interval_seconds: 1800,
  auto_publish: true,
  enabled: true,
}

function statusTag(status: string) {
  switch (status) {
    case 'running':
      return <Tag color="red">运行中</Tag>
    case 'paused':
      return <Tag color="pink">已暂停</Tag>
    case 'error':
      return <Tag color="orange">错误</Tag>
    default:
      return <Tag color="green">空闲</Tag>
  }
}

export default function YouTubeJobsPage() {
  const { Header, Content } = Layout
  const [visible, setVisible] = useState(false)
  const [formData, setFormData] = useState<JobFormData>(defaultFormData)

  const {
    data: jobsResp,
    mutate,
    isLoading,
  } = useSWR<YouTubeJobListResponse>('/v1/youtube/jobs', youtubeFetcher, {
    refreshInterval: 10_000,
  })

  const { data: templates } = useSWR<StudioEntity[]>('/v1/upload/streamers', commonFetcher)

  const templateOptions = useMemo(
    () =>
      (templates ?? []).map(item => ({
        label: item.template_name,
        value: item.id,
      })),
    [templates]
  )

  const openCreate = () => {
    setFormData(defaultFormData)
    setVisible(true)
  }

  const openEdit = (job: YouTubeJobEntity) => {
    setFormData({
      id: job.id,
      name: job.name,
      source_url: job.source_url,
      source_type: job.source_type,
      upload_streamer_id: job.upload_streamer_id,
      sync_interval_seconds: job.sync_interval_seconds,
      auto_publish: job.auto_publish === 1,
      enabled: job.enabled === 1,
    })
    setVisible(true)
  }

  const saveJob = async () => {
    if (!formData.name.trim() || !formData.source_url.trim() || !formData.upload_streamer_id) {
      Notification.warning({
        title: '参数缺失',
        content: '请填写任务名、源地址、上传模板',
        position: 'top',
      })
      return
    }
    try {
      if (formData.id) {
        await youtubePut(`/v1/youtube/jobs/${formData.id}`, {
          ...formData,
          upload_streamer_id: formData.upload_streamer_id,
        })
      } else {
        await youtubePost('/v1/youtube/jobs', {
          name: formData.name,
          source_url: formData.source_url,
          source_type: formData.source_type,
          upload_streamer_id: formData.upload_streamer_id,
          sync_interval_seconds: formData.sync_interval_seconds,
          auto_publish: formData.auto_publish,
          enabled: formData.enabled,
        })
      }
      setVisible(false)
      await mutate()
    } catch (error: any) {
      Notification.error({
        title: '保存失败',
        content: error.message,
        position: 'top',
      })
    }
  }

  const triggerNow = async (jobId: number) => {
    try {
      await youtubePost(`/v1/youtube/jobs/${jobId}/run`)
      await mutate()
    } catch (error: any) {
      Notification.error({
        title: '触发失败',
        content: error.message,
        position: 'top',
      })
    }
  }

  const togglePause = async (jobId: number) => {
    try {
      await youtubePost(`/v1/youtube/jobs/${jobId}/pause`)
      await mutate()
    } catch (error: any) {
      Notification.error({
        title: '操作失败',
        content: error.message,
        position: 'top',
      })
    }
  }

  const retryFailed = async (jobId: number) => {
    try {
      const result = await youtubePost<{ ok: boolean; retried_count: number }>(`/v1/youtube/jobs/${jobId}/retry_failed`)
      if (!result.retried_count) {
        Notification.info({
          title: '没有失败项',
          content: '当前任务没有失败视频',
          position: 'top',
        })
      } else {
        Notification.success({
          title: '已触发重试',
          content: `已重试 ${result.retried_count} 个失败视频`,
          position: 'top',
        })
      }
      await mutate()
    } catch (error: any) {
      Notification.error({
        title: '重试失败',
        content: error.message,
        position: 'top',
      })
    }
  }

  const removeJob = async (jobId: number) => {
    try {
      await youtubeDelete(`/v1/youtube/jobs/${jobId}`)
      Notification.success({
        title: '删除成功',
        content: `任务 #${jobId} 已删除`,
        position: 'top',
      })
      await mutate()
    } catch (error: any) {
      Notification.error({
        title: '删除失败',
        content: error.message,
        position: 'top',
      })
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
          <div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
            <IconCloudStroked
              size="large"
              style={{
                backgroundColor: 'rgba(var(--semi-orange-4), 1)',
                borderRadius: 'var(--semi-border-radius-large)',
                color: 'var(--semi-color-bg-0)',
                padding: '6px',
              }}
            />
            <h4>YouTube 任务</h4>
          </div>
          <Space wrap>
            <Button icon={<IconRefresh />} onClick={() => mutate()}>
              刷新
            </Button>
            <Button theme="solid" icon={<IconPlusCircle />} onClick={openCreate}>
              新建任务
            </Button>
          </Space>
        </nav>
      </Header>

      <Content style={{ padding: 24, backgroundColor: 'var(--semi-color-bg-0)' }}>
        <div className="yt-panel" style={{ marginBottom: 16 }}>
          <div className="yt-panel-header">
            <Typography.Text strong>概览</Typography.Text>
          </div>
          <div className="yt-panel-body">
            <div className="yt-stats-grid">
              <div className="yt-stat">
                <div className="yt-stat-label">任务数</div>
                <div className="yt-stat-value">{jobsResp?.summary.total_jobs ?? 0}</div>
              </div>
              <div className="yt-stat">
                <div className="yt-stat-label">待处理</div>
                <div className="yt-stat-value">{jobsResp?.summary.pending_items ?? 0}</div>
              </div>
              <div className="yt-stat">
                <div className="yt-stat-label">失败</div>
                <div className="yt-stat-value">{jobsResp?.summary.failed_items ?? 0}</div>
              </div>
              <div className="yt-stat">
                <div className="yt-stat-label">已上传</div>
                <div className="yt-stat-value">{jobsResp?.summary.uploaded_items ?? 0}</div>
              </div>
            </div>
          </div>
        </div>

        {!isLoading && (jobsResp?.jobs.length ?? 0) === 0 ? (
          <Empty title="暂无 YouTube 任务" />
        ) : (
          <List
            grid={{ gutter: 12, xs: 24, sm: 24, md: 12, lg: 8, xl: 6, xxl: 4 }}
            dataSource={jobsResp?.jobs ?? []}
            renderItem={job => (
              <List.Item>
                <Card
                  shadows="hover"
                  style={{ width: '100%', height: '100%' }}
                  title={
                    <div
                      style={{
                        display: 'flex',
                        justifyContent: 'space-between',
                        alignItems: 'flex-start',
                        flexWrap: 'wrap',
                        gap: 8,
                      }}
                    >
                      <Typography.Text
                        style={{
                          flex: 1,
                          minWidth: 0,
                          wordBreak: 'break-word',
                        }}
                      >
                        {job.name}
                      </Typography.Text>
                      {statusTag(job.status)}
                    </div>
                  }
                >
                  <Typography.Text
                    type="tertiary"
                    style={{ display: 'block', wordBreak: 'break-all' }}
                    ellipsis={{ showTooltip: true }}
                  >
                    {job.source_url}
                  </Typography.Text>
                  <div style={{ marginTop: 10, marginBottom: 10 }}>
                    <Tag color="blue">{getYouTubeSourceTypeLabel(job.source_type)}</Tag>
                    <Tag color={job.enabled === 1 ? 'green' : 'grey'}>
                      {job.enabled === 1 ? '启用' : '禁用'}
                    </Tag>
                  </div>
                  <div className="yt-action-grid">
                    <Link href={`/youtube/${job.id}`} style={{ width: '100%' }}>
                      <Button block>详情</Button>
                    </Link>
                    <Button block icon={<IconPlay />} theme="solid" onClick={() => triggerNow(job.id)}>
                      立即同步
                    </Button>
                    <Button block icon={<IconRefresh />} onClick={() => retryFailed(job.id)}>
                      失败重试
                    </Button>
                    <Button block icon={<IconPause />} onClick={() => togglePause(job.id)}>
                      {job.enabled === 1 ? '暂停' : '恢复'}
                    </Button>
                    <Button block icon={<IconEdit2Stroked />} onClick={() => openEdit(job)}>
                      编辑
                    </Button>
                    <Popconfirm
                      title="确定删除任务？"
                      content="会同时删除该任务的历史条目和日志"
                      onConfirm={() => removeJob(job.id)}
                    >
                      <Button block icon={<IconDeleteStroked />} type="danger">
                        删除
                      </Button>
                    </Popconfirm>
                  </div>
                  {job.last_error ? (
                    <Typography.Text type="danger" style={{ marginTop: 10, display: 'block' }}>
                      {job.last_error}
                    </Typography.Text>
                  ) : null}
                </Card>
              </List.Item>
            )}
          />
        )}
      </Content>

      <Modal
        title={formData.id ? '编辑任务' : '新建任务'}
        visible={visible}
        onOk={saveJob}
        onCancel={() => setVisible(false)}
        style={{ width: 'min(720px, 92vw)' }}
      >
        <Form
          labelPosition="left"
          labelWidth={140}
          initValues={formData}
          onValueChange={values => setFormData(v => ({ ...v, ...values }))}
        >
          <Form.Input field="name" label="任务名" placeholder="例如：某频道自动同步" />
          <Form.Input field="source_url" label="源地址" placeholder="频道 / 播放列表 / shorts 链接" />
          <Form.Select field="source_type" label="源类型">
            {YOUTUBE_SOURCE_TYPE_OPTIONS.map(option => (
              <Select.Option key={option.value} value={option.value}>
                {option.label}
              </Select.Option>
            ))}
          </Form.Select>
          <Form.Select field="upload_streamer_id" label="投稿模板" style={{ width: '100%', maxWidth: 360 }}>
            {templateOptions.map(item => (
              <Select.Option key={item.value} value={item.value}>
                {item.label}
              </Select.Option>
            ))}
          </Form.Select>
          <Form.InputNumber
            field="sync_interval_seconds"
            label="同步间隔(秒)"
            min={60}
            max={86400}
            style={{ width: '100%', maxWidth: 240 }}
          />
          <Form.Switch field="enabled" label="启用任务" />
          <Form.Switch field="auto_publish" label="自动发布" />
        </Form>
      </Modal>
    </>
  )
}

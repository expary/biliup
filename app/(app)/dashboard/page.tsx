'use client'

import React, { useRef } from 'react'
import {
  Button,
  Collapse,
  Form,
  Layout,
  Nav,
  Notification,
  Select,
  Toast,
  Typography,
} from '@douyinfe/semi-ui'
import { IconCloudStroked, IconLink, IconSetting, IconStar } from '@douyinfe/semi-icons'
import useSWR from 'swr'
import useSWRMutation from 'swr/mutation'
import { fetcher, put } from '@/app/lib/api-streamer'
import { FormApi } from '@douyinfe/semi-ui/lib/es/form'

function mergeConfig(base: any, patch: any) {
  return {
    ...base,
    ...patch,
    user: {
      ...(base?.user ?? {}),
      ...(patch?.user ?? {}),
    },
    LOGGING: {
      ...(base?.LOGGING ?? {}),
      ...(patch?.LOGGING ?? {}),
      root: {
        ...(base?.LOGGING?.root ?? {}),
        ...(patch?.LOGGING?.root ?? {}),
      },
      loggers: {
        ...(base?.LOGGING?.loggers ?? {}),
        ...(patch?.LOGGING?.loggers ?? {}),
        biliup: {
          ...(base?.LOGGING?.loggers?.biliup ?? {}),
          ...(patch?.LOGGING?.loggers?.biliup ?? {}),
        },
      },
    },
  }
}

export default function DashboardPage() {
  const { Header, Content } = Layout
  const { data: entity, error, isLoading } = useSWR<any>('/v1/configuration', fetcher)
  const { trigger } = useSWRMutation('/v1/configuration', put)
  const formRef = useRef<FormApi>()

  if (isLoading) {
    return <>Loading</>
  }
  if (error) {
    return <>error {JSON.stringify(error)}</>
  }
  if (!entity) {
    return null
  }

  return (
    <>
      <Header
        style={{
          backgroundColor: 'var(--semi-color-bg-1)',
          position: 'sticky',
          top: 0,
          zIndex: 1,
        }}
      >
        <Nav
          header={
            <>
              <div
                style={{
                  backgroundColor: '#6b6c75ff',
                  borderRadius: 'var(--semi-border-radius-large)',
                  color: 'var(--semi-color-bg-0)',
                  display: 'flex',
                  padding: '6px',
                }}
              >
                <IconStar size="large" />
              </div>
              <h4 style={{ marginLeft: '12px' }}>空间配置</h4>
            </>
          }
          footer={
            <Button
              onClick={() => formRef.current?.submitForm()}
              theme="solid"
              icon={<IconStar />}
              style={{ marginRight: 10 }}
            >
              保存
            </Button>
          }
          mode="horizontal"
        />
      </Header>
      <Content style={{ padding: 16, backgroundColor: 'var(--semi-color-bg-0)' }}>
        <Form
          initValues={entity}
          getFormApi={api => {
            formRef.current = api
          }}
          onSubmit={async values => {
            try {
              await trigger(mergeConfig(entity, values))
              Toast.success('保存成功')
            } catch (e: any) {
              Notification.error({
                title: '保存失败',
                content: <Typography.Text>{e.message}</Typography.Text>,
                style: { width: 'min-content' },
              })
              throw e
            }
          }}
        >
          <Collapse keepDOM defaultActiveKey={['global', 'youtube', 'deepseek', 'developer']}>
            <Collapse.Panel
              header={
                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
                  <IconCloudStroked /> 通用设置
                </span>
              }
              itemKey="global"
            >
              <Form.Input
                field="proxy"
                label="网络代理（proxy）"
                placeholder="socks5://127.0.0.1:10808"
                showClear
              />
              <Form.InputNumber field="threads" label="上传线程（threads）" min={1} max={16} />
              <Form.Select field="lines" label="上传线路（lines）" showClear>
                <Select.Option value="AUTO">AUTO</Select.Option>
                <Select.Option value="Bldsa">Bldsa</Select.Option>
                <Select.Option value="Cnbldsa">Cnbldsa</Select.Option>
                <Select.Option value="Andsa">Andsa</Select.Option>
                <Select.Option value="Atdsa">Atdsa</Select.Option>
                <Select.Option value="Bda2">Bda2</Select.Option>
                <Select.Option value="Cnbd">Cnbd</Select.Option>
                <Select.Option value="Anbd">Anbd</Select.Option>
                <Select.Option value="Atbd">Atbd</Select.Option>
                <Select.Option value="Tx">Tx</Select.Option>
                <Select.Option value="Cntx">Cntx</Select.Option>
                <Select.Option value="Antx">Antx</Select.Option>
                <Select.Option value="Attx">Attx</Select.Option>
                <Select.Option value="Txa">Txa</Select.Option>
                <Select.Option value="Alia">Alia</Select.Option>
              </Form.Select>
              <Form.Select
                field="submit_api"
                label="投稿接口（submit_api）"
                placeholder="Web（默认）"
                showClear
              >
                <Select.Option value="web">Web</Select.Option>
                <Select.Option value="app">APP</Select.Option>
                <Select.Option value="bcut_android">安卓剪辑</Select.Option>
              </Form.Select>
            </Collapse.Panel>

            <Collapse.Panel
              header={
                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
                  <IconCloudStroked /> YouTube 同步
                </span>
              }
              itemKey="youtube"
            >
              <Form.Input field="user.youtube_cookie" label="YouTube Cookie（user.youtube_cookie）" showClear />
              <Form.Switch field="youtube_enable_download_live" label="下载直播（youtube_enable_download_live）" />
              <Form.Switch
                field="youtube_enable_download_playback"
                label="下载回放（youtube_enable_download_playback）"
              />
              <Form.Input field="youtube_after_date" label="下载起始日期（youtube_after_date）" showClear />
              <Form.Input field="youtube_before_date" label="下载截止日期（youtube_before_date）" showClear />
              <Form.Input field="youtube_max_videosize" label="视频大小上限（youtube_max_videosize）" showClear />
              <Form.InputNumber
                field="youtube_max_resolution"
                label="视频分辨率上限（youtube_max_resolution）"
                min={144}
                max={4320}
              />
              <Form.Input field="youtube_prefer_vcodec" label="偏好视频编码（youtube_prefer_vcodec）" showClear />
              <Form.Input field="youtube_prefer_acodec" label="偏好音频编码（youtube_prefer_acodec）" showClear />
            </Collapse.Panel>

            <Collapse.Panel
              header={
                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
                  <IconLink /> DeepSeek
                </span>
              }
              itemKey="deepseek"
            >
              <Form.Input field="deepseek_api_key" label="DeepSeek API Key（deepseek_api_key）" mode="password" showClear />
              <Form.Input field="deepseek_api_base" label="DeepSeek API 地址（deepseek_api_base）" showClear />
              <Form.Input field="deepseek_model" label="DeepSeek 模型（deepseek_model）" showClear />
            </Collapse.Panel>

            <Collapse.Panel
              header={
                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
                  <IconSetting /> 开发者选项
                </span>
              }
              itemKey="developer"
            >
              <Form.Select field="LOGGING.root.level" label="ds_update.log 输出等级" showClear>
                <Select.Option value="DEBUG">DEBUG</Select.Option>
                <Select.Option value="INFO">INFO</Select.Option>
                <Select.Option value="WARNING">WARNING</Select.Option>
                <Select.Option value="ERROR">ERROR</Select.Option>
                <Select.Option value="CRITICAL">CRITICAL</Select.Option>
              </Form.Select>
              <Form.Select field="LOGGING.loggers.biliup.level" label="biliup 输出等级" showClear>
                <Select.Option value="DEBUG">DEBUG</Select.Option>
                <Select.Option value="INFO">INFO</Select.Option>
                <Select.Option value="WARNING">WARNING</Select.Option>
                <Select.Option value="ERROR">ERROR</Select.Option>
                <Select.Option value="CRITICAL">CRITICAL</Select.Option>
              </Form.Select>
              <Form.Select field="loggers_level" label="文件日志等级（loggers_level）" showClear>
                <Select.Option value="DEBUG">DEBUG</Select.Option>
                <Select.Option value="INFO">INFO</Select.Option>
                <Select.Option value="WARNING">WARNING</Select.Option>
                <Select.Option value="ERROR">ERROR</Select.Option>
                <Select.Option value="CRITICAL">CRITICAL</Select.Option>
              </Form.Select>
            </Collapse.Panel>
          </Collapse>
        </Form>
      </Content>
    </>
  )
}

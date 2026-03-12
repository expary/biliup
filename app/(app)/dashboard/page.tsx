'use client'

import React, { useEffect, useMemo, useRef } from 'react'
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
import { IconLink, IconSetting, IconStar } from '@douyinfe/semi-icons'
import useSWR from 'swr'
import useSWRMutation from 'swr/mutation'
import { fetcher, put } from '@/app/lib/api-streamer'
import { FormApi } from '@douyinfe/semi-ui/lib/es/form'

const DEFAULT_SUBMIT_API = 'web'
const DEFAULT_LINES = 'AUTO'
const DEFAULT_THREADS = 3
const DEFAULT_LOG_LEVEL = 'INFO'
const DEFAULT_DEEPSEEK_API_BASE = 'https://api.deepseek.com/chat/completions'
const DEFAULT_DEEPSEEK_MODEL = 'deepseek-chat'

type DashboardFormValues = {
  proxy: string
  threads: number
  lines: string
  submit_api: string
  deepseek_api_key: string
  deepseek_api_base: string
  deepseek_model: string
  loggers_level: string
}

function normalizeConfigForForm(entity: any): DashboardFormValues {
  return {
    proxy: typeof entity?.proxy === 'string' ? entity.proxy : '',
    threads: typeof entity?.threads === 'number' ? entity.threads : DEFAULT_THREADS,
    lines: typeof entity?.lines === 'string' && entity.lines ? entity.lines : DEFAULT_LINES,
    submit_api:
      typeof entity?.submit_api === 'string' && entity.submit_api ? entity.submit_api : DEFAULT_SUBMIT_API,
    deepseek_api_key: typeof entity?.deepseek_api_key === 'string' ? entity.deepseek_api_key : '',
    deepseek_api_base:
      typeof entity?.deepseek_api_base === 'string' && entity.deepseek_api_base
        ? entity.deepseek_api_base
        : DEFAULT_DEEPSEEK_API_BASE,
    deepseek_model:
      typeof entity?.deepseek_model === 'string' && entity.deepseek_model
        ? entity.deepseek_model
        : DEFAULT_DEEPSEEK_MODEL,
    loggers_level:
      typeof entity?.loggers_level === 'string' && entity.loggers_level ? entity.loggers_level : DEFAULT_LOG_LEVEL,
  }
}

function toOptionalString(value?: string | null) {
  const trimmed = typeof value === 'string' ? value.trim() : ''
  return trimmed ? trimmed : null
}

function toOptionalStringUnlessDefault(value: string | null | undefined, defaultValue: string) {
  const trimmed = typeof value === 'string' ? value.trim() : ''
  if (!trimmed || trimmed === defaultValue) {
    return null
  }
  return trimmed
}

function buildConfigPayload(base: any, values: DashboardFormValues) {
  return {
    ...base,
    proxy: toOptionalString(values.proxy),
    threads: typeof values.threads === 'number' ? values.threads : DEFAULT_THREADS,
    lines: values.lines || DEFAULT_LINES,
    submit_api: values.submit_api && values.submit_api !== DEFAULT_SUBMIT_API ? values.submit_api : null,
    deepseek_api_key: toOptionalString(values.deepseek_api_key),
    deepseek_api_base: toOptionalStringUnlessDefault(values.deepseek_api_base, DEFAULT_DEEPSEEK_API_BASE),
    deepseek_model: toOptionalStringUnlessDefault(values.deepseek_model, DEFAULT_DEEPSEEK_MODEL),
    loggers_level: values.loggers_level || DEFAULT_LOG_LEVEL,
    user: {
      ...(base?.user ?? {}),
    },
  }
}

export default function DashboardPage() {
  const { Header, Content } = Layout
  const { data: entity, error, isLoading, mutate } = useSWR<any>('/v1/configuration', fetcher)
  const { trigger } = useSWRMutation('/v1/configuration', put)
  const formRef = useRef<FormApi<DashboardFormValues>>()
  const formValues = useMemo(() => normalizeConfigForForm(entity), [entity])

  useEffect(() => {
    if (entity && formRef.current) {
      formRef.current.setValues(formValues, { isOverride: true })
    }
  }, [entity, formValues])

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
          initValues={formValues}
          getFormApi={api => {
            formRef.current = api
          }}
          onSubmit={async values => {
            try {
              const payload = buildConfigPayload(entity, values as DashboardFormValues)
              await trigger(payload)
              const latest = (await fetcher('/v1/configuration')) as any
              await mutate(latest, { revalidate: false })
              formRef.current?.setValues(normalizeConfigForForm(latest), { isOverride: true })
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
          <Collapse keepDOM defaultActiveKey={['global', 'deepseek', 'developer']}>
            <Collapse.Panel
              header={
                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
                  <IconStar /> 基础设置
                </span>
              }
              itemKey="global"
            >
              <Typography.Text type="tertiary" style={{ display: 'block', marginBottom: 12 }}>
                这里只保留会实际写入并在重启后继续生效的全局字段。YouTube 同步下载参数已从这个页面移除。
              </Typography.Text>
              <Form.Input
                field="proxy"
                label="网络代理"
                placeholder="http://127.0.0.1:7890 或 socks5://127.0.0.1:10808"
                showClear
                extraText="留空表示不走代理。"
              />
              <Form.InputNumber
                field="threads"
                label="上传线程"
                min={1}
                max={16}
                extraText="线程越高并不一定越快，通常 3 到 5 就够用。"
              />
              <Form.Select field="lines" label="上传线路">
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
                label="投稿接口"
                extraText="默认使用 Web。留在这里显示，是因为这个字段会真实持久化。"
              >
                <Select.Option value="web">Web（默认）</Select.Option>
                <Select.Option value="app">APP</Select.Option>
                <Select.Option value="bcut_android">安卓剪辑</Select.Option>
              </Form.Select>
            </Collapse.Panel>

            <Collapse.Panel
              header={
                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
                  <IconLink /> DeepSeek
                </span>
              }
              itemKey="deepseek"
            >
              <Typography.Text type="tertiary" style={{ display: 'block', marginBottom: 12 }}>
                API 地址和模型即使未显式保存，也会按默认值工作。这里会直接显示当前有效默认值，避免重启后看起来像“丢了”。
              </Typography.Text>
              <Form.Input
                field="deepseek_api_key"
                label="DeepSeek API Key"
                mode="password"
                showClear
                extraText="不填写则不会启用 DeepSeek 标题/简介生成。"
              />
              <Form.Input
                field="deepseek_api_base"
                label="DeepSeek API 地址"
                showClear
                placeholder={DEFAULT_DEEPSEEK_API_BASE}
                extraText={`默认值：${DEFAULT_DEEPSEEK_API_BASE}`}
              />
              <Form.Input
                field="deepseek_model"
                label="DeepSeek 模型"
                showClear
                placeholder={DEFAULT_DEEPSEEK_MODEL}
                extraText={`默认值：${DEFAULT_DEEPSEEK_MODEL}`}
              />
            </Collapse.Panel>

            <Collapse.Panel
              header={
                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
                  <IconSetting /> 日志与调试
                </span>
              }
              itemKey="developer"
            >
              <Typography.Text type="tertiary" style={{ display: 'block', marginBottom: 12 }}>
                当前服务只持久化一个全局日志等级字段 `loggers_level`。之前页面里的 `LOGGING.root.level` 和 `LOGGING.loggers.biliup.level` 不会真正保存。
              </Typography.Text>
              <Form.Select
                field="loggers_level"
                label="全局日志等级"
                extraText="保存后会立即应用到当前服务，并在重启后继续保留。"
              >
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

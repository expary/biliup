'use client'
import React from 'react'
import styles from '../../styles/dashboard.module.scss'
import { Form } from '@douyinfe/semi-ui'
import { IconLink } from '@douyinfe/semi-icons'

const DeepSeek: React.FC = () => {
  return (
    <div className={styles.frameDeveloper}>
      <div className={styles.frameInside}>
        <div className={styles.group}>
          <div className={styles.buttonOnlyIconSecond} />
          <div
            className={styles.lineStory}
            style={{
              color: 'var(--semi-color-bg-0)',
              display: 'flex',
            }}
          >
            <IconLink size="small" />
          </div>
        </div>
        <p className={styles.meegoSharedWebWorkIt}>DeepSeek 密钥设置</p>
      </div>

      <Form.Input
        label="DeepSeek API Key（deepseek_api_key）"
        field="deepseek_api_key"
        type="password"
        placeholder="sk-..."
        style={{ width: '100%' }}
        fieldStyle={{
          alignSelf: 'stretch',
          padding: 0,
        }}
        extraText={
          <div style={{ fontSize: '14px' }}>
            用于 YouTube 标题“翻译 + 润色”策略。
            <br />
            获取地址：
            <a
              href="https://platform.deepseek.com/api_keys"
              target="_blank"
              rel="noopener noreferrer"
              style={{ marginLeft: 6 }}
            >
              DeepSeek API Keys
            </a>
          </div>
        }
        showClear
      />

      <Form.Input
        label="DeepSeek API 地址（deepseek_api_base）"
        field="deepseek_api_base"
        placeholder="https://api.deepseek.com/chat/completions"
        style={{ width: '100%' }}
        fieldStyle={{
          alignSelf: 'stretch',
          padding: 0,
        }}
        extraText="可选。默认使用 DeepSeek 官方地址。"
        showClear
      />

      <Form.Input
        label="DeepSeek 模型（deepseek_model）"
        field="deepseek_model"
        placeholder="deepseek-chat"
        style={{ width: '100%' }}
        fieldStyle={{
          alignSelf: 'stretch',
          padding: 0,
        }}
        extraText="可选。默认 deepseek-chat。"
        showClear
      />
    </div>
  )
}

export default DeepSeek

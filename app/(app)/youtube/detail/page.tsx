'use client'

import { Suspense, useEffect } from 'react'
import { useRouter, useSearchParams } from 'next/navigation'
import { Empty } from '@douyinfe/semi-ui'

function YouTubeJobDetailRedirect() {
  const searchParams = useSearchParams()
  const router = useRouter()
  const jobId = searchParams.get('id')

  useEffect(() => {
    if (!jobId) {
      return
    }
    router.replace(`/youtube/${jobId}`)
  }, [jobId, router])

  if (!jobId) {
    return <Empty title="缺少任务 ID" />
  }
  return null
}

export default function YouTubeJobDetailLegacyPage() {
  return (
    <Suspense>
      <YouTubeJobDetailRedirect />
    </Suspense>
  )
}


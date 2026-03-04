import YouTubeJobDetailClient from './YouTubeJobDetailClient'

export async function generateStaticParams() {
  return [{ id: '0' }]
}

export default function YouTubeJobDetailPage() {
  return <YouTubeJobDetailClient />
}

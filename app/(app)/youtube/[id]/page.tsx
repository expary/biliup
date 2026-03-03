import YouTubeJobDetailClient from './YouTubeJobDetailClient'

export async function generateStaticParams() {
  return [{ id: '0' }]
}

export default function YouTubeJobDetailPage({ params }: { params: { id: string } }) {
  return <YouTubeJobDetailClient jobId={Number(params.id) || 0} />
}

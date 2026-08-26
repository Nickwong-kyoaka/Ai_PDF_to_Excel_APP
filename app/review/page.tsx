'use client';

import Link from 'next/link';
import { useEffect, useState } from 'react';
import { AppShell, useLocale } from '../components/app-shell';
import { api } from '../lib/api';
import type { Job } from '../lib/types';

function ReviewQueue() {
  const { t } = useLocale();
  const [jobs, setJobs] = useState<Job[]>([]);
  useEffect(() => { api.jobs().then((items) => setJobs(items.filter((job) => job.status === 'review_needed'))).catch(() => undefined); }, []);
  return <>
    <header className="page-header"><div><p>{t('Quality control', '品質控制')}</p><h1>{t('Human review queue', '人工覆核佇列')}</h1><span>{t('Resolve model corrections and extraction conflicts before final export.', '最終匯出前，處理模型修正及擷取衝突。')}</span></div></header>
    <section className="review-queue-grid">
      {jobs.map((job) => <Link className="review-queue-card" href={`/jobs/${job.id}`} key={job.id}><div className="review-card-top"><span className="file-icon">PDF</span><b>REVIEW</b></div><h2>{job.filename}</h2><p>{job.stage_message}</p><div><span>{job.page_count} {t('pages', '頁')}</span><strong>{t('Open review', '開啟覆核')} →</strong></div></Link>)}
      {!jobs.length ? <div className="all-clear"><b>✓</b><h2>{t('Review queue is clear', '覆核佇列已清空')}</h2><p>{t('There are no flagged jobs waiting for a reviewer.', '目前沒有需要覆核的工作。')}</p></div> : null}
    </section>
  </>;
}

export default function ReviewPage() { return <AppShell><ReviewQueue /></AppShell>; }

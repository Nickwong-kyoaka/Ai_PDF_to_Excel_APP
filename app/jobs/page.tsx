'use client';

import Link from 'next/link';
import { useEffect, useMemo, useState } from 'react';
import { AppShell, useLocale } from '../components/app-shell';
import { api } from '../lib/api';
import type { Job } from '../lib/types';

const filters = ['all', 'active', 'review_needed', 'ready', 'finalized', 'failed'];

function JobsContent() {
  const { t } = useLocale();
  const [jobs, setJobs] = useState<Job[]>([]);
  const [filter, setFilter] = useState('all');
  useEffect(() => { api.jobs().then(setJobs).catch(() => undefined); }, []);
  const filtered = useMemo(() => jobs.filter((job) => {
    if (filter === 'all') return true;
    if (filter === 'active') return ['queued', 'extracting', 'judging'].includes(job.status);
    return job.status === filter;
  }), [jobs, filter]);
  return <>
    <header className="page-header"><div><p>{t('Operations', '操作')}</p><h1>{t('Scan jobs', '掃描工作')}</h1><span>{t('Monitor uploads, processing, review and exports.', '監察上載、處理、覆核及匯出狀態。')}</span></div><Link href="/" className="secondary-action">＋ {t('New upload', '新增上載')}</Link></header>
    <div className="filter-bar">{filters.map((item) => <button className={filter === item ? 'active' : ''} key={item} onClick={() => setFilter(item)}>{item.replaceAll('_', ' ')}</button>)}</div>
    <section className="table-card">
      <div className="data-table jobs-list-table"><div className="table-head"><span>{t('Document', '文件')}</span><span>{t('Status', '狀態')}</span><span>{t('Progress', '進度')}</span><span>{t('Uploaded', '上載時間')}</span><span /></div>
      {filtered.map((job) => <Link className="table-row" href={`/jobs/${job.id}`} key={job.id}><div className="document-cell"><span className="file-icon">{job.filename.split('.').pop()?.toUpperCase()}</span><div><strong>{job.filename}</strong><small>{job.page_count} {t('pages', '頁')} · {job.groups.length} {t('group(s)', '份問卷')}</small></div></div><span className={`status-chip status-${job.status}`}>{job.status.replaceAll('_', ' ')}</span><div className="inline-progress"><i style={{ width: `${job.progress * 100}%` }} /><b>{Math.round(job.progress * 100)}%</b></div><span className="muted-cell">{new Date(job.created_at).toLocaleString()}</span><span className="row-arrow">→</span></Link>)}
      {!filtered.length ? <div className="table-empty">{t('No jobs match this filter.', '沒有符合此篩選條件的工作。')}</div> : null}</div>
    </section>
  </>;
}

export default function JobsPage() { return <AppShell><JobsContent /></AppShell>; }

'use client';

import Link from 'next/link';
import { FormEvent, useEffect, useMemo, useState } from 'react';
import { useRouter } from 'next/navigation';
import { AppShell, useLocale } from './components/app-shell';
import { api } from './lib/api';
import type { Job, ModelProfile } from './lib/types';

function Dashboard() {
  const { t } = useLocale();
  const router = useRouter();
  const [jobs, setJobs] = useState<Job[]>([]);
  const [profiles, setProfiles] = useState<ModelProfile[]>([]);
  const [uploadOpen, setUploadOpen] = useState(false);
  const [file, setFile] = useState<File | null>(null);
  const [profileId, setProfileId] = useState('');
  const [language, setLanguage] = useState('auto');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    Promise.all([api.jobs(), api.profiles()]).then(([jobData, profileData]) => {
      setJobs(jobData); setProfiles(profileData);
      setProfileId(profileData.find((item) => item.is_default)?.id || profileData[0]?.id || '');
    }).catch(() => undefined);
    const interval = window.setInterval(() => api.jobs().then(setJobs).catch(() => undefined), 5000);
    return () => window.clearInterval(interval);
  }, []);

  const reviewCount = useMemo(() => jobs.filter((job) => job.status === 'review_needed').length, [jobs]);
  const active = jobs.find((job) => ['queued', 'extracting', 'judging'].includes(job.status));

  async function upload(event: FormEvent) {
    event.preventDefault();
    if (!file || !profileId) return;
    setBusy(true); setError('');
    try {
      const job = await api.upload(file, profileId, language);
      router.push(`/jobs/${job.id}`);
    } catch (reason) { setError(reason instanceof Error ? reason.message : 'Upload failed'); }
    finally { setBusy(false); }
  }

  return (
    <>
      <header className="topbar">
        <div><p>{t('Questionnaire scanner', '問卷掃描系統')}</p><h1>{t('Trusted extraction, with humans in control.', '可靠擷取，最終決定由人掌握。')}</h1></div>
      </header>
      <section className="hero-grid">
        <article className="upload-card">
          <div className="eyebrow"><span className="live-dot" /> {active ? t('GPU worker processing', 'GPU 正在處理') : t('GPU worker ready', 'GPU 已準備')}</div>
          <h2>{t('Turn questionnaires into trusted data.', '將問卷轉化為可信數據。')}</h2>
          <p>{t('Upload Chinese or English forms. Qwen reads the content, YOLO verifies every mark, and reviewers keep the final say.', '上載中英文問卷。Qwen 讀取內容、YOLO 核實記號，並由覆核員作最終決定。')}</p>
          <button type="button" onClick={() => setUploadOpen(true)}>＋ {t('Upload questionnaires', '上載問卷')}</button>
          <small>PDF, PNG, JPEG, TIFF · PRIVATE LAN / VPN</small>
          <div className="page-motif" aria-hidden="true"><i /><i /><i /><em>✓</em></div>
        </article>
        <aside className="system-card">
          <div className="card-heading"><div><span>{t('Processing profile', '處理設定')}</span><h3>RTX 5060 Ti · Maximum</h3></div><b>{active ? 'BUSY' : 'READY'}</b></div>
          <dl>
            <div><dt>{t('Vision model', '視覺模型')}</dt><dd>Qwen3-VL 8B<span className="ok-dot" /></dd></div>
            <div><dt>{t('Mark detector', '記號偵測')}</dt><dd>YOLO custom<span className="warn-dot" /></dd></div>
            <div><dt>{t('Queue', '佇列')}</dt><dd>{jobs.filter((job) => job.status === 'queued').length} {t('waiting', '等待中')}</dd></div>
            <div><dt>{t('Retention', '保留期限')}</dt><dd>30 {t('days', '日')}</dd></div>
          </dl>
          <div className="meter"><i style={{ width: active ? `${Math.max(8, active.progress * 100)}%` : '0%' }} /></div>
          <p>{active?.stage_message || t('No active processing job', '暫無處理中的工作')} <Link href="/admin">{t('Details', '詳情')} →</Link></p>
        </aside>
      </section>
      <section className="content-grid">
        <article className="jobs-card">
          <div className="section-title"><div><p>{t('Recent activity', '最近活動')}</p><h2>{t('Scan jobs', '掃描工作')}</h2></div><Link href="/jobs">{t('View all jobs', '查看全部')} →</Link></div>
          <div className="job-table">
            {jobs.slice(0, 4).map((job) => <JobRow key={job.id} job={job} />)}
            {!jobs.length ? <div className="empty-row">{t('No scans yet. Upload your first questionnaire.', '尚未有掃描工作。請上載第一份問卷。')}</div> : null}
          </div>
        </article>
        <aside className="review-card">
          <div className="section-title"><div><p>{t('Human review', '人工覆核')}</p><h2>{reviewCount} {t('jobs need attention', '項工作需要處理')}</h2></div></div>
          <div className="review-visual"><span>Q12</span><div className="fake-lines"><i /><i /><i /></div><b>✓</b></div>
          <h3>{t('Original answers always stay visible', '原始答案永久保留')}</h3>
          <p>{t('Accept, edit, or revert every Qwen correction before final export.', '最終匯出前，接受、修改或還原每項 Qwen 修正。')}</p>
          <Link className="review-link" href="/review">{t('Start review', '開始覆核')} <span>→</span></Link>
        </aside>
      </section>

      {uploadOpen ? <div className="modal-backdrop" role="presentation" onMouseDown={() => !busy && setUploadOpen(false)}><form className="modal" onSubmit={upload} onMouseDown={(event) => event.stopPropagation()}>
        <div className="modal-head"><div><p>{t('New scan job', '新增掃描工作')}</p><h2>{t('Upload questionnaires', '上載問卷')}</h2></div><button type="button" onClick={() => setUploadOpen(false)}>×</button></div>
        <label className="drop-field"><input type="file" accept=".pdf,.png,.jpg,.jpeg,.tif,.tiff" onChange={(event) => setFile(event.target.files?.[0] || null)} required /><strong>{file?.name || t('Choose a PDF or image', '選擇 PDF 或圖片')}</strong><span>{t('Maximum 250 MB. Password-protected PDFs are rejected.', '上限 250 MB。不接受加密 PDF。')}</span></label>
        <label>{t('Model profile', '模型設定')}<select value={profileId} onChange={(event) => setProfileId(event.target.value)} required>{profiles.map((profile) => <option value={profile.id} key={profile.id}>{profile.name}</option>)}</select></label>
        <label>{t('Expected language', '預期語言')}<select value={language} onChange={(event) => setLanguage(event.target.value)}><option value="auto">{t('Auto detect', '自動偵測')}</option><option value="English">English</option><option value="Traditional Chinese">繁體中文</option><option value="Simplified Chinese">简体中文</option><option value="Mixed">Mixed / 混合</option></select></label>
        {error ? <p className="form-error">{error}</p> : null}
        <button className="primary-action" disabled={busy || !file || !profileId}>{busy ? t('Validating…', '驗證中…') : t('Upload and group pages', '上載並分析頁面')}<span>→</span></button>
      </form></div> : null}
    </>
  );
}

function JobRow({ job }: { job: Job }) {
  const { t } = useLocale();
  const status = job.status.replaceAll('_', ' ');
  const statusClass = job.status === 'review_needed' ? 'review' : ['extracting', 'judging'].includes(job.status) ? 'working' : 'queued';
  return <Link className="job-row" href={`/jobs/${job.id}`}>
    <span className="file-icon">{job.filename.split('.').pop()?.toUpperCase()}</span>
    <div className="job-name"><strong>{job.filename}</strong><small>{job.page_count} {t('pages', '頁')} · {new Date(job.created_at).toLocaleString()}</small></div>
    <div className="progress"><i style={{ width: `${job.progress * 100}%` }} /></div>
    <span className={`status ${statusClass}`}>{status}</span><span>→</span>
  </Link>;
}

export default function Home() {
  return <AppShell><Dashboard /></AppShell>;
}

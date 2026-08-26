/* eslint-disable @next/next/no-img-element */
'use client';

import Link from 'next/link';
import { useParams } from 'next/navigation';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { AppShell, useLocale } from '../../components/app-shell';
import { api } from '../../lib/api';
import type { Answer, Group, Job, ResultV2 } from '../../lib/types';

function formatValue(value: unknown): string {
  if (value === null || value === undefined || value === '') return 'N/A';
  return typeof value === 'object' ? JSON.stringify(value, null, 2) : String(value);
}

function JobDetail({ id }: { id: string }) {
  const { t } = useLocale();
  const [job, setJob] = useState<Job | null>(null);
  const [result, setResult] = useState<ResultV2 | null>(null);
  const [groups, setGroups] = useState<Group[]>([]);
  const [activeAnswer, setActiveAnswer] = useState<Answer | null>(null);
  const [activePage, setActivePage] = useState(1);
  const [editValue, setEditValue] = useState('');
  const [comment, setComment] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');

  const load = useCallback(async () => {
    const jobData = await api.job(id); setJob(jobData); setGroups(jobData.groups);
    if (['review_needed', 'ready', 'finalized'].includes(jobData.status) || jobData.draft_artifacts_ready) {
      setResult(await api.result(id));
    }
  }, [id]);
  useEffect(() => {
    async function initialize() {
      try { await load(); } catch (reason) { setError(String(reason)); }
    }
    void initialize();
  }, [load]);
  useEffect(() => {
    if (!job || !['queued', 'extracting', 'judging'].includes(job.status)) return;
    const timer = window.setInterval(() => load().catch(() => undefined), 3000);
    return () => window.clearInterval(timer);
  }, [job, load]);

  const pending = useMemo(() => result?.answers.filter((answer) => answer.review_status === 'pending') || [], [result]);
  const pageAnswers = useMemo(() => result?.answers.filter((answer) => answer.page_number === activePage) || [], [result, activePage]);

  async function confirm() {
    setBusy(true); setError('');
    try { const updated = await api.confirmGroups(id, groups.map((group) => ({ start_page: Number(group.start_page), end_page: Number(group.end_page), participant_id: group.participant_id }))); setJob(updated); }
    catch (reason) { setError(reason instanceof Error ? reason.message : String(reason)); }
    finally { setBusy(false); }
  }

  async function review(action: 'accept_qwen' | 'revert_scanner' | 'edit') {
    if (!activeAnswer) return;
    setBusy(true); setError('');
    try { await api.review(id, activeAnswer.id, action, action === 'edit' ? editValue : undefined, comment); setActiveAnswer(null); setComment(''); await load(); }
    catch (reason) { setError(reason instanceof Error ? reason.message : String(reason)); }
    finally { setBusy(false); }
  }

  async function finalize() {
    setBusy(true); setError('');
    try { setJob(await api.finalize(id)); await load(); }
    catch (reason) { setError(reason instanceof Error ? reason.message : String(reason)); }
    finally { setBusy(false); }
  }

  if (!job) return <div className="loading-state">{error || t('Loading job…', '載入工作中…')}</div>;
  return <>
    <header className="job-detail-header"><div><Link href="/jobs">← {t('Scan jobs', '掃描工作')}</Link><h1>{job.filename}</h1><p><span className={`status-chip status-${job.status}`}>{job.status.replaceAll('_', ' ')}</span>{job.page_count} {t('pages', '頁')} · {job.groups.length} {t('questionnaire(s)', '份問卷')}</p></div><div className="header-actions">{job.status === 'ready' ? <button className="primary-action compact" onClick={finalize} disabled={busy}>{t('Create final export', '建立最終匯出')} →</button> : null}{job.status === 'finalized' ? <b className="final-badge">✓ {t('Finalized', '已完成')}</b> : null}</div></header>
    {error ? <div className="inline-error">{error}</div> : null}
    {job.status === 'awaiting_confirmation' ? <section className="grouping-layout">
      <div className="grouping-main"><div className="section-title"><div><p>{t('Step 1 of 2', '步驟 1 / 2')}</p><h2>{t('Confirm questionnaire groups', '確認問卷分組')}</h2></div></div><p className="section-copy">{t('Every page must appear exactly once. Adjust page ranges or participant IDs before the GPU scan begins.', '每頁必須只出現一次。GPU 掃描前，請調整頁面範圍或參與者編號。')}</p><div className="group-list">{groups.map((group, index) => <div className="group-editor" key={group.id}><b>{String(index + 1).padStart(2, '0')}</b><label>{t('From page', '由頁')}<input type="number" min="1" max={job.page_count} value={group.start_page} onChange={(event) => setGroups((items) => items.map((item) => item.id === group.id ? { ...item, start_page: Number(event.target.value) } : item))} /></label><label>{t('To page', '至頁')}<input type="number" min="1" max={job.page_count} value={group.end_page} onChange={(event) => setGroups((items) => items.map((item) => item.id === group.id ? { ...item, end_page: Number(event.target.value) } : item))} /></label><label className="pid-field">{t('Participant ID', '參與者編號')}<input value={group.participant_id || ''} placeholder="Optional" onChange={(event) => setGroups((items) => items.map((item) => item.id === group.id ? { ...item, participant_id: event.target.value } : item))} /></label><span className="confidence">{Math.round(group.confidence * 100)}%<small>{t('auto confidence', '自動信心度')}</small></span></div>)}</div><button className="primary-action confirm-groups" onClick={confirm} disabled={busy}>{busy ? t('Queuing…', '加入佇列中…') : t('Confirm groups and start scan', '確認分組並開始掃描')}<span>→</span></button></div>
      <aside className="page-strip"><h3>{t('Page preview', '頁面預覽')}</h3>{Array.from({ length: job.page_count }, (_, index) => <button key={index} onClick={() => setActivePage(index + 1)}><img src={api.pageUrl(id, index + 1)} alt={`Page ${index + 1}`} /><span>{index + 1}</span></button>)}</aside>
    </section> : null}

    {['queued', 'extracting', 'judging'].includes(job.status) ? <section className="processing-card"><div className="processing-orbit"><i /><b>{Math.round(job.progress * 100)}%</b></div><div><p>{t('Maximum accuracy pipeline', '最高準確度流程')}</p><h2>{job.stage_message}</h2><span>{t('You may leave this page. The durable worker will continue and resume after a restart.', '你可以離開此頁。持久工作程序會繼續執行，並可在重啟後恢復。')}</span><div className="big-progress"><i style={{ width: `${job.progress * 100}%` }} /></div></div></section> : null}

    {result ? <section className="result-layout">
      <div className="evidence-panel"><div className="evidence-toolbar"><h2>{t('Page evidence', '頁面證據')}</h2><select value={activePage} onChange={(event) => setActivePage(Number(event.target.value))}>{Array.from({ length: job.page_count }, (_, i) => <option key={i} value={i + 1}>{t('Page', '頁')} {i + 1}</option>)}</select></div><div className="page-canvas"><img src={api.pageUrl(id, activePage)} alt={`Questionnaire page ${activePage}`} />{pageAnswers.flatMap((answer) => answer.evidence.map((evidence, index) => { const box = evidence.bbox as number[] | undefined; return box?.length === 4 ? <button title={`${answer.question_id} · ${String(evidence.source)}`} onClick={() => { setActiveAnswer(answer); setEditValue(formatValue(answer.final_value)); }} className={`evidence-box source-${evidence.source}`} key={`${answer.id}-${index}`} style={{ left: `${box[0] * 100}%`, top: `${box[1] * 100}%`, width: `${(box[2] - box[0]) * 100}%`, height: `${(box[3] - box[1]) * 100}%` }}><span>{answer.question_id}</span></button> : null; }))}</div></div>
      <div className="answers-panel"><div className="answers-toolbar"><div><p>{t('ResultV2', 'ResultV2')}</p><h2>{t('Extracted answers', '已擷取答案')}</h2></div><div><span className="pending-pill">{pending.length} {t('pending', '待覆核')}</span></div></div><div className="answer-list">{result.answers.map((answer) => <button className={`answer-row ${answer.review_status === 'pending' ? 'needs-review' : ''}`} key={answer.id} onClick={() => { setActiveAnswer(answer); setActivePage(answer.page_number); setEditValue(formatValue(answer.final_value)); }}><span className="answer-id">{answer.question_id}</span><div><strong>{answer.question_text}</strong><small>{answer.section || answer.answer_type}</small></div><div className="answer-values"><span>{formatValue(answer.scanner_value)}</span>{answer.final_source !== 'scanner' ? <b>→ {formatValue(answer.final_value)}</b> : null}</div><span className={`review-dot ${answer.review_status}`} /></button>)}</div></div>
    </section> : null}

    {job.artifacts.length ? <section className="artifacts-card"><div><p>{job.status === 'finalized' ? t('Approved artifacts', '已批准檔案') : t('Draft artifacts', '草稿檔案')}</p><h2>{t('Exports', '匯出')}</h2></div><div>{job.artifacts.filter((item) => job.status === 'finalized' ? !item.draft : item.draft).map((artifact) => <a href={api.artifactUrl(id, artifact.id)} key={artifact.id} target="_blank" rel="noreferrer"><span>{artifact.kind === 'excel' ? 'XLSX' : artifact.kind === 'json' ? 'JSON' : 'PDF'}</span><div><strong>{artifact.filename}</strong><small>{artifact.draft ? t('DRAFT — unresolved status included', '草稿 — 包含未解決狀態') : t('Final approved export', '最終批准匯出')}</small></div>↓</a>)}</div></section> : null}

    {activeAnswer ? <div className="review-drawer-backdrop" onMouseDown={() => setActiveAnswer(null)}><aside className="review-drawer" onMouseDown={(event) => event.stopPropagation()}><button className="drawer-close" onClick={() => setActiveAnswer(null)}>×</button><p className="form-eyebrow">{activeAnswer.question_id} · {t('Page', '頁')} {activeAnswer.page_number}</p><h2>{activeAnswer.question_text}</h2><div className="provenance-grid"><ValueBox title="Qwen" value={activeAnswer.qwen_value} /><ValueBox title="YOLO" value={activeAnswer.yolo_value} /><ValueBox title={t('Immutable scan', '不可變掃描值')} value={activeAnswer.scanner_value} strong /><ValueBox title={t('Current final', '目前最終值')} value={activeAnswer.final_value} strong /></div><div className="judge-callout"><b>{activeAnswer.reasonableness_status === 'corrected' ? 'QWEN CORRECTED · REVIEW REQUIRED' : activeAnswer.reasonableness_status.replaceAll('_', ' ')}</b><p>{activeAnswer.judge_reason || activeAnswer.fusion_reason}</p>{activeAnswer.judge_suggestion !== null ? <strong>{t('Suggestion', '建議')}: {formatValue(activeAnswer.judge_suggestion)}</strong> : null}</div><label>{t('Reviewer edit', '覆核員修改')}<textarea value={editValue} onChange={(event) => setEditValue(event.target.value)} /></label><label>{t('Review comment', '覆核備註')}<textarea value={comment} onChange={(event) => setComment(event.target.value)} placeholder={t('Record the evidence behind your decision…', '記錄決定所依據的證據…')} /></label><div className="review-actions"><button onClick={() => review('revert_scanner')} disabled={busy}>{t('Revert to scan', '還原掃描值')}</button>{activeAnswer.judge_suggestion !== null ? <button onClick={() => review('accept_qwen')} disabled={busy}>{t('Accept Qwen', '接受 Qwen')}</button> : null}<button className="primary" onClick={() => review('edit')} disabled={busy}>{t('Save human edit', '儲存人工修改')}</button></div></aside></div> : null}
  </>;
}

function ValueBox({ title, value, strong = false }: { title: string; value: unknown; strong?: boolean }) { return <div className={strong ? 'strong' : ''}><span>{title}</span><pre>{formatValue(value)}</pre></div>; }

export default function JobDetailPage() {
  const params = useParams<{ id: string }>();
  return <AppShell><JobDetail id={params.id} /></AppShell>;
}

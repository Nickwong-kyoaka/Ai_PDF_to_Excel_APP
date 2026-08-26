/* eslint-disable @next/next/no-img-element */
'use client';

import { PointerEvent, useEffect, useMemo, useRef, useState } from 'react';
import { AppShell, useLocale } from '../components/app-shell';
import { api } from '../lib/api';
import type { Job } from '../lib/types';

const classes = ['tick', 'cross', 'filled_mark', 'circle', 'underline_selection', 'strikeout'];

function AnnotationWorkspace() {
  const { t } = useLocale();
  const [jobs, setJobs] = useState<Job[]>([]);
  const [jobId, setJobId] = useState('');
  const [page, setPage] = useState(1);
  const [markClass, setMarkClass] = useState('tick');
  const [split, setSplit] = useState('train');
  const [boxes, setBoxes] = useState<Array<Record<string, unknown>>>([]);
  const [draft, setDraft] = useState<number[] | null>(null);
  const [start, setStart] = useState<[number, number] | null>(null);
  const [message, setMessage] = useState('');
  const canvas = useRef<HTMLDivElement>(null);

  useEffect(() => { api.jobs().then((items) => { setJobs(items); setJobId(items[0]?.id || ''); }).catch(() => undefined); }, []);
  useEffect(() => { if (jobId) api.annotations(jobId).then(setBoxes).catch(() => setBoxes([])); }, [jobId]);
  const job = useMemo(() => jobs.find((item) => item.id === jobId), [jobs, jobId]);
  const pageBoxes = boxes.filter((box) => Number(box.page_number) === page);

  function point(event: PointerEvent): [number, number] {
    const rect = canvas.current!.getBoundingClientRect();
    return [Math.max(0, Math.min(1, (event.clientX - rect.left) / rect.width)), Math.max(0, Math.min(1, (event.clientY - rect.top) / rect.height))];
  }
  function pointerDown(event: PointerEvent) { if (!jobId) return; const value = point(event); setStart(value); setDraft([value[0], value[1], value[0], value[1]]); event.currentTarget.setPointerCapture(event.pointerId); }
  function pointerMove(event: PointerEvent) { if (!start) return; const end = point(event); setDraft([Math.min(start[0], end[0]), Math.min(start[1], end[1]), Math.max(start[0], end[0]), Math.max(start[1], end[1])]); }
  function pointerUp() { setStart(null); }
  async function save() {
    if (!draft || draft[2] - draft[0] < .003 || draft[3] - draft[1] < .003) return;
    try {
      const annotation = await api.createAnnotation({ source_id: jobId, page_number: page, image_path: 'derived', mark_class: markClass, bbox: draft, split });
      setBoxes((items) => [...items, annotation as Record<string, unknown>]); setDraft(null); setMessage(t('Annotation saved.', '標註已儲存。'));
    } catch (reason) { setMessage(reason instanceof Error ? reason.message : String(reason)); }
  }

  return <>
    <header className="page-header"><div><p>{t('YOLO training data', 'YOLO 訓練資料')}</p><h1>{t('Mark annotation workspace', '記號標註工作區')}</h1><span>{t('Draw tight boxes around real handwritten selection marks. Never split pages from the same participant across train and test.', '在真實手寫選擇記號周圍繪製緊密方框。切勿將同一參與者的頁面分散至訓練及測試集。')}</span></div></header>
    <section className="annotation-layout">
      <aside className="annotation-controls">
        <label>{t('Source document', '來源文件')}<select value={jobId} onChange={(event) => { setJobId(event.target.value); setPage(1); }}><option value="">{t('Choose a job', '選擇工作')}</option>{jobs.map((item) => <option key={item.id} value={item.id}>{item.filename}</option>)}</select></label>
        <label>{t('Page', '頁')}<select value={page} onChange={(event) => setPage(Number(event.target.value))}>{Array.from({ length: job?.page_count || 0 }, (_, index) => <option key={index} value={index + 1}>{index + 1}</option>)}</select></label>
        <div><span className="control-label">{t('Mark class', '記號類別')}</span><div className="class-picker">{classes.map((item) => <button className={markClass === item ? 'active' : ''} onClick={() => setMarkClass(item)} key={item}><i className={`mark-sample mark-${item}`} />{item.replaceAll('_', ' ')}</button>)}</div></div>
        <label>{t('Dataset split', '資料集分組')}<select value={split} onChange={(event) => setSplit(event.target.value)}><option value="train">Train · 70%</option><option value="val">Validation · 15%</option><option value="test">Held-out test · 15%</option></select></label>
        <div className="annotation-stat"><span>{t('This page', '本頁')}</span><strong>{pageBoxes.length}</strong><small>{t('saved marks', '個已儲存記號')}</small></div>
        <button className="primary-action" disabled={!draft} onClick={save}>{t('Save selected box', '儲存選取方框')}<span>→</span></button>
        {message ? <p className="annotation-message">{message}</p> : null}
      </aside>
      <div className="annotation-stage"><div className="annotation-instruction">⌖ {t('Drag a box around one physical mark', '拖曳方框圈出一個實體記號')}</div>{jobId ? <div ref={canvas} className="annotation-canvas" onPointerDown={pointerDown} onPointerMove={pointerMove} onPointerUp={pointerUp}><img src={api.pageUrl(jobId, page)} alt={`Annotation page ${page}`} draggable={false} />{pageBoxes.map((box) => { const bbox = box.bbox as number[]; return <span className="saved-annotation" key={String(box.id)} style={{ left: `${bbox[0] * 100}%`, top: `${bbox[1] * 100}%`, width: `${(bbox[2] - bbox[0]) * 100}%`, height: `${(bbox[3] - bbox[1]) * 100}%` }}><b>{String(box.mark_class)}</b></span>; })}{draft ? <span className="draft-annotation" style={{ left: `${draft[0] * 100}%`, top: `${draft[1] * 100}%`, width: `${(draft[2] - draft[0]) * 100}%`, height: `${(draft[3] - draft[1]) * 100}%` }} /> : null}</div> : <div className="stage-empty">{t('Choose a processed document to begin annotation.', '選擇已處理文件以開始標註。')}</div>}</div>
    </section>
  </>;
}

export default function AnnotationsPage() { return <AppShell><AnnotationWorkspace /></AppShell>; }

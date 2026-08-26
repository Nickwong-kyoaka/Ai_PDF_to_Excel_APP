export type Role = 'admin' | 'operator' | 'reviewer';

export interface User {
  id: string;
  email: string;
  display_name: string;
  role: Role;
  is_active: boolean;
}

export interface ModelProfile {
  id: string;
  slug: string;
  name: string;
  extractor_model_id: string;
  judge_model_id: string;
  quantization: string;
  verification_mode: string;
  is_default: boolean;
}

export interface Group {
  id: string;
  group_index: number;
  start_page: number;
  end_page: number;
  participant_id: string | null;
  confidence: number;
  reason: string;
  confirmed: boolean;
}

export interface Artifact {
  id: string;
  kind: 'json' | 'excel' | 'annotated_pdf';
  draft: boolean;
  filename: string;
  created_at: string;
}

export interface Job {
  id: string;
  filename: string;
  media_type: string;
  status: string;
  page_count: number;
  language: string;
  groups_confirmed: boolean;
  progress: number;
  stage_message: string;
  error: string | null;
  draft_artifacts_ready: boolean;
  profile_snapshot: Record<string, unknown>;
  groups: Group[];
  artifacts: Artifact[];
  created_at: string;
  updated_at: string;
  expires_at: string;
}

export interface Answer {
  id: string;
  group_id: string;
  page_number: number;
  question_id: string;
  question_text: string;
  section: string;
  answer_type: string;
  allowed_options: Array<string | { label: string; bbox?: number[] }>;
  selected_options: unknown[];
  qwen_value: unknown;
  yolo_value: unknown;
  scanner_value: unknown;
  scanner_confidence: number;
  fusion_reason: string;
  evidence: Array<Record<string, unknown>>;
  reasonableness_status: string;
  judge_suggestion: unknown;
  judge_reason: string;
  judge_confidence: number;
  final_value: unknown;
  final_source: string;
  review_status: string;
  review_comment: string;
}

export interface ResultV2 {
  schema_version: '2.0';
  job: Record<string, unknown>;
  groups: Group[];
  answers: Answer[];
  unresolved_count: number;
}

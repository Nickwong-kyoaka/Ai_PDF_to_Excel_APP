import type { Job, ModelProfile, ResultV2, User } from './types';

const configuredBase = process.env.NEXT_PUBLIC_API_URL;
export const API_BASE = configuredBase || (
  typeof window !== 'undefined' && window.location.port === '3000'
    ? 'http://localhost:8000/api'
    : '/api'
);

let csrfMemory = '';

function csrfToken(): string {
  if (csrfMemory) return csrfMemory;
  if (typeof window !== 'undefined') csrfMemory = window.sessionStorage.getItem('formsight_csrf') || '';
  return csrfMemory;
}

function setCsrf(value: string): void {
  csrfMemory = value;
  if (typeof window !== 'undefined') window.sessionStorage.setItem('formsight_csrf', value);
}

async function request<T>(path: string, options: RequestInit = {}): Promise<T> {
  const method = (options.method || 'GET').toUpperCase();
  const headers = new Headers(options.headers);
  if (!(options.body instanceof FormData) && options.body && !headers.has('Content-Type')) {
    headers.set('Content-Type', 'application/json');
  }
  if (!['GET', 'HEAD', 'OPTIONS'].includes(method) && csrfToken()) {
    headers.set('X-CSRF-Token', csrfToken());
  }
  const response = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers,
    credentials: 'include',
  });
  if (response.status === 204) return undefined as T;
  const contentType = response.headers.get('content-type') || '';
  const data = contentType.includes('json') ? await response.json() : await response.text();
  if (!response.ok) {
    const message = typeof data === 'object' && data?.detail ? data.detail : `Request failed (${response.status})`;
    throw new Error(message);
  }
  return data as T;
}

export const api = {
  async login(email: string, password: string) {
    const result = await request<{ user: User; csrf_token: string }>('/auth/login', {
      method: 'POST',
      body: JSON.stringify({ email, password }),
    });
    setCsrf(result.csrf_token);
    return result.user;
  },
  async me() {
    const result = await request<{ user: User; csrf_token: string }>('/auth/me');
    setCsrf(result.csrf_token);
    return result.user;
  },
  async logout() {
    await request<void>('/auth/logout', { method: 'POST' });
    setCsrf('');
  },
  jobs: () => request<Job[]>('/jobs'),
  job: (id: string) => request<Job>(`/jobs/${id}`),
  profiles: () => request<ModelProfile[]>('/model-profiles'),
  result: (id: string) => request<ResultV2>(`/jobs/${id}/result`),
  async upload(file: File, profileId: string, language: string) {
    const body = new FormData();
    body.append('file', file);
    body.append('profile_id', profileId);
    body.append('language', language);
    return request<Job>('/jobs', { method: 'POST', body });
  },
  confirmGroups: (jobId: string, groups: Array<{ start_page: number; end_page: number; participant_id?: string | null }>) =>
    request<Job>(`/jobs/${jobId}/groups/confirm`, { method: 'POST', body: JSON.stringify({ groups }) }),
  cancel: (jobId: string) => request<Job>(`/jobs/${jobId}/cancel`, { method: 'POST' }),
  retry: (jobId: string) => request<Job>(`/jobs/${jobId}/retry`, { method: 'POST' }),
  review: (jobId: string, answerId: string, action: string, value?: unknown, comment = '') =>
    request(`/jobs/${jobId}/answers/${answerId}/review`, {
      method: 'POST',
      body: JSON.stringify({ action, value, comment }),
    }),
  finalize: (jobId: string) => request<Job>(`/jobs/${jobId}/finalize`, { method: 'POST' }),
  preflight: () => request<Record<string, unknown>>('/system/preflight'),
  rules: () => request<Array<Record<string, unknown>>>('/rules'),
  users: () => request<User[]>('/admin/users'),
  createUser: (data: Record<string, unknown>) => request<User>('/admin/users', { method: 'POST', body: JSON.stringify(data) }),
  createRule: (data: Record<string, unknown>) => request('/admin/rules', { method: 'POST', body: JSON.stringify(data) }),
  annotations: (sourceId?: string) => request<Array<Record<string, unknown>>>(`/admin/annotations${sourceId ? `?source_id=${encodeURIComponent(sourceId)}` : ''}`),
  createAnnotation: (data: Record<string, unknown>) => request('/admin/annotations', { method: 'POST', body: JSON.stringify(data) }),
  artifactUrl: (jobId: string, artifactId: string) => `${API_BASE}/jobs/${jobId}/artifacts/${artifactId}`,
  pageUrl: (jobId: string, page: number) => `${API_BASE}/jobs/${jobId}/pages/${page}`,
};

const e = React.createElement;
const { useEffect, useMemo, useRef, useState } = React;

const REASON_OPTIONS = {
  creator: [
    ['Creator / personal brand', 'creator_individual_or_creator_brand'],
    ['Social profile evidence', 'creator_individual_or_creator_brand'],
    ['UGC / product review creator', 'creator_individual_or_creator_brand'],
    ['Creator-commerce / affiliate', 'creator_individual_or_creator_brand'],
    ['Other creator evidence', 'other'],
  ],
  not_creator: [
    ['Business / company', 'business_or_company'],
    ['Agency / network', 'agency_or_network'],
    ['Media / editorial publisher', 'publisher_or_media'],
    ['Utility or non-creator site', 'utility_or_non_creator'],
    ['Not enough creator evidence', 'insufficient_information'],
    ['Other', 'other'],
  ],
  unsure: [
    ['Missing or weak website', 'insufficient_information'],
    ['Missing or weak description', 'insufficient_information'],
    ['Conflicting signals', 'insufficient_information'],
    ['Needs manual escalation', 'insufficient_information'],
    ['Other', 'other'],
  ],
};

const DECISION_META = {
  creator: {
    title: 'Creator',
    subtitle: 'You think this publisher belongs in the Influencer / Content Creator cluster.',
    className: 'accept',
  },
  not_creator: {
    title: 'Not creator',
    subtitle: 'You think this publisher does not belong in the Influencer / Content Creator cluster.',
    className: 'reject',
  },
  unsure: {
    title: 'Unsure',
    subtitle: 'There is not enough evidence to make a confident decision.',
    className: 'unsure',
  },
};

function safe(value, fallback = '') {
  if (value === null || value === undefined) return fallback;
  return String(value);
}

function pct(done, total) {
  if (!total) return '0.0';
  return ((done / total) * 100).toFixed(1);
}

function formatScore(value) {
  if (value === null || value === undefined || value === '') return '—';
  const num = Number(value);
  if (Number.isNaN(num)) return String(value);
  return Number.isInteger(num) ? String(num) : num.toFixed(1);
}

function normalizeUrl(url) {
  const u = safe(url).trim();
  if (!u) return '';
  if (/^https?:\/\//i.test(u)) return u;
  return `https://${u}`;
}

function extractDomain(url) {
  const u = normalizeUrl(url);
  if (!u) return '';
  try {
    return new URL(u).hostname.replace(/^www\./, '');
  } catch (_) {
    return u.replace(/^https?:\/\//, '').replace(/^www\./, '').split('/')[0];
  }
}

function bucketChip(bucket) {
  if (['p1_current_cluster_strong', 'p3_hidden_positive_strong'].includes(bucket)) return 'green';
  if (['p2_current_cluster', 'p4_hidden_positive'].includes(bucket)) return 'blue';
  if (['p5_adjacent_supported', 'p6_social_and_keyword'].includes(bucket)) return 'amber';
  return 'slate';
}

function confidenceChip(conf) {
  if (conf === 'likely_creator') return 'green';
  if (conf === 'possible_creator') return 'blue';
  if (conf === 'creator_commercial_business_like') return 'amber';
  if (conf === 'likely_not_creator') return 'red';
  return 'slate';
}

async function apiGet(path) {
  const res = await fetch(path, { credentials: 'include' });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.detail || data.message || `Request failed: ${res.status}`);
  return data;
}

async function apiPost(path, body) {
  const res = await fetch(path, {
    method: 'POST',
    credentials: 'include',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.detail || data.message || `Request failed: ${res.status}`);
  return data;
}

function Topbar({ reviewer, progress }) {
  const total = progress?.total_rows || 0;
  const reviewed = progress?.reviewed_rows || 0;
  const percent = pct(reviewed, total);

  return e('div', { className: 'topbar' },
    e('div', { className: 'brand-row' },
      e('div', { className: 'logo-orb' }, 'IR'),
      e('div', null,
        e('div', { className: 'brand-title' }, 'Influencer / Content Creator Review'),
        e('div', { className: 'brand-subtitle' }, 'Flashcard feedback loop for Publisher Success')
      )
    ),
    e('div', { className: 'progress-zone' },
      e('div', { className: 'progress-label' },
        e('span', null, `${reviewed} / ${total} reviewed`),
        e('span', null, `${percent}% complete`)
      ),
      e('div', { className: 'progress-track' },
        e('div', { className: 'progress-fill', style: { width: `${percent}%` } })
      )
    ),
    e('div', { className: 'reviewer-badge' },
      e('div', { className: 'reviewer-label' }, 'Signed in as'),
      e('div', { className: 'reviewer-name' }, safe(reviewer?.reviewer_name, 'Reviewer')),
      e('div', { className: 'reviewer-email' }, safe(reviewer?.reviewer_email, ''))
    )
  );
}

function Chip({ label, color }) {
  return e('span', { className: `chip ${color || 'slate'}` }, label);
}

function Flashcard({ row, index, total, remaining, direction }) {
  if (!row) return null;

  const website = normalizeUrl(row.PublisherWebSite);
  const domain = extractDomain(row.PublisherWebSite);
  const bucket = safe(row.priority_bucket);
  const conf = safe(row.review_confidence_hint);
  const bucketLabel = safe(row.priority_bucket_label, bucket || 'Priority bucket');
  const confLabel = safe(row.review_confidence_hint_label, conf || 'No hint');
  const description = safe(row.PublisherDescription, 'No description provided.');

  return e('div', { className: 'card-wrap' },
    e('div', { key: `${row.review_batch_id}-${row.PublisherKey}-${index}`, className: `flashcard ${direction === 'prev' ? 'slide-prev' : 'slide-next'}` },
      e('div', { className: 'card-kicker' }, `Card ${index + 1} of ${total} unreviewed · ${remaining} remaining`),
      e('div', { className: 'chip-row' },
        e(Chip, { label: bucketLabel, color: bucketChip(bucket) }),
        e(Chip, { label: confLabel, color: confidenceChip(conf) }),
        e(Chip, { label: `Website: ${safe(row.website_type, 'unknown')}`, color: 'slate' })
      ),
      e('div', { className: 'publisher-title' }, safe(row.Publisher, 'Unknown publisher')),
      e('div', { className: 'publisher-site' },
        website ? e('a', { href: website, target: '_blank', rel: 'noopener noreferrer' }, domain || website) : 'No website provided'
      ),
      e('div', { className: 'description' }, description),
      e('div', { className: 'stats-grid' },
        e('div', { className: 'stat' },
          e('div', { className: 'stat-label' }, 'Current type'),
          e('div', { className: 'stat-value' }, safe(row.current_publisher_type_group, '—'))
        ),
        e('div', { className: 'stat' },
          e('div', { className: 'stat-label' }, 'Current subvertical'),
          e('div', { className: 'stat-value' }, safe(row.current_publisher_subvertical, '—'))
        ),
        e('div', { className: 'stat' },
          e('div', { className: 'stat-label' }, 'Evidence / risk'),
          e('div', { className: 'stat-value' }, `${formatScore(row.creator_evidence_score)} / ${formatScore(row.non_creator_risk_score)}`)
        )
      )
    )
  );
}

function ContextPanel({ row }) {
  const [open, setOpen] = useState(false);
  if (!row) return null;

  return e('div', { className: 'context-card' },
    e('button', { className: 'context-toggle', onClick: () => setOpen(!open) },
      open ? 'Hide queue context' : 'Why this publisher is in the queue'
    ),
    open && e('div', { className: 'context-body' },
      e('div', { className: 'context-row' },
        e('div', { className: 'context-label' }, 'Why this publisher is here'),
        e('div', { className: 'context-value' }, safe(row.priority_bucket_description, 'No bucket explanation available.'))
      ),
      e('div', { className: 'context-row' },
        e('div', { className: 'context-label' }, 'Reviewer guidance'),
        e('div', { className: 'context-value' }, safe(row.reviewer_guidance_hint, 'Reviewer should inspect manually.'))
      ),
      e('div', { className: 'context-row' },
        e('div', { className: 'context-label' }, 'Evidence detected'),
        e('div', { className: 'context-value' }, safe(row.review_evidence_summary, 'No evidence summary available.'))
      ),
      safe(row.reviewer_warning_message) && e('div', { className: 'warning-box' }, safe(row.reviewer_warning_message)),
      e('div', { className: 'context-note' }, 'These are supporting signals only. The final decision should still be based on reviewer judgement.')
    )
  );
}

function ActionButtons({ onDecision }) {
  return e('div', { className: 'action-zone' },
    e('div', { className: 'action-help' }, 'Browse freely with the side arrows. Choose a decision only when you are ready.'),
    e('div', { className: 'action-grid' },
      e('button', { className: 'action-btn reject', onClick: () => onDecision('not_creator') },
        e('span', { className: 'action-icon' }, '←'),
        e('span', null, e('div', { className: 'action-title' }, 'Not creator'), e('div', { className: 'action-sub' }, 'Remove / exclude'))
      ),
      e('button', { className: 'action-btn unsure', onClick: () => onDecision('unsure') },
        e('span', { className: 'action-icon' }, '?'),
        e('span', null, e('div', { className: 'action-title' }, 'Unsure'), e('div', { className: 'action-sub' }, 'Needs judgement'))
      ),
      e('button', { className: 'action-btn accept', onClick: () => onDecision('creator') },
        e('span', null, e('div', { className: 'action-title' }, 'Creator'), e('div', { className: 'action-sub' }, 'Keep / add')),
        e('span', { className: 'action-icon' }, '→')
      )
    )
  );
}

function ReasonPanel({ row, decisionType, onCancel, onSaved, setToast }) {
  const panelRef = useRef(null);
  const options = REASON_OPTIONS[decisionType] || [];
  const meta = DECISION_META[decisionType];
  const [selected, setSelected] = useState(options[0]?.[0] || '');
  const [comment, setComment] = useState('');
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    setSelected(options[0]?.[0] || '');
    setComment('');
    setTimeout(() => {
      panelRef.current?.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }, 80);
  }, [decisionType, row?.PublisherKey]);

  async function saveDecision() {
    const found = options.find(([label]) => label === selected) || options[0];
    if (!found) return;
    setSaving(true);
    try {
      await apiPost('/api/decision', {
        review_batch_id: row.review_batch_id,
        PublisherKey: Number(row.PublisherKey),
        decision_type: decisionType,
        review_reason_category: found[1],
        review_reason_detail: found[0],
        review_comment: comment,
      });
      setToast({ type: 'success', message: 'Decision saved. Loading next publisher…' });
      onSaved(row);
    } catch (err) {
      setToast({ type: 'error', message: err.message || 'Failed to save decision.' });
    } finally {
      setSaving(false);
    }
  }

  return e('div', { className: 'reason-panel', ref: panelRef },
    e('div', { className: 'reason-title' }, meta.title),
    e('div', { className: 'reason-subtitle' }, meta.subtitle),
    e('div', { className: 'reason-options' },
      options.map(([label]) => e('button', {
        key: label,
        className: `reason-chip ${selected === label ? 'selected' : ''}`,
        onClick: () => setSelected(label),
      }, label))
    ),
    e('textarea', {
      className: 'comment-box',
      value: comment,
      onChange: (ev) => setComment(ev.target.value),
      placeholder: 'Optional comment. Add detail only if this case is ambiguous, misleading, or useful for future model improvement.',
    }),
    e('div', { className: 'reason-actions' },
      e('button', { className: 'save-btn', onClick: saveDecision, disabled: saving }, saving ? 'Saving…' : 'Save decision & next'),
      e('button', { className: 'cancel-btn', onClick: onCancel, disabled: saving }, 'Cancel')
    )
  );
}

function Toast({ toast, onClose }) {
  useEffect(() => {
    if (!toast) return undefined;
    const id = setTimeout(onClose, 4200);
    return () => clearTimeout(id);
  }, [toast]);
  if (!toast) return null;
  return e('div', { className: `toast ${toast.type}` }, toast.message);
}

function App() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [reviewer, setReviewer] = useState({});
  const [queue, setQueue] = useState([]);
  const [progress, setProgress] = useState({ total_rows: 0, reviewed_rows: 0, remaining_rows: 0 });
  const [index, setIndex] = useState(0);
  const [direction, setDirection] = useState('next');
  const [decisionType, setDecisionType] = useState(null);
  const [toast, setToast] = useState(null);

  const current = queue[index] || null;

  async function load() {
    setLoading(true);
    setError(null);
    try {
      const data = await apiGet('/api/queue');
      setReviewer(data.reviewer || {});
      setQueue(data.queue || []);
      setProgress(data.progress || { total_rows: 0, reviewed_rows: 0, remaining_rows: 0 });
      setIndex((prev) => Math.min(prev, Math.max(0, (data.queue || []).length - 1)));
    } catch (err) {
      setError(err.message || 'Failed to load review queue.');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { load(); }, []);

  useEffect(() => {
    function onKey(ev) {
      if (ev.key === 'ArrowRight') next();
      if (ev.key === 'ArrowLeft') prev();
      if (!decisionType) {
        if (ev.key.toLowerCase() === 'c') chooseDecision('creator');
        if (ev.key.toLowerCase() === 'n') chooseDecision('not_creator');
        if (ev.key.toLowerCase() === 'u') chooseDecision('unsure');
      }
    }
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [index, queue.length, decisionType, current]);

  function prev() {
    setDirection('prev');
    setDecisionType(null);
    setIndex((i) => Math.max(0, i - 1));
  }

  function next() {
    setDirection('next');
    setDecisionType(null);
    setIndex((i) => Math.min(queue.length - 1, i + 1));
  }

  function chooseDecision(type) {
    setDecisionType(type);
  }

  function onSaved(savedRow) {
    const newQueue = queue.filter((item) => !(item.review_batch_id === savedRow.review_batch_id && Number(item.PublisherKey) === Number(savedRow.PublisherKey)));
    setQueue(newQueue);
    setProgress((p) => ({
      ...p,
      reviewed_rows: (p.reviewed_rows || 0) + 1,
      remaining_rows: Math.max(0, (p.remaining_rows || newQueue.length + 1) - 1),
    }));
    setDecisionType(null);
    setDirection('next');
    setIndex((i) => Math.min(i, Math.max(0, newQueue.length - 1)));
  }

  if (loading) {
    return e('div', { className: 'app-shell' },
      e('div', { className: 'loading-state' },
        e('div', { className: 'empty-title' }, 'Loading review queue…'),
        e('div', { className: 'empty-subtitle' }, 'Connecting to Databricks Delta tables and preparing the flashcards.')
      )
    );
  }

  if (error) {
    return e('div', { className: 'app-shell' },
      e('div', { className: 'error-state' },
        e('div', { className: 'empty-title' }, 'Something went wrong'),
        e('div', { className: 'empty-subtitle' }, error),
        e('button', { className: 'save-btn', onClick: load, style: { marginTop: 18, minWidth: 180 } }, 'Retry')
      )
    );
  }

  if (!current) {
    return e('div', { className: 'app-shell' },
      e(Topbar, { reviewer, progress }),
      e('div', { className: 'empty-state' },
        e('div', { className: 'empty-title' }, 'All cards reviewed'),
        e('div', { className: 'empty-subtitle' }, 'This review batch is complete. Decisions are available in the Databricks Delta decisions table and joined results view.')
      ),
      e(Toast, { toast, onClose: () => setToast(null) })
    );
  }

  return e('div', { className: 'app-shell' },
    e(Topbar, { reviewer, progress }),
    e('main', { className: 'main-view' },
      e('div', { className: 'stage' },
        e('button', { className: 'nav-button', onClick: prev, disabled: index <= 0, title: 'Previous card' }, '‹'),
        e(Flashcard, { row: current, index, total: queue.length, remaining: queue.length, direction }),
        e('button', { className: 'nav-button', onClick: next, disabled: index >= queue.length - 1, title: 'Next card' }, '›')
      ),
      e(ContextPanel, { row: current }),
      e(ActionButtons, { onDecision: chooseDecision }),
      decisionType && e(ReasonPanel, {
        row: current,
        decisionType,
        onCancel: () => setDecisionType(null),
        onSaved,
        setToast,
      })
    ),
    e(Toast, { toast, onClose: () => setToast(null) })
  );
}

ReactDOM.createRoot(document.getElementById('root')).render(e(App));

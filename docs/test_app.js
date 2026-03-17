
/* ──────────────────────────────────────────────────────────
   STATE
   ────────────────────────────────────────────────────────── */
let connections = [];
let editingConnectionName = null;
let activeConnectionName = null;

/* ──────────────────────────────────────────────────────────
   HELPERS
   ────────────────────────────────────────────────────────── */
function $(sel) { return document.querySelector(sel); }
function $$(sel) { return document.querySelectorAll(sel); }

async function api(method, path, body) {
  const opts = { method, headers: { 'Content-Type': 'application/json' } };
  if (body !== undefined) opts.body = JSON.stringify(body);
  const res = await fetch(path, opts);
  if (!res.ok) {
    let msg = res.statusText;
    try { const j = await res.json(); msg = j.detail || j.error || msg; } catch(_) {}
    throw new Error(msg);
  }
  const text = await res.text();
  return text ? JSON.parse(text) : null;
}

function esc(s) {
  if (s == null) return '';
  const d = document.createElement('div');
  d.textContent = String(s);
  return d.innerHTML;
}

/* ── Toast ── */
function toast(message, type) {
  type = type || 'info';
  const el = document.createElement('div');
  el.className = 'toast ' + type;
  const icons = { success: '\u2713', error: '\u2717', info: '\u2139' };
  el.innerHTML = '<strong>' + (icons[type] || '') + '</strong> ' + esc(message);
  const container = $('#toast-container');
  container.appendChild(el);
  setTimeout(function() {
    el.classList.add('fade-out');
    setTimeout(function() { el.remove(); }, 250);
  }, 3500);
}

function toggleMask(id) {
  const el = document.getElementById(id);
  el.type = el.type === 'password' ? 'text' : 'password';
}

/* ──────────────────────────────────────────────────────────
   TABS
   ────────────────────────────────────────────────────────── */
$('#tab-nav').addEventListener('click', function(e) {
  if (e.target.tagName !== 'BUTTON') return;
  $$('#tab-nav button').forEach(function(b) { b.classList.remove('active'); });
  e.target.classList.add('active');
  $$('.tab-panel').forEach(function(p) { p.classList.remove('active'); });
  $('#panel-' + e.target.dataset.tab).classList.add('active');

  // Lazy-load data for the tab
  var tab = e.target.dataset.tab;
  if (tab === 'status') loadStatus();
  if (tab === 'knowledge') { loadRules(); loadNotes(); loadKeywords(); loadPatterns(); loadEmbeddings(); }
  if (tab === 'datamanagement') { loadSources(); loadJobs(); }
  if (tab === 'connections') loadConnections();
});


/* ──────────────────────────────────────────────────────────
   CONNECTIONS
   ────────────────────────────────────────────────────────── */
function getConnFormData() {
  return {
    name:         $('#conn-name').value.trim(),
    dialect:      $('#conn-dialect').value,
    host:         $('#conn-host').value.trim(),
    port:         $('#conn-port').value.trim(),
    database:     $('#conn-database').value.trim(),
    schema:       $('#conn-schema').value.trim(),
    user:         $('#conn-user').value.trim(),
    password_env: $('#conn-password-env').value.trim()
  };
}

function clearConnForm() {
  ['conn-name','conn-dialect','conn-host','conn-port','conn-database','conn-schema','conn-user','conn-password-env'].forEach(function(id) {
    var el = document.getElementById(id);
    if (el.tagName === 'SELECT') el.selectedIndex = 0; else el.value = '';
  });
  editingConnectionName = null;
  $('#conn-form-heading').textContent = 'Add Connection';
  $('#conn-edit-badge').style.display = 'none';
  $('#conn-cancel-btn').style.display = 'none';
  $('#conn-name').removeAttribute('readonly');
}

function editConnection(name) {
  var conn = connections.find(function(c) { return c.name === name; });
  if (!conn) return;
  editingConnectionName = name;
  $('#conn-name').value = conn.name || '';
  $('#conn-dialect').value = conn.dialect || '';
  $('#conn-host').value = conn.host || '';
  $('#conn-port').value = conn.port || '';
  $('#conn-database').value = conn.database || '';
  $('#conn-schema').value = conn.schema || '';
  $('#conn-user').value = conn.user || '';
  $('#conn-password-env').value = conn.password_env || '';
  $('#conn-form-heading').textContent = 'Edit Connection';
  $('#conn-edit-badge').style.display = 'inline';
  $('#conn-cancel-btn').style.display = 'inline-flex';
  $('#conn-name').setAttribute('readonly', 'readonly');
  window.scrollTo({ top: 0, behavior: 'smooth' });
}

function cancelEditConnection() {
  clearConnForm();
}

async function saveConnection() {
  var data = getConnFormData();
  if (!data.name) { toast('Connection name is required.', 'error'); return; }
  if (!data.dialect) { toast('Dialect is required.', 'error'); return; }
  try {
    await api('POST', '/api/connections', data);
    toast('Connection "' + data.name + '" saved.', 'success');
    clearConnForm();
    loadConnections();
  } catch (e) {
    toast('Save failed: ' + e.message, 'error');
  }
}

async function testConnection() {
  var data = getConnFormData();
  if (!data.dialect) { toast('Select a dialect first.', 'error'); return; }
  try {
    var result = await api('POST', '/api/connections/test', data);
    if (result && result.success) {
      toast('Connection test successful!', 'success');
    } else {
      toast('Connection test failed: ' + (result && result.error ? result.error : 'Unknown error'), 'error');
    }
  } catch (e) {
    toast('Connection test failed: ' + e.message, 'error');
  }
}

async function deleteConnection(name) {
  if (!confirm('Delete connection "' + name + '"?')) return;
  try {
    await api('DELETE', '/api/connections/' + encodeURIComponent(name));
    toast('Connection "' + name + '" deleted.', 'success');
    loadConnections();
  } catch (e) {
    toast('Delete failed: ' + e.message, 'error');
  }
}

function renderConnections() {
  var list = $('#connections-list');
  if (!connections || connections.length === 0) {
    list.innerHTML = '<div class="empty-state">No connections configured yet.</div>';
    return;
  }
  var html = '<div class="item-list">';
  connections.forEach(function(c) {
    var nameSafe = esc(c.name).replace(/'/g, "\\'");
    var isActive = c.name === activeConnectionName;
    var dialectBadge = c.dialect ? '<code>' + esc(c.dialect) + '</code>' : '';
    var hostInfo = c.host ? esc(c.host) + (c.port ? ':' + esc(c.port) : '') : '';
    var dbInfo = c.database ? '/' + esc(c.database) : '';
    var schemaInfo = c.schema ? ' (' + esc(c.schema) + ')' : '';
    var activeDot = isActive ? '<span style="color:var(--success);font-size:10px;margin-right:6px" title="Active connection">&#9679;</span>' : '';
    var activeLabel = isActive ? ' <span style="font-size:11px;color:var(--success);font-weight:600">ACTIVE</span>' : '';
    var activeBtn = isActive
      ? ''
      : '<button class="btn btn-sm btn-secondary" onclick="setActiveConnection(\'' + nameSafe + '\')">Set Active</button>';
    html += '<div class="item-row"' + (isActive ? ' style="border-left:2px solid var(--success);padding-left:10px"' : '') + '>'
      + '<div class="item-info">'
      + '  <span class="item-label">' + activeDot + esc(c.name) + ' ' + dialectBadge + activeLabel + '</span>'
      + '  <span class="item-meta">' + hostInfo + dbInfo + schemaInfo + '</span>'
      + '</div>'
      + '<div style="display:flex;gap:6px">'
      + activeBtn
      + '  <button class="btn btn-sm btn-secondary" onclick="editConnection(\'' + nameSafe + '\')">Edit</button>'
      + '  <button class="btn btn-sm btn-danger" onclick="deleteConnection(\'' + nameSafe + '\')">Delete</button>'
      + '</div>'
      + '</div>';
  });
  html += '</div>';
  list.innerHTML = html;
}

async function loadConnections() {
  try {
    var data = await api('GET', '/api/connections');
    connections = Array.isArray(data) ? data : (data && data.connections ? data.connections : []);
    // Load active connection
    try {
      var cfg = await api('GET', '/api/config');
      activeConnectionName = (cfg && cfg.default_connection) || null;
    } catch (_) {
      activeConnectionName = null;
    }
    renderConnections();
  } catch (e) {
    connections = [];
    renderConnections();
  }
}

async function setActiveConnection(name) {
  try {
    await api('POST', '/api/config', { default_connection: name });
    activeConnectionName = name;
    renderConnections();
    toast('Active connection set to ' + name, 'success');
  } catch (e) {
    toast('Failed: ' + e.message, 'error');
  }
}

/* ──────────────────────────────────────────────────────────
   SOURCES (embedding ingestion credentials)
   ────────────────────────────────────────────────────────── */
function _loadCred(el, val) {
  // val is either a string (non-sensitive) or {has_value: bool} (sensitive)
  if (val && typeof val === 'object' && 'has_value' in val) {
    el.value = '';
    el.placeholder = val.has_value ? 'Configured (leave blank to keep)' : el.dataset.hint || 'Not set';
  } else {
    el.value = val || '';
  }
}

async function loadSources() {
  try {
    var data = await api('GET', '/api/credentials');
    if (data) {
      if (data.openai) {
        _loadCred($('#cred-openai-key'), data.openai.api_key);
      }
      if (data.looker) {
        _loadCred($('#cred-looker-url'), data.looker.base_url);
        _loadCred($('#cred-looker-id'), data.looker.client_id);
        _loadCred($('#cred-looker-secret'), data.looker.client_secret);
      }
      if (data.databricks) {
        _loadCred($('#cred-dbx-url'), data.databricks.workspace_url);
        _loadCred($('#cred-dbx-token'), data.databricks.token);
      }
      if (data.mode) {
        _loadCred($('#cred-mode-workspace'), data.mode.workspace);
        _loadCred($('#cred-mode-token'), data.mode.token);
        _loadCred($('#cred-mode-secret'), data.mode.secret);
      }
      if (data.redash) {
        _loadCred($('#cred-redash-url'), data.redash.url);
        _loadCred($('#cred-redash-key'), data.redash.api_key);
      }
      if (data.sigma) {
        _loadCred($('#cred-sigma-host'), data.sigma.host);
        _loadCred($('#cred-sigma-id'), data.sigma.client_id);
        _loadCred($('#cred-sigma-secret'), data.sigma.client_secret);
      }
      if (data.superset) {
        _loadCred($('#cred-superset-url'), data.superset.url);
        _loadCred($('#cred-superset-user'), data.superset.username);
        _loadCred($('#cred-superset-pass'), data.superset.password);
      }
    }
  } catch (_) {}
}

async function saveSources() {
  var payload = {
    openai: {
      api_key: $('#cred-openai-key').value.trim()
    },
    looker: {
      base_url:      $('#cred-looker-url').value.trim(),
      client_id:     $('#cred-looker-id').value.trim(),
      client_secret: $('#cred-looker-secret').value.trim()
    },
    databricks: {
      workspace_url: $('#cred-dbx-url').value.trim(),
      token:         $('#cred-dbx-token').value.trim()
    },
    mode: {
      workspace: $('#cred-mode-workspace').value.trim(),
      token:     $('#cred-mode-token').value.trim(),
      secret:    $('#cred-mode-secret').value.trim()
    },
    redash: {
      url:     $('#cred-redash-url').value.trim(),
      api_key: $('#cred-redash-key').value.trim()
    },
    sigma: {
      host:          $('#cred-sigma-host').value.trim(),
      client_id:     $('#cred-sigma-id').value.trim(),
      client_secret: $('#cred-sigma-secret').value.trim()
    },
    superset: {
      url:      $('#cred-superset-url').value.trim(),
      username: $('#cred-superset-user').value.trim(),
      password: $('#cred-superset-pass').value.trim()
    }
  };
  try {
    await api('POST', '/api/credentials', payload);
    toast('Sources saved.', 'success');
  } catch (e) {
    toast('Save failed: ' + e.message, 'error');
  }
}

/* ──────────────────────────────────────────────────────────
   KNOWLEDGE — Inner tab switching
   ────────────────────────────────────────────────────────── */
document.addEventListener('click', function(e) {
  var btn = e.target.closest('.k-tab');
  if (!btn) return;
  var card = btn.closest('.card');
  card.querySelectorAll('.k-tab').forEach(function(t) { t.classList.remove('active'); });
  card.querySelectorAll('.k-panel').forEach(function(p) { p.classList.remove('active'); });
  btn.classList.add('active');
  var tab = btn.dataset.ktab;
  var panel = card.querySelector('#kpanel-' + tab);
  if (panel) panel.classList.add('active');
});

function kShowForm(name) {
  document.getElementById(name + '-form').style.display = 'flex';
  document.getElementById(name + '-add-btn').style.display = 'none';
}
function kHideForm(name) {
  document.getElementById(name + '-form').style.display = 'none';
  document.getElementById(name + '-add-btn').style.display = 'inline-flex';
}

/* ──────────────────────────────────────────────────────────
   KNOWLEDGE — Global Rules
   ────────────────────────────────────────────────────────── */
var rulesData = {};

async function loadRules() {
  try {
    var data = await api('GET', '/api/rules');
    rulesData = (data && typeof data === 'object' && !Array.isArray(data)) ? data : {};
    renderRules();
  } catch (_) {
    rulesData = {};
    renderRules();
  }
}

function renderRules() {
  var list = $('#rules-list');
  var entries = Object.entries(rulesData);
  if (entries.length === 0) {
    list.innerHTML = '<div class="k-empty">No rules defined yet. Add brand colors, coding preferences, or conventions.</div>';
    return;
  }
  var html = '';
  entries.sort(function(a, b) { return a[0].localeCompare(b[0]); }).forEach(function(e) {
    var name = e[0], content = e[1] || '';
    var nameSafe = esc(name).replace(/'/g, "\\'");
    html += '<div class="k-item">'
      + '<div class="k-item-body">'
      + '  <div class="k-item-title">' + esc(name) + '</div>'
      + (content ? '<div class="k-item-text">' + esc(content) + '</div>' : '')
      + '</div>'
      + '<div class="k-actions">'
      + '  <button class="k-icon-btn" title="Edit" onclick="editRule(\'' + nameSafe + '\')">&#9998;</button>'
      + '  <button class="k-icon-btn danger" title="Delete" onclick="deleteRule(\'' + nameSafe + '\')">&#10005;</button>'
      + '</div>'
      + '</div>';
  });
  list.innerHTML = html;
}

function addNewRule() {
  $('#rm-old-name').value = '';
  $('#rm-name').value = '';
  $('#rm-content').value = '';
  document.querySelector('#rule-modal .modal-title').textContent = 'Add Rule';
  $('#rule-modal').classList.add('active');
}

function editRule(name) {
  $('#rm-old-name').value = name;
  $('#rm-name').value = name;
  $('#rm-content').value = rulesData[name] || '';
  document.querySelector('#rule-modal .modal-title').textContent = 'Edit Rule';
  $('#rule-modal').classList.add('active');
}

function closeRuleModal() {
  $('#rule-modal').classList.remove('active');
}

async function saveRuleEdit() {
  var oldName = $('#rm-old-name').value;
  var newName = $('#rm-name').value.trim();
  var content = $('#rm-content').value.trim();
  if (!newName) { toast('Name is required.', 'error'); return; }
  try {
    if (oldName && oldName !== newName) {
      await api('DELETE', '/api/rules/' + encodeURIComponent(oldName));
    }
    await api('POST', '/api/rules', { name: newName, content: content });
    toast('Rule saved.', 'success');
    closeRuleModal();
    loadRules();
  } catch (e) {
    toast('Save failed: ' + e.message, 'error');
  }
}

async function deleteRule(name) {
  if (!confirm('Delete rule "' + name + '"?')) return;
  try {
    await api('DELETE', '/api/rules/' + encodeURIComponent(name));
    toast('Rule deleted.', 'success');
    loadRules();
  } catch (e) {
    toast('Delete failed: ' + e.message, 'error');
  }
}

/* ──────────────────────────────────────────────────────────
   KNOWLEDGE — Project Notes
   ────────────────────────────────────────────────────────── */
var notesData = {};

async function loadNotes() {
  try {
    var data = await api('GET', '/api/notes');
    notesData = (data && typeof data === 'object' && !Array.isArray(data)) ? data : {};
    renderNotes();
  } catch (_) {
    notesData = {};
    renderNotes();
  }
}

function renderNotes() {
  var list = $('#notes-list');
  var entries = Object.entries(notesData);
  if (entries.length === 0) {
    list.innerHTML = '<div class="k-empty">No project notes yet. Add data quirks, schema info, or findings.</div>';
    return;
  }
  var html = '';
  entries.sort(function(a, b) { return a[0].localeCompare(b[0]); }).forEach(function(e) {
    var name = e[0], content = e[1] || '';
    var nameSafe = esc(name).replace(/'/g, "\\'");
    html += '<div class="k-item">'
      + '<div class="k-item-body">'
      + '  <div class="k-item-title">' + esc(name) + '</div>'
      + (content ? '<div class="k-item-text">' + esc(content) + '</div>' : '')
      + '</div>'
      + '<div class="k-actions">'
      + '  <button class="k-icon-btn" title="Edit" onclick="editNote(\'' + nameSafe + '\')">&#9998;</button>'
      + '  <button class="k-icon-btn danger" title="Delete" onclick="deleteNote(\'' + nameSafe + '\')">&#10005;</button>'
      + '</div>'
      + '</div>';
  });
  list.innerHTML = html;
}

function addNewNote() {
  $('#nm-old-name').value = '';
  $('#nm-name').value = '';
  $('#nm-content').value = '';
  document.querySelector('#note-modal .modal-title').textContent = 'Add Note';
  $('#note-modal').classList.add('active');
}

function editNote(name) {
  $('#nm-old-name').value = name;
  $('#nm-name').value = name;
  $('#nm-content').value = notesData[name] || '';
  document.querySelector('#note-modal .modal-title').textContent = 'Edit Note';
  $('#note-modal').classList.add('active');
}

function closeNoteModal() {
  $('#note-modal').classList.remove('active');
}

async function saveNoteEdit() {
  var oldName = $('#nm-old-name').value;
  var newName = $('#nm-name').value.trim();
  var content = $('#nm-content').value.trim();
  if (!newName) { toast('Name is required.', 'error'); return; }
  try {
    if (oldName && oldName !== newName) {
      await api('DELETE', '/api/notes/' + encodeURIComponent(oldName));
    }
    await api('POST', '/api/notes', { name: newName, content: content });
    toast('Note saved.', 'success');
    closeNoteModal();
    loadNotes();
  } catch (e) {
    toast('Save failed: ' + e.message, 'error');
  }
}

async function deleteNote(name) {
  if (!confirm('Delete note "' + name + '"?')) return;
  try {
    await api('DELETE', '/api/notes/' + encodeURIComponent(name));
    toast('Note deleted.', 'success');
    loadNotes();
  } catch (e) {
    toast('Delete failed: ' + e.message, 'error');
  }
}

/* ──────────────────────────────────────────────────────────
   KNOWLEDGE — Glossary
   API returns { term: definition, ... } dict
   ────────────────────────────────────────────────────────── */
var glossaryTerms = {};

async function loadGlossary() {
  try {
    var data = await api('GET', '/api/glossary');
    glossaryTerms = (data && typeof data === 'object' && !Array.isArray(data)) ? data : {};
    renderGlossary();
  } catch (_) {
    glossaryTerms = {};
    renderGlossary();
  }
}

function renderGlossary() {
  var list = $('#glossary-list');
  var entries = Object.entries(glossaryTerms);
  if (entries.length === 0) {
    list.innerHTML = '<div class="k-empty">No glossary terms defined yet. Add acronyms, definitions, or business concepts.</div>';
    return;
  }
  var html = '';
  entries.sort(function(a, b) { return a[0].localeCompare(b[0]); }).forEach(function(e) {
    var term = e[0], def = e[1] || '';
    html += '<div class="k-item">'
      + '<div class="k-item-body">'
      + '  <div class="k-item-title">' + esc(term) + '</div>'
      + (def ? '<div class="k-item-text">' + esc(def) + '</div>' : '')
      + '</div>'
      + '<div class="k-actions">'
      + '  <button class="k-icon-btn danger" title="Delete" onclick="deleteGlossaryTerm(\'' + esc(term).replace(/'/g, "\\'") + '\')">&#10005;</button>'
      + '</div>'
      + '</div>';
  });
  list.innerHTML = html;
}

async function addGlossaryTerm() {
  var term = $('#glossary-term').value.trim();
  var def = $('#glossary-def').value.trim();
  if (!term) { toast('Term is required.', 'error'); return; }
  try {
    await api('POST', '/api/glossary', { term: term, definition: def });
    toast('Term added.', 'success');
    $('#glossary-term').value = '';
    $('#glossary-def').value = '';
    kHideForm('glossary');
    loadGlossary();
  } catch (e) {
    toast('Failed: ' + e.message, 'error');
  }
}

async function deleteGlossaryTerm(term) {
  if (!confirm('Delete "' + term + '"?')) return;
  try {
    await api('DELETE', '/api/glossary/' + encodeURIComponent(term));
    toast('Term deleted.', 'success');
    loadGlossary();
  } catch (e) {
    toast('Delete failed: ' + e.message, 'error');
  }
}

/* ──────────────────────────────────────────────────────────
   KNOWLEDGE — Keywords
   API returns { keyword: content, ... } dict
   ────────────────────────────────────────────────────────── */
var keywordsData = {};

async function loadKeywords() {
  try {
    var data = await api('GET', '/api/keywords');
    keywordsData = (data && typeof data === 'object' && !Array.isArray(data)) ? data : {};
    renderKeywords();
  } catch (_) {
    keywordsData = {};
    renderKeywords();
  }
}

function renderKeywords() {
  var list = $('#keywords-list');
  var entries = Object.entries(keywordsData);
  if (entries.length === 0) {
    list.innerHTML = '<div class="k-empty">No keywords defined yet. Add triggers to inject context into the agent.</div>';
    return;
  }
  var html = '';
  entries.sort(function(a, b) { return a[0].localeCompare(b[0]); }).forEach(function(e) {
    var kw = e[0], content = e[1] || '';
    var kwSafe = esc(kw).replace(/'/g, "\\'");
    html += '<div class="k-item">'
      + '<div class="k-item-body">'
      + '  <div class="k-item-title">' + esc(kw) + '</div>'
      + (content ? '<div class="k-item-text">' + esc(content) + '</div>' : '')
      + '</div>'
      + '<div class="k-actions">'
      + '  <button class="k-icon-btn" title="Edit" onclick="editKeyword(\'' + kwSafe + '\')">&#9998;</button>'
      + '  <button class="k-icon-btn danger" title="Delete" onclick="deleteKeyword(\'' + kwSafe + '\')">&#10005;</button>'
      + '</div>'
      + '</div>';
  });
  list.innerHTML = html;
}

async function addKeyword() {
  var kw = $('#keyword-key').value.trim();
  var content = $('#keyword-content').value.trim();
  if (!kw) { toast('Keyword is required.', 'error'); return; }
  try {
    await api('POST', '/api/keywords', { keyword: kw, content: content });
    toast('Keyword added.', 'success');
    $('#keyword-key').value = '';
    $('#keyword-content').value = '';
    kHideForm('keywords');
    loadKeywords();
  } catch (e) {
    toast('Failed: ' + e.message, 'error');
  }
}

async function deleteKeyword(kw) {
  if (!confirm('Delete keyword "' + kw + '"?')) return;
  try {
    await api('DELETE', '/api/keywords/' + encodeURIComponent(kw));
    toast('Keyword deleted.', 'success');
    loadKeywords();
  } catch (e) {
    toast('Delete failed: ' + e.message, 'error');
  }
}

function addNewKeyword() {
  $('#km-old-keyword').value = '';
  $('#km-keyword').value = '';
  $('#km-content').value = '';
  document.querySelector('#keyword-modal .modal-title').textContent = 'Add Keyword';
  $('#keyword-modal').classList.add('active');
}

function editKeyword(kw) {
  $('#km-old-keyword').value = kw;
  $('#km-keyword').value = kw;
  $('#km-content').value = keywordsData[kw] || '';
  document.querySelector('#keyword-modal .modal-title').textContent = 'Edit Keyword';
  $('#keyword-modal').classList.add('active');
}

function closeKeywordModal() {
  $('#keyword-modal').classList.remove('active');
}

async function saveKeywordEdit() {
  var oldKw = $('#km-old-keyword').value;
  var newKw = $('#km-keyword').value.trim();
  var content = $('#km-content').value.trim();
  if (!newKw) { toast('Keyword is required.', 'error'); return; }
  try {
    // Delete old keyword if renamed
    if (oldKw && oldKw !== newKw) {
      await api('DELETE', '/api/keywords/' + encodeURIComponent(oldKw));
    }
    await api('POST', '/api/keywords', { keyword: newKw, content: content });
    toast('Keyword updated.', 'success');
    closeKeywordModal();
    loadKeywords();
  } catch (e) {
    toast('Save failed: ' + e.message, 'error');
  }
}

/* ──────────────────────────────────────────────────────────
   KNOWLEDGE — Patterns
   ────────────────────────────────────────────────────────── */
async function loadPatterns() {
  try {
    var data = await api('GET', '/api/patterns');
    allPatterns = Array.isArray(data) ? data : [];
    renderPatterns(allPatterns);
  } catch (_) {
    allPatterns = [];
    renderPatterns([]);
  }
}

var allPatterns = [];

function filterPatterns() {
  var q = ($('#pattern-search').value || '').toLowerCase();
  var filtered = q ? allPatterns.filter(function(p) {
    var name = (p.name || p.filename || '').toLowerCase();
    var q2 = (p.question || '').toLowerCase();
    return name.indexOf(q) !== -1 || q2.indexOf(q) !== -1;
  }) : allPatterns;
  renderPatterns(filtered);
}

function clearPatternSearch() {
  $('#pattern-search').value = '';
  renderPatterns(allPatterns);
}

function renderPatterns(patterns) {
  var list = $('#patterns-list');
  if (!list) return;
  if (!patterns || patterns.length === 0) {
    list.innerHTML = '<div class="k-empty">No patterns saved yet. Use <code>dante_save_pattern</code> in Claude to save SQL patterns.</div>';
    return;
  }
  var html = '';
  patterns.forEach(function(p) {
    var filename = p.filename || (p.name || '') + '.sql';
    var question = p.question || filename;
    var desc = p.description || '';
    var fnSafe = esc(filename).replace(/'/g, "\\'");
    html += '<div class="k-item">'
      + '<div class="k-item-body">'
      + '  <div class="k-item-title">' + esc(question) + '</div>'
      + (desc ? '<div class="k-item-text">' + esc(desc) + '</div>' : '')
      + '  <div class="k-item-meta">' + esc(filename) + '</div>'
      + '</div>'
      + '<div class="k-actions">'
      + '  <button class="k-icon-btn" title="Edit" onclick="editPattern(\'' + fnSafe + '\')">&#9998;</button>'
      + '  <button class="k-icon-btn danger" title="Delete" onclick="deletePattern(\'' + fnSafe + '\')">&#10005;</button>'
      + '</div>'
      + '</div>';
  });
  list.innerHTML = html;
}

async function deletePattern(filename) {
  if (!confirm('Delete pattern "' + filename + '"?')) return;
  try {
    await api('DELETE', '/api/patterns/' + encodeURIComponent(filename));
    toast('Pattern deleted.', 'success');
    loadPatterns();
  } catch (e) {
    toast('Delete failed: ' + e.message, 'error');
  }
}

function addNewPattern() {
  $('#pm-old-filename').value = '';
  $('#pm-question').value = '';
  $('#pm-description').value = '';
  $('#pm-tables').value = '';
  $('#pm-sql').value = '';
  document.querySelector('#pattern-modal .modal-title').textContent = 'Add Embedding';
  $('#pattern-modal').classList.add('active');
}

function editPattern(filename) {
  var p = allPatterns.find(function(x) { return x.filename === filename; });
  if (!p) { toast('Pattern not found.', 'error'); return; }
  $('#pm-old-filename').value = filename;
  $('#pm-question').value = p.question || '';
  $('#pm-description').value = p.description || '';
  $('#pm-tables').value = (p.tables || []).join(', ');
  $('#pm-sql').value = p.sql || '';
  document.querySelector('#pattern-modal .modal-title').textContent = 'Edit Embedding';
  $('#pattern-modal').classList.add('active');
}

function closePatternModal() {
  $('#pattern-modal').classList.remove('active');
}

async function savePatternEdit() {
  var filename = $('#pm-old-filename').value;
  var question = $('#pm-question').value.trim();
  if (!question) { toast('Question is required.', 'error'); return; }
  var payload = {
    question: question,
    description: $('#pm-description').value.trim(),
    tables: $('#pm-tables').value.split(',').map(function(t) { return t.trim(); }).filter(Boolean),
    sql: $('#pm-sql').value
  };
  try {
    if (filename) {
      await api('PUT', '/api/patterns/' + encodeURIComponent(filename), payload);
    } else {
      await api('POST', '/api/patterns', payload);
    }
    toast(filename ? 'Pattern updated.' : 'Pattern created.', 'success');
    closePatternModal();
    loadPatterns();
  } catch (e) {
    toast('Save failed: ' + e.message, 'error');
  }
}

/* ──────────────────────────────────────────────────────────
   KNOWLEDGE — Embeddings (from SQLite database)
   ────────────────────────────────────────────────────────── */
var allEmbeddings = [];

async function loadEmbeddings() {
  console.log('[dante] loadEmbeddings called');
  try {
    var data = await api('GET', '/api/embeddings');
    console.log('[dante] embeddings response:', data ? data.length : 'null');
    allEmbeddings = Array.isArray(data) ? data : [];
    renderEmbeddings(allEmbeddings);
  } catch (err) {
    console.error('[dante] loadEmbeddings error:', err);
    allEmbeddings = [];
    renderEmbeddings([]);
  }
}

function filterEmbeddings() {
  var q = ($('#embedding-search').value || '').toLowerCase();
  var filtered = q ? allEmbeddings.filter(function(e) {
    return (e.question || '').toLowerCase().indexOf(q) !== -1
      || (e.source || '').toLowerCase().indexOf(q) !== -1
      || (e.dashboard || '').toLowerCase().indexOf(q) !== -1
      || (e.description || '').toLowerCase().indexOf(q) !== -1;
  }) : allEmbeddings;
  renderEmbeddings(filtered);
}

function clearEmbeddingSearch() {
  $('#embedding-search').value = '';
  renderEmbeddings(allEmbeddings);
}

function renderEmbeddings(embeddings) {
  var list = $('#embeddings-list');
  if (!embeddings || embeddings.length === 0) {
    list.innerHTML = '<div class="k-empty">No embeddings yet. Run ingestion from the Data Management tab.</div>';
    return;
  }
  var html = '<div style="font-size:12px;color:var(--text-secondary);margin-bottom:8px">' + embeddings.length + ' embeddings</div>';
  embeddings.forEach(function(e) {
    var source = e.source || 'unknown';
    var badge = '<span style="display:inline-block;font-size:10px;padding:1px 6px;border-radius:3px;background:var(--card-bg);border:1px solid var(--border);margin-right:6px">' + esc(source) + '</span>';
    var dashboard = e.dashboard ? '<span style="font-size:11px;color:var(--text-muted)"> from ' + esc(e.dashboard) + '</span>' : '';
    var desc = e.description ? '<div class="k-item-text">' + esc(e.description) + '</div>' : '';
    html += '<div class="k-item">'
      + '<div class="k-item-body">'
      + '  <div class="k-item-title">' + badge + esc(e.question || e.id) + dashboard + '</div>'
      + desc
      + '  <div class="k-item-meta">' + esc(e.id) + ' &middot; ' + esc(e.updated_at || '') + '</div>'
      + '</div>'
      + '</div>';
  });
  list.innerHTML = html;
}

/* ──────────────────────────────────────────────────────────
   STATUS
   ────────────────────────────────────────────────────────── */
async function loadStatus() {
  var container = $('#status-content');
  container.innerHTML = '<div class="empty-state"><span class="spinner"></span> Loading status...</div>';
  try {
    var data = await api('GET', '/api/status');
    renderStatus(data);
  } catch (e) {
    container.innerHTML = '<div class="empty-state">Failed to load status: ' + esc(e.message) + '</div>';
  }
}

function renderStatus(data) {
  var container = $('#status-content');
  if (!data) { container.innerHTML = '<div class="empty-state">No status data available.</div>'; return; }

  var html = '';

  // Connection info
  var conns = data.connections || [];
  var connCount = typeof conns === 'number' ? conns : (Array.isArray(conns) ? conns.length : 0);
  var defaultConn = data.default_connection || data.config && data.config.default_connection || '';

  // Knowledge stats
  var glossaryCount = data.glossary_count != null ? data.glossary_count : (data.glossary ? (Array.isArray(data.glossary) ? data.glossary.length : data.glossary) : '?');
  var keywordsCount = data.keywords_count != null ? data.keywords_count : (data.keywords ? (Array.isArray(data.keywords) ? data.keywords.length : data.keywords) : '?');
  var notesLen = data.notes_length != null ? data.notes_length : (data.notes ? (typeof data.notes === 'string' ? data.notes.length : data.notes) : '?');

  // Credential status
  var lookerOk = data.looker_configured || (data.credentials && data.credentials.looker);
  var dbxOk = data.databricks_configured || (data.credentials && data.credentials.databricks);
  var modeOk = data.mode_configured || false;
  var redashOk = data.redash_configured || false;
  var sigmaOk = data.sigma_configured || false;
  var supersetOk = data.superset_configured || false;

  html += '<div class="stat-grid">';
  html += '<div class="stat-box"><div class="stat-value">' + esc(connCount) + '</div><div class="stat-label">Connections</div></div>';
  html += '<div class="stat-box"><div class="stat-value">' + esc(glossaryCount) + '</div><div class="stat-label">Glossary Terms</div></div>';
  html += '<div class="stat-box"><div class="stat-value">' + esc(keywordsCount) + '</div><div class="stat-label">Keywords</div></div>';
  html += '<div class="stat-box"><div class="stat-value">' + esc(notesLen) + '</div><div class="stat-label">Notes (chars)</div></div>';
  html += '</div>';

  html += '<hr class="divider">';
  html += '<h3 style="margin-bottom:10px;font-size:13px;color:#8899aa;text-transform:uppercase;letter-spacing:.5px">Integration Status</h3>';

  html += '<div class="item-list">';
  // Default connection
  html += '<div class="item-row"><span class="item-label">'
    + '<span class="status-dot ' + (defaultConn ? 'green' : 'yellow') + '"></span>'
    + 'Default Connection</span>'
    + '<span class="item-meta">' + (defaultConn ? esc(defaultConn) : 'Not set') + '</span></div>';

  // Looker
  html += '<div class="item-row"><span class="item-label">'
    + '<span class="status-dot ' + (lookerOk ? 'green' : 'red') + '"></span>'
    + 'Looker</span>'
    + '<span class="item-meta">' + (lookerOk ? 'Configured' : 'Not configured') + '</span></div>';

  // Databricks
  html += '<div class="item-row"><span class="item-label">'
    + '<span class="status-dot ' + (dbxOk ? 'green' : 'red') + '"></span>'
    + 'Databricks</span>'
    + '<span class="item-meta">' + (dbxOk ? 'Configured' : 'Not configured') + '</span></div>';

  // Mode
  html += '<div class="item-row"><span class="item-label">'
    + '<span class="status-dot ' + (modeOk ? 'green' : 'red') + '"></span>'
    + 'Mode Analytics</span>'
    + '<span class="item-meta">' + (modeOk ? 'Configured' : 'Not configured') + '</span></div>';

  // Redash
  html += '<div class="item-row"><span class="item-label">'
    + '<span class="status-dot ' + (redashOk ? 'green' : 'red') + '"></span>'
    + 'Redash</span>'
    + '<span class="item-meta">' + (redashOk ? 'Configured' : 'Not configured') + '</span></div>';

  // Sigma
  html += '<div class="item-row"><span class="item-label">'
    + '<span class="status-dot ' + (sigmaOk ? 'green' : 'red') + '"></span>'
    + 'Sigma Computing</span>'
    + '<span class="item-meta">' + (sigmaOk ? 'Configured' : 'Not configured') + '</span></div>';

  // Superset
  html += '<div class="item-row"><span class="item-label">'
    + '<span class="status-dot ' + (supersetOk ? 'green' : 'red') + '"></span>'
    + 'Apache Superset</span>'
    + '<span class="item-meta">' + (supersetOk ? 'Configured' : 'Not configured') + '</span></div>';

  html += '</div>';

  // Show any extra info from the API
  if (data.version) {
    html += '<hr class="divider">';
    html += '<p style="font-size:12px;color:#556677">Dante version: <code>' + esc(data.version) + '</code></p>';
  }

  container.innerHTML = html;
}


/* ──────────────────────────────────────────────────────────
   DATA MANAGEMENT
   ────────────────────────────────────────────────────────── */
function toggleIngestLimit() {
  var checked = document.getElementById('ingest-limit-check').checked;
  document.getElementById('ingest-limit-row').style.display = checked ? 'block' : 'none';
  document.getElementById('ingest-limit-hint').style.display = checked ? 'none' : 'inline';
}

function toggleCredentials() {
  var section = document.getElementById('credentials-section');
  var icon = document.getElementById('creds-toggle-icon');
  var visible = section.style.display !== 'none';
  section.style.display = visible ? 'none' : 'block';
  icon.innerHTML = visible ? '&#9660; Show' : '&#9650; Hide';
  if (!visible) loadSources();
}

var _currentJobs = [];
var _pollInterval = null;

async function loadJobs() {
  try {
    var data = await api('GET', '/api/jobs');
    _currentJobs = Array.isArray(data) ? data : [];
    renderJobs(_currentJobs);
  } catch (_) {
    _currentJobs = [];
  }
}

function renderJobs(jobs) {
  var list = document.getElementById('jobs-list');
  if (!jobs || jobs.length === 0) {
    list.innerHTML = '<div class="k-empty">No jobs yet. Click Generate Embeddings to start.</div>';
    return;
  }
  var html = '<div class="item-list">';
  jobs.slice(0, 10).forEach(function(job) {
    var s = job.status || 'unknown';
    var statusStyle = s === 'completed' ? 'background:#14532d;color:var(--success);border-color:var(--success)' :
                      s === 'failed'    ? 'background:#7f1d1d;color:var(--danger);border-color:var(--danger)' :
                      s === 'running'   ? 'background:#2d2000;color:var(--warn);border-color:var(--warn)' :
                                         'background:#2f2f2f;color:var(--text-secondary);border-color:var(--border)';
    var resultStr = '';
    if (job.result) {
      var r = job.result;
      resultStr = '+' + r.created + ' / ~' + r.updated + ' / -' + r.skipped;
    } else if (s === 'running' || s === 'pending') {
      resultStr = 'in progress...';
    }
    var dateStr = '';
    if (job.started_at) {
      var d = new Date(job.started_at);
      dateStr = (d.getMonth()+1) + '/' + d.getDate() + '/' + d.getFullYear();
    }
    html += '<div class="item-row">'
      + '<div style="display:flex;align-items:center;gap:12px;flex:1">'
      + '  <span style="border:1px solid;border-radius:3px;padding:2px 8px;font-size:11px;font-weight:700;' + statusStyle + '">' + esc(s.charAt(0).toUpperCase() + s.slice(1)) + '</span>'
      + '  <span class="item-label">' + esc(job.source || 'all') + '</span>'
      + '</div>'
      + '<div style="display:flex;align-items:center;gap:16px">'
      + (resultStr ? '<span class="item-meta" style="font-family:var(--mono)">' + esc(resultStr) + '</span>' : '')
      + '<span class="item-meta">' + esc(dateStr) + '</span>'
      + '</div>'
      + '</div>';
  });
  html += '</div>';
  list.innerHTML = html;
}

async function runIngest() {
  var source = document.getElementById('ingest-source').value;
  var skipExisting = document.getElementById('ingest-skip-existing').checked;
  var limitCheck = document.getElementById('ingest-limit-check').checked;
  var dashboardLimit = limitCheck ? parseInt(document.getElementById('ingest-limit-num').value || '50') : 0;

  var btn = document.getElementById('ingest-btn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span> Starting...';

  try {
    await api('POST', '/api/ingest', {
      source: source,
      skip_existing: skipExisting,
      dashboard_limit: dashboardLimit,
    });
    toast('Ingestion job started.', 'info');
    await loadJobs();
    if (_pollInterval) clearInterval(_pollInterval);
    _pollInterval = setInterval(async function() {
      await loadJobs();
      var anyActive = _currentJobs.some(function(j) { return j.status === 'pending' || j.status === 'running'; });
      if (!anyActive) {
        clearInterval(_pollInterval);
        _pollInterval = null;
        btn.disabled = false;
        btn.innerHTML = 'Generate Embeddings';
        var lastJob = _currentJobs[0];
        if (lastJob && lastJob.status === 'completed' && lastJob.result) {
          var r = lastJob.result;
          toast('Done: +' + r.created + ' created, ~' + r.updated + ' updated', 'success');
        } else if (lastJob && lastJob.status === 'failed') {
          toast('Ingestion failed: ' + (lastJob.error || 'unknown error'), 'error');
        }
      }
    }, 2000);
  } catch (e) {
    toast('Failed to start ingestion: ' + e.message, 'error');
    btn.disabled = false;
    btn.innerHTML = 'Generate Embeddings';
  }
}

/* ──────────────────────────────────────────────────────────
   KEYBOARD SHORTCUTS
   ────────────────────────────────────────────────────────── */
document.addEventListener('keydown', function(e) {
  if (e.key === 'Escape') { closePatternModal(); closeKeywordModal(); closeRuleModal(); closeNoteModal(); }
  // Enter in keyword/glossary inline forms
  if (e.key === 'Enter' && e.target.id === 'keyword-input') addKeyword();
  if (e.key === 'Enter' && (e.target.id === 'glossary-term' || e.target.id === 'glossary-def')) addGlossaryTerm();
});

/* ──────────────────────────────────────────────────────────
   INIT
   ────────────────────────────────────────────────────────── */
(function init() {
  loadRules();
  loadNotes();
  loadKeywords();
  loadPatterns();
})();

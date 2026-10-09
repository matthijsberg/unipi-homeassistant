// A small fake browser + fake Blockly, just enough to EXECUTE the real inline script of web/index.html in tests.
// Where it matters it is as strict as the real thing: setFieldValue on an unknown field throws, a dropdown ignores
// values that are not one of its options, getFieldValue of an unknown field returns null.
var __errors = [], __timers = [], __timeouts = [], __calls = [], __state = { rules: [], trace: [], putStatus: 200, putBody: null, nextId: 1,
  confirmAnswer: true, testStatus: 200, testBody: { ok: true, events: [] } };
var __confirms = [];
function confirm(msg) { __confirms.push(String(msg)); return __state.confirmAnswer; }
var console = { log() {}, warn() {}, info() {}, error() { __errors.push(Array.prototype.map.call(arguments, String).join(' ')); } };

class ClassList {
  constructor() { this.s = new Set(); }
  add(c) { this.s.add(c); } remove(c) { this.s.delete(c); } contains(c) { return this.s.has(c); }
  toggle(c) { this.s.has(c) ? this.s.delete(c) : this.s.add(c); }
}
class El {
  constructor(tag) { this.tagName = tag; this.children = []; this.style = {}; this.classList = new ClassList(); this._text = ''; this.onclick = null; this.value = ''; this.listeners = {}; this.parentNode = null; this.className = ''; this.innerHTMLSet = []; }
  get textContent() { return this._text + this.children.map(c => c.textContent).join(''); }
  set textContent(v) { this._text = String(v); this.children = []; }
  get innerHTML() { return ''; }
  set innerHTML(v) { this.innerHTMLSet.push(String(v)); this._text = ''; this.children = []; }
  appendChild(c) { this.children.push(c); c.parentNode = this; return c; }
  append() { for (const c of arguments) this.appendChild(typeof c === 'string' ? Object.assign(new El('#text'), { _text: c }) : c); }
  insertBefore(n, ref) { const i = this.children.indexOf(ref); if (i < 0) this.children.push(n); else this.children.splice(i, 0, n); n.parentNode = this; return n; }
  removeChild(c) { this.children = this.children.filter(x => x !== c); return c; }
  remove() { if (this.parentNode) this.parentNode.removeChild(this); }
  get firstChild() { return this.children[0] || null; }
  get lastChild() { return this.children[this.children.length - 1] || null; }
  querySelector(sel) { const cls = sel.replace(/^\./, ''); const f = (n) => { for (const c of n.children) { if ((c.className || '').split(' ').includes(cls)) return c; const r = f(c); if (r) return r; } return null; }; return f(this); }
  addEventListener(t, fn) { (this.listeners[t] = this.listeners[t] || []).push(fn); }
  setAttribute() {} getAttribute() { return null; } focus() {}
}
var __els = {};
var document = {
  getElementById(id) { if (!__els[id]) { __els[id] = new El('div'); __els[id].id = id; if (id === 'app') __els[id].style.display = 'block'; } return __els[id]; },
  createElement(tag) { return new El(tag); },
};
var window = { location: { reload() {} }, onclick: null };
function setInterval(fn, ms) { __timers.push({ fn, ms }); return __timers.length; }
function setTimeout(fn, ms) { __timeouts.push(fn); return __timeouts.length; }
function __runTimeouts() { const t = __timeouts; __timeouts = []; t.forEach(f => f()); }

function __resp(status, body) { return Promise.resolve({ status, ok: status >= 200 && status < 300, json: () => Promise.resolve(body) }); }
function fetch(url, opts) {
  opts = opts || {}; const method = opts.method || 'GET';
  __calls.push({ url, method, body: opts.body || null });
  if (url === '/api/rules' && method === 'GET') return __resp(200, JSON.parse(JSON.stringify(__state.rules)));
  if (url === '/api/rules' && method === 'PUT') {
    __state.putBody = JSON.parse(opts.body);
    if (__state.putStatus !== 200) return __resp(__state.putStatus, { error: __state.putError || 'refused' });
    __state.rules = __state.putBody.map(r => Object.assign({}, r, { id: r.id || ('new-' + (__state.nextId++)) }));
    return __resp(200, { status: 'ok', count: __state.rules.length });
  }
  if (url.indexOf('/api/rule_trace') === 0) { const since = parseInt(url.split('since=')[1] || '0', 10); return __resp(200, { last: __state.trace.length ? __state.trace[__state.trace.length - 1].seq : since, events: __state.trace.filter(e => e.seq > since) }); }
  if (/^\/api\/rules\/[^/]+\/test$/.test(url) && method === 'POST') return __resp(__state.testStatus, JSON.parse(JSON.stringify(__state.testBody)));
  if (url === '/api/auth_check') return __resp(200, { authenticated: true, username: 'tester' });
  if (url === '/api/info') return __resp(200, { name: 'Neuron_S103_2258', model: 'Neuron S103', sn: 2258, ip: '10.0.0.2' }); // pii-ok (fake test data)
  if (url === '/api/status') return __resp(200, {});
  if (url === '/api/inputs') return __resp(200, []);
  return __resp(404, {});
}

// ---------------- fake Blockly ----------------
class FField { constructor(kind, value) { this.kind = kind; this.value = value; } }
var Blockly = {
  Events: { BLOCK_CHANGE: 'change', BLOCK_CREATE: 'create' },
  Blocks: {},
  FieldTextInput: class extends FField { constructor(v) { super('text', String(v == null ? '' : v)); } set(v) { this.value = String(v); } },
  FieldNumber: class extends FField { constructor(v, min, max) { super('number', Number(v)); this.min = min; this.max = max; } set(v) { let n = Number(v); if (isNaN(n)) return; if (this.min != null && n < this.min) n = this.min; if (this.max != null && n > this.max) n = this.max; this.value = n; } },
  FieldCheckbox: class extends FField { constructor(v) { super('checkbox', (v === true || v === 'TRUE') ? 'TRUE' : 'FALSE'); } set(v) { if (v === true || v === 'TRUE') this.value = 'TRUE'; else if (v === false || v === 'FALSE') this.value = 'FALSE'; } },
  FieldDropdown: class extends FField { constructor(options) { super('dropdown', options[0][1]); this.options = options; } set(v) { if (this.options.some(o => o[1] === String(v))) this.value = String(v); /* real Blockly ignores unknown options */ } },
  lastOptions: null,
  svgResize() {},
  selected: null,
  getSelected() { return Blockly.selected; },
  inject(id, opts) { Blockly.lastOptions = opts; Blockly.ws = new FakeWorkspace(); return Blockly.ws; },
};
class FakeBlock {
  constructor(ws, type) {
    this.workspace = ws; this.type = type; this.id = 'blk' + (++ws.seq); this.fields = {}; this.inputs = {}; this.inputList = [];
    this.xy = { x: 0, y: 0 }; this.collapsed = false; this.data = null; this.parent = null; this.nextBlock = null;
    this.root = { style: {}, classList: new ClassList() }; this.warning = null; this.comment = null; this.tooltip = null;
    const self = this;
    this.outputConnection = { block: this }; this.previousConnection = { block: this };
    this.nextConnection = { block: this, connect(other) { self.nextBlock = other.block; other.block.parent = self; } };
    const def = Blockly.Blocks[type]; if (!def) throw new Error('Unknown block type ' + type); def.init.call(this);
  }
  _input(kind, name) {
    const self = this; const inp = { kind, name, fields: [], target: null,
      appendField(f, fname) { if (typeof f === 'string') inp.fields.push({ label: f }); else { f.name = fname; self.fields[fname] = f; inp.fields.push(f); } return inp; }, setCheck() { return inp; } };
    if (kind !== 'dummy') inp.connection = { block: this, connect(other) { inp.target = other.block; other.block.parent = self; } };
    this.inputList.push(inp); if (name) this.inputs[name] = inp; return inp;
  }
  appendDummyInput() { return this._input('dummy'); } appendValueInput(n) { return this._input('value', n); } appendStatementInput(n) { return this._input('statement', n); }
  setOutput() {} setColour() {} setHelpUrl() {} setTooltip(t) { this.tooltip = t; } setCommentText(t) { this.comment = t; } setWarningText(t) { this.warning = t; }
  getParent() { return this.parent; }
  getInput(n) { return this.inputs[n]; } getInputTargetBlock(n) { const i = this.inputs[n]; return i ? i.target : null; } getNextBlock() { return this.nextBlock; }
  getFieldValue(n) { const f = this.fields[n]; return f ? f.value : null; }
  setFieldValue(v, n) { const f = this.fields[n]; if (!f) throw new Error('Field "' + n + '" not found.'); const old = f.value; f.set(v); if (f.value !== old) this.workspace.fire({ type: Blockly.Events.BLOCK_CHANGE, element: 'field', name: n, blockId: this.id }); }
  moveBy(dx, dy) { this.xy.x += dx; this.xy.y += dy; } getRelativeToSurfaceXY() { return { x: this.xy.x, y: this.xy.y }; }
  getSvgRoot() { return this.root; } setCollapsed(f) { this.collapsed = !!f; } initSvg() {} render() {}
  getHeightWidth() { if (this.collapsed) return { height: 32, width: 300 }; const kids = this.inputList.map(i => i.target).filter(Boolean); const kidRows = kids.reduce((m, k) => Math.max(m, k.inputList.length), 0); return { height: 40 + 28 * Math.max(this.inputList.length, kidRows), width: 500 }; }
  toString() { const parts = []; (function walk(b) { b.inputList.forEach(i => { i.fields.forEach(f => parts.push(f.label || String(f.value))); if (i.target) walk(i.target); }); if (b.nextBlock) walk(b.nextBlock); })(this); return parts.join(' '); }
}
class FakeWorkspace {
  constructor() { this.seq = 0; this.blocks = []; this.listeners = []; this.zoomedToFit = 0; }
  newBlock(type) { const b = new FakeBlock(this, type); this.blocks.push(b); this.fire({ type: Blockly.Events.BLOCK_CREATE, ids: [b.id] }); return b; }
  fire(ev) { this.listeners.slice().forEach(l => l(ev)); }
  addChangeListener(fn) { this.listeners.push(fn); }
  getTopBlocks(ordered) { const t = this.blocks.filter(b => !b.parent); if (ordered) t.sort((a, b) => a.xy.y - b.xy.y); return t; }
  getAllBlocks() { return this.blocks.slice(); } getBlockById(id) { return this.blocks.find(b => b.id === id) || null; }
  clear() { this.blocks = []; } zoomToFit() { this.zoomedToFit++; }
}

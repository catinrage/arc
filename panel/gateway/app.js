const state = {
  config: null,
  refreshTimer: null,
};

const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => Array.from(document.querySelectorAll(selector));

function setText(id, value) {
  const node = document.getElementById(id);
  if (node) node.textContent = value;
}

function toast(message, isError = false) {
  const node = $("#toast");
  node.textContent = message;
  node.classList.toggle("error", isError);
  node.hidden = false;
  clearTimeout(node.hideTimer);
  node.hideTimer = setTimeout(() => {
    node.hidden = true;
  }, 4200);
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });
  const text = await response.text();
  const data = text ? JSON.parse(text) : null;
  if (!response.ok) {
    throw new Error(data?.error || response.statusText);
  }
  return data;
}

async function refreshState() {
  try {
    const data = await api("/api/state");
    renderState(data);
    setOnline(true);
  } catch (error) {
    setOnline(false);
    toast(error.message, true);
  }
}

async function loadConfig() {
  try {
    const data = await api("/api/config");
    state.config = data.config;
    fillConfigForm(data.config);
    $("#configPath").textContent = data.config_path || "Running with default config";
  } catch (error) {
    toast(error.message, true);
  }
}

function setOnline(online) {
  $(".pulse").classList.toggle("online", online);
  $("#sidebarStatus").textContent = online ? "Live" : "Disconnected";
}

function renderState(data) {
  setText("buildInfo", `version ${data.version} / ${data.commit} / ${data.build_date}`);
  setText("socksAddress", data.socks.address);
  setText("socksState", data.socks.listening ? "Listening" : "Not listening");
  setText("activeSocks", data.socks.active);
  setText("totalSocks", data.socks.total);
  setText("readySessions", data.relay.ready_sessions);
  setText("configSessions", data.relay.config_sessions);
  setText("activeStreams", data.relay.active_streams);
  setText("burstActive", data.relay.burst_active);
  setText("burstConfigured", data.relay.burst_configured);
  setText("maxStreams", data.relay.max_streams_per_session);
  setText("requestCount", `${data.requests.length} rows`);
  if (data.service_status) {
    $("#serviceOutput").textContent = data.service_status;
  }
  renderRequests(data.requests);
}

function renderRequests(requests) {
  const table = $("#requestTable");
  if (!requests.length) {
    table.innerHTML = '<tr><td colspan="6" class="empty">Waiting for requests</td></tr>';
    return;
  }
  table.innerHTML = requests.map((item) => {
    const age = item.ended_at ? duration(new Date(item.started_at), new Date(item.ended_at)) : duration(new Date(item.started_at), new Date());
    const status = escapeHTML(item.status || "unknown");
    return `
      <tr title="${escapeHTML(item.error || "")}">
        <td>#${item.id}</td>
        <td>${escapeHTML(item.command)}</td>
        <td>${escapeHTML(item.host)}</td>
        <td>${item.port}</td>
        <td><span class="status ${status}">${status}</span></td>
        <td>${age}</td>
      </tr>
    `;
  }).join("");
}

function duration(start, end) {
  const seconds = Math.max(0, Math.round((end - start) / 1000));
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const rem = seconds % 60;
  if (minutes < 60) return `${minutes}m ${rem}s`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes % 60}m`;
}

function fillConfigForm(cfg) {
  const form = $("#configForm");
  for (const [key, value] of Object.entries(cfg)) {
    const input = form.elements[key];
    if (!input) continue;
    if (input.type === "checkbox") {
      input.checked = Boolean(value);
    } else if (key === "admin_password") {
      input.value = "";
    } else {
      input.value = value ?? "";
    }
  }
}

function readConfigForm() {
  const form = $("#configForm");
  const intFields = new Set([
    "listen_port",
    "connections",
    "burst_connections",
    "max_streams_per_session",
    "buffer_size",
  ]);
  const boolFields = new Set(["udp_enabled", "insecure_tls"]);
  const cfg = {};
  for (const element of Array.from(form.elements)) {
    if (!element.name) continue;
    if (boolFields.has(element.name)) {
      cfg[element.name] = element.checked;
    } else if (intFields.has(element.name)) {
      cfg[element.name] = Number(element.value);
    } else {
      cfg[element.name] = element.value.trim();
    }
  }
  return cfg;
}

async function saveConfig(event) {
  event.preventDefault();
  try {
    const data = await api("/api/config", {
      method: "POST",
      body: JSON.stringify(readConfigForm()),
    });
    toast(data.message || "Config saved");
    await loadConfig();
  } catch (error) {
    toast(error.message, true);
  }
}

async function serviceAction(action) {
  const output = $("#serviceOutput");
  output.textContent = `Running ${action}...`;
  try {
    const data = await api("/api/service", {
      method: "POST",
      body: JSON.stringify({ action }),
    });
    output.textContent = data.output || data.message || `${action} complete`;
    toast(data.message || `${action} complete`);
    setTimeout(refreshState, 800);
  } catch (error) {
    output.textContent = error.message;
    toast(error.message, true);
  }
}

function escapeHTML(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function initNavigation() {
  const links = $$(".nav-link");
  links.forEach((link) => {
    link.addEventListener("click", () => {
      links.forEach((item) => item.classList.remove("active"));
      link.classList.add("active");
    });
  });
}

function init() {
  initNavigation();
  $("#refreshButton").addEventListener("click", refreshState);
  $("#configForm").addEventListener("submit", saveConfig);
  $$(".service-actions button").forEach((button) => {
    button.addEventListener("click", () => serviceAction(button.dataset.action));
  });
  loadConfig();
  refreshState();
  state.refreshTimer = setInterval(refreshState, 2000);
}

init();

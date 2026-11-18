const $ = (sel) => document.querySelector(sel);

let busy = false;
let handoverReport = null;
let handoverBusy = false;

const parseWhen = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const normalized = typeof value === "number" ? value * 1000 : value;
  const d = new Date(normalized);
  return Number.isNaN(d.getTime()) ? null : d;
};

const fmt = (value) => {
  const d = parseWhen(value);
  return d ? d.toLocaleString("zh-CN", { hour12: false }) : "—";
};

const fmtTime = (value) => {
  const d = parseWhen(value);
  return d ? d.toLocaleTimeString("zh-CN", { hour12: false }) : "—";
};

function toast(message, isError = false) {
  const el = $("#toast");
  el.textContent = message;
  el.classList.toggle("error", isError);
  el.classList.add("show");
  clearTimeout(toast._timer);
  toast._timer = setTimeout(() => el.classList.remove("show"), 4200);
}

async function api(path, options = {}) {
  const resp = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  let body = null;
  try { body = await resp.json(); } catch (_) { /* ignore */ }
  if (!resp.ok) {
    const detail = body && body.detail ? body.detail : `请求失败（HTTP ${resp.status}）`;
    throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
  }
  return body;
}

async function perform(fn) {
  if (busy) return;
  busy = true;
  document.querySelectorAll("button").forEach((b) => (b.disabled = true));
  try {
    await fn();
  } catch (err) {
    toast(err.message, true);
  } finally {
    busy = false;
    document.querySelectorAll("button").forEach((b) => (b.disabled = false));
    await refresh();
  }
}

function render(state) {
  $("#device-name").textContent = state.device;
  $("#server-clock").textContent = new Date(state.now).toLocaleTimeString("zh-CN", {
    hour12: false,
  });
  const worker = $("#worker-state");
  worker.textContent = state.worker_running ? "投递工作线程运行中" : "投递工作线程未运行";
  worker.classList.toggle("live", state.worker_running);

  const maint = state.maintenance;
  const maintBadge = $("#maint-status");
  if (maint) {
    maintBadge.textContent = "检修中";
    maintBadge.className = "badge active";
  } else if (state.latest_maintenance) {
    maintBadge.textContent = state.latest_maintenance.close_reason === "cutoff" ? "已到截止时刻" : "已结束";
    maintBadge.className = "badge idle";
  } else {
    maintBadge.textContent = "无检修";
    maintBadge.className = "badge idle";
  }

  if (maint) {
    const remain = Math.max(0, Math.ceil(maint.remaining_seconds));
    const mm = String(Math.floor(remain / 60)).padStart(2, "0");
    const ss = String(remain % 60).padStart(2, "0");
    $("#maint-info").textContent =
      `检修开始 ${fmt(maint.started_at)} · 截止 ${fmt(maint.ends_at)} · 剩余 ${mm}:${ss}`;
    $("#end-maint").disabled = busy;
  } else {
    $("#maint-info").textContent = state.latest_maintenance
      ? `最近一次检修：${fmt(state.latest_maintenance.started_at)} → ${fmt(state.latest_maintenance.ends_at)}` +
        (state.latest_maintenance.close_reason === "cutoff" ? "（到达截止时刻）" : "（手动结束）")
      : "尚未设置检修窗口";
    $("#end-maint").disabled = true;
  }

  const active = state.active_round;
  const activeBadge = $("#active-alarm");
  if (active) {
    activeBadge.textContent = `第 ${active.id} 轮告警未恢复`;
    activeBadge.className = active.group === "suppressed" ? "badge active" : "badge alarm";
  } else {
    activeBadge.textContent = "无活跃告警";
    activeBadge.className = "badge idle";
  }
  $("#trigger").disabled = busy || Boolean(active);
  $("#recover").disabled = busy || !active;
  $("#set-maint").disabled = busy;

  const c = state.counts;
  const values = [c.suppressed, c.pending, c.sent, c.skipped, c.cancelled];
  document.querySelectorAll("#stats .stat .n").forEach((el, idx) => {
    el.textContent = values[idx];
  });

  const tbody = $("#rounds tbody");
  tbody.innerHTML = "";
  for (const r of state.rounds) {
    const tr = document.createElement("tr");
    const deliveryCell = r.state === "sent"
      ? `已受理 ${fmtTime(r.accepted_at)}`
      : r.state === "delivering"
        ? "投递中…"
        : r.needs_verify
          ? "待按通知号核实"
          : r.state === "pending"
            ? "等待投递"
            : "—";
    const cells = [
      `<b>#${r.id}</b>`,
      `<span class="mono">${r.notification_no}</span>`,
      `<span class="tag ${r.group}">${r.state_label}</span>`,
      fmtTime(r.triggered_at),
      r.recovered_at ? fmtTime(r.recovered_at) : "仍活跃",
      deliveryCell,
      String(r.attempts),
      "",
    ];
    tr.innerHTML = cells
      .map((html, i) => `<td class="${i === 6 ? "num" : i === 7 ? "note" : "no-wrap"}">${html}</td>`)
      .join("");
    const noteTd = tr.lastElementChild;
    const note = document.createElement("div");
    note.textContent = r.note || "";
    noteTd.appendChild(note);
    if (r.last_error) {
      const err = document.createElement("div");
      err.className = "mono";
      err.style.color = "var(--red)";
      err.textContent = `错误：${r.last_error}`;
      noteTd.appendChild(err);
    }
    tbody.appendChild(tr);
  }
  $("#empty").classList.toggle("hidden", state.rounds.length > 0);
}

async function refresh() {
  try {
    const state = await api("/api/state");
    render(state);
    renderHandoverWindows(state);
  } catch (err) {
    $("#worker-state").textContent = "状态获取失败";
    $("#worker-state").classList.remove("live");
  }
  refreshNotifier();
}

function renderHandoverWindows(state) {
  const select = $("#handover-window");
  const current = select.value;
  // Ended windows come from the latest_maintenance plus any window seen in rounds.
  const windows = new Map();
  if (state.latest_maintenance && state.latest_maintenance.closed_at) {
    windows.set(String(state.latest_maintenance.id), state.latest_maintenance);
  }
  for (const r of state.rounds || []) {
    if (r.maintenance_id !== null && r.maintenance_id !== undefined) {
      const key = String(r.maintenance_id);
      if (!windows.has(key)) windows.set(key, { id: r.maintenance_id });
    }
  }
  const optionHtml = ['<option value="">全部已结束检修窗口</option>'];
  for (const w of windows.values()) {
    const label = w.closed_at
      ? `#${w.id} · ${fmt(w.started_at)} → ${fmt(w.ends_at)}`
      : `#${w.id}`;
    optionHtml.push(`<option value="${w.id}">${label}</option>`);
  }
  const html = optionHtml.join("");
  if (select.innerHTML !== html) {
    select.innerHTML = html;
    if ([...select.options].some((o) => o.value === current)) select.value = current;
  }
}

function renderHandover() {
  const report = handoverReport;
  const badge = $("#handover-badge");
  const table = $("#handover-table");
  const empty = $("#handover-empty");
  const summaryEl = $("#handover-summary");
  const noticeEl = $("#handover-notice");
  const confirmBtn = $("#confirm-handover");

  if (!report) {
    badge.textContent = "尚未核查";
    badge.className = "badge idle";
    table.classList.add("hidden");
    summaryEl.classList.add("hidden");
    noticeEl.classList.add("hidden");
    empty.classList.remove("hidden");
    confirmBtn.disabled = true;
    return;
  }

  const summary = report.summary;
  $("#sum-consistent").textContent = summary.consistent;
  $("#sum-conflict").textContent = summary.conflict;
  $("#sum-undecidable").textContent = summary.undecidable;
  $("#handover-generated").textContent = `核查时间 ${fmt(report.generated_at)}`;
  summaryEl.classList.remove("hidden");
  empty.classList.add("hidden");

  const blocked = report.stale || report.changed_during_check;
  if (blocked) {
    badge.textContent = report.stale ? "结论已过期" : "核查期间状态变化";
    badge.className = "badge alarm";
  } else if (report.can_handover) {
    badge.textContent = "可交接";
    badge.className = "badge ok";
  } else {
    badge.textContent = "存在待处理轮次";
    badge.className = "badge active";
  }
  if (report.notice) {
    noticeEl.textContent = report.notice;
    noticeEl.classList.remove("hidden");
    noticeEl.classList.toggle("warn", !report.can_handover);
  } else {
    noticeEl.classList.add("hidden");
  }
  confirmBtn.disabled = !report.can_handover || handoverBusy;

  const tbody = $("#handover-table tbody");
  tbody.innerHTML = "";
  for (const r of report.rounds) {
    const tr = document.createElement("tr");
    const notifier = r.notifier || {};
    let notifierText = "查询失败";
    if (notifier.status === "accepted") {
      notifierText =
        `已受理 ${fmtTime(notifier.accepted_at)}` +
        ` · 轮次 ${notifier.round_id ?? "—"}` +
        ` · 触发 ${fmtTime(notifier.triggered_at)}` +
        (notifier.deliveries > 1 ? ` · 交付 ${notifier.deliveries} 次` : "");
    } else if (notifier.status === "not_found") {
      notifierText = "无该通知号记录";
    } else if (notifier.detail) {
      notifierText = notifier.detail;
    }
    const cells = [
      `<b>#${r.round_id}</b>`,
      `<span class="mono">${r.notification_no || "—"}</span>`,
      `<span class="tag ${r.state === "cancelled" || r.state === "skipped" ? "skipped" : r.state}">${r.state_label}</span>`,
      `触发 ${fmtTime(r.alarm_triggered_at)}<br>受理 ${r.alarm_accepted_at ? fmtTime(r.alarm_accepted_at) : "—"}`,
      escapeHtml(notifierText),
      `<span class="verdict ${r.verdict}">${r.verdict_label}</span>`,
      r.reasons.map((x) => `<div>${escapeHtml(x)}</div>`).join(""),
    ];
    tr.innerHTML = cells
      .map(
        (html, i) =>
          `<td class="${i === 6 ? "note" : i === 5 ? "no-wrap" : ""}">${html}</td>`
      )
      .join("");
    tr.classList.add(`verdict-row-${r.verdict}`);
    tbody.appendChild(tr);
  }
  table.classList.remove("hidden");
}

function escapeHtml(text) {
  return String(text ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

async function runHandover() {
  const select = $("#handover-window");
  const params = select.value ? `?maintenance_id=${encodeURIComponent(select.value)}` : "";
  handoverBusy = true;
  $("#run-handover").disabled = true;
  $("#run-handover").textContent = "核查中…";
  try {
    handoverReport = await api(`/api/handover/check${params}`, { method: "POST" });
    renderHandover();
    toast(
      handoverReport.can_handover
        ? "核查完成：全部轮次两端一致"
        : "核查完成：存在冲突或暂无法判定，请逐轮核对"
    );
  } catch (err) {
    toast(err.message, true);
  } finally {
    handoverBusy = false;
    $("#run-handover").disabled = false;
    $("#run-handover").textContent = "开始核查";
    renderHandover();
  }
}

async function refreshHandoverFreshness() {
  if (!handoverReport || !handoverReport.check_id) return;
  // A confirm already locked this report in; no need to keep polling it.
  if (handoverReport.confirmed_at) return;
  try {
    const fresh = await api(`/api/handover/reports/${handoverReport.check_id}`);
    const changed =
      fresh.stale !== handoverReport.stale ||
      fresh.can_handover !== handoverReport.can_handover;
    handoverReport = fresh;
    if (changed) renderHandover();
  } catch (_) {
    /* keep the last report visible */
  }
}

async function confirmHandover() {
  if (!handoverReport) return;
  handoverBusy = true;
  renderHandover();
  try {
    await api(`/api/handover/reports/${handoverReport.check_id}/confirm`, {
      method: "POST",
    });
    toast("交接已确认");
    handoverReport = await api(`/api/handover/reports/${handoverReport.check_id}`);
    renderHandover();
  } catch (err) {
    toast(err.message, true);
    await refreshHandoverFreshness();
  } finally {
    handoverBusy = false;
    renderHandover();
  }
}



async function refreshNotifier() {
  try {
    const data = await api("/api/notifier/observations");
    const list = $("#notifier-list");
    if (!data.items || data.items.length === 0) {
      list.innerHTML = '<div class="muted">通知端尚未受理任何通知。</div>';
      return;
    }
    list.innerHTML = data.items
      .map(
        (n) =>
          `<div class="nrow"><span>${n.notification_no}</span>` +
          `<span>${fmt(n.accepted_at)}</span>` +
          `<span>${n.message || ""} · 交付次数 ${n.deliveries} · 模式 ${data.mode}</span></div>`
      )
      .join("");
  } catch (err) {
    $("#notifier-list").innerHTML = `<div class="muted">通知端不可达：${err.message}</div>`;
  }
}

function toLocalInputValue(date) {
  const pad = (n) => String(n).padStart(2, "0");
  return (
    `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}` +
    `T${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`
  );
}

function initCutoffDefault(minutes = 10) {
  const d = new Date(Date.now() + minutes * 60 * 1000);
  $("#cutoff").value = toLocalInputValue(d);
}

$("#set-maint").addEventListener("click", () => {
  const value = $("#cutoff").value;
  if (!value) {
    toast("请选择检修截止时刻", true);
    return;
  }
  const ts = new Date(value).getTime() / 1000;
  if (!Number.isFinite(ts)) {
    toast("截止时刻格式无效", true);
    return;
  }
  if (ts <= Date.now() / 1000) {
    toast("检修截止时刻必须晚于当前时间", true);
    return;
  }
  perform(async () => {
    const w = await api("/api/maintenance", {
      method: "POST",
      body: JSON.stringify({ ends_at: ts }),
    });
    toast(`检修窗口已设置，截止 ${fmt(w.ends_at)}`);
  });
});

document.querySelectorAll("[data-min]").forEach((btn) => {
  btn.addEventListener("click", () => {
    const minutes = Number(btn.dataset.min);
    initCutoffDefault(minutes);
    const ts = Date.now() / 1000 + minutes * 60;
    perform(async () => {
      const w = await api("/api/maintenance", {
        method: "POST",
        body: JSON.stringify({ ends_at: ts }),
      });
      toast(`检修窗口 ${minutes} 分钟，截止 ${fmt(w.ends_at)}`);
    });
  });
});

$("#end-maint").addEventListener("click", () => {
  if (!confirm("立即结束检修？仍活跃且被抑制的告警会按固定通知号送达。")) return;
  perform(async () => {
    await api("/api/maintenance/end", { method: "POST" });
    toast("检修已结束，仍活跃的告警已转入送达");
  });
});

$("#trigger").addEventListener("click", () => {
  perform(async () => {
    const r = await api("/api/alarm/trigger", { method: "POST" });
    toast(
      r.state === "suppressed"
        ? `第 ${r.id} 轮告警已触发，检修中暂缓通知（${r.notification_no}）`
        : `第 ${r.id} 轮告警已触发，立即通知（${r.notification_no}）`
    );
  });
});

$("#recover").addEventListener("click", () => {
  perform(async () => {
    const r = await api("/api/alarm/recover", { method: "POST" });
    toast(`第 ${r.id} 轮告警已恢复：${r.state_label}`);
  });
});

$("#run-handover").addEventListener("click", runHandover);
$("#confirm-handover").addEventListener("click", confirmHandover);

initCutoffDefault();
refresh();
renderHandover();
setInterval(refresh, 1000);
setInterval(refreshHandoverFreshness, 2000);

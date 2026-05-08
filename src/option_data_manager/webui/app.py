"""FastAPI WebUI for inspecting current option data slices."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sqlite3

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, PlainTextResponse
import uvicorn

from option_data_manager.api.app import create_app as create_local_api_app
from option_data_manager.service_state import ServiceStateRepository
from .read_model import WebuiReadModel


DEFAULT_DATABASE_PATH = "data/option-data-current.sqlite3"


def create_webui_app(
    connection: sqlite3.Connection,
    *,
    database_path: str | None = None,
) -> FastAPI:
    """Create the local WebUI and its read-only JSON endpoints."""

    connection.row_factory = sqlite3.Row
    read_model = WebuiReadModel(connection)
    service_state = ServiceStateRepository(connection)
    app = FastAPI(title="期权数据管理工具 WebUI")
    local_api = create_local_api_app(connection, database_path=database_path)
    for route in local_api.router.routes:
        if getattr(route, "path", "") == "/":
            continue
        app.router.routes.append(route)

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        overview = _overview_payload(
            read_model,
            service_state=service_state,
            database_path=database_path,
            limit=500,
        )
        selected = (
            overview["underlyings"][0]["underlying_symbol"]
            if overview["underlyings"]
            else None
        )
        initial_state = {
            "overview": overview,
            "selectedUnderlying": selected,
            "quote": read_model.tquote(underlying_symbol=selected) if selected else None,
            "databasePath": database_path,
        }
        return INDEX_HTML.replace(
            "__ODM_INITIAL_STATE__",
            json.dumps(initial_state, ensure_ascii=False),
        )

    @app.get("/assets/webui.css", response_class=PlainTextResponse)
    def css() -> PlainTextResponse:
        return PlainTextResponse(WEBUI_CSS, media_type="text/css; charset=utf-8")

    @app.get("/assets/webui.js", response_class=PlainTextResponse)
    def js() -> PlainTextResponse:
        return PlainTextResponse(
            WEBUI_JS,
            media_type="application/javascript; charset=utf-8",
        )

    @app.get("/api/webui/overview")
    def overview(limit: int = Query(default=80, ge=1, le=500)) -> dict:
        return {
            **_overview_payload(
                read_model,
                service_state=service_state,
                database_path=database_path,
                limit=limit,
            ),
        }

    @app.get("/api/webui/tquote")
    def tquote(underlying: str | None = None) -> dict:
        return read_model.tquote(underlying_symbol=underlying)

    @app.get("/api/webui/runs")
    def runs(limit: int = Query(default=30, ge=1, le=100)) -> dict:
        return read_model.runs(limit=limit)

    return app


def _overview_payload(
    read_model: WebuiReadModel,
    *,
    service_state: ServiceStateRepository,
    database_path: str | None,
    limit: int,
) -> dict:
    return {
        "database_path": database_path,
        **read_model.overview(limit=limit),
        "refresh": {
            "running": (
                service_state.get_value("collection.refresh_running") or "false"
            )
            == "true",
            "message": service_state.get_value("collection.refresh_message"),
            "window_count": _int_or_none(
                service_state.get_value("collection.refresh_window_count")
            ),
            "remaining_batches": _int_or_none(
                service_state.get_value("collection.refresh_remaining_batches")
            ),
            "started_at": service_state.get_value("collection.refresh_started_at"),
            "finished_at": service_state.get_value("collection.refresh_finished_at"),
        },
    }


def _int_or_none(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def create_app_from_database() -> FastAPI:
    """ASGI factory for uvicorn."""

    database_path = os.environ.get("ODM_DATABASE_PATH", DEFAULT_DATABASE_PATH)
    Path(database_path).parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path, check_same_thread=False)
    return create_webui_app(connection, database_path=database_path)


def main() -> None:
    """Start a local development WebUI server."""

    host = os.environ.get("ODM_WEB_HOST", "127.0.0.1")
    port = int(os.environ.get("ODM_WEB_PORT", "8765"))
    uvicorn.run(
        "option_data_manager.webui.app:create_app_from_database",
        host=host,
        port=port,
        factory=True,
        reload=False,
    )


INDEX_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>期权数据管理工具</title>
  <link
    href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"
    rel="stylesheet"
    integrity="sha384-QWTKZyjpPEjISv5WaRU9Oer+R5dB8QyLZ6lGR4xjLxjUaaV+7pD1Q+KjF2S8J7h"
    crossorigin="anonymous"
  />
  <link rel="stylesheet" href="/assets/webui.css" />
</head>
<body>
  <div class="container-fluid app-shell p-0">
    <div class="row g-0 min-vh-100">
      <aside class="col-auto sidebar d-flex flex-column">
        <div class="brand d-flex align-items-center gap-2">
          <span class="brand-dot" aria-hidden="true"></span>
          <strong>期权数据管理工具</strong>
        </div>
        <nav class="nav nav-pills flex-column sidebar-nav" aria-label="主导航">
          <button class="nav-link active" type="button" data-page="overview">总览</button>
          <button class="nav-link" type="button" data-page="quote">T型报价</button>
          <button class="nav-link" type="button" data-page="runs">采集日志</button>
          <button class="nav-link" type="button" data-page="api">API</button>
          <button class="nav-link" type="button" data-page="settings">设置</button>
        </nav>
        <div class="health-pill mt-auto"><span class="status-dot" id="health-dot"></span><span id="health-text">等待数据</span></div>
      </aside>

      <main class="col workspace">
        <section id="overview" class="page active">
          <div class="hero-card card">
            <div class="hero-title-block">
              <h1>采集健康总览</h1>
              <p>按交易所、品种和标的定位当前切片覆盖缺口。</p>
            </div>
            <div class="hero-metrics">
              <div class="metric-card"><span>最新刷新</span><strong id="latest-update">--</strong></div>
              <div class="metric-card"><span>活跃期权</span><strong id="summary-options">--</strong></div>
              <div class="metric-card"><span>Greeks/IV 覆盖</span><strong class="good" id="summary-iv">--</strong></div>
              <div class="metric-card"><span>20日K线</span><strong class="good" id="summary-kline">正常</strong></div>
              <div class="metric-card"><span>采集分片</span><strong id="summary-batches">--</strong></div>
            </div>
          </div>

          <section class="panel card">
            <div class="panel-header">
              <div><span class="panel-title">交易所采集状态</span><span class="panel-note">覆盖率、品种数和标的数用于判断全市场缺口。</span></div>
            </div>
            <div class="exchange-cards" id="exchange-cards"></div>
          </section>

          <section class="panel card">
            <div class="panel-header">
              <div><span class="panel-title">采集分片进度</span><span class="panel-note">手动刷新会按分片窗口推进，失败分片会保留以便后续重试。</span></div>
            </div>
            <div class="progress-cards" id="collection-progress"></div>
            <div class="notice d-none mt-3" id="collection-failures"></div>
          </section>

          <section class="panel card">
            <div class="panel-header">
              <div><span class="panel-title">标的汇总表</span><span class="panel-note">一行一个标的，进入 T型报价查看 CALL/PUT 全链。</span></div>
              <div class="btn-group btn-group-sm" role="group" aria-label="状态筛选">
                <button class="btn btn-light active" type="button" id="show-all">全部</button>
                <button class="btn btn-outline-warning" type="button" id="show-issues">只看缺口</button>
              </div>
            </div>
            <div class="table-responsive table-shell">
              <table class="table table-sm align-middle summary-table">
                <thead><tr><th>交易所</th><th>品种</th><th>标的合约</th><th>到期月</th><th>CALL</th><th>PUT</th><th>Quote</th><th>Greeks/IV</th><th>20D K线</th><th>行情时间</th><th>状态</th><th>操作</th></tr></thead>
                <tbody id="underlying-rows"></tbody>
              </table>
              <div class="empty-state d-none" id="overview-empty">
                当前 SQLite 运行库还没有合约或行情切片。请先在设置页确认 TQSDK 凭据，然后点击手动刷新或运行 odm-collect。
              </div>
            </div>
          </section>
        </section>

        <section id="quote" class="page">
          <div class="hero-card card">
            <div class="hero-title-block">
              <h1 id="quote-title">T型报价</h1>
              <p>标的合约 · T型报价 · SQLite 当前切片</p>
            </div>
            <div class="hero-metrics">
              <div class="metric-card"><span>K线最后时间</span><strong id="quote-book-time">--</strong></div>
              <div class="metric-card"><span>活跃期权</span><strong id="quote-active-options">--</strong></div>
              <div class="metric-card"><span>Greeks/IV 覆盖</span><strong class="good" id="quote-iv-coverage">--</strong></div>
              <div class="metric-card"><span>20日K线</span><strong class="good" id="quote-kline-status">正常</strong></div>
            </div>
          </div>

          <section class="quote-shell card">
            <div class="quote-toolbar d-flex align-items-center gap-3">
              <label class="form-label mb-0">交易所</label>
              <select class="form-select form-select-sm" id="exchange-select"></select>
              <label class="form-label mb-0">品种</label>
              <select class="form-select form-select-sm" id="product-select"></select>
              <label class="form-label mb-0">到期月</label>
              <select class="form-select form-select-sm" id="expiry-select"></select>
              <span class="ms-auto">K线时间 <strong id="toolbar-book-time">--</strong></span>
            </div>
            <div class="quote-table-wrap">
              <table class="table table-sm mb-0 quote-table">
                <colgroup>
                  <col class="option-col"><col class="option-col"><col class="option-col"><col class="option-col"><col class="option-col">
                  <col class="option-col"><col class="option-col"><col class="option-col"><col class="option-col"><col class="option-col">
                  <col class="central-col">
                  <col class="option-col"><col class="option-col"><col class="option-col"><col class="option-col"><col class="option-col">
                  <col class="option-col"><col class="option-col"><col class="option-col"><col class="option-col"><col class="option-col">
                </colgroup>
                <thead id="quote-head"></thead>
                <tbody id="quote-rows"></tbody>
              </table>
            </div>
          </section>
        </section>

        <section id="runs" class="page">
          <div class="hero-card card"><div class="hero-title-block"><h1>采集日志</h1><p>追踪发现、Quote、Greeks/IV、20D K线写入和自动重试结果。</p></div><div class="hero-metrics"><div class="metric-card"><span>最近运行</span><strong id="latest-run">--</strong></div><div class="metric-card"><span>异常</span><strong id="latest-errors">--</strong></div></div></div>
          <section class="panel card"><div class="panel-header"><span class="panel-title">采集运行记录</span></div><div id="runs-list" class="log-list"></div></section>
        </section>

        <section id="api" class="page">
          <div class="hero-card card"><div class="hero-title-block"><h1>本地 API</h1><p>为未来比例价差监控平台提供本地只读数据入口。</p></div><div class="hero-metrics"><div class="metric-card"><span>模式</span><strong class="good">只读</strong></div><div class="metric-card"><span>Swagger</span><a class="btn btn-sm btn-primary" href="/docs" target="_blank" rel="noreferrer">文档入口</a></div></div></div>
          <section class="panel card"><div class="notice">API 仅绑定本机，当前 DEV-024 只交付展示切片。</div></section>
        </section>

        <section id="settings" class="page">
          <div class="hero-card card"><div class="hero-title-block"><h1>设置</h1><p>TQSDK、SQLite 和本地 API 设置入口。</p></div><div class="hero-metrics"><div class="metric-card"><span>凭据状态</span><strong class="good">已隔离</strong></div></div></div>
          <section class="panel card">
            <div class="panel-header"><span class="panel-title">TQSDK 凭据</span><span class="panel-note">密码保存后脱敏显示，不写入日志或页面状态。</span></div>
            <div class="settings-grid">
              <label>账号<input class="form-control form-control-sm" id="setting-account" autocomplete="username" /></label>
              <label>密码<input class="form-control form-control-sm" id="setting-password" type="password" autocomplete="current-password" placeholder="输入新密码后保存" /></label>
              <button class="btn btn-sm btn-primary" type="button" id="save-credentials">保存凭据</button>
              <button class="btn btn-sm btn-light" type="button" id="test-credentials">测试连接</button>
            </div>
            <div class="notice mt-3" id="settings-message">设置加载中。</div>
          </section>
          <section class="panel card">
            <div class="panel-header"><span class="panel-title">本地 API</span><span class="panel-note">默认只绑定本机，API Key 强制认证可配置。</span></div>
            <div class="settings-grid">
              <label>Bind<input class="form-control form-control-sm" id="setting-api-bind" /></label>
              <label>Port<input class="form-control form-control-sm" id="setting-api-port" type="number" min="1" max="65535" /></label>
              <label>刷新间隔秒<input class="form-control form-control-sm" id="setting-refresh-interval" type="number" min="1" /></label>
              <label>分片大小<input class="form-control form-control-sm" id="setting-batch-size" type="number" min="1" /></label>
              <label>等待轮数<input class="form-control form-control-sm" id="setting-wait-cycles" type="number" min="0" /></label>
              <label>后台窗口分片<input class="form-control form-control-sm" id="setting-max-batches" type="number" min="1" /></label>
              <label class="checkline"><input class="form-check-input" id="setting-auth-required" type="checkbox" /> 强制 API Key</label>
              <button class="btn btn-sm btn-primary" type="button" id="save-runtime-settings">保存运行设置</button>
              <button class="btn btn-sm btn-warning" type="button" id="trigger-refresh">手动刷新</button>
            </div>
          </section>
          <section class="panel card">
            <div class="panel-header"><span class="panel-title">API Key</span><span class="panel-note">完整 Key 只在创建时显示。</span></div>
            <div class="settings-grid">
              <label>名称<input class="form-control form-control-sm" id="api-key-name" placeholder="local-monitor" /></label>
              <label>Scope<input class="form-control form-control-sm" id="api-key-scope" value="read" /></label>
              <button class="btn btn-sm btn-primary" type="button" id="create-api-key">创建 Key</button>
            </div>
            <div class="table-responsive table-shell mt-3">
              <table class="table table-sm align-middle summary-table">
                <thead><tr><th>ID</th><th>名称</th><th>指纹</th><th>Scope</th><th>状态</th><th>最后使用</th></tr></thead>
                <tbody id="api-key-rows"></tbody>
              </table>
            </div>
          </section>
        </section>
      </main>
    </div>
  </div>

  <div class="drawer-backdrop" id="drawer-backdrop"></div>
  <aside class="drawer" id="detail-drawer" aria-hidden="true">
    <button class="btn btn-sm btn-light float-end" id="close-drawer" type="button">关闭</button>
    <h3 id="drawer-title">行权价详情</h3>
    <p class="muted">CALL/PUT 合约、Quote 原始字段、Greeks、IV、20日K线切片与最近采集日志。</p>
    <div id="drawer-body"></div>
  </aside>
  <script>window.__ODM_INITIAL_STATE__ = __ODM_INITIAL_STATE__;</script>
  <script src="/assets/webui.js"></script>
</body>
</html>
"""


WEBUI_CSS = r"""
:root {
  --panel: #fffdf8;
  --panel-2: #fff7f3;
  --line: #eaded6;
  --line-2: #f2e6d5;
  --text: #3f3033;
  --muted: #7c6a6a;
  --sakura: #f2c6d4;
  --brand: #8a4246;
  --matcha: #e4f1e0;
  --aqua: #2b6e6e;
  --warn: #9b641f;
  --mono: "SFMono-Regular", "Cascadia Mono", Consolas, Menlo, monospace;
  --sans: Inter, "Segoe UI", "Microsoft YaHei", system-ui, sans-serif;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  color: var(--text);
  font: 13px/1.45 var(--sans);
  background:
    linear-gradient(90deg, rgba(138, 66, 70, .07) 1px, transparent 1px),
    linear-gradient(180deg, rgba(138, 66, 70, .055) 1px, transparent 1px),
    #fbfaf3;
  background-size: 36px 36px;
  letter-spacing: 0;
}
.app-shell { min-width: 1180px; }
.app-shell > .row {
  display: grid;
  grid-template-columns: clamp(216px, 11.5vw, 246px) minmax(0, 1fr);
  min-height: 100vh;
  margin: 0;
}
.app-shell > .row > .sidebar,
.app-shell > .row > .workspace {
  width: auto;
  max-width: none;
  padding-left: 0;
  padding-right: 0;
}
.sidebar {
  position: sticky;
  top: 0;
  width: clamp(216px, 11.5vw, 246px);
  height: 100vh;
  padding: 22px 16px;
  border-right: 1px solid rgba(138, 66, 70, .16);
  background: var(--panel);
}
.brand {
  padding: 0 4px 10px;
  border-bottom: 1px solid rgba(138, 66, 70, .13);
  color: #4b292c;
}
.brand-dot {
  width: 22px;
  height: 22px;
  border-radius: 4px;
  background: var(--sakura);
  box-shadow: 0 0 0 1px rgba(138, 66, 70, .16);
}
.sidebar-nav { gap: 6px; margin-top: 22px; }
.sidebar .nav-link {
  width: 100%;
  height: 38px;
  padding: 0 12px;
  border: 1px solid transparent;
  border-radius: 4px;
  color: #6e5758;
  text-align: left;
  font-weight: 700;
}
.sidebar .nav-link:hover { background: rgba(242, 230, 213, .72); color: #4b292c; }
.sidebar .nav-link.active { color: #fffdf8; background: var(--brand); border-color: var(--brand); }
.health-pill {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  height: 28px;
  border: 1px solid rgba(59, 143, 143, .28);
  border-radius: 4px;
  background: var(--matcha);
  color: var(--aqua);
  font-weight: 800;
}
.status-dot { width: 7px; height: 7px; border-radius: 50%; background: currentColor; }
.status-dot.warn { color: var(--warn); }
.workspace { min-width: 0; padding: clamp(18px, 2vw, 34px) !important; }
.page { display: none; }
.page.active { display: block; }
.card, .panel, .hero-card, .quote-shell {
  border: 1px solid rgba(138, 66, 70, .14);
  border-radius: 4px;
  background: var(--panel);
  box-shadow: none;
}
.hero-card {
  min-height: 116px;
  display: grid;
  grid-template-columns: minmax(360px, .9fr) minmax(520px, 1.35fr);
  overflow: hidden;
  margin-bottom: 18px;
  border-color: var(--brand);
}
.hero-title-block {
  display: flex;
  flex-direction: column;
  justify-content: center;
  padding: 20px 22px;
  background: var(--brand);
  color: #fffdf8;
}
.hero-title-block h1 { margin: 0 0 6px; font-size: 23px; font-weight: 800; }
.hero-title-block p { margin: 0; color: rgba(255, 253, 248, .82); }
.hero-metrics {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(118px, 1fr));
  gap: 12px;
  align-items: center;
  padding: 20px 22px;
}
.metric-card {
  min-height: 58px;
  padding: 10px 12px;
  border: 1px solid rgba(138, 66, 70, .13);
  border-radius: 4px;
  background: var(--panel);
}
.metric-card span { display: block; color: var(--muted); font-size: 11px; font-weight: 700; }
.metric-card strong { display: block; margin-top: 4px; color: var(--text); font: 800 13px/1.2 var(--mono); }
.good { color: var(--aqua) !important; }
.warn { color: var(--warn) !important; }
.bad { color: var(--brand) !important; }
.panel { padding: 17px 18px; margin-bottom: 16px; }
.panel-header { display: flex; justify-content: space-between; align-items: center; gap: 14px; margin-bottom: 14px; }
.panel-title { font-size: 15px; font-weight: 800; }
.panel-note { color: var(--muted); font-size: 12px; margin-left: 8px; }
.exchange-cards { display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px; }
.progress-cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 12px; }
.settings-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(160px, 1fr));
  gap: 12px;
  align-items: end;
}
.settings-grid label {
  color: var(--muted);
  font-size: 12px;
}
.settings-grid .checkline {
  display: flex;
  align-items: center;
  gap: 8px;
  min-height: 32px;
  color: var(--ink);
}
.exchange-card {
  min-height: 74px;
  padding: 12px 14px;
  border: 1px solid rgba(138, 66, 70, .13);
  border-radius: 4px;
  background: var(--panel);
}
.exchange-card strong { font-size: 16px; margin-right: 10px; }
.exchange-card div:last-child { color: var(--aqua); font-size: 12px; margin-top: 5px; }
.progress-card {
  min-height: 70px;
  padding: 11px 13px;
  border: 1px solid rgba(138, 66, 70, .13);
  border-radius: 4px;
  background: #fffdf8;
}
.progress-card span { display: block; color: var(--muted); font-size: 11px; font-weight: 800; }
.progress-card strong { display: block; margin-top: 5px; font: 800 16px/1.2 var(--mono); color: var(--text); }
.progress-card small { display: block; margin-top: 4px; color: var(--muted); font-size: 11px; }
.table-shell, .quote-table-wrap {
  border: 1px solid var(--line);
  border-radius: 4px;
  background: var(--panel);
}
.table { table-layout: fixed; margin: 0; }
.table th, .table td {
  height: 34px;
  padding: 0 10px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  vertical-align: middle;
  border-bottom: 1px solid var(--line);
}
.table th { color: #5f4645; background: var(--line-2); font-size: 11px; font-weight: 800; }
.table td { color: var(--text); background: rgba(255, 253, 248, .98); }
.summary-table tbody tr:nth-child(even) td { background: #f7fbf3; }
.summary-table tbody tr.clickable:hover td { background: rgba(242, 198, 212, .22); }
.mono, .num { font-family: var(--mono); font-weight: 800; }
.tag {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 48px;
  height: 22px;
  padding: 0 8px;
  border-radius: 4px;
  border: 1px solid rgba(138, 66, 70, .12);
  background: #f7f1ed;
  font-size: 11px;
  font-weight: 800;
}
.tag.good { background: var(--matcha); color: var(--aqua); }
.tag.warn { background: #fff2dc; color: var(--warn); }
.tag.bad { background: #fae3e6; color: var(--brand); }
.quote-shell { overflow: hidden; }
.quote-toolbar {
  min-height: 54px;
  padding: 10px 18px;
  border-bottom: 1px solid var(--line);
  background: var(--panel);
  flex-wrap: wrap;
}
.quote-toolbar .form-select { width: 106px; border-color: rgba(138, 66, 70, .18); background-color: var(--panel); }
.quote-table-wrap { overflow: visible; min-height: clamp(430px, 52vh, 620px); }
.quote-table { width: 100%; min-width: 100%; border-collapse: collapse; table-layout: fixed; }
.quote-table col.option-col { width: 4.6%; }
.quote-table col.central-col { width: 8%; }
.quote-table th, .quote-table td {
  height: 32px;
  padding: 0;
  text-align: center;
  font-size: clamp(10px, .66vw, 12px);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.quote-table th { height: 34px; color: #5f4645; background: var(--line-2); font: 800 11px/1 var(--sans); }
.quote-table .market-strip th { height: 56px; background: var(--panel); border-bottom-color: #eaded6; }
.quote-table .call-title { color: #b06c6f; text-align: left; padding-left: 20px; font-size: 15px; }
.quote-table .put-title { color: #3b8f8f; text-align: right; padding-right: 20px; font-size: 15px; }
.quote-table .under-bid,
.quote-table .under-ask { position: relative; overflow: visible; background: var(--panel); }
.quote-table .under-bid .tag,
.quote-table .under-ask .tag {
  position: absolute;
  top: 50%;
  width: max-content;
  max-width: none;
  height: 22px;
  padding: 0 6px;
  font-size: 11px;
  line-height: 22px;
}
.quote-table .under-bid .tag { right: 4px; transform: translateY(-50%); }
.quote-table .under-ask .tag { left: 4px; transform: translateY(-50%); }
.quote-table .under-last {
  background: var(--panel);
  color: var(--text);
  font-family: var(--mono);
  border-left: 1px solid var(--line);
  border-right: 1px solid var(--line);
}
.quote-table .under-last span { color: var(--muted); font: 700 11px/1.1 var(--sans); }
.quote-table .call-side.itm { background: #fff1ed; }
.quote-table .call-side.otm { background: #fffaf6; }
.quote-table .put-side.itm { background: #eaf5e8; }
.quote-table .put-side.otm { background: #f8fcf5; }
.quote-table .call-side.atm-neutral,
.quote-table .put-side.atm-neutral { background: var(--panel); }
.quote-table .strike-cell {
  color: var(--text);
  background: var(--panel);
  font: 900 12px/1 var(--mono);
  border-left: 1px solid var(--line);
  border-right: 1px solid var(--line);
}
.strike-cell.atm {
  color: var(--text) !important;
  background: var(--panel) !important;
  border-color: #d8b0a6 !important;
  box-shadow: inset 0 0 0 2px rgba(138, 66, 70, .28);
}
.iv { color: #7657ff; }
.price-bid { color: var(--aqua); }
.price-ask { color: var(--brand); }
.latest-price { font-family: var(--mono); font-weight: 900; }
.latest-price.latest-up { animation: latestUpFade 1.6s ease-out; }
.latest-price.latest-down { animation: latestDownFade 1.6s ease-out; }
@keyframes latestUpFade { 0% { color: var(--brand); } 72% { color: var(--brand); } 100% { color: var(--text); } }
@keyframes latestDownFade { 0% { color: var(--aqua); } 72% { color: var(--aqua); } 100% { color: var(--text); } }
.bar-cell { position: relative; overflow: hidden; font-family: var(--mono); font-weight: 800; }
.bar-cell .bar {
  position: absolute;
  top: 8px;
  bottom: 8px;
  z-index: 0;
  max-width: calc(100% - 4px);
  background: #c3e4d4;
}
.bar-cell.ask .bar { background: var(--sakura); }
.bar-cell.call.quantity .bar { left: 2px; }
.bar-cell.put.quantity .bar { right: 2px; }
.bar-cell.call.depth .bar { right: 2px; }
.bar-cell.put.depth .bar { left: 2px; }
.bar-cell .value { position: relative; z-index: 1; }
.log-list { display: grid; gap: 8px; }
.log-row { border: 1px solid var(--line); border-radius: 4px; padding: 12px; background: var(--panel); }
.notice {
  padding: 12px 14px;
  border: 1px solid rgba(163, 108, 46, .22);
  border-radius: 4px;
  background: #fff2dc;
  color: var(--warn);
}
.notice.good { border-color: rgba(59,143,143,.25); background: var(--matcha); color: var(--aqua); }
.empty-state {
  padding: 18px;
  color: var(--muted);
  font-size: 13px;
  border-top: 1px solid var(--line);
  background: rgba(255,255,255,.45);
}
.drawer-backdrop { position: fixed; inset: 0; display: none; background: rgba(63,48,51,.24); z-index: 40; }
.drawer-backdrop.open { display: block; }
.drawer {
  position: fixed;
  top: 0;
  right: 0;
  width: 460px;
  height: 100vh;
  padding: 20px;
  overflow: auto;
  border-left: 1px solid rgba(138, 66, 70, .16);
  background: var(--panel);
  z-index: 50;
  transform: translateX(100%);
  transition: transform .18s ease;
}
.drawer.open { transform: translateX(0); }
.drawer h3 { margin: 18px 0 8px; font-size: 18px; }
.muted { color: var(--muted); }
.kv { display: grid; grid-template-columns: 130px 1fr; gap: 8px; padding: 10px 0; border-bottom: 1px solid var(--line); }
"""


WEBUI_JS = r"""
const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => [...document.querySelectorAll(selector)];

const state = {
  overview: null,
  quote: null,
  selectedUnderlying: null,
  showIssuesOnly: false,
  previousLatest: new Map(),
};

const productNames = {
  AP: "苹果",
  ap: "苹果",
  JD: "鸡蛋",
  jd: "鸡蛋",
  M: "豆粕",
  m: "豆粕",
  CU: "铜",
  cu: "铜",
  SC: "原油",
  sc: "原油",
  C: "玉米",
  c: "玉米",
  CS: "玉米淀粉",
  cs: "玉米淀粉",
};

const escapeHtml = (value) => String(value ?? "").replace(/[&<>"']/g, (char) => ({
  "&": "&amp;",
  "<": "&lt;",
  ">": "&gt;",
  '"': "&quot;",
  "'": "&#39;",
}[char]));

function fmtNum(value, digits = 0, dash = "--") {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return dash;
  const fixed = Number(value).toFixed(digits);
  if (digits === 0) return fixed;
  return fixed;
}

function fmtMaybeTrim(value, digits, trim) {
  const rendered = fmtNum(value, digits);
  return trim ? rendered.replace(/(\.\d*?[1-9])0+$/, "$1").replace(/\.0+$/, "") : rendered;
}

function fmtPct(value) {
  if (value === null || value === undefined) return "--";
  return `${(Number(value) * 100).toFixed(1)}%`;
}

function fmtDateTime(value) {
  if (!value) return "--";
  const text = String(value);
  if (text.includes("T") && /(?:Z|[+-]\d{2}:?\d{2})$/.test(text)) {
    const date = new Date(text);
    if (!Number.isNaN(date.getTime())) {
      const parts = new Intl.DateTimeFormat("zh-CN", {
        timeZone: "Asia/Hong_Kong",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false,
      }).formatToParts(date).reduce((acc, part) => {
        acc[part.type] = part.value;
        return acc;
      }, {});
      return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second}`;
    }
  }
  return text
    .replace("T", " ")
    .replace(/(\.\d+)(?=(Z|[+-]\d{2}:?\d{2})?$)/, "")
    .replace(/(Z|[+-]\d{2}:?\d{2})$/, "");
}

function symbolParts(symbol) {
  const [exchange = "--", instrument = symbol ?? "--"] = String(symbol ?? "--").split(".");
  const product = instrument.match(/^[A-Za-z]+/)?.[0] ?? "--";
  const expiry = instrument.match(/(\d{3,4})/)?.[1] ?? "--";
  return { exchange, product, productName: productNames[product] ?? product, expiry };
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`${url} ${response.status}`);
  return response.json();
}

async function init() {
  bindNavigation();
  bindControls();
  await loadSettings();
  showPage(location.hash.slice(1) || "overview", { updateHash: false });
  if (window.__ODM_INITIAL_STATE__?.overview) {
    state.overview = window.__ODM_INITIAL_STATE__.overview;
    state.selectedUnderlying = window.__ODM_INITIAL_STATE__.selectedUnderlying;
    state.quote = window.__ODM_INITIAL_STATE__.quote;
    renderOverview();
    renderSelectors();
    if (state.quote) renderQuote({ animate: false });
    await renderRuns();
  } else {
    state.overview = await fetchJson("/api/webui/overview?limit=500");
    renderOverview();
    const first = state.overview.underlyings[0];
    if (first) {
      state.selectedUnderlying = first.underlying_symbol;
      renderSelectors();
      await loadQuote(state.selectedUnderlying, { animate: false });
    }
    await renderRuns();
  }
  setInterval(refreshCurrentQuote, 2500);
  setInterval(refreshOverview, 10000);
}

function bindNavigation() {
  $$(".nav-link[data-page]").forEach((tab) => {
    tab.addEventListener("click", () => showPage(tab.dataset.page));
  });
  window.addEventListener("hashchange", () => showPage(location.hash.slice(1), { updateHash: false }));
}

function showPage(page, options = {}) {
  const id = $(`#${page}`) ? page : "overview";
  $$(".page").forEach((node) => node.classList.toggle("active", node.id === id));
  $$(".nav-link[data-page]").forEach((node) => node.classList.toggle("active", node.dataset.page === id));
  if (options.updateHash !== false) history.replaceState(null, "", `#${id}`);
}

function bindControls() {
  $("#show-all").addEventListener("click", () => {
    state.showIssuesOnly = false;
    $("#show-all").classList.add("active");
    $("#show-issues").classList.remove("active");
    renderUnderlyingRows();
  });
  $("#show-issues").addEventListener("click", () => {
    state.showIssuesOnly = true;
    $("#show-issues").classList.add("active");
    $("#show-all").classList.remove("active");
    renderUnderlyingRows();
  });
  ["exchange-select", "product-select", "expiry-select"].forEach((id) => {
    $(`#${id}`).addEventListener("change", onSelectorChange);
  });
  $("#close-drawer").addEventListener("click", closeDrawer);
  $("#drawer-backdrop").addEventListener("click", closeDrawer);
  $("#save-credentials").addEventListener("click", saveCredentials);
  $("#test-credentials").addEventListener("click", testCredentials);
  $("#save-runtime-settings").addEventListener("click", saveRuntimeSettings);
  $("#trigger-refresh").addEventListener("click", triggerRefresh);
  $("#create-api-key").addEventListener("click", createApiKey);
}

async function loadSettings() {
  try {
    const settings = await fetchJson("/api/settings");
    $("#setting-account").value = settings.tqsdk.account ?? "";
    $("#setting-api-bind").value = settings.api.bind ?? "127.0.0.1";
    $("#setting-api-port").value = settings.api.port ?? 8770;
    $("#setting-auth-required").checked = Boolean(settings.api.auth_required);
    $("#setting-refresh-interval").value = settings.collection.refresh_interval_seconds ?? 30;
    $("#setting-batch-size").value = settings.collection.option_batch_size ?? 20;
    $("#setting-wait-cycles").value = settings.collection.wait_cycles ?? 1;
    $("#setting-max-batches").value = settings.collection.max_batches ?? 100;
    $("#settings-message").textContent = settings.tqsdk.password_configured ? "TQSDK 密码已配置。" : "TQSDK 密码尚未配置。";
    await loadApiKeys();
  } catch (error) {
    $("#settings-message").textContent = `设置加载失败：${error.message}`;
  }
}

async function saveCredentials() {
  const account = $("#setting-account").value.trim();
  const password = $("#setting-password").value;
  if (!account || !password) {
    $("#settings-message").textContent = "账号和新密码都不能为空。";
    return;
  }
  await fetchJsonWithBody("/api/settings/tqsdk-credentials", "PUT", { account, password });
  $("#setting-password").value = "";
  $("#settings-message").textContent = "TQSDK 凭据已保存。";
  await loadSettings();
}

async function testCredentials() {
  const result = await fetchJsonWithBody("/api/settings/test-tqsdk-connection", "POST", {});
  $("#settings-message").textContent = result.message;
}

async function saveRuntimeSettings() {
  const updates = [
    ["api.bind", $("#setting-api-bind").value.trim() || "127.0.0.1"],
    ["api.port", $("#setting-api-port").value || "8770"],
    ["api.auth_required", $("#setting-auth-required").checked ? "true" : "false"],
    ["collection.refresh_interval_seconds", $("#setting-refresh-interval").value || "30"],
    ["collection.option_batch_size", $("#setting-batch-size").value || "20"],
    ["collection.wait_cycles", $("#setting-wait-cycles").value || "1"],
    ["collection.max_batches", $("#setting-max-batches").value || "100"],
  ];
  for (const [key, value] of updates) {
    await fetchJsonWithBody(`/api/settings/${encodeURIComponent(key)}`, "PUT", { value });
  }
  $("#settings-message").textContent = "运行设置已保存。";
  await loadSettings();
}

async function triggerRefresh() {
  $("#settings-message").textContent = "正在触发采集刷新。";
  const result = await fetchJsonWithBody("/api/refresh", "POST", {});
  $("#settings-message").textContent = `${result.message} 报告：${result.report_path}`;
  await refreshOverview();
}

async function loadApiKeys() {
  const keys = await fetchJson("/api/api-keys");
  $("#api-key-rows").innerHTML = keys.map((key) => `<tr>
    <td>${key.key_id}</td><td>${escapeHtml(key.name)}</td><td class="mono">${escapeHtml(key.fingerprint)}</td>
    <td>${escapeHtml(key.scope)}</td><td>${key.enabled ? "启用" : "停用"}</td><td>${escapeHtml(fmtDateTime(key.last_used_at))}</td>
  </tr>`).join("");
}

async function createApiKey() {
  const name = $("#api-key-name").value.trim();
  const scope = $("#api-key-scope").value.trim() || "read";
  if (!name) {
    $("#settings-message").textContent = "API Key 名称不能为空。";
    return;
  }
  const created = await fetchJsonWithBody("/api/api-keys", "POST", { name, scope });
  $("#settings-message").textContent = `新 API Key：${created.secret}`;
  await loadApiKeys();
}

async function fetchJsonWithBody(url, method, body) {
  const response = await fetch(url, {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!response.ok) throw new Error(`${url} ${response.status}`);
  return response.json();
}

function renderOverview() {
  const summary = state.overview.summary;
  $("#latest-update").textContent = fmtDateTime(summary.latest_update ?? state.overview.latest_run?.finished_at);
  $("#summary-options").textContent = fmtNum(summary.active_options);
  $("#summary-iv").textContent = fmtPct(summary.iv_coverage);
  $("#summary-kline").textContent = summary.kline_coverage >= 0.999 ? "正常" : "补齐中";
  $("#summary-batches").textContent = fmtCollectionProgress(state.overview.collection);
  updateHealthPill(summary);
  renderExchangeCards();
  renderCollectionProgress();
  renderUnderlyingRows();
}

function updateHealthPill(summary) {
  const hasData = Number(summary.active_options) > 0;
  $("#health-text").textContent = hasData ? "数据已加载" : "等待采集";
  $("#health-dot").classList.toggle("warn", !hasData);
}

function renderExchangeCards() {
  const exchanges = state.overview.exchanges?.length ? state.overview.exchanges : [];
  $("#exchange-cards").innerHTML = exchanges.slice(0, 5).map((group) => {
    const quoteCoverage = group.quote_coverage ?? 0;
    const ivCoverage = group.iv_coverage ?? 0;
    const cls = quoteCoverage < 0.95 ? "bad" : ivCoverage < 0.98 ? "warn" : "";
    return `<div class="exchange-card ${cls}">
      <strong>${escapeHtml(group.exchange_id)}</strong><span>${fmtNum(group.product_count)}品种 / ${fmtNum(group.underlying_count)}标的</span>
      <div>Quote ${fmtPct(quoteCoverage)} · IV ${fmtPct(ivCoverage)}</div>
    </div>`;
  }).join("");
}

function fmtCollectionProgress(progress) {
  if (!progress || !progress.active_batches) return "--";
  return `${fmtNum(progress.success_batches)} / ${fmtNum(progress.active_batches)}`;
}

function renderCollectionProgress() {
  const progress = state.overview.collection ?? {};
  const refresh = state.overview.refresh ?? {};
  const active = Number(progress.active_batches ?? 0);
  const cards = [
    ["总分片", fmtNum(active), `${fmtNum(progress.planned_underlyings ?? 0)} 标的`],
    ["已完成", fmtNum(progress.success_batches ?? 0), fmtPct(progress.completion_ratio ?? 0)],
    ["待采集", fmtNum(progress.pending_batches ?? 0), "等待窗口执行"],
    ["失败待重试", fmtNum(progress.failed_batches ?? 0), "不会丢失进度"],
    [
      "后台任务",
      refresh.running ? "运行中" : "空闲",
      refresh.message ?? (refresh.finished_at ? fmtDateTime(refresh.finished_at) : "等待触发"),
    ],
    ["最近分片更新", fmtDateTime(progress.latest_batch_update), progress.scope ?? "--"],
  ];
  $("#collection-progress").innerHTML = cards.map(([label, value, hint]) => `
    <div class="progress-card">
      <span>${escapeHtml(label)}</span>
      <strong>${escapeHtml(value)}</strong>
      <small>${escapeHtml(hint)}</small>
    </div>
  `).join("");
  const failures = progress.recent_failures ?? [];
  $("#collection-failures").classList.toggle("d-none", failures.length === 0);
  $("#collection-failures").innerHTML = failures.length
    ? `最近失败分片：${failures.map((item) => `${escapeHtml(item.underlying_symbol)}#${escapeHtml(item.batch_index)} ${escapeHtml(item.last_error ?? item.status)}`).join("；")}`
    : "";
}

function renderUnderlyingRows() {
  const rows = state.overview.underlyings.filter((row) => !state.showIssuesOnly || row.status !== "正常");
  $("#overview-empty").classList.toggle("d-none", rows.length !== 0);
  $("#underlying-rows").innerHTML = rows.map((row) => {
    const statusClass = row.status === "正常" ? "good" : row.status === "采集中" ? "warn" : "bad";
    return `<tr class="clickable" data-underlying="${escapeHtml(row.underlying_symbol)}">
      <td>${escapeHtml(row.exchange_id)}</td>
      <td>${escapeHtml(productNames[row.product_id] ?? row.product_id)}</td>
      <td class="mono">${escapeHtml(row.underlying_symbol)}</td>
      <td class="mono">${escapeHtml(row.expiry_month ?? "--")}</td>
      <td class="mono">${fmtNum(row.call_count)}</td>
      <td class="mono">${fmtNum(row.put_count)}</td>
      <td class="good">${fmtPct(row.quote_coverage)}</td>
      <td class="${row.iv_coverage < 0.98 ? "warn" : "good"}">${fmtPct(row.iv_coverage)}</td>
      <td class="${row.kline_count ? "good" : "warn"}">${row.kline_count ? escapeHtml(fmtDateTime(row.display_kline_time ?? row.latest_kline_time)) : "缺口"}</td>
      <td class="mono">${escapeHtml(fmtDateTime(row.display_market_time ?? row.latest_quote_time))}</td>
      <td><span class="tag ${statusClass}">${escapeHtml(row.status)}</span></td>
      <td><button class="btn btn-sm btn-light" type="button">进入T型</button></td>
    </tr>`;
  }).join("");
  $$("#underlying-rows tr").forEach((row) => row.addEventListener("click", async () => {
    state.selectedUnderlying = row.dataset.underlying;
    renderSelectors();
    await loadQuote(state.selectedUnderlying, { animate: false });
    showPage("quote");
  }));
}

function selectorRows() {
  return state.overview?.underlyings ?? [];
}

function renderSelectors() {
  const rows = selectorRows();
  const current = rows.find((row) => row.underlying_symbol === state.selectedUnderlying) ?? rows[0];
  if (!current) return;
  const exchangeRows = rows.filter((row) => row.exchange_id === current.exchange_id);
  const productRows = exchangeRows.filter((row) => row.product_id === current.product_id);
  fillSelect($("#exchange-select"), unique(rows.map((row) => row.exchange_id)), current.exchange_id);
  fillSelect($("#product-select"), unique(exchangeRows.map((row) => row.product_id)), current.product_id, (value) => productNames[value] ?? value);
  fillSelectOptions(
    $("#expiry-select"),
    productRows.map((row) => ({
      value: row.underlying_symbol,
      label: row.expiry_month ?? row.underlying_symbol,
    })),
    current.underlying_symbol,
  );
}

function unique(values) {
  return [...new Set(values.filter(Boolean))].sort();
}

function fillSelect(select, values, selected, labelFn = (value) => value) {
  select.innerHTML = values.map((value) => `<option value="${escapeHtml(value)}">${escapeHtml(labelFn(value))}</option>`).join("");
  select.value = selected;
}

function fillSelectOptions(select, options, selected) {
  const seen = new Set();
  const uniqueOptions = [];
  for (const option of options) {
    if (seen.has(option.value)) continue;
    seen.add(option.value);
    uniqueOptions.push(option);
  }
  uniqueOptions.sort((left, right) => String(left.label).localeCompare(String(right.label)));
  select.innerHTML = uniqueOptions
    .map((option) => `<option value="${escapeHtml(option.value)}">${escapeHtml(option.label)}</option>`)
    .join("");
  select.value = selected;
}

async function onSelectorChange() {
  const rows = selectorRows();
  const exchange = $("#exchange-select").value;
  const product = $("#product-select").value;
  const selectedUnderlying = $("#expiry-select").value;
  const current =
    rows.find((row) => row.exchange_id === exchange && row.product_id === product && row.underlying_symbol === selectedUnderlying) ??
    rows.find((row) => row.exchange_id === exchange && row.product_id === product) ??
    rows.find((row) => row.exchange_id === exchange) ??
    rows[0];
  if (current) {
    state.selectedUnderlying = current.underlying_symbol;
    renderSelectors();
    await loadQuote(current.underlying_symbol, { animate: false });
  }
}

async function loadQuote(underlying, options = {}) {
  state.quote = await fetchJson(`/api/webui/tquote?underlying=${encodeURIComponent(underlying)}`);
  renderQuote(options);
}

async function refreshCurrentQuote() {
  if (!state.selectedUnderlying || !$("#quote").classList.contains("active")) return;
  try {
    state.quote = await fetchJson(`/api/webui/tquote?underlying=${encodeURIComponent(state.selectedUnderlying)}`);
    renderQuote({ animate: true });
  } catch {
    // Keep the last rendered quote visible if the local API is temporarily unavailable.
  }
}

async function refreshOverview() {
  try {
    state.overview = await fetchJson("/api/webui/overview?limit=500");
    if (
      state.selectedUnderlying &&
      !state.overview.underlyings.some((row) => row.underlying_symbol === state.selectedUnderlying)
    ) {
      state.selectedUnderlying = state.overview.underlyings[0]?.underlying_symbol ?? null;
      if (state.selectedUnderlying) {
        await loadQuote(state.selectedUnderlying, { animate: false });
      }
    }
    renderOverview();
    renderSelectors();
  } catch {
    // Keep the existing selector universe visible if the local API is temporarily unavailable.
  }
}

function renderQuote(options = {}) {
  const data = state.quote;
  const underlying = data.underlying ?? {};
  const parts = symbolParts(underlying.symbol);
  const activeOptions = data.strikes.reduce((count, row) => count + (row.CALL ? 1 : 0) + (row.PUT ? 1 : 0), 0);
  const withIv = data.strikes.reduce((count, row) => count + (row.CALL?.iv ? 1 : 0) + (row.PUT?.iv ? 1 : 0), 0);
  const withKline = data.strikes.reduce((count, row) => count + (row.CALL?.has_kline ? 1 : 0) + (row.PUT?.has_kline ? 1 : 0), 0);
  $("#quote-title").textContent = `${underlying.symbol ?? "T型报价"} ${parts.productName}期货`;
  const displayTime = underlying.last_kline_display_datetime ?? underlying.source_datetime ?? underlying.received_at;
  $("#quote-book-time").textContent = fmtDateTime(displayTime);
  $("#toolbar-book-time").textContent = fmtDateTime(displayTime);
  $("#quote-active-options").textContent = fmtNum(activeOptions);
  $("#quote-iv-coverage").textContent = activeOptions ? fmtPct(withIv / activeOptions) : "--";
  $("#quote-kline-status").textContent = activeOptions && withKline >= activeOptions ? "正常" : "补齐中";
  $("#quote-head").innerHTML = `<tr class="market-strip">
    <th class="call-title" colspan="9">看涨期权 CALL</th>
    <th class="under-bid"><span class="tag good">买价 ${fmtNum(underlying.bid_price1, 0)}</span></th>
    <th class="under-last"><span>标的最新</span><br>${fmtNum(underlying.last_price, 0)}</th>
    <th class="under-ask"><span class="tag bad">卖价 ${fmtNum(underlying.ask_price1, 0)}</span></th>
    <th class="put-title" colspan="9">看跌期权 PUT</th>
  </tr>
  <tr>
    <th>成交量</th><th>持仓量</th><th>IV</th><th>Vega</th><th>Theta</th><th>Gamma</th><th>Delta</th><th>最新价</th><th>卖价</th><th>买价</th>
    <th>行权价</th>
    <th>买价</th><th>卖价</th><th>最新价</th><th>Delta</th><th>Gamma</th><th>Theta</th><th>Vega</th><th>IV</th><th>持仓量</th><th>成交量</th>
  </tr>`;
  const maxima = sideMaxima(data.strikes);
  $("#quote-rows").innerHTML = data.strikes.map((row) => quoteRow(row, data, maxima)).join("");
  $$("#quote-rows tr").forEach((row) => row.addEventListener("click", () => openDetail(JSON.parse(row.dataset.payload))));
  applyLatestAnimation(options.animate !== false);
}

function sideMaxima(rows) {
  const maxima = {
    CALL: { volume: 0, open_interest: 0, bid_volume1: 0, ask_volume1: 0 },
    PUT: { volume: 0, open_interest: 0, bid_volume1: 0, ask_volume1: 0 },
  };
  for (const row of rows) {
    for (const side of ["CALL", "PUT"]) {
      const option = row[side];
      if (!option) continue;
      for (const key of Object.keys(maxima[side])) {
        maxima[side][key] = Math.max(maxima[side][key], Number(option[key]) || 0);
      }
    }
  }
  return maxima;
}

function moneynessClass(strike, side, data) {
  if (Number(strike) === Number(data.atm_strike)) return "atm-neutral";
  const last = Number(data.underlying?.last_price);
  if (!Number.isFinite(last)) return "otm";
  if (side === "CALL") return Number(strike) < last ? "itm" : "otm";
  return Number(strike) > last ? "itm" : "otm";
}

function quoteRow(row, data, maxima) {
  const payload = JSON.stringify({ strike: row.strike_price, call: row.CALL, put: row.PUT }).replace(/"/g, "&quot;");
  const atm = Number(row.strike_price) === Number(data.atm_strike) ? " atm" : "";
  return `<tr class="clickable" data-payload="${payload}">
    ${callCells(row.CALL, row.strike_price, data, maxima)}
    <td class="strike-cell${atm}">${fmtNum(row.strike_price, 0)}</td>
    ${putCells(row.PUT, row.strike_price, data, maxima)}
  </tr>`;
}

function callCells(option, strike, data, maxima) {
  const tone = `call-side ${moneynessClass(strike, "CALL", data)}`;
  return [
    barCell(option, { displayKey: "volume", max: maxima.CALL.volume, side: "call", kind: "quantity", className: tone }),
    barCell(option, { displayKey: "open_interest", max: maxima.CALL.open_interest, side: "call", kind: "quantity", className: tone }),
    textCell(option?.iv, `${tone} iv`, 3),
    textCell(option?.vega, tone, 3),
    textCell(option?.theta, tone, 3),
    textCell(option?.gamma, tone, 3),
    textCell(option?.delta, tone, 3),
    textCell(option?.last_price, `${tone} latest-price`, 3, true, option?.symbol),
    barCell(option, { displayKey: "ask_price1", energyKey: "ask_volume1", max: maxima.CALL.ask_volume1, side: "call", kind: "depth", className: `${tone} ask`, digits: 3, priceClass: "price-ask" }),
    barCell(option, { displayKey: "bid_price1", energyKey: "bid_volume1", max: maxima.CALL.bid_volume1, side: "call", kind: "depth", className: tone, digits: 3, priceClass: "price-bid" }),
  ].join("");
}

function putCells(option, strike, data, maxima) {
  const tone = `put-side ${moneynessClass(strike, "PUT", data)}`;
  return [
    barCell(option, { displayKey: "bid_price1", energyKey: "bid_volume1", max: maxima.PUT.bid_volume1, side: "put", kind: "depth", className: tone, digits: 3, priceClass: "price-bid" }),
    barCell(option, { displayKey: "ask_price1", energyKey: "ask_volume1", max: maxima.PUT.ask_volume1, side: "put", kind: "depth", className: `${tone} ask`, digits: 3, priceClass: "price-ask" }),
    textCell(option?.last_price, `${tone} latest-price`, 3, true, option?.symbol),
    textCell(option?.delta, tone, 3),
    textCell(option?.gamma, tone, 3),
    textCell(option?.theta, tone, 3),
    textCell(option?.vega, tone, 3),
    textCell(option?.iv, `${tone} iv`, 3),
    barCell(option, { displayKey: "open_interest", max: maxima.PUT.open_interest, side: "put", kind: "quantity", className: tone }),
    barCell(option, { displayKey: "volume", max: maxima.PUT.volume, side: "put", kind: "quantity", className: tone }),
  ].join("");
}

function textCell(value, className = "", digits = 0, trim = false, symbol = "") {
  const dataAttrs = symbol ? ` data-symbol="${escapeHtml(symbol)}" data-value="${escapeHtml(value ?? "")}"` : "";
  return `<td class="${className}"${dataAttrs}>${fmtMaybeTrim(value, digits, trim)}</td>`;
}

function energyWidth(value, max) {
  const numeric = Number(value) || 0;
  const maximum = Number(max) || 0;
  if (!numeric || !maximum) return 0;
  return Math.max(1, Math.round((numeric / maximum) * 100));
}

function barCell(option, config) {
  const value = option?.[config.displayKey];
  const energyValue = option?.[config.energyKey ?? config.displayKey];
  const pct = energyWidth(energyValue, config.max);
  const depthAttr = config.energyKey ? ` data-order-qty="${escapeHtml(energyValue ?? "")}"` : "";
  const priceClass = config.priceClass ?? "";
  const bar = pct ? `<span class="bar" style="width:${pct}%"></span>` : "";
  return `<td class="bar-cell ${config.className} ${config.side} ${config.kind ?? "quantity"}"${depthAttr}>
    ${bar}<span class="value ${priceClass}">${fmtMaybeTrim(value, config.digits ?? 0, (config.digits ?? 0) > 0)}</span>
  </td>`;
}

function applyLatestAnimation(animate) {
  $$(".latest-price[data-symbol]").forEach((cell) => {
    const symbol = cell.dataset.symbol;
    const current = Number(cell.dataset.value);
    const previous = state.previousLatest.get(symbol);
    cell.classList.remove("latest-up", "latest-down");
    if (animate && previous !== undefined && Number.isFinite(current) && current !== previous) {
      void cell.offsetWidth;
      cell.classList.add(current > previous ? "latest-up" : "latest-down");
    }
    if (Number.isFinite(current)) state.previousLatest.set(symbol, current);
  });
}

async function renderRuns() {
  try {
    const data = await fetchJson("/api/webui/runs?limit=20");
    $("#latest-run").textContent = data.runs[0]?.finished_at ?? "--";
    $("#latest-errors").textContent = fmtNum(data.errors.length);
    $("#runs-list").innerHTML = data.runs.map((run) => `<div class="log-row">
      <strong>${escapeHtml(run.status)}</strong>
      <span class="mono">${escapeHtml(run.finished_at ?? run.started_at ?? "--")}</span>
      <div class="muted">${escapeHtml(run.message ?? "")}</div>
    </div>`).join("");
  } catch {
    $("#runs-list").innerHTML = `<div class="notice">采集日志暂不可用。</div>`;
  }
}

function openDetail(payload) {
  $("#drawer-title").textContent = `行权价 ${fmtNum(payload.strike, 0)} 详情`;
  $("#drawer-body").innerHTML = [
    ["CALL 合约", payload.call?.symbol ?? "--"],
    ["PUT 合约", payload.put?.symbol ?? "--"],
    ["CALL Delta", fmtNum(payload.call?.delta, 3)],
    ["PUT Delta", fmtNum(payload.put?.delta, 3)],
    ["IV", `CALL ${fmtNum(payload.call?.iv, 3)} / PUT ${fmtNum(payload.put?.iv, 3)}`],
    ["K线", payload.call?.has_kline || payload.put?.has_kline ? "20D 完整" : "缺口"],
  ].map(([label, value]) => `<div class="kv"><span class="muted">${escapeHtml(label)}</span><span class="mono">${escapeHtml(value)}</span></div>`).join("");
  $("#drawer-backdrop").classList.add("open");
  $("#detail-drawer").classList.add("open");
  $("#detail-drawer").setAttribute("aria-hidden", "false");
}

function closeDrawer() {
  $("#drawer-backdrop").classList.remove("open");
  $("#detail-drawer").classList.remove("open");
  $("#detail-drawer").setAttribute("aria-hidden", "true");
}

window.__odmTestHooks = {
  renderQuote,
  applyLatestAnimation,
  state,
};

init().catch((error) => {
  document.body.innerHTML = `<main class="p-4"><div class="notice">WebUI 加载失败：${escapeHtml(error.message)}</div></main>`;
});
"""


if __name__ == "__main__":
    main()

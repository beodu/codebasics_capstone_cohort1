"""Live executive dashboard rendered from gold.* on SQL Server."""
from __future__ import annotations

from datetime import datetime

from . import config as cfg
from .sqlclient import Sql


def _f(v) -> float:
    return float(v or 0)


def _bars(rows, label_key, value_key, width=760, bar_h=30, gap=10, color="#2D6A9F"):
    if not rows:
        return "<p>No data.</p>"
    label_w, pad = 190, 90
    chart_w = width - label_w - pad
    peak = max(r[value_key] for r in rows) or 1
    height = len(rows) * (bar_h + gap) + 10
    out = [f'<svg viewBox="0 0 {width} {height}" class="chart" role="img">']
    for i, r in enumerate(rows):
        y = i * (bar_h + gap)
        w = max(2, chart_w * r[value_key] / peak)
        out.append(
            f'<text x="{label_w - 10}" y="{y + bar_h * 0.68}" text-anchor="end" '
            f'class="lbl">{r[label_key]}</text>'
            f'<rect x="{label_w}" y="{y}" width="{w:.1f}" height="{bar_h}" '
            f'rx="3" fill="{color}" opacity="0.88"/>'
            f'<text x="{label_w + w + 8}" y="{y + bar_h * 0.68}" class="val">'
            f'{r[value_key]:,.0f}</text>'
        )
    out.append("</svg>")
    return "".join(out)


def _line(rows, width=760, height=260):
    if not rows:
        return "<p>No data.</p>"
    ml, mr, mt, mb = 80, 30, 24, 46
    pw, ph = width - ml - mr, height - mt - mb
    peak = max(r["v"] for r in rows) or 1
    step = pw / max(1, len(rows) - 1)
    pts = [(ml + i * step, mt + ph - (ph * r["v"] / peak)) for i, r in enumerate(rows)]
    path = " ".join(f"{'M' if i == 0 else 'L'}{x:.1f},{y:.1f}" for i, (x, y) in enumerate(pts))
    area = (
        f"M{pts[0][0]:.1f},{mt + ph} "
        + " ".join(f"L{x:.1f},{y:.1f}" for x, y in pts)
        + f" L{pts[-1][0]:.1f},{mt + ph} Z"
    )
    g = [f'<svg viewBox="0 0 {width} {height}" class="chart" role="img">']
    for frac in (0, 0.25, 0.5, 0.75, 1):
        y = mt + ph - ph * frac
        g.append(
            f'<line x1="{ml}" y1="{y:.1f}" x2="{ml + pw}" y2="{y:.1f}" '
            f'stroke="#E3E6EA" stroke-width="1"/>'
            f'<text x="{ml - 10}" y="{y + 4:.1f}" text-anchor="end" '
            f'class="ax">{peak * frac / 1000:,.0f}k</text>'
        )
    g.append(f'<path d="{area}" fill="#2D6A9F" opacity="0.10"/>')
    g.append(f'<path d="{path}" fill="none" stroke="#2D6A9F" stroke-width="2.5"/>')
    for (x, y), r in zip(pts, rows):
        g.append(
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="#fff" '
            f'stroke="#2D6A9F" stroke-width="2.5"/>'
            f'<text x="{x:.1f}" y="{y - 12:.1f}" text-anchor="middle" class="val">'
            f'{r["v"] / 1000:,.0f}k</text>'
            f'<text x="{x:.1f}" y="{mt + ph + 22}" text-anchor="middle" class="ax">{r["k"]}</text>'
        )
    g.append("</svg>")
    return "".join(g)


def _grouped(rows, width=760, height=270):
    if not rows:
        return "<p>No data.</p>"
    ml, mr, mt, mb = 60, 30, 30, 48
    pw, ph = width - ml - mr, height - mt - mb
    peak = max(max(r["new"], r["ret"]) for r in rows) or 1
    slot = pw / len(rows)
    bw = min(26, slot / 2.6)
    g = [f'<svg viewBox="0 0 {width} {height}" class="chart" role="img">']
    for frac in (0, 0.5, 1):
        y = mt + ph - ph * frac
        g.append(
            f'<line x1="{ml}" y1="{y:.1f}" x2="{ml + pw}" y2="{y:.1f}" stroke="#E3E6EA"/>'
            f'<text x="{ml - 8}" y="{y + 4:.1f}" text-anchor="end" class="ax">{peak * frac:,.0f}</text>'
        )
    for i, r in enumerate(rows):
        cx = ml + slot * i + slot / 2
        for j, (key, color) in enumerate((("new", "#4A9D7C"), ("ret", "#2D6A9F"))):
            h = ph * r[key] / peak
            x = cx - bw - 2 + j * (bw + 4)
            g.append(
                f'<rect x="{x:.1f}" y="{mt + ph - h:.1f}" width="{bw:.1f}" '
                f'height="{max(1, h):.1f}" rx="2" fill="{color}"/>'
                f'<text x="{x + bw / 2:.1f}" y="{mt + ph - h - 6:.1f}" '
                f'text-anchor="middle" class="val">{r[key]}</text>'
            )
        g.append(
            f'<text x="{cx:.1f}" y="{mt + ph + 22}" text-anchor="middle" class="ax">{r["k"]}</text>'
        )
    g.append("</svg>")
    return "".join(g)


def _filters(from_date, to_date, category):
    parts, params = [], []
    if from_date:
        parts.append("f.order_date >= %s")
        params.append(from_date)
    if to_date:
        parts.append("f.order_date <= %s")
        params.append(to_date)
    if category:
        parts.append(
            "EXISTS (SELECT 1 FROM gold.dim_product px "
            "WHERE px.product_id = f.product_id AND px.category = %s)"
        )
        params.append(category)
    extra = (" AND " + " AND ".join(parts)) if parts else ""
    return extra, params


def load(from_date=None, to_date=None, category=None) -> dict:
    extra, params = _filters(from_date, to_date, category)
    dw = Sql(cfg.DW_DB)
    try:
        categories = [r["category"] for r in dw.query(
            "SELECT DISTINCT category FROM gold.dim_product ORDER BY category"
        )]
        kpi = dw.query(
            f"""
            SELECT
                CAST(ROUND(SUM(gross_revenue), 2) AS DECIMAL(14,2)) AS gross,
                CAST(ROUND(SUM(CASE WHEN is_revenue = 1 THEN gross_revenue ELSE 0 END), 2) AS DECIMAL(14,2)) AS net,
                CAST(ROUND(SUM(CASE WHEN is_returned = 1 THEN gross_revenue ELSE 0 END), 2) AS DECIMAL(14,2)) AS returned,
                COUNT(DISTINCT order_id) AS orders,
                COUNT(*) AS items,
                COUNT(DISTINCT customer_id) AS customers
            FROM gold.fact_sales f
            WHERE 1=1 {extra}
            """,
            params or None,
        )[0]
        trend = [
            {"k": r["year_month"].strip() if isinstance(r["year_month"], str) else r["year_month"],
             "v": _f(r["revenue"])}
            for r in dw.query(
                f"""
                SELECT d.year_month, SUM(f.gross_revenue) AS revenue
                FROM gold.fact_sales f
                JOIN gold.dim_date d ON f.date_key = d.date_key
                WHERE f.is_revenue = 1 {extra}
                GROUP BY d.year_month
                ORDER BY d.year_month
                """,
                params or None,
            )
        ]
        products = [
            {"n": r["product_name"], "v": _f(r["revenue"])}
            for r in dw.query(
                f"""
                SELECT TOP 8 p.product_name, SUM(f.gross_revenue) AS revenue
                FROM gold.fact_sales f
                JOIN gold.dim_product p ON f.product_id = p.product_id
                WHERE f.is_revenue = 1 {extra}
                GROUP BY p.product_name
                ORDER BY SUM(f.gross_revenue) DESC
                """,
                params or None,
            )
        ]
        cities = [
            {"n": r["city"], "v": _f(r["revenue"])}
            for r in dw.query(
                f"""
                SELECT TOP 8 c.city, SUM(f.gross_revenue) AS revenue
                FROM gold.fact_sales f
                JOIN gold.dim_customer c ON f.customer_id = c.customer_id
                WHERE f.is_revenue = 1 {extra}
                GROUP BY c.city
                ORDER BY SUM(f.gross_revenue) DESC
                """,
                params or None,
            )
        ]
        cohort = [
            {"k": r["year_month"].strip() if isinstance(r["year_month"], str) else r["year_month"],
             "new": int(r["new_customers"]), "ret": int(r["returning_customers"])}
            for r in dw.query(
                f"""
                WITH first_order AS (
                    SELECT f.customer_id, MIN(d.year_month) AS first_month
                    FROM gold.fact_sales f
                    JOIN gold.dim_date d ON f.date_key = d.date_key
                    WHERE f.is_revenue = 1 {extra}
                    GROUP BY f.customer_id
                )
                SELECT d.year_month,
                       COUNT(DISTINCT CASE WHEN fo.first_month = d.year_month THEN f.customer_id END) AS new_customers,
                       COUNT(DISTINCT CASE WHEN fo.first_month < d.year_month THEN f.customer_id END) AS returning_customers
                FROM gold.fact_sales f
                JOIN gold.dim_date d ON f.date_key = d.date_key
                JOIN first_order fo ON f.customer_id = fo.customer_id
                WHERE f.is_revenue = 1 {extra}
                GROUP BY d.year_month
                ORDER BY d.year_month
                """,
                (params + params) if params else None,
            )
        ]
        status = dw.query(
            f"""
            SELECT f.status,
                   COUNT(DISTINCT f.order_id) AS orders,
                   CAST(ROUND(SUM(f.gross_revenue), 2) AS DECIMAL(14,2)) AS revenue
            FROM gold.fact_sales f
            WHERE 1=1 {extra}
            GROUP BY f.status
            ORDER BY SUM(f.gross_revenue) DESC
            """,
            params or None,
        )
        runs = dw.query(
            """
            SELECT TOP 8 run_id, stage, COUNT(*) AS tables,
                   SUM(rows_in) AS rows_in, MAX(status) AS status,
                   CAST(ROUND(SUM(duration_secs), 2) AS DECIMAL(10,2)) AS secs
            FROM ops.pipeline_run_log
            GROUP BY run_id, stage
            ORDER BY MIN(started_at) DESC
            """
        )
        return {
            "gross": _f(kpi["gross"]),
            "net": _f(kpi["net"]),
            "returned": _f(kpi["returned"]),
            "orders": int(kpi["orders"]),
            "items": int(kpi["items"]),
            "customers": int(kpi["customers"]),
            "trend": trend,
            "products": products,
            "cities": cities,
            "cohort": cohort,
            "status": status,
            "runs": runs,
            "categories": categories,
            "from_date": from_date or "",
            "to_date": to_date or "",
            "category": category or "",
        }
    finally:
        dw.close()


def render_html(from_date=None, to_date=None, category=None) -> str:
    d = load(from_date, to_date, category)
    cat_opts = "".join(
        f"<option value='{c}'{' selected' if c == d['category'] else ''}>{c}</option>"
        for c in d["categories"]
    )
    gross, net, returned = d["gross"], d["net"], d["returned"]
    aov = net / d["orders"] if d["orders"] else 0
    status_rows = "".join(
        f"<tr><td>{r['status']}</td><td class='n'>{int(r['orders']):,}</td>"
        f"<td class='n'>{_f(r['revenue']):,.2f}</td>"
        f"<td class='n'>{_f(r['revenue']) / gross * 100 if gross else 0:.1f}%</td></tr>"
        for r in d["status"]
    )
    run_rows = "".join(
        f"<tr><td class='mono'>{r['run_id']}</td><td>{r['stage']}</td>"
        f"<td class='n'>{int(r['tables'])}</td><td class='n'>{int(r['rows_in'] or 0):,}</td>"
        f"<td>{r['status']}</td><td class='n'>{r['secs']}s</td></tr>"
        for r in d["runs"]
    )
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>AtliQ Commerce — Executive Dashboard</title>
<style>
  :root {{ --ink:#1B2733; --muted:#66788A; --line:#E3E6EA; --bg:#F4F6F8; }}
  * {{ box-sizing:border-box; }}
  body {{ margin:0; background:var(--bg); color:var(--ink);
    font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif; }}
  .wrap {{ max-width:1180px; margin:0 auto; padding:28px 20px 60px; }}
  header {{ border-bottom:2px solid var(--ink); padding-bottom:14px; margin-bottom:22px; }}
  h1 {{ margin:0; font-size:26px; letter-spacing:-0.02em; }}
  .sub {{ color:var(--muted); font-size:13px; margin-top:6px; }}
  .live {{ display:inline-block; margin-left:8px; font-size:11px; letter-spacing:.08em;
    text-transform:uppercase; background:#1B2733; color:#9FB2C4; padding:3px 8px; border-radius:4px; }}
  .kpis {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(168px,1fr));
    gap:12px; margin-bottom:24px; }}
  .kpi {{ background:#fff; border:1px solid var(--line); border-radius:8px; padding:16px; }}
  .kpi .k {{ font-size:11px; text-transform:uppercase; letter-spacing:.07em; color:var(--muted); }}
  .kpi .v {{ font-size:24px; font-weight:650; margin-top:6px; letter-spacing:-0.02em; }}
  .kpi.hero {{ background:var(--ink); color:#fff; border-color:var(--ink); }}
  .kpi.hero .k {{ color:#9FB2C4; }}
  .grid {{ display:grid; grid-template-columns:1fr 1fr; gap:18px; }}
  .card {{ background:#fff; border:1px solid var(--line); border-radius:8px; padding:18px 20px 20px; }}
  .card.full {{ grid-column:1/-1; }}
  h2 {{ font-size:14px; margin:0 0 4px; letter-spacing:-0.01em; }}
  .hint {{ font-size:12px; color:var(--muted); margin:0 0 14px; }}
  .chart {{ width:100%; height:auto; }}
  .lbl {{ font-size:12px; fill:var(--ink); }}
  .val {{ font-size:11px; fill:var(--muted); }}
  .ax  {{ font-size:11px; fill:var(--muted); }}
  table {{ width:100%; border-collapse:collapse; font-size:13px; }}
  th {{ text-align:left; font-size:11px; text-transform:uppercase; letter-spacing:.06em;
    color:var(--muted); border-bottom:1px solid var(--line); padding:8px 6px; }}
  td {{ padding:8px 6px; border-bottom:1px solid #F0F2F4; }}
  td.n {{ text-align:right; font-variant-numeric:tabular-nums; }}
  .mono {{ font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:12px; }}
  .legend {{ font-size:12px; color:var(--muted); margin-top:10px; }}
  .dot {{ display:inline-block; width:9px; height:9px; border-radius:2px; margin:0 5px 0 14px; }}
  .note {{ background:#FFF8E7; border:1px solid #E8D9A8; border-radius:8px;
    padding:14px 18px; font-size:13px; margin-top:22px; line-height:1.55; }}
  footer {{ margin-top:26px; font-size:12px; color:var(--muted);
    border-top:1px solid var(--line); padding-top:14px; line-height:1.6; }}
  a.refresh {{ float:right; font-size:13px; color:#2D6A9F; }}
  .filters {{ display:flex; flex-wrap:wrap; gap:10px; align-items:end; margin:14px 0 0;
    background:#fff; border:1px solid var(--line); border-radius:8px; padding:12px 14px; }}
  .filters label {{ font-size:11px; text-transform:uppercase; letter-spacing:.06em; color:var(--muted); display:block; }}
  .filters input, .filters select {{ margin-top:4px; padding:6px 8px; border:1px solid var(--line); border-radius:4px; }}
  .filters button {{ padding:8px 14px; background:var(--ink); color:#fff; border:0; border-radius:4px; cursor:pointer; }}
  @media (max-width:820px) {{ .grid {{ grid-template-columns:1fr; }} }}
</style></head><body><div class="wrap">

<header>
  <a class="refresh" href="/">Reset</a>
  <h1>AtliQ Commerce — Executive Dashboard
    <span class="live">live · SQL Server</span></h1>
  <div class="sub">gold.fact_sales grain: one row per order item ·
    queried {datetime.now():%d %b %Y %H:%M} from {cfg.DW_DB}.gold on {cfg.SQL_SERVER},{cfg.SQL_PORT}</div>
  <form class="filters" method="get" action="/">
    <div><label>From</label>
      <input type="date" name="from" value="{d['from_date']}"></div>
    <div><label>To</label>
      <input type="date" name="to" value="{d['to_date']}"></div>
    <div><label>Category</label>
      <select name="category"><option value="">All categories</option>{cat_opts}</select></div>
    <button type="submit">Apply slicers</button>
  </form>
</header>

<div class="kpis">
  <div class="kpi hero"><div class="k">Net revenue</div><div class="v">{net:,.0f}</div></div>
  <div class="kpi"><div class="k">Gross revenue</div><div class="v">{gross:,.0f}</div></div>
  <div class="kpi"><div class="k">Orders</div><div class="v">{d['orders']:,}</div></div>
  <div class="kpi"><div class="k">Avg order value</div><div class="v">{aov:,.0f}</div></div>
  <div class="kpi"><div class="k">Line items</div><div class="v">{d['items']:,}</div></div>
  <div class="kpi"><div class="k">Customers</div><div class="v">{d['customers']:,}</div></div>
</div>

<div class="grid">
  <div class="card full">
    <h2>Revenue trend by month</h2>
    <p class="hint">Net revenue. Cancelled orders excluded. Via gold.dim_date.</p>
    {_line(d["trend"])}
  </div>
  <div class="card">
    <h2>Top products</h2>
    <p class="hint">Net revenue by product. Via gold.dim_product.</p>
    {_bars(d["products"], "n", "v", color="#4A9D7C")}
  </div>
  <div class="card">
    <h2>Top cities</h2>
    <p class="hint">Net revenue by customer city. Via gold.dim_customer.</p>
    {_bars(d["cities"], "n", "v", color="#C77D3A")}
  </div>
  <div class="card full">
    <h2>New vs returning customers</h2>
    <p class="hint">A customer is new in the month of their first purchase, and returning every month after.</p>
    {_grouped(d["cohort"])}
    <div class="legend">
      <span class="dot" style="background:#4A9D7C"></span>New
      <span class="dot" style="background:#2D6A9F"></span>Returning
    </div>
  </div>
  <div class="card">
    <h2>Orders and revenue by status</h2>
    <p class="hint">Why the revenue definition has to be stated.</p>
    <table><thead><tr><th>Status</th><th class="n">Orders</th>
      <th class="n">Gross revenue</th><th class="n">Share</th></tr></thead>
      <tbody>{status_rows}</tbody></table>
  </div>
  <div class="card">
    <h2>Pipeline run log</h2>
    <p class="hint">ops.pipeline_run_log — every stage of every run.</p>
    <table><thead><tr><th>Run</th><th>Stage</th><th class="n">Tables</th>
      <th class="n">Rows in</th><th>Status</th><th class="n">Time</th></tr></thead>
      <tbody>{run_rows}</tbody></table>
  </div>
</div>

<div class="note">
  <strong>Revenue definition.</strong> Net revenue excludes Cancelled orders.
  Returned orders ({returned:,.0f}) are shown separately and are not deducted from
  net revenue. Gross and net differ by {gross - net:,.0f}.
</div>

<footer>
  Served from <span class="mono">{cfg.DW_DB}.gold</span> on this machine's SQL Server.
  Azure deployment reads the same star schema through Fabric / OneLake.
</footer>
</div></body></html>"""


def write_snapshot(path=None):
    path = path or (cfg.LAKEHOUSE / "dashboard.html")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_html(), encoding="utf-8")
    return path

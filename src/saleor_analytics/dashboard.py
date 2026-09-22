"""A deliberately small Dash reader for the published gold-layer tables."""

from __future__ import annotations

from pathlib import Path

import duckdb
import plotly.express as px
from dash import Dash, Input, Output, dcc, html


def create_app(database: Path) -> Dash:
    """Load Gold aggregates once and construct the demo Dash app (A/F).

    Reads the local analytics catalog/schema and closes the connection before
    serving. This is a startup snapshot, not automatic release refresh. Current
    KPI/product aggregations mix currencies and product rankings ignore channel
    selection; use currency-separated SQL for validation pending those fixes.
    """
    if not database.is_file():
        raise FileNotFoundError("Published warehouse not found; run build-warehouse first")
    connection = duckdb.connect(str(database), read_only=True)
    daily = connection.execute(
        "select order_date, channel, currency, order_count, gross_amount, average_order_value "
        "from analytics.analytics.daily_order_metrics order by order_date"
    ).fetchdf()
    products = connection.execute(
        "select order_date, product_name, units_ordered, gross_amount "
        "from analytics.analytics.daily_product_metrics"
    ).fetchdf()
    connection.close()
    channels = sorted(daily["channel"].dropna().unique())
    app = Dash(__name__)
    app.layout = html.Main(
        [
            html.H1("Saleor analytics demo"),
            html.P("Synthetic local data. Gold-layer metrics are refreshed by the pipeline."),
            dcc.Dropdown(channels, channels, id="channel-filter", multi=True),
            html.Div(id="kpis"),
            dcc.Graph(id="revenue-trend"),
            dcc.Graph(id="product-ranking"),
        ],
        style={"maxWidth": "1100px", "margin": "auto", "fontFamily": "Arial"},
    )

    @app.callback(
        Output("kpis", "children"),
        Output("revenue-trend", "figure"),
        Output("product-ranking", "figure"),
        Input("channel-filter", "value"),
    )
    def update(selected_channels):
        filtered = daily[daily["channel"].isin(selected_channels or channels)]
        gross = filtered["gross_amount"].sum()
        count = filtered["order_count"].sum()
        trend = px.line(
            filtered,
            x="order_date",
            y="gross_amount",
            color="currency",
            title="Daily gross revenue",
        )
        ranked = products.groupby("product_name", as_index=False)["gross_amount"].sum()
        ranked = ranked.nlargest(10, "gross_amount")
        product_chart = px.bar(
            ranked, x="gross_amount", y="product_name", orientation="h", title="Top products"
        )
        return html.H2(f"{count:,} orders · {gross:,.2f} gross revenue"), trend, product_chart

    return app

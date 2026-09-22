"""Currency-correct Gold reporting with release metadata (assessment A/F)."""

from datetime import UTC, datetime

import duckdb
import pandas as pd
import plotly.express as px
from dash import Dash, Input, Output, dcc, html

from saleor_analytics.config import Settings
from saleor_analytics.pipeline import published_database


def load_report(settings: Settings):
    """Resolve one release once per refresh and read both models from it."""
    path, pointer = published_database(settings)
    with duckdb.connect(str(path), read_only=True) as connection:
        daily = connection.sql("select * from analytics.analytics.daily_order_metrics").fetchdf()
        products = connection.sql(
            "select * from analytics.analytics.daily_product_metrics"
        ).fetchdf()
    return daily, products, pointer


def filter_report(daily, products, currency, channels, start, end):
    """Apply identical currency/channel/date selections to all metrics."""

    def selected(frame):
        mask = (frame.currency == currency) & frame.channel.isin(channels or [])
        dates = pd.to_datetime(frame.order_date).dt.date
        if start:
            mask &= dates >= pd.Timestamp(start).date()
        if end:
            mask &= dates <= pd.Timestamp(end).date()
        return frame.loc[mask].copy()

    return selected(daily), selected(products)


def create_app(settings: Settings) -> Dash:
    daily, _, _ = load_report(settings)
    currencies = sorted(daily.currency.unique())
    channels = sorted(daily.channel.unique())
    app = Dash(__name__)
    app.layout = html.Main(
        [
            html.H1("Saleor order analytics"),
            html.P("Synthetic data. Gross order values exclude canceled and draft orders."),
            dcc.Dropdown(
                currencies, currencies[0] if currencies else None, id="currency", clearable=False
            ),
            dcc.Dropdown(channels, channels, id="channels", multi=True),
            dcc.DatePickerRange(id="dates"),
            dcc.Interval(id="refresh", interval=30000),
            html.Pre(id="quality"),
            html.Div(id="kpis"),
            dcc.Graph(id="trend"),
            dcc.Graph(id="products"),
        ],
        style={"maxWidth": "1100px", "margin": "auto", "fontFamily": "Arial"},
    )

    @app.callback(
        Output("kpis", "children"),
        Output("trend", "figure"),
        Output("products", "figure"),
        Output("quality", "children"),
        Output("currency", "options"),
        Output("channels", "options"),
        Input("currency", "value"),
        Input("channels", "value"),
        Input("dates", "start_date"),
        Input("dates", "end_date"),
        Input("refresh", "n_intervals"),
    )
    def update(currency, selected_channels, start, end, _):
        daily, products, pointer = load_report(settings)
        currencies, channels = sorted(daily.currency.unique()), sorted(daily.channel.unique())
        filtered, lines = filter_report(daily, products, currency, selected_channels, start, end)
        total, count = filtered.gross_amount.sum(), int(filtered.order_count.sum())
        trend = px.line(
            filtered,
            x="order_date",
            y="gross_amount",
            color="channel",
            title=f"Daily gross order value ({currency})",
        )
        ranked = lines.groupby(
            ["sku", "product_name"], dropna=False, as_index=False
        ).gross_amount.sum()
        bars = px.bar(
            ranked.nlargest(10, "gross_amount"),
            x="gross_amount",
            y="product_name",
            orientation="h",
            title=f"Product gross value ({currency})",
        )
        age = (datetime.now(UTC) - datetime.fromisoformat(pointer["published_at"])).total_seconds()
        quality = (
            f"Release: {pointer['release_id']} | published: {pointer['published_at']}\n"
            f"Publication age: {age / 3600:.1f}h | "
            f"{'STALE (>24h)' if age > 86400 else 'within 24h'}\n"
            f"dbt tests passed: {pointer['data_tests_passed']} | "
            f"input rejects: {pointer['rejected_count']} | "
            f"duplicates collapsed: {pointer['duplicate_count']}\n"
            "Fresh publication does not guarantee source completeness."
        )
        return (
            html.H2(f"{count:,} orders | {currency or ''} {total:,.2f}"),
            trend,
            bars,
            quality,
            currencies,
            channels,
        )

    return app

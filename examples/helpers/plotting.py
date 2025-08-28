"""
Helper functions for plotting and visualizing data using Plotly.
"""

import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from typing import Optional, Dict


def daily_supply(
    data: pd.DataFrame,
    token_symbol: Optional[str] = None,
    token_address: Optional[str] = None,
    fig_height: int = 700,
    color_scheme: Optional[Dict[str, str]] = None,
    title: Optional[str] = None,
    save_path: Optional[str] = None,
) -> go.Figure:
    """
    Create a comprehensive plot showing daily mint/burn, net supply change, and cumulative supply.

    Args:
        data: DataFrame with columns: date, daily_mint_amount, daily_burn_amount,
              mint_count, burn_count
        token_symbol: Optional token symbol for display
        token_address: Optional token address for display
        fig_height: Height of the figure in pixels
        color_scheme: Optional custom colors for mint/burn
        title: Custom title for the plot
        save_path: Optional path to save the plot as HTML

    Returns:
        Plotly figure object
    """

    # Default color scheme
    if color_scheme is None:
        color_scheme = {
            "mint": "#00C851",  # Green for minting
            "burn": "#FF4444",  # Red for burning
            "net_change": "#666666",  # Gray for net change
            "cumulative": "#2196F3",  # Blue for cumulative supply
        }

    # Create subplots: 2 rows, 1 column
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        row_heights=[0.6, 0.4],
    )

    # Convert amounts to human-readable format (assuming 18 decimals)
    data = data.copy()
    data["daily_mint_amount_hr"] = data["daily_mint_amount"] / 1e18
    data["daily_burn_amount_hr"] = data["daily_burn_amount"] / 1e18
    data["net_supply_change"] = (
        data["daily_mint_amount_hr"] - data["daily_burn_amount_hr"]
    )

    # Calculate cumulative values
    data["cumulative_mint"] = data["daily_mint_amount_hr"].cumsum()
    data["cumulative_burn"] = data["daily_burn_amount_hr"].cumsum()
    data["cumulative_supply"] = data["cumulative_mint"] - data["cumulative_burn"]

    # Sort by date to ensure proper ordering
    data = data.sort_values("date")

    # Plot 1: Daily mint/burn bars and net supply change line (top subplot)
    # Mint bars
    fig.add_trace(
        go.Bar(
            x=data["date"],
            y=data["daily_mint_amount_hr"],
            name="Mint",
            marker_color=color_scheme["mint"],
            opacity=0.7,
            hovertemplate="<b>%{x}</b><br>" + "Mint: %{y:,.2f}<br>" + "<extra></extra>",
        ),
        row=1,
        col=1,
    )

    # Burn bars
    fig.add_trace(
        go.Bar(
            x=data["date"],
            y=data["daily_burn_amount_hr"],
            name="Burn",
            marker_color=color_scheme["burn"],
            opacity=0.7,
            hovertemplate="<b>%{x}</b><br>" + "Burn: %{y:,.2f}<br>" + "<extra></extra>",
        ),
        row=1,
        col=1,
    )

    # Net supply change line (overlay on bars)
    fig.add_trace(
        go.Scatter(
            x=data["date"],
            y=data["net_supply_change"],
            name="Net Change",
            line=dict(color=color_scheme["net_change"], width=1),
            mode="lines+markers",
            marker=dict(size=3),
            hovertemplate="<b>%{x}</b><br>"
            + "Net Change: %{y:,.2f}<br>"
            + "<extra></extra>",
        ),
        row=1,
        col=1,
    )

    # Add horizontal line at y=0 for reference
    fig.add_hline(y=0, line_dash="dash", line_color="gray", row=1, col=1)

    # Plot 2: Cumulative supply (bottom subplot)
    fig.add_trace(
        go.Scatter(
            x=data["date"],
            y=data["cumulative_supply"],
            name="Supply",
            line=dict(color=color_scheme["cumulative"], width=3),
            hovertemplate="<b>%{x}</b><br>"
            + "Cumulative Supply: %{y:,.2f}<br>"
            + "<extra></extra>",
        ),
        row=2,
        col=1,
    )

    # Update layout
    if title is None:
        title_parts = []
        if token_symbol:
            title_parts.append(f"Token: {token_symbol}")
        if token_address:
            title_parts.append(f"Address: {token_address[:10]}...{token_address[-8:]}")
        title = " | ".join(title_parts) if title_parts else "Token Supply"

    fig.update_layout(
        title=dict(text=title, x=0.5, font=dict(size=18)),
        height=fig_height,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            bgcolor="rgba(255,255,255,0.8)",
        ),
        hovermode="x unified",
        barmode="overlay",  # Overlay bars for better visibility
    )

    # Update x-axis (shared between subplots)
    fig.update_xaxes(
        title_text="Date", tickformat="%Y-%m-%d", tickangle=45, row=2, col=1
    )

    # Update y-axes
    fig.update_yaxes(
        title_text="Daily Mint/Burn & Net Change", row=1, col=1, side="left"
    )
    fig.update_yaxes(title_text="Token Supply", row=2, col=1, side="left")

    # Save if path provided
    if save_path:
        fig.write_html(save_path)
        print(f"Plot saved to: {save_path}")

    return fig

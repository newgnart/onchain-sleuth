# Plotting Functions for Token Mint/Burn Analysis

This module provides comprehensive plotting functions using Plotly to visualize daily mint and burn data for ERC-20 tokens.

## Features

- **Daily Mint/Burn Analysis**: Multi-panel visualization showing volume, transaction counts, and net supply changes
- **Cumulative Supply Tracking**: Line charts showing cumulative minted, burned, and net supply over time
- **Activity Heatmap**: Visual representation of mint/burn activity by day of week and hour
- **Comprehensive Dashboard**: All visualizations combined in a single interactive dashboard
- **Interactive HTML Output**: All plots are saved as interactive HTML files that can be shared and viewed in any browser

## Installation Requirements

```bash
pip install plotly pandas numpy
```

## Quick Start

```python
from helpers.plotting import plot_daily_mint_burn, create_dashboard
import pandas as pd

# Your data from the SQL query
data = pd.DataFrame({
    'date': ['2025-01-01', '2025-01-02', ...],
    'daily_mint_amount': [1000000000000000000, 0, ...],
    'daily_burn_amount': [0, 500000000000000000, ...],
    'mint_count': [1, 0, ...],
    'burn_count': [0, 1, ...]
})

# Create a daily mint/burn plot
fig = plot_daily_mint_burn(
    data=data,
    token_symbol="YOUR_TOKEN",
    token_address="0x323c03c48660fe31186fa82c289b0766d331ce21",
    save_path="token_analysis.html"
)

# Or create a comprehensive dashboard
dashboard = create_dashboard(
    data=data,
    token_symbol="YOUR_TOKEN",
    token_address="0x323c03c48660fe31186fa82c289b0766d331ce21",
    save_path="token_dashboard.html"
)
```

## Available Functions

### 1. `plot_daily_mint_burn()`

Creates a comprehensive multi-panel visualization of daily mint and burn data.

**Parameters:**
- `data`: DataFrame with columns: `date`, `daily_mint_amount`, `daily_burn_amount`, `mint_count`, `burn_count`
- `token_symbol`: Optional token symbol for display
- `token_address`: Optional token address for display
- `fig_height`: Height of the figure in pixels (default: 600)
- `show_volume`: Whether to show volume bars (default: True)
- `show_counts`: Whether to show transaction count lines (default: True)
- `color_scheme`: Optional custom colors for mint/burn
- `title`: Custom title for the plot
- `save_path`: Optional path to save the plot as HTML

**Returns:** Plotly figure object

**Features:**
- Daily volume bars (mint in green, burn in red)
- Transaction count lines
- Net supply change area chart
- Interactive hover tooltips
- Responsive layout

### 2. `plot_cumulative_supply()`

Creates a line chart showing cumulative supply changes over time.

**Parameters:**
- `data`: DataFrame with columns: `date`, `daily_mint_amount`, `daily_burn_amount`
- `token_symbol`: Optional token symbol for display
- `token_address`: Optional token address for display
- `fig_height`: Height of the figure in pixels (default: 500)
- `color_scheme`: Optional custom colors
- `title`: Custom title for the plot
- `save_path`: Optional path to save the plot as HTML

**Returns:** Plotly figure object

**Features:**
- Cumulative minted amount (green line)
- Cumulative burned amount (red line)
- Net supply (blue line)
- Filled areas for better visualization

### 3. `plot_mint_burn_heatmap()`

Creates a heatmap showing mint/burn activity by day of week and hour.

**Parameters:**
- `data`: DataFrame with columns: `date`, `daily_mint_amount`, `daily_burn_amount`
- `token_symbol`: Optional token symbol for display
- `token_address`: Optional token address for display
- `fig_height`: Height of the figure in pixels (default: 400)
- `color_scheme`: Optional color scheme for the heatmap
- `title`: Custom title for the plot
- `save_path`: Optional path to save the plot as HTML

**Returns:** Plotly figure object

**Features:**
- Activity intensity by day and hour
- Color-coded activity levels
- Interactive hover information

### 4. `create_dashboard()`

Creates a comprehensive dashboard with all visualizations combined.

**Parameters:**
- `data`: DataFrame with mint/burn data
- `token_symbol`: Optional token symbol for display
- `token_address`: Optional token address for display
- `fig_height`: Height of the figure in pixels (default: 800)
- `save_path`: Optional path to save the plot as HTML

**Returns:** Plotly figure object

**Features:**
- 6-panel layout with all visualizations
- Summary statistics table
- Consistent styling and layout
- Interactive elements

## Data Format

Your DataFrame should have the following columns:

```python
data = pd.DataFrame({
    'date': ['2025-01-01', '2025-01-02', ...],  # Date strings or datetime objects
    'daily_mint_amount': [1000000000000000000, 0, ...],  # Raw amounts (wei)
    'daily_burn_amount': [0, 500000000000000000, ...],   # Raw amounts (wei)
    'mint_count': [1, 0, ...],                          # Integer counts
    'burn_count': [0, 1, ...]                           # Integer counts
})
```

**Note:** The functions automatically convert amounts from wei (18 decimals) to human-readable format by dividing by 1e18.

## Customization

### Color Schemes

You can customize colors by passing a `color_scheme` dictionary:

```python
custom_colors = {
    'mint': '#00FF00',      # Bright green
    'burn': '#FF0000',      # Bright red
    'mint_light': '#90EE90', # Light green
    'burn_light': '#FFB6C1'  # Light red
}

fig = plot_daily_mint_burn(
    data=data,
    color_scheme=custom_colors,
    save_path="custom_colors.html"
)
```

### Titles and Labels

All functions support custom titles and will automatically generate descriptive titles if none provided:

```python
fig = plot_daily_mint_burn(
    data=data,
    token_symbol="USDC",
    token_address="0xa0b86a33e6441b8c4c8b0b8c4c8b0b8c4c8b0b8c",
    title="Custom Analysis Title",
    save_path="custom_title.html"
)
```

## Output

All functions save plots as interactive HTML files that can be:
- Opened in any web browser
- Shared via email or web
- Embedded in web applications
- Viewed offline

## Example Usage with Real Data

```python
# Run your SQL query
query = """
SELECT 
    DATE(datetime) as date,
    SUM(CASE WHEN from_address = '0x0000000000000000000000000000000000000000' 
        THEN amount ELSE 0 END) as daily_mint_amount,
    SUM(CASE WHEN to_address = '0x0000000000000000000000000000000000000000' 
        THEN amount ELSE 0 END) as daily_burn_amount,
    COUNT(CASE WHEN from_address = '0x0000000000000000000000000000000000000000' 
        THEN 1 END) as mint_count,
    COUNT(CASE WHEN to_address = '0x0000000000000000000000000000000000000000' 
        THEN 1 END) as burn_count
FROM staging.erc20_transfer
WHERE token_address = '0x323c03c48660fe31186fa82c289b0766d331ce21'
GROUP BY DATE(datetime)
ORDER BY date DESC;
"""

# Execute query and get results
# results = execute_query(query)

# Convert to DataFrame
# data = pd.DataFrame(results)

# Create visualizations
fig = plot_daily_mint_burn(
    data=data,
    token_symbol="YOUR_TOKEN",
    token_address="0x323c03c48660fe31186fa82c289b0766d331ce21",
    save_path="token_analysis.html"
)
```

## Running Examples

To see all functions in action, run the example script:

```bash
cd examples/helpers
python example_usage.py
```

This will generate sample visualizations and save them as HTML files.

## Tips for Best Results

1. **Data Quality**: Ensure your dates are properly formatted and amounts are numeric
2. **Time Range**: For better visualizations, include at least 30 days of data
3. **Token Information**: Include token symbols for better readability
4. **File Naming**: Use descriptive names for saved HTML files
5. **Browser Compatibility**: HTML files work best in modern browsers (Chrome, Firefox, Safari, Edge)

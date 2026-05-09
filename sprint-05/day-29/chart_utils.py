"""
chart_utils.py — Reusable Chart Helpers
========================================
Imported by charts.py and future Streamlit dashboard.

YOUR TASK: Implement these 3 utility functions.

HINTS for each:

save_figure(fig, filename, output_dir):
  - fig.tight_layout()
  - out = Path(output_dir) / filename
  - fig.savefig(out, dpi=150, bbox_inches="tight")
  - plt.close(fig)
  - return out

set_chart_style(style="whitegrid", palette="husl"):
  - sns.set_theme(style=style, palette=palette, font_scale=1.1)

get_color_palette(n_colors=8):
  - return sns.color_palette("husl", n_colors)
"""
# YOUR CODE HERE
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns   

def save_figure(fig, filename, output_dir):
    fig.tight_layout()
    out = Path(output_dir) / filename
    fig.savefig(out, dpi=150, bbox_inches="tight")  
    plt.close(fig)
    return out

def set_chart_style(style="whitegrid", palette="husl"):
    sns.set_theme(style=style, palette=palette, font_scale=1.1)

def get_color_palette(n_colors=8):
    return sns.color_palette("husl", n_colors)
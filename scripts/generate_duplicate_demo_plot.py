from datetime import datetime, timedelta
import math
from pathlib import Path
import sys

import matplotlib


matplotlib.use('Agg')

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Circle, FancyBboxPatch


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT))

from lib.intra_session_duplicate import (  # noqa: E402
    detect_intra_session_duplicates,
    haversine_distance,
)


OUTPUT_PATH = (
    REPOSITORY_ROOT / 'docs' / 'images' / 'intra_session_duplicate_logic.png'
)
ORIGIN_LATITUDE = 8.45
ORIGIN_LONGITUDE = -13.17
METERS_PER_DEGREE_LATITUDE = 111320.0
METERS_PER_DEGREE_LONGITUDE = (
    METERS_PER_DEGREE_LATITUDE * math.cos(math.radians(ORIGIN_LATITUDE))
)

COLORS = {
    'accepted': '#4E9A62',
    'rejected': '#B64342',
    'delayed': '#0F4D92',
    'path': '#767676',
    'grid': '#E4E4E4',
    'text': '#272727',
    'panel': '#F7F7F7',
}


def to_lat_lon(east_meters, north_meters):
    return (
        ORIGIN_LATITUDE + north_meters / METERS_PER_DEGREE_LATITUDE,
        ORIGIN_LONGITUDE + east_meters / METERS_PER_DEGREE_LONGITUDE,
    )


def capture(capture_id, order, seconds, east_meters, north_meters, kind):
    latitude, longitude = to_lat_lon(east_meters, north_meters)
    return {
        'id': capture_id,
        'order': order,
        'lat': latitude,
        'lon': longitude,
        'captured_at': datetime(2026, 1, 1, 8, 0, 0)
        + timedelta(seconds=seconds),
        'east_meters': east_meters,
        'north_meters': north_meters,
        'kind': kind,
    }


def build_demo_captures():
    return [
        capture('walk_1', 1, 0, 0.0, 0.0, 'normal'),
        capture('walk_2', 2, 35, 12.0, 2.0, 'normal'),
        capture('burst_tree_a', 3, 40, 12.6, 2.3, 'duplicate'),
        capture('walk_3', 4, 75, 24.0, 7.0, 'normal'),
        capture('walk_4', 5, 115, 35.0, 12.0, 'normal'),
        capture('delayed_tree_b', 6, 265, 35.8, 12.4, 'delayed'),
        capture('walk_5', 7, 305, 47.0, 18.0, 'normal'),
    ]


def add_order_label(axis, item, offset):
    axis.annotate(
        str(item['order']),
        (item['east_meters'], item['north_meters']),
        xytext=offset,
        textcoords='offset points',
        ha='center',
        va='center',
        fontsize=9,
        fontweight='bold',
        color='white',
        bbox={
            'boxstyle': 'circle,pad=0.28',
            'facecolor': COLORS['text'],
            'edgecolor': 'white',
            'linewidth': 0.8,
        },
        zorder=8,
    )


def add_decision_card(axis, y, title, detail, result, color):
    card = FancyBboxPatch(
        (0.04, y),
        0.92,
        0.20,
        boxstyle='round,pad=0.025,rounding_size=0.025',
        facecolor='white',
        edgecolor=color,
        linewidth=1.8,
        transform=axis.transAxes,
    )
    axis.add_patch(card)
    axis.text(
        0.09,
        y + 0.145,
        title,
        transform=axis.transAxes,
        fontsize=11,
        fontweight='bold',
        color=COLORS['text'],
    )
    axis.text(
        0.09,
        y + 0.09,
        detail,
        transform=axis.transAxes,
        fontsize=10,
        color=COLORS['text'],
    )
    axis.text(
        0.09,
        y + 0.035,
        result,
        transform=axis.transAxes,
        fontsize=10,
        fontweight='bold',
        color=color,
    )


def generate_plot():
    captures = build_demo_captures()
    duplicate_results = detect_intra_session_duplicates(captures)
    duplicate_ids = {result['duplicate_id'] for result in duplicate_results}

    if duplicate_ids != {'burst_tree_a'}:
        raise RuntimeError(
            f'Unexpected demo result. Duplicate IDs were: {sorted(duplicate_ids)}'
        )

    normal_captures = [item for item in captures if item['kind'] == 'normal']
    duplicate_capture = next(
        item for item in captures if item['id'] == 'burst_tree_a'
    )
    delayed_capture = next(
        item for item in captures if item['id'] == 'delayed_tree_b'
    )
    tree_a_capture = next(item for item in captures if item['id'] == 'walk_2')
    tree_b_capture = next(item for item in captures if item['id'] == 'walk_4')

    rapid_distance = haversine_distance(
        tree_a_capture['lat'],
        tree_a_capture['lon'],
        duplicate_capture['lat'],
        duplicate_capture['lon'],
    )
    delayed_distance = haversine_distance(
        tree_b_capture['lat'],
        tree_b_capture['lon'],
        delayed_capture['lat'],
        delayed_capture['lon'],
    )

    plt.rcParams.update({
        'font.family': 'sans-serif',
        'font.sans-serif': ['Arial', 'Helvetica', 'DejaVu Sans'],
        'font.size': 11,
        'axes.spines.top': False,
        'axes.spines.right': False,
        'axes.linewidth': 1.2,
        'legend.frameon': False,
    })

    figure = plt.figure(figsize=(12, 6.8), facecolor='white')
    grid = figure.add_gridspec(1, 2, width_ratios=[3.4, 1.25], wspace=0.12)
    axis = figure.add_subplot(grid[0, 0])
    summary_axis = figure.add_subplot(grid[0, 1])

    for current_capture, next_capture in zip(captures, captures[1:]):
        arrow_color = COLORS['path']
        arrow_style = '-'
        if next_capture['kind'] == 'duplicate':
            arrow_color = COLORS['rejected']
            arrow_style = ':'
        elif (
            current_capture['kind'] == 'delayed'
            or next_capture['kind'] == 'delayed'
        ):
            arrow_color = COLORS['delayed']
            arrow_style = '--'
        axis.annotate(
            '',
            xy=(next_capture['east_meters'], next_capture['north_meters']),
            xytext=(current_capture['east_meters'], current_capture['north_meters']),
            arrowprops={
                'arrowstyle': '-|>',
                'color': arrow_color,
                'linewidth': 1.8,
                'linestyle': arrow_style,
                'shrinkA': 8,
                'shrinkB': 8,
            },
            zorder=1,
        )

    threshold_circle = Circle(
        (tree_a_capture['east_meters'], tree_a_capture['north_meters']),
        radius=5.0,
        facecolor=COLORS['rejected'],
        edgecolor=COLORS['rejected'],
        linestyle='--',
        linewidth=1.8,
        alpha=0.12,
        zorder=0,
    )
    axis.add_patch(threshold_circle)

    axis.scatter(
        [item['east_meters'] for item in normal_captures],
        [item['north_meters'] for item in normal_captures],
        s=115,
        marker='o',
        facecolor=COLORS['accepted'],
        edgecolor='white',
        linewidth=1.5,
        zorder=4,
    )
    axis.scatter(
        duplicate_capture['east_meters'],
        duplicate_capture['north_meters'],
        s=165,
        marker='X',
        facecolor=COLORS['rejected'],
        edgecolor='white',
        linewidth=1.2,
        zorder=6,
    )
    axis.scatter(
        delayed_capture['east_meters'],
        delayed_capture['north_meters'],
        s=135,
        marker='s',
        facecolor=COLORS['delayed'],
        edgecolor='white',
        linewidth=1.5,
        zorder=5,
    )

    label_offsets = {
        'walk_1': (-12, 12),
        'walk_2': (-12, 15),
        'burst_tree_a': (14, -12),
        'walk_3': (0, 15),
        'walk_4': (-12, 15),
        'walk_5': (10, 13),
        'delayed_tree_b': (15, -12),
    }
    for item in captures:
        add_order_label(axis, item, label_offsets[item['id']])

    axis.annotate(
        '5 m distance limit',
        xy=(tree_a_capture['east_meters'] + 3.7, tree_a_capture['north_meters'] + 3.4),
        xytext=(20, 0.0),
        fontsize=10,
        color=COLORS['rejected'],
        arrowprops={
            'arrowstyle': '->',
            'color': COLORS['rejected'],
            'linewidth': 1.2,
        },
    )
    axis.text(
        tree_a_capture['east_meters'] - 0.2,
        tree_a_capture['north_meters'] - 5.9,
        f'Tree A: {rapid_distance:.1f} m in 5 s',
        ha='center',
        fontsize=10,
        color=COLORS['rejected'],
        fontweight='bold',
    )
    axis.text(
        tree_b_capture['east_meters'] - 0.5,
        tree_b_capture['north_meters'] - 3.0,
        f'Tree B: {delayed_distance:.1f} m after 150 s',
        ha='center',
        fontsize=10,
        color=COLORS['delayed'],
        fontweight='bold',
    )

    legend_handles = [
        Line2D(
            [0], [0], marker='o', color='none', markerfacecolor=COLORS['accepted'],
            markeredgecolor='white', markersize=10,
            label='Normal Capture (Accepted)',
        ),
        Line2D(
            [0], [0], marker='X', color='none', markerfacecolor=COLORS['rejected'],
            markeredgecolor='white', markersize=11,
            label='Rapid Duplicate (Rejected)',
        ),
        Line2D(
            [0], [0], marker='s', color='none', markerfacecolor=COLORS['delayed'],
            markeredgecolor='white', markersize=10,
            label='Delayed Re-take (Accepted)',
        ),
    ]
    axis.legend(
        handles=legend_handles,
        loc='upper left',
        ncol=1,
        fontsize=10,
        borderaxespad=0.2,
    )

    axis.set_xlim(-5, 54)
    axis.set_ylim(-7, 25)
    axis.set_aspect('equal', adjustable='box')
    axis.set_xlabel('East–west position (meters)')
    axis.set_ylabel('North–south position (meters)')
    axis.grid(True, color=COLORS['grid'], linewidth=0.8, zorder=-2)
    axis.set_title('One session, seven captures', loc='left', fontweight='bold')

    summary_axis.set_facecolor(COLORS['panel'])
    summary_axis.set_xticks([])
    summary_axis.set_yticks([])
    for spine in summary_axis.spines.values():
        spine.set_visible(False)
    summary_axis.text(
        0.06,
        0.94,
        'The rule',
        transform=summary_axis.transAxes,
        fontsize=14,
        fontweight='bold',
        color=COLORS['text'],
    )
    summary_axis.text(
        0.06,
        0.855,
        'Reject the later photo only when\nboth checks are true:',
        transform=summary_axis.transAxes,
        fontsize=10.5,
        color=COLORS['text'],
        linespacing=1.4,
    )
    summary_axis.text(
        0.08,
        0.735,
        '≤ 20 seconds\n≤ 5 meters',
        transform=summary_axis.transAxes,
        fontsize=15,
        fontweight='bold',
        color=COLORS['text'],
        linespacing=1.5,
    )
    add_decision_card(
        summary_axis,
        0.39,
        'Tree A · captures 2 → 3',
        f'5 seconds · {rapid_distance:.1f} meters',
        'REJECTED — both limits match',
        COLORS['rejected'],
    )
    add_decision_card(
        summary_axis,
        0.10,
        'Tree B · captures 5 → 6',
        f'150 seconds · {delayed_distance:.1f} meters',
        'ACCEPTED — over 20 seconds',
        COLORS['delayed'],
    )

    figure.suptitle(
        'How the duplicate check handles a realistic session',
        x=0.05,
        y=0.98,
        ha='left',
        fontsize=17,
        fontweight='bold',
        color=COLORS['text'],
    )
    figure.text(
        0.05,
        0.935,
        'Normal walking stays green. A rapid second photo at the same tree is rejected. '
        'A later re-take stays valid.',
        ha='left',
        fontsize=10.5,
        color='#4D4D4D',
    )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    figure.subplots_adjust(
        left=0.075,
        right=0.98,
        bottom=0.12,
        top=0.83,
        wspace=0.15,
    )
    figure.savefig(
        OUTPUT_PATH,
        dpi=300,
        bbox_inches='tight',
        facecolor='white',
    )
    plt.close(figure)

    accepted_count = len(captures) - len(duplicate_ids)
    print(
        f'Generated {OUTPUT_PATH} '
        f'({len(duplicate_ids)} rejected, {accepted_count} accepted)'
    )


if __name__ == '__main__':
    generate_plot()

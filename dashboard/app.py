import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from scipy import stats
import sys
import os
from dotenv import load_dotenv

load_dotenv()

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from analytics.or_optimizer import optimize_composition, sensitivity_analysis

PROJECT_ID = os.getenv("GCP_PROJECT_ID")

st.set_page_config(
    page_title="TFT Meta Pipeline",
    page_icon="🎮",
    layout="wide"
)


@st.cache_data(ttl=3600)
def load_unit_data() -> pd.DataFrame:
    client = bigquery.Client(project=PROJECT_ID)
    query = """
        select *
        from `tft-meta-pipeline.tft_dev.mart_unit_stats`
        order by games_played desc
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=3600)
def load_trait_data() -> pd.DataFrame:
    client = bigquery.Client(project=PROJECT_ID)
    query = """
        select *
        from `tft-meta-pipeline.tft_dev.mart_trait_stats`
        order by games_played desc
    """
    return client.query(query).to_dataframe()


def compute_zscores(df: pd.DataFrame) -> pd.DataFrame:
    sufficient = df[df['sufficient_sample'] == True].copy()
    mean = sufficient['top4_rate'].mean()
    std = sufficient['top4_rate'].std()
    sufficient['zscore'] = ((sufficient['top4_rate'] - mean) / std).round(3)
    return sufficient


# header
st.title("TFT Meta Analytics Pipeline")
st.caption("Set 16 challenger data — units, traits, and composition optimizer")

# tabs
tab1, tab2, tab3, tab4 = st.tabs([
    "Unit Stats",
    "Trait Stats",
    "Statistical Analysis",
    "Comp Optimizer"
])

with tab1:
    st.subheader("Unit performance")

    unit_df = load_unit_data()
    sufficient_units = unit_df[unit_df['sufficient_sample'] == True].copy()

    col1, col2, col3 = st.columns(3)
    col1.metric("Total units tracked", len(unit_df))
    col2.metric("Units with sufficient sample", len(sufficient_units))
    col3.metric(
        "Best unit",
        sufficient_units.loc[sufficient_units['top4_rate'].idxmax(), 'unit_name'].replace('TFT16_', '')
    )

    min_games = st.slider("Minimum games played", 10, 200, 30, key="unit_slider")
    filtered = unit_df[unit_df['games_played'] >= min_games].copy()
    filtered['unit_label'] = filtered['unit_name'].str.replace('TFT16_', '')

    fig = px.bar(
        filtered.sort_values('top4_rate', ascending=False).head(20),
        x='unit_label',
        y='top4_rate',
        color='avg_placement',
        color_continuous_scale='RdYlGn_r',
        title="Top 20 units by top4 rate",
        labels={
            'unit_label': 'Unit',
            'top4_rate': 'Top 4 rate',
            'avg_placement': 'Avg placement'
        }
    )
    fig.update_layout(xaxis_tickangle=-45)
    fig.add_hline(y=0.5, line_dash="dash", line_color="gray", annotation_text="50% baseline")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Full unit table")
    display_cols = ['unit_name', 'games_played', 'top4_rate', 'win_rate', 'avg_placement', 'avg_star_level']
    st.dataframe(
        filtered[display_cols].sort_values('top4_rate', ascending=False),
        use_container_width=True
    )


with tab2:
    st.subheader("Trait performance by tier")

    trait_df = load_trait_data()
    sufficient_traits = trait_df[trait_df['sufficient_sample'] == True].copy()
    sufficient_traits['trait_label'] = (
        sufficient_traits['trait_name'].str.replace('TFT16_', '') +
        ' T' +
        sufficient_traits['tier_current'].astype(str)
    )

    fig2 = px.scatter(
        sufficient_traits.sort_values('top4_rate', ascending=False).head(30),
        x='trait_label',
        y='top4_rate',
        size='games_played',
        color='avg_placement',
        color_continuous_scale='RdYlGn_r',
        title="Top 30 trait-tier combinations by top4 rate",
        labels={
            'trait_label': 'Trait (tier)',
            'top4_rate': 'Top 4 rate',
            'games_played': 'Games played',
            'avg_placement': 'Avg placement'
        }
    )
    fig2.update_layout(xaxis_tickangle=-45)
    fig2.add_hline(y=0.5, line_dash="dash", line_color="gray", annotation_text="50% baseline")
    st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Full trait table")
    display_trait_cols = ['trait_name', 'tier_current', 'games_played', 'top4_rate', 'win_rate', 'avg_placement']
    st.dataframe(
        sufficient_traits[display_trait_cols].sort_values('top4_rate', ascending=False),
        use_container_width=True
    )


with tab3:
    st.subheader("Statistical analysis")

    unit_df = load_unit_data()
    sufficient = compute_zscores(unit_df)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Z-score outlier detection**")
        st.caption("Units beyond ±2 standard deviations from mean top4 rate")

        fig3 = px.scatter(
            sufficient,
            x='unit_name',
            y='zscore',
            color='zscore',
            color_continuous_scale='RdYlGn',
            title="Unit top4 rate z-scores"
        )
        fig3.add_hline(y=2, line_dash="dash", line_color="red", annotation_text="+2σ")
        fig3.add_hline(y=-2, line_dash="dash", line_color="red", annotation_text="-2σ")
        fig3.update_layout(xaxis_visible=False)
        st.plotly_chart(fig3, use_container_width=True)

        outliers = sufficient[sufficient['zscore'].abs() > 2].copy()
        outliers['unit_label'] = outliers['unit_name'].str.replace('TFT16_', '')
        outliers['direction'] = outliers['zscore'].apply(
            lambda z: 'Overperforming' if z > 0 else 'Underperforming'
        )
        st.dataframe(
            outliers[['unit_label', 'top4_rate', 'zscore', 'direction']],
            use_container_width=True
        )

    with col2:
        st.markdown("**OLS regression: top4 rate → avg placement**")
        st.caption("R² and coefficient from manual matrix implementation (Linear Algebra)")

        x = sufficient['top4_rate'].values
        y = sufficient['avg_placement'].values
        x_const = np.column_stack([np.ones(len(x)), x])
        beta = np.linalg.lstsq(x_const, y, rcond=None)[0]
        y_pred = x_const @ beta
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r_squared = 1 - (ss_res / ss_tot)

        sufficient['predicted_placement'] = y_pred
        sufficient['unit_label'] = sufficient['unit_name'].str.replace('TFT16_', '')

        fig4 = px.scatter(
            sufficient,
            x='top4_rate',
            y='avg_placement',
            hover_name='unit_label',
            title=f"OLS regression (R²={r_squared:.3f})"
        )
        x_line = np.linspace(x.min(), x.max(), 100)
        y_line = beta[0] + beta[1] * x_line
        fig4.add_trace(go.Scatter(
            x=x_line, y=y_line,
            mode='lines',
            name=f'y = {beta[0]:.2f} + {beta[1]:.2f}x',
            line=dict(color='red', width=2)
        ))
        fig4.update_yaxes(autorange='reversed')
        st.plotly_chart(fig4, use_container_width=True)

        st.metric("R-squared", f"{r_squared:.4f}")
        st.metric("Coefficient", f"{beta[1]:.4f}")
        st.caption(f"Every 10% increase in top4 rate → {abs(beta[1]) * 0.1:.2f} lower placement")


with tab4:
    st.subheader("Composition optimizer")
    st.caption("Binary linear program — OR Deterministic Methods")

    unit_df = load_unit_data()
    sufficient_units = unit_df[unit_df['sufficient_sample'] == True].copy()
    all_unit_names = sufficient_units['unit_name'].str.replace('TFT16_', '').tolist()

    col1, col2 = st.columns([2, 1])

    with col2:
        board_size = st.selectbox("Board size (your level)", [6, 7, 8, 9], index=1)
        contested_raw = st.multiselect(
            "Contested units in lobby",
            all_unit_names
        )
        contested_units = [f"TFT16_{u}" for u in contested_raw]
        run_button = st.button("Optimize composition", type="primary")

    with col1:
        if run_button:
            result = optimize_composition(
                sufficient_units,
                contested_units=contested_units,
                board_size=board_size
            )

            if result['status'] == 'optimal':
                st.success(f"Optimal composition found — expected top4 rate: {result['expected_top4_rate']:.1%}")

                comp_data = []
                for unit in result['composition']:
                    row = sufficient_units[sufficient_units['unit_name'] == unit].iloc[0]
                    comp_data.append({
                        'Unit': unit.replace('TFT16_', ''),
                        'Top4 rate': f"{row['top4_rate']:.1%}",
                        'Avg placement': f"{row['avg_placement']:.2f}",
                        'Contested': '⚠️' if unit in contested_units else '✓'
                    })

                st.dataframe(pd.DataFrame(comp_data), use_container_width=True)

                if result['contested_units_avoided']:
                    avoided = [u.replace('TFT16_', '') for u in result['contested_units_avoided']]
                    st.info(f"Avoided contested: {', '.join(avoided)}")

                st.markdown("**Sensitivity analysis — alternative compositions:**")
                alternatives = sensitivity_analysis(sufficient_units, result['composition'])
                for alt in alternatives:
                    excluded = alt['excluded_unit'].replace('TFT16_', '')
                    rate = alt['expected_top4_rate']
                    drop = alt['rate_drop']
                    comp = [u.replace('TFT16_', '') for u in alt['alternative_composition']]
                    st.markdown(f"Without **{excluded}**: top4={rate:.1%} (drop={drop:.1%}) → {', '.join(comp)}")

            else:
                st.error(f"No solution found: {result.get('reason', 'unknown error')}")
        else:
            st.info("Select your board size and any contested units, then click Optimize.")


if __name__ == "__main__":
    pass
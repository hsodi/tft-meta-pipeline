import pandas as pd
import numpy as np
from scipy import stats
from google.cloud import bigquery
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")


def load_unit_data() -> pd.DataFrame:
    """Load unit stats from BigQuery mart."""
    client = bigquery.Client(project=PROJECT_ID)
    query = """
        select *
        from `tft-meta-pipeline.tft_dev.mart_unit_stats`
        order by games_played desc
    """
    return client.query(query).to_dataframe()


def load_trait_data() -> pd.DataFrame:
    """Load trait stats from BigQuery mart."""
    client = bigquery.Client(project=PROJECT_ID)
    query = """
        select *
        from `tft-meta-pipeline.tft_dev.mart_trait_stats`
        order by games_played desc
    """
    return client.query(query).to_dataframe()


def detect_outlier_units(df: pd.DataFrame) -> pd.DataFrame:
    """
    Identify units that are statistically anomalous using z-scores.

    Z = (x - mu) / sigma
    Flag anything beyond 2 standard deviations from the mean.
    Directly from your Probability & Statistics module.
    """
    sufficient = df[df['sufficient_sample'] == True].copy()

    mean_wr = sufficient['top4_rate'].mean()
    std_wr = sufficient['top4_rate'].std()

    sufficient['top4_zscore'] = (
        (sufficient['top4_rate'] - mean_wr) / std_wr
    ).round(3)

    sufficient['is_outlier'] = sufficient['top4_zscore'].abs() > 2

    outliers = sufficient[sufficient['is_outlier']].copy()
    outliers = outliers.sort_values('top4_zscore', ascending=False)

    print(f"\nUnit outliers (|z| > 2 standard deviations):")
    print(f"  Mean top4 rate: {mean_wr:.3f}")
    print(f"  Std dev: {std_wr:.3f}")

    for _, row in outliers.iterrows():
        direction = "OVERPERFORMING" if row['top4_zscore'] > 0 else "UNDERPERFORMING"
        print(f"  {row['unit_name']}: z={row['top4_zscore']} ({direction})")

    return outliers


def fit_placement_distribution(df: pd.DataFrame) -> dict:
    """
    Fit probability distributions to unit placement data.

    Which distribution best describes how TFT placements are distributed?
    Uses Kolmogorov-Smirnov test for goodness of fit.
    From your Probability & Statistics module.
    """
    sufficient = df[df['sufficient_sample'] == True].copy()
    placements = sufficient['avg_placement'].dropna().values

    distributions = {
        'normal': stats.norm,
        'beta': stats.beta,
        'gamma': stats.gamma,
    }

    results = {}
    for name, dist in distributions.items():
        try:
            params = dist.fit(placements)
            ks_stat, p_value = stats.kstest(placements, dist.name, args=params)
            results[name] = {
                'params': params,
                'ks_statistic': round(ks_stat, 4),
                'p_value': round(p_value, 4)
            }
        except Exception as e:
            print(f"  Could not fit {name}: {e}")

    print(f"\nPlacement distribution fitting:")
    for name, result in results.items():
        print(f"  {name}: KS={result['ks_statistic']}, p={result['p_value']}")

    if results:
        best = max(results.items(), key=lambda x: x[1]['p_value'])
        print(f"  Best fit: {best[0]} (p={best[1]['p_value']})")

    return results


def unit_placement_regression(df: pd.DataFrame) -> dict:
    """
    OLS regression: does top4 rate predict average placement?

    This is your Econometrics + Linear Algebra modules applied directly.
    Beta = (X'X)^-1 X'y

    H0: top4_rate has no linear relationship with avg_placement
    We expect negative coefficient — higher top4 rate = lower placement number
    """
    sufficient = df[df['sufficient_sample'] == True].copy()

    x = sufficient['top4_rate'].values
    y = sufficient['avg_placement'].values
    n = len(x)

    if n < 5:
        print("Not enough data for regression")
        return {}

    # design matrix with intercept
    x_with_const = np.column_stack([np.ones(n), x])

    # OLS: beta = (X'X)^-1 X'y
    beta = np.linalg.lstsq(x_with_const, y, rcond=None)[0]

    # predicted values and residuals
    y_pred = x_with_const @ beta
    residuals = y - y_pred

    # R-squared
    ss_res = np.sum(residuals ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r_squared = 1 - (ss_res / ss_tot)

    # standard errors and t-statistics
    k = 2
    mse = ss_res / (n - k)
    var_beta = mse * np.linalg.inv(x_with_const.T @ x_with_const)
    se_beta = np.sqrt(np.diag(var_beta))
    t_stats = beta / se_beta
    p_values = [2 * (1 - stats.t.cdf(abs(t), df=n-k)) for t in t_stats]

    result = {
        'intercept': round(beta[0], 4),
        'top4_rate_coef': round(beta[1], 4),
        'r_squared': round(r_squared, 4),
        'top4_rate_t_stat': round(t_stats[1], 4),
        'top4_rate_p_value': round(p_values[1], 4),
        'n_observations': n
    }

    print(f"\nOLS Regression: avg_placement ~ top4_rate")
    print(f"  Intercept: {result['intercept']}")
    print(f"  top4_rate coefficient: {result['top4_rate_coef']}")
    print(f"  R-squared: {result['r_squared']}")
    print(f"  t-statistic: {result['top4_rate_t_stat']}")
    print(f"  p-value: {result['top4_rate_p_value']}")
    print(f"  Significant at 5%: {result['top4_rate_p_value'] < 0.05}")
    print(f"  Observations: {result['n_observations']}")

    return result


def trait_tier_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compare performance across trait tiers.

    Does activating a higher trait tier actually improve placement?
    Uses one-way ANOVA — your Stats module.
    H0: mean placement is equal across all tiers of a trait
    """
    sufficient = df[df['sufficient_sample'] == True].copy()

    # group by trait and compare tiers
    trait_groups = sufficient.groupby('trait_name')

    significant_traits = []

    for trait_name, group in trait_groups:
        if len(group) < 2:
            continue

        tiers = group['tier_current'].unique()
        if len(tiers) < 2:
            continue

        # one-way ANOVA across tiers
        tier_placements = [
            group[group['tier_current'] == t]['avg_placement'].values
            for t in tiers
        ]

        try:
            if any(len(p) < 2 for p in tier_placements):
                continue

            f_stat, p_value = stats.f_oneway(*tier_placements)
            if p_value < 0.05:
                best_tier = group.loc[group['avg_placement'].idxmin()]
                significant_traits.append({
                    'trait_name': trait_name,
                    'n_tiers': len(tiers),
                    'f_statistic': round(f_stat, 4),
                    'p_value': round(p_value, 4),
                    'best_tier': best_tier['tier_current'],
                    'best_tier_top4_rate': best_tier['top4_rate']
                })
        except Exception:
            continue

    result_df = pd.DataFrame(significant_traits)

    print(f"\nTraits where tier significantly affects performance (p < 0.05):")
    if len(result_df) > 0:
        for _, row in result_df.iterrows():
            print(f"  {row['trait_name']}: F={row['f_statistic']}, "
                  f"p={row['p_value']}, best tier={row['best_tier']}")
    else:
        print("  None found — need more data for ANOVA")

    return result_df


if __name__ == "__main__":
    print("Loading data from BigQuery...")
    unit_df = load_unit_data()
    trait_df = load_trait_data()
    print(f"Loaded {len(unit_df)} units, {len(trait_df)} trait-tier combinations")

    detect_outlier_units(unit_df)
    fit_placement_distribution(unit_df)
    unit_placement_regression(unit_df)
    trait_tier_analysis(trait_df)
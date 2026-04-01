import pandas as pd
import numpy as np
import pulp
from google.cloud import bigquery
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")


def load_unit_data() -> pd.DataFrame:
    """Load unit performance data from BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)
    query = """
        select 
            unit_name,
            top4_rate,
            win_rate,
            avg_placement,
            sufficient_sample
        from `tft-meta-pipeline.tft_dev.mart_unit_stats`
        where sufficient_sample = true
        order by top4_rate desc
    """
    return client.query(query).to_dataframe()


def load_trait_data() -> pd.DataFrame:
    """Load trait performance data from BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)
    query = """
        select
            trait_name,
            tier_current,
            top4_rate,
            games_played
        from `tft-meta-pipeline.tft_dev.mart_trait_stats`
        where sufficient_sample = true
        order by top4_rate desc
    """
    return client.query(query).to_dataframe()


def optimize_composition(
    unit_df: pd.DataFrame,
    contested_units: list = None,
    board_size: int = 7,
    contest_penalty: float = 0.15
) -> dict:
    """
    Binary linear program to find optimal TFT composition.

    Objective: maximise expected top4 rate across selected units
    Constraints:
      - Exactly board_size units selected
      - Contested units incur a penalty on their effective rate
      - At most 2 contested units in the final composition

    This is directly from OR Deterministic Methods:
    binary LP with objective function and hard constraints.

    Decision variable: x_i in {0, 1}
    x_i = 1 means unit i is in the composition

    Objective: maximise sum((top4_rate_i - penalty_i) * x_i)
    Subject to:
      sum(x_i) = board_size
      sum(x_i for contested i) <= 2
    """
    if contested_units is None:
        contested_units = []

    units = unit_df['unit_name'].tolist()
    n = len(units)

    if n < board_size:
        return {
            'status': 'infeasible',
            'reason': f'Only {n} units available, need {board_size}',
            'composition': []
        }

    # define the LP problem
    prob = pulp.LpProblem("TFT_Composition_Optimizer", pulp.LpMaximize)

    # binary decision variables
    x = pulp.LpVariable.dicts("unit", range(n), cat='Binary')

    # objective: maximise top4 rate with contest penalty
    objective_terms = []
    for i, row in unit_df.iterrows():
        idx = units.index(row['unit_name'])
        base_rate = row['top4_rate']
        penalty = contest_penalty if row['unit_name'] in contested_units else 0
        effective_rate = base_rate - penalty
        objective_terms.append(effective_rate * x[idx])

    prob += pulp.lpSum(objective_terms)

    # constraint 1: exactly board_size units
    prob += pulp.lpSum([x[i] for i in range(n)]) == board_size

    # constraint 2: at most 2 contested units
    if contested_units:
        contested_indices = [
            units.index(u) for u in contested_units
            if u in units
        ]
        if contested_indices:
            prob += pulp.lpSum([x[i] for i in contested_indices]) <= 2

    # solve silently
    prob.solve(pulp.GLPK_CMD(msg=0))

    status = pulp.LpStatus[prob.status]

    if status != 'Optimal':
        return {
            'status': 'infeasible',
            'reason': f'Solver returned: {status}',
            'composition': []
        }

    # extract selected units
    selected = [units[i] for i in range(n) if x[i].value() == 1]

    # calculate expected top4 rate of composition
    selected_rates = unit_df[
        unit_df['unit_name'].isin(selected)
    ]['top4_rate'].values
    expected_top4 = round(float(np.mean(selected_rates)), 4)

    # which contested units were avoided
    avoided_contested = [
        u for u in contested_units
        if u in units and u not in selected
    ]

    return {
        'status': 'optimal',
        'composition': selected,
        'expected_top4_rate': expected_top4,
        'board_size': board_size,
        'contested_units_input': contested_units,
        'contested_units_avoided': avoided_contested,
        'contested_units_included': [u for u in contested_units if u in selected]
    }


def sensitivity_analysis(
    unit_df: pd.DataFrame,
    base_composition: list,
    n_alternatives: int = 3
) -> list:
    """
    Find alternative compositions when the optimal one is contested.

    Runs the optimizer with each unit in the base composition
    forcibly excluded — shows how robust the solution is.
    From OR sensitivity analysis concepts.
    """
    alternatives = []

    for excluded_unit in base_composition:
        filtered_df = unit_df[unit_df['unit_name'] != excluded_unit].copy()
        result = optimize_composition(filtered_df, board_size=len(base_composition))

        if result['status'] == 'optimal':
            alternatives.append({
                'excluded_unit': excluded_unit,
                'alternative_composition': result['composition'],
                'expected_top4_rate': result['expected_top4_rate'],
                'rate_drop': round(
                    max(
                        unit_df[unit_df['unit_name'].isin(base_composition)]['top4_rate'].mean()
                        - result['expected_top4_rate'], 0
                    ), 4
                )
            })

    alternatives.sort(key=lambda x: x['rate_drop'])
    return alternatives[:n_alternatives]


if __name__ == "__main__":
    print("Loading unit data from BigQuery...")
    unit_df = load_unit_data()
    print(f"Loaded {len(unit_df)} units with sufficient sample")

    print("\n--- Base optimization (no contested units) ---")
    result = optimize_composition(unit_df, board_size=7)

    if result['status'] == 'optimal':
        print(f"Optimal composition ({result['board_size']} units):")
        for unit in result['composition']:
            rate = unit_df[unit_df['unit_name'] == unit]['top4_rate'].values[0]
            print(f"  {unit}: {rate:.3f} top4 rate")
        print(f"Expected top4 rate: {result['expected_top4_rate']}")

    print("\n--- Optimization with contested units ---")
    # simulate top 3 units being heavily contested
    top_units = unit_df.head(3)['unit_name'].tolist()
    print(f"Contested units: {top_units}")

    contested_result = optimize_composition(
        unit_df,
        contested_units=top_units,
        board_size=7
    )

    if contested_result['status'] == 'optimal':
        print(f"Composition avoiding contest:")
        for unit in contested_result['composition']:
            rate = unit_df[unit_df['unit_name'] == unit]['top4_rate'].values[0]
            print(f"  {unit}: {rate:.3f} top4 rate")
        print(f"Expected top4 rate: {contested_result['expected_top4_rate']}")
        print(f"Avoided: {contested_result['contested_units_avoided']}")
        print(f"Still included: {contested_result['contested_units_included']}")

    print("\n--- Sensitivity analysis ---")
    if result['status'] == 'optimal':
        alternatives = sensitivity_analysis(unit_df, result['composition'])
        print("Alternative compositions if a unit becomes unavailable:")
        for alt in alternatives:
            print(f"  Without {alt['excluded_unit']}: "
                  f"top4={alt['expected_top4_rate']} "
                  f"(drop={alt['rate_drop']})")
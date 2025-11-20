# backend/rule_engine/simple_deterministic_miner.py

import pandas as pd

def mine_simple_rules(df: pd.DataFrame):
    df = df.copy().fillna("")

    # Columns to exclude completely
    exclude_cols = {"part dscr long", "part description"}

    cols = [c for c in df.columns if c.lower() not in exclude_cols]

    rules = []

    for colA in cols:
        # Skip columns that are mostly blank
        if df[colA].replace("", pd.NA).isna().mean() > 0.5:
            continue

        for valA, subset in df.groupby(colA):
            if valA == "":
                continue

            for colB in cols:
                if colB == colA:
                    continue

                unique_vals = subset[colB].unique()

                # deterministic rule = only 1 unique value AND not blank
                if len(unique_vals) == 1 and unique_vals[0] != "":
                    rule = f"IF {colA} = {valA} THEN {colB} = {unique_vals[0]}"
                    rules.append(rule)

    # remove duplicates
    return sorted(set(rules))

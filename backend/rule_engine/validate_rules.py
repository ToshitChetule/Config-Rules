    # backend/rule_engine/validate_rules.py

import pandas as pd

def parse_rule(rule: str):
    """
    Parses rules of the form:
    IF A = x AND B = y THEN C = z

    Returns:
        conditions = List of (column, value)
        result = (column, value)
    """
    rule = rule.strip()

    if not rule.startswith("IF ") or " THEN " not in rule:
        return None, None

    if_part, then_part = rule[3:].split(" THEN ")

    # Parse IF part
    conditions = []
    for cond in if_part.split(" AND "):
        if "=" not in cond:
            return None, None
        col, val = cond.split("=", 1)
        conditions.append((col.strip(), val.strip()))

    # Parse THEN part
    if "=" not in then_part:
        return None, None

    then_col, then_val = then_part.split("=", 1)

    return conditions, (then_col.strip(), then_val.strip())


def validate_rule(rule: str, df: pd.DataFrame):
    """
    Validates multi-condition rules:
    - IF one or more conditions hold
    - THEN column MUST have exactly ONE unique value in all matching rows
    """
    parsed = parse_rule(rule)
    if parsed is None:
        return False

    conditions, (then_col, then_val) = parsed

    # Filter according to all conditions
    subset = df.copy()
    for col, val in conditions:
        if col not in df.columns:
            return False
        subset = subset[subset[col] == val]

    # If no rows support the IF-part → invalid rule
    if subset.empty:
        return False

    # Extract unique THEN-values
    unique_vals = subset[then_col].unique()

    # Deterministic rule must give exactly 1 unique value
    if len(unique_vals) != 1:
        return False

    # Unique value must match rule's THEN value
    return str(unique_vals[0]).strip() == then_val.strip()

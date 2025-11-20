# import pandas as pd

# def discover_candidate_rules(df, confidence_threshold=0.95):
#     results = []
#     columns = df.columns.tolist()

#     for A in columns:
#         for B in columns:
#             if A == B:
#                 continue

#             groups = df.groupby(A)[B].value_counts(normalize=True)
#             for (a_val, b_val), conf in groups.items():
#                 if conf >= confidence_threshold:
#                     results.append({
#                         "if": {A: a_val},
#                         "then": {B: b_val},
#                         "confidence": float(conf)
#                     })

#     return results

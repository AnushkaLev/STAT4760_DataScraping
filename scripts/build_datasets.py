import pandas as pd
import os

# Load all four raw files
def load(label):
    path = f"raw_{label}.csv"
    if not os.path.exists(path):
        print(f"WARNING: {path} not found, skipping")
        return None
    df = pd.read_csv(path)
    df["thread"] = label
    return df

p1 = load("bills_broncos_p1")
p2 = load("bills_broncos_p2")
packers = load("packers_cowboys")
bears = load("bears_eagles")

# Combine Bills/Broncos threads
if p1 is not None and p2 is not None:
    bills_raw = pd.concat([p1, p2]).drop_duplicates(subset="comment_id")
elif p2 is not None:
    bills_raw = p2  # fallback if p1 failed
    print("WARNING: Only second half Bills thread available")
else:
    bills_raw = None

# User-level aggregation function
def make_user_counts(df, game_label):
    df_clean = df[df["author"].notna() & (df["author"] != "[deleted]")].copy()
    user_counts = (
        df_clean.groupby("author")
        .agg(
            comment_count=("comment_id", "count"),
            avg_score=("score", "mean"),
            total_score=("score", "sum"),
            first_comment=("created_utc", "min"),
            last_comment=("created_utc", "max"),
        )
        .reset_index()
        .sort_values("comment_count", ascending=False)
    )
    user_counts["game"] = game_label
    return user_counts

# Build user-level datasets
datasets = {}

if bills_raw is not None:
    datasets["bills_broncos"] = make_user_counts(bills_raw, "Bills vs Broncos")

if packers is not None:
    datasets["packers_cowboys"] = make_user_counts(packers, "Packers vs Cowboys")

if bears is not None:
    datasets["bears_eagles"] = make_user_counts(bears, "Bears vs Eagles")

# Save individual user-count files
for name, df in datasets.items():
    df.to_csv(f"users_{name}.csv", index=False)
    print(f"\n{'='*50}")
    print(f"{name}: {len(df)} unique users")
    print(f"  Total comments:  {df['comment_count'].sum()}")
    print(f"  Mean comments:   {df['comment_count'].mean():.3f}")
    print(f"  % with 1 comment: {(df['comment_count']==1).mean()*100:.1f}%")
    print(f"  Max comments:    {df['comment_count'].max()}")
    print(df[["author","comment_count","avg_score"]].head(10).to_string())

# Save combined file for cross-game analysis
if datasets:
    combined = pd.concat(datasets.values())
    combined.to_csv("users_all_games.csv", index=False)
    print(f"\nCombined file saved: users_all_games.csv ({len(combined)} total users across all games)")

# NBD-ready histogram per game
print("\n\nHistogram summary (for NBD fitting):")
for name, df in datasets.items():
    print(f"\n{name}:")
    hist = df["comment_count"].value_counts().sort_index()
    hist.name = "actual_count"
    hist_df = hist.reset_index()
    hist_df.columns = ["x", "actual_count"]
    hist_df["proportion"] = hist_df["actual_count"] / hist_df["actual_count"].sum()
    print(hist_df[hist_df["x"] <= 20].to_string(index=False))
    hist_df.to_csv(f"histogram_{name}.csv", index=False)
    print(f"  Full histogram saved to histogram_{name}.csv")
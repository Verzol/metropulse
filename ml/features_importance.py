import xgboost as xgb

MODELS = {
    "Demand": "models/demand_xgb.json",
    "Fare":   "models/fare_xgb.json",
    "Tip":    "models/tip_xgb.json",
}

IMPORTANCE_TYPE = "gain" 
TOP_N = 10

for name, path in MODELS.items():
    model = xgb.XGBRegressor()
    model.load_model(path)
    booster = model.get_booster()

    raw = booster.get_score(importance_type=IMPORTANCE_TYPE)
    if not raw:
        print(f"\n--- {name} --- (no importance scores found)")
        continue

    # Tổng gain của toàn bộ features
    total_gain = sum(raw.values())

    # Sắp xếp giảm dần
    sorted_items = sorted(raw.items(), key=lambda x: x[1], reverse=True)
    top_items = sorted_items[:TOP_N]

    print(f"\n--- {name} (top {TOP_N}, {IMPORTANCE_TYPE}) ---")
    for feature, score in top_items:
        pct = (score / total_gain) * 100
        print(f"  {feature:<30} {pct:6.2f}%")

    # Coverage của top N so với tổng gain
    top_gain_sum = sum(score for _, score in top_items)
    coverage = (top_gain_sum / total_gain) * 100
    print(f"  {'[top-N coverage]':<30} {coverage:6.2f}% of total gain")
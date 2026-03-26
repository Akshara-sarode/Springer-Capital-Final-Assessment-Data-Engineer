import pandas as pd

# ===============================
# 1. LOAD DATA
# ===============================
user_referrals = pd.read_csv("/content/user_referrals.csv")
user_logs = pd.read_csv("/content/user_logs.csv")
referral_logs = pd.read_csv("/content/user_referral_logs.csv")
statuses = pd.read_csv("/content/user_referral_statuses.csv")
rewards = pd.read_csv("/content/referral_rewards.csv")
transactions = pd.read_csv("/content/paid_transactions.csv")
leads = pd.read_csv("/content/lead_log.csv")

# ===============================
# 2. DATA PROFILING
# ===============================
def profile(df, name):
    result = []
    for col in df.columns:
        result.append({
            "table": name,
            "column": col,
            "null_count": df[col].isnull().sum(),
            "distinct_count": df[col].nunique()
        })
    return pd.DataFrame(result)

profile_df = pd.concat([
    profile(user_referrals, "user_referrals"),
    profile(user_logs, "user_logs"),
    profile(referral_logs, "user_referral_logs"),
    profile(statuses, "user_referral_statuses"),
    profile(rewards, "referral_rewards"),
    profile(transactions, "paid_transactions"),
    profile(leads, "lead_log")
])

profile_df.to_csv("data_profiling.csv", index=False)

# ===============================
# 3. CLEANING & TYPE FIX
# ===============================
user_referrals["referral_at"] = pd.to_datetime(user_referrals["referral_at"])
user_referrals["updated_at"] = pd.to_datetime(user_referrals["updated_at"])

transactions["transaction_at"] = pd.to_datetime(transactions["transaction_at"])
user_logs["membership_expired_date"] = pd.to_datetime(user_logs["membership_expired_date"])

# Fix numeric
rewards["reward_value"] = pd.to_numeric(rewards["reward_value"], errors="coerce")

# ===============================
# 4. REMOVE DUPLICATES (CRITICAL)
# ===============================
referral_logs = referral_logs.sort_values("created_at").drop_duplicates("user_referral_id", keep="last")
user_logs = user_logs.sort_values("membership_expired_date").drop_duplicates("user_id", keep="last")
transactions = transactions.drop_duplicates("transaction_id")
leads = leads.drop_duplicates("lead_id")

# ===============================
# 5. RENAME COLUMNS (FIX MERGE ERRORS)
# ===============================
user_logs = user_logs.rename(columns={"id": "user_log_id"})
statuses = statuses.rename(columns={"id": "status_id"})
rewards = rewards.rename(columns={"id": "reward_id"})
referral_logs = referral_logs.rename(columns={"id": "referral_log_id"})

# FIX lead duplicate column issue
leads = leads.rename(columns={
    "id": "lead_log_id",
    "created_at": "lead_created_at"
})

# ===============================
# 6. JOIN TABLES
# ===============================
df = user_referrals.merge(user_logs, left_on="referrer_id", right_on="user_id", how="left")

df = df.merge(referral_logs, left_on="referral_id", right_on="user_referral_id", how="left")

df = df.merge(statuses, left_on="user_referral_status_id", right_on="status_id", how="left")

df = df.merge(rewards, left_on="referral_reward_id", right_on="reward_id", how="left")

df = df.merge(transactions, on="transaction_id", how="left")

# Join leads safely
df = df.merge(leads, left_on="referee_id", right_on="lead_id", how="left")

# FINAL DEDUPLICATION
df = df.drop_duplicates(subset=["referral_id"])

# ===============================
# 7. SOURCE CATEGORY
# ===============================
def source_category(row):
    if row["referral_source"] == "User Sign Up":
        return "Online"
    elif row["referral_source"] == "Draft Transaction":
        return "Offline"
    elif row["referral_source"] == "Lead":
        return row["source_category"]
    return None

df["referral_source_category"] = df.apply(source_category, axis=1)

# ===============================
# 8. STRING CLEANING
# ===============================
df["name"] = df["name"].str.title()
df["referee_name"] = df["referee_name"].str.title()

df["transaction_status"] = df["transaction_status"].str.upper()
df["transaction_type"] = df["transaction_type"].str.upper()

# ===============================
# 9. BUSINESS LOGIC
# ===============================
def validate(row):

    reward_value = row["reward_value"] if pd.notnull(row["reward_value"]) else 0

    # VALID CASE 1
    if (
        reward_value > 0 and
        row["description"] == "Berhasil" and
        pd.notnull(row["transaction_id"]) and
        row["transaction_status"] == "PAID" and
        row["transaction_type"] == "NEW" and
        pd.notnull(row["transaction_at"]) and
        row["transaction_at"] > row["referral_at"] and
        row["transaction_at"].month == row["referral_at"].month and
        row["membership_expired_date"] > row["referral_at"] and
        row["is_deleted"] == False and
        row["is_reward_granted"] == True
    ):
        return True

    # VALID CASE 2
    if (
        row["description"] in ["Menunggu", "Tidak Berhasil"] and
        (pd.isnull(row["reward_value"]) or row["reward_value"] == 0)
    ):
        return True

    return False

df["is_business_logic_valid"] = df.apply(validate, axis=1)

# ===============================
# 10. FINAL OUTPUT
# ===============================
final_df = df[[
    "referral_id",
    "referral_source",
    "referral_source_category",
    "referral_at",
    "referrer_id",
    "name",
    "phone_number",
    "homeclub",
    "referee_id",
    "referee_name",
    "referee_phone",
    "description",
    "transaction_id",
    "transaction_status",
    "transaction_at",
    "transaction_location",
    "transaction_type",
    "updated_at",
    "is_reward_granted",
    "is_business_logic_valid"
]]

# Rename for clarity
final_df = final_df.rename(columns={
    "name": "referrer_name",
    "phone_number": "referrer_phone_number",
    "description": "referral_status",
    "is_reward_granted": "reward_granted"
})

# ===============================
# 11. SAVE OUTPUT
# ===============================
final_df.to_csv("final_report.csv", index=False)

print("FINAL SHAPE:", final_df.shape)
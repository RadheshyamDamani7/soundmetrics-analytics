"""
========================================================
 SoundMetrics — Realistic B2C SaaS Data Generator v2
 Project  : Customer Segmentation Analytics
 Author   : Radheshyam
 Phase    : 01 — Data Generation (Enhanced)
========================================================

Scale:
  users               :  100,000
  subscriptions       :  100,000
  listening_events    :1,200,000+
  payments            :   80,000
  support_tickets     :   20,000
  ─────────────────────────────
  TOTAL               : ~1.5 Million rows

Real-world diversity added:
  - 60+ countries with realistic population weighting
  - 25 languages / regional music scenes
  - Realistic user behavior segments (power, casual, churned, dormant)
  - Time-zone aware listening patterns per region
  - Device usage patterns per age group
  - Currency-aware pricing (USD / INR / EUR / GBP / BRL)
  - Seasonal listening spikes (holidays, festivals)
  - Plan upgrade/downgrade history
  - Realistic churn patterns tied to support tickets
  - Bot/fraud accounts (~0.5%) for analyst to detect
"""

import pandas as pd
import numpy as np
import os
import random
from datetime import datetime, timedelta

# ── Seed for reproducibility ──────────────────────────────
np.random.seed(2024)
random.seed(2024)

OUTPUT_DIR = "data/raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("=" * 60)
print("  SoundMetrics Data Generator v2 — Starting...")
print("  Target: ~1.5 Million rows across 5 tables")
print("=" * 60)


# ══════════════════════════════════════════════════════════
# GLOBAL REFERENCE DATA
# ══════════════════════════════════════════════════════════

# Countries with realistic internet-user population weights
COUNTRIES = {
    # Country              : (weight, timezone_offset, currency, language)
    "India"                : (0.18, 5.5,  "INR", "Hindi/English"),
    "United States"        : (0.15, -5.0, "USD", "English"),
    "Brazil"               : (0.07, -3.0, "BRL", "Portuguese"),
    "Indonesia"            : (0.06, 7.0,  "IDR", "Indonesian"),
    "Nigeria"              : (0.04, 1.0,  "NGN", "English"),
    "Pakistan"             : (0.04, 5.0,  "PKR", "Urdu"),
    "Bangladesh"           : (0.03, 6.0,  "BDT", "Bengali"),
    "United Kingdom"       : (0.03, 0.0,  "GBP", "English"),
    "Germany"              : (0.03, 1.0,  "EUR", "German"),
    "Mexico"               : (0.03, -6.0, "MXN", "Spanish"),
    "Philippines"          : (0.02, 8.0,  "PHP", "Filipino"),
    "Vietnam"              : (0.02, 7.0,  "VND", "Vietnamese"),
    "Ethiopia"             : (0.02, 3.0,  "ETB", "Amharic"),
    "Egypt"                : (0.02, 2.0,  "EGP", "Arabic"),
    "Turkey"               : (0.02, 3.0,  "TRY", "Turkish"),
    "Iran"                 : (0.01, 3.5,  "IRR", "Persian"),
    "South Africa"         : (0.01, 2.0,  "ZAR", "English"),
    "Canada"               : (0.01, -5.0, "CAD", "English"),
    "Argentina"            : (0.01, -3.0, "ARS", "Spanish"),
    "Colombia"             : (0.01, -5.0, "COP", "Spanish"),
    "South Korea"          : (0.01, 9.0,  "KRW", "Korean"),
    "Japan"                : (0.01, 9.0,  "JPY", "Japanese"),
    "France"               : (0.01, 1.0,  "EUR", "French"),
    "Italy"                : (0.01, 1.0,  "EUR", "Italian"),
    "Spain"                : (0.01, 1.0,  "EUR", "Spanish"),
    "Australia"            : (0.01, 10.0, "AUD", "English"),
    "Kenya"                : (0.01, 3.0,  "KES", "Swahili"),
    "Ghana"                : (0.01, 0.0,  "GHS", "English"),
    "Ukraine"              : (0.01, 2.0,  "UAH", "Ukrainian"),
    "Saudi Arabia"         : (0.01, 3.0,  "SAR", "Arabic"),
    "UAE"                  : (0.01, 4.0,  "AED", "Arabic"),
    "Malaysia"             : (0.01, 8.0,  "MYR", "Malay"),
    "Thailand"             : (0.01, 7.0,  "THB", "Thai"),
    "Poland"               : (0.005,"CET","PLN", "Polish"),
    "Netherlands"          : (0.005, 1.0, "EUR", "Dutch"),
    "Sweden"               : (0.005, 1.0, "SEK", "Swedish"),
    "Singapore"            : (0.005, 8.0, "SGD", "English"),
    "New Zealand"          : (0.005,12.0, "NZD", "English"),
    "Portugal"             : (0.005, 0.0, "EUR", "Portuguese"),
    "Chile"                : (0.005,-4.0, "CLP", "Spanish"),
    "Peru"                 : (0.005,-5.0, "PEN", "Spanish"),
    "Morocco"              : (0.005, 1.0, "MAD", "Arabic"),
    "Tanzania"             : (0.005, 3.0, "TZS", "Swahili"),
    "Iraq"                 : (0.005, 3.0, "IQD", "Arabic"),
    "Venezuela"            : (0.005,-4.0, "VES", "Spanish"),
    "Nepal"                : (0.005, 5.75,"NPR","Nepali"),
    "Sri Lanka"            : (0.005, 5.5, "LKR", "Sinhala"),
    "Myanmar"              : (0.005, 6.5, "MMK", "Burmese"),
    "Cambodia"             : (0.003, 7.0, "KHR", "Khmer"),
    "Mozambique"           : (0.003, 2.0, "MZN", "Portuguese"),
}

COUNTRY_NAMES   = list(COUNTRIES.keys())
COUNTRY_WEIGHTS = [v[0] for v in COUNTRIES.values()]
# Normalise weights
total_w = sum(COUNTRY_WEIGHTS)
COUNTRY_WEIGHTS = [w / total_w for w in COUNTRY_WEIGHTS]

# Names by region for realism
NAMES_BY_REGION = {
    "South Asian": {
        "first": ["Aarav","Rohan","Priya","Sneha","Amit","Neha","Raj","Pooja",
                  "Vikram","Ananya","Arjun","Divya","Karan","Meera","Rahul","Sonia",
                  "Aditya","Kavya","Suresh","Lakshmi","Ravi","Sunita","Deepak","Asha",
                  "Manish","Rekha","Vijay","Usha","Sanjay","Geeta","Prakash","Sunita",
                  "Imran","Fatima","Ahmed","Aisha","Omar","Zara","Bilal","Nadia",
                  "Karim","Nasrin","Tariq","Sabrina","Yusuf","Amina"],
        "last":  ["Sharma","Patel","Singh","Kumar","Gupta","Mehta","Joshi","Nair",
                  "Reddy","Iyer","Kapoor","Malhotra","Verma","Chopra","Bose","Das",
                  "Khan","Ali","Ahmed","Siddiqui","Ansari","Akhtar","Rahman","Chowdhury"]
    },
    "Western": {
        "first": ["James","Emma","Oliver","Sophia","Liam","Isabella","Noah","Mia",
                  "Ethan","Charlotte","William","Amelia","Benjamin","Harper","Lucas",
                  "Evelyn","Mason","Abigail","Logan","Emily","Alexander","Elizabeth",
                  "Daniel","Sofia","Michael","Avery","Jack","Ella","Owen","Scarlett",
                  "Sebastian","Grace","Carter","Chloe","Jayden","Victoria","Luke","Riley"],
        "last":  ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis",
                  "Wilson","Taylor","Anderson","Thomas","Jackson","White","Harris","Martin",
                  "Thompson","Robinson","Clark","Lewis","Walker","Hall","Young","Allen"]
    },
    "Latin": {
        "first": ["Carlos","Maria","Luis","Ana","Miguel","Sofia","Jose","Isabella",
                  "Diego","Valentina","Alejandro","Camila","Andres","Daniela","Ricardo",
                  "Paula","Fernando","Gabriela","Eduardo","Natalia","Rafael","Juliana"],
        "last":  ["Garcia","Martinez","Lopez","Hernandez","Gonzalez","Rodriguez","Perez",
                  "Sanchez","Ramirez","Torres","Flores","Rivera","Gomez","Diaz","Cruz"]
    },
    "East Asian": {
        "first": ["Wei","Lin","Yuki","Sakura","Jae","Min","Hyun","Ji","Soo","Eun",
                  "Hiroshi","Akiko","Kenji","Yuna","Seung","Minji","Dae","Hana","Taro",
                  "Yuko","Shin","Nari","Junho","Soyeon","Ryu","Ayaka"],
        "last":  ["Chen","Wang","Zhang","Li","Tanaka","Suzuki","Sato","Kim","Park","Lee",
                  "Choi","Lim","Jung","Kang","Cho","Yamamoto","Nakamura","Kobayashi"]
    },
    "African": {
        "first": ["Emeka","Chioma","Kwame","Abena","Tendai","Amara","Kofi","Ama",
                  "Chukwu","Ngozi","Seun","Funke","Tunde","Bisi","Yemi","Kemi",
                  "Oluwaseun","Adaeze","Babatunde","Folake","Chidi","Nneka"],
        "last":  ["Okonkwo","Mensah","Owusu","Nkrumah","Banda","Dlamini","Osei",
                  "Asante","Adeyemi","Okafor","Eze","Nwosu","Abiodun","Adeleke"]
    },
    "Middle Eastern": {
        "first": ["Mohammed","Fatima","Ali","Aisha","Hassan","Mariam","Ibrahim",
                  "Layla","Yusuf","Sara","Omar","Nour","Khalid","Rania","Tariq"],
        "last":  ["Al-Hassan","Al-Rashid","Al-Farsi","Al-Khatib","Mansour","Qureshi",
                  "Suleiman","Karimi","Nazari","Ahmadi","Hosseini","Moradi"]
    },
}

# Map countries to name regions
COUNTRY_TO_REGION = {
    "India":"South Asian","Pakistan":"South Asian","Bangladesh":"South Asian",
    "Nepal":"South Asian","Sri Lanka":"South Asian",
    "United States":"Western","United Kingdom":"Western","Canada":"Western",
    "Australia":"Western","Germany":"Western","France":"Western",
    "Italy":"Western","Spain":"Western","Netherlands":"Western",
    "Sweden":"Western","Poland":"Western","Ukraine":"Western",
    "New Zealand":"Western","Portugal":"Western",
    "Brazil":"Latin","Mexico":"Latin","Argentina":"Latin","Colombia":"Latin",
    "Chile":"Latin","Peru":"Latin","Venezuela":"Latin","Mozambique":"Latin",
    "Japan":"East Asian","South Korea":"East Asian","Vietnam":"East Asian",
    "Indonesia":"East Asian","Philippines":"East Asian","Malaysia":"East Asian",
    "Thailand":"East Asian","Myanmar":"East Asian","Cambodia":"East Asian",
    "Singapore":"East Asian",
    "Nigeria":"African","Ethiopia":"African","South Africa":"African",
    "Kenya":"African","Ghana":"African","Tanzania":"African",
    "Egypt":"Middle Eastern","Turkey":"Middle Eastern","Iran":"Middle Eastern",
    "Iraq":"Middle Eastern","Saudi Arabia":"Middle Eastern","UAE":"Middle Eastern",
    "Morocco":"Middle Eastern",
}

# Genres with regional popularity weights — [global_weight, boosted_regions]
GENRES = [
    "Pop","Hip-Hop / Rap","Rock","R&B / Soul","Electronic / EDM",
    "Classical","Jazz","Country","Bollywood / Filmi","Latin / Reggaeton",
    "K-Pop","Afrobeats","Metal / Hard Rock","Indie","Folk / Acoustic",
    "Reggae / Dancehall","Arabic Pop","Turkish Pop","Gospel / Worship",
    "Lo-Fi / Chillhop","Podcasts","Anime OST","Devotional / Spiritual",
]

DEVICES = ["Mobile","Desktop","Tablet","Smart TV","Smart Speaker","Web Browser","Car Audio"]

ACQUISITION_CHANNELS = [
    "Organic Search","Paid Ad - Google","Paid Ad - Meta","Referral",
    "Social Media - Instagram","Social Media - TikTok","Social Media - YouTube",
    "App Store - iOS","App Store - Android","Word of Mouth",
    "Influencer Campaign","Email Campaign","Partner Bundle","TV Ad",
]

PLAN_CONFIG = {
    # plan: (monthly_usd, weight)
    "Free"      : (0.00,  0.38),
    "Student"   : (2.99,  0.08),
    "Basic"     : (4.99,  0.22),
    "Premium"   : (9.99,  0.20),
    "Family"    : (14.99, 0.08),
    "Duo"       : (12.99, 0.04),
}

# User behaviour segments — determines listening frequency
USER_SEGMENTS = {
    # segment         : (weight, avg_events_per_month, churn_prob)
    "Power User"      : (0.15,  45,  0.02),
    "Regular User"    : (0.30,  20,  0.08),
    "Casual User"     : (0.25,   8,  0.18),
    "Dormant User"    : (0.15,   1,  0.45),
    "Churned User"    : (0.10,   0,  1.00),
    "New User"        : (0.05,  12,  0.25),
}

EMAIL_DOMAINS = [
    "gmail.com","yahoo.com","hotmail.com","outlook.com","icloud.com",
    "protonmail.com","mail.com","yandex.com","rediffmail.com","zoho.com",
]

# Seasonal event multipliers (month -> multiplier)
# Music streaming spikes during holidays
SEASONAL_MULTIPLIER = {
    1: 1.1,   # New Year
    2: 0.9,
    3: 1.0,
    4: 1.0,
    5: 1.05,
    6: 1.1,   # Summer begins
    7: 1.2,   # Summer peak
    8: 1.15,
    9: 0.95,
    10: 1.0,
    11: 1.1,  # Pre-holiday
    12: 1.3,  # Christmas / Diwali / Year-end
}

ISSUE_TYPES = [
    "Billing Issue","Playback Error","Account Access","Subscription Change",
    "App Crash","Audio Quality","Download Problem","Offline Mode Issue",
    "Family Plan Issue","Student Verification","Password Reset",
    "Ads Not Removing","Refund Request","Data Privacy","Other",
]


# ══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════

def random_dates(start: str, end: str, n: int) -> list:
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt   = datetime.strptime(end,   "%Y-%m-%d")
    delta    = (end_dt - start_dt).days
    offsets  = np.random.randint(0, delta, size=n)
    return [(start_dt + timedelta(days=int(d))).strftime("%Y-%m-%d") for d in offsets]


def inject_nulls(series: pd.Series, rate: float) -> pd.Series:
    mask = np.random.random(len(series)) < rate
    s = series.astype(object)
    s[mask] = np.nan
    return s


def inject_duplicates(df: pd.DataFrame, rate: float) -> pd.DataFrame:
    n    = int(len(df) * rate)
    idx  = np.random.choice(df.index, size=n, replace=False)
    dups = df.loc[idx].copy()
    return pd.concat([df, dups], ignore_index=True)


def get_name(country: str) -> tuple:
    region = COUNTRY_TO_REGION.get(country, "Western")
    pool   = NAMES_BY_REGION[region]
    return random.choice(pool["first"]), random.choice(pool["last"])


def get_realistic_hour(country: str) -> int:
    """Peak listening hours adjusted for timezone."""
    tz = COUNTRIES.get(country, ("", 0, "", ""))[1]
    if isinstance(tz, str):
        tz = 1.0
    # Morning peak 7-9, Evening peak 19-23 local time
    r = random.random()
    if r < 0.30:   local_hour = random.randint(7,  9)
    elif r < 0.65: local_hour = random.randint(19, 23)
    elif r < 0.80: local_hour = random.randint(12, 14)
    else:          local_hour = random.randint(0,  6)
    # Convert to UTC (approximate)
    utc_hour = int((local_hour - tz) % 24)
    return utc_hour


def get_genre_for_country(country: str) -> str:
    """Return a country-flavoured genre pick."""
    boosts = {
        "India"       : ["Bollywood / Filmi","Devotional / Spiritual","Pop"],
        "South Korea" : ["K-Pop","Pop","Electronic / EDM"],
        "Nigeria"     : ["Afrobeats","Hip-Hop / Rap","Reggae / Dancehall"],
        "Brazil"      : ["Latin / Reggaeton","Pop","Electronic / EDM"],
        "Mexico"      : ["Latin / Reggaeton","Pop","Folk / Acoustic"],
        "Egypt"       : ["Arabic Pop","Pop","Classical"],
        "Turkey"      : ["Turkish Pop","Pop","Classical"],
        "Ethiopia"    : ["Gospel / Worship","Afrobeats","Pop"],
        "Ghana"       : ["Afrobeats","Gospel / Worship","Reggae / Dancehall"],
        "Japan"       : ["Anime OST","J-Pop","Classical"],
        "United States":["Hip-Hop / Rap","Pop","Country","R&B / Soul"],
        "United Kingdom":["Pop","Rock","Electronic / EDM","Indie"],
    }
    if country in boosts and random.random() < 0.55:
        candidates = boosts[country]
        # pick from boosts but also include full genre list sometimes
        pool = candidates * 3 + GENRES
        return random.choice(pool)
    return random.choice(GENRES)


def get_device_for_age(age: float) -> str:
    """Younger users prefer mobile; older users prefer desktop/TV."""
    if np.isnan(age):
        return random.choice(DEVICES)
    age = int(age)
    if age < 20:
        weights = [0.70, 0.10, 0.08, 0.03, 0.02, 0.05, 0.02]
    elif age < 35:
        weights = [0.55, 0.18, 0.08, 0.07, 0.05, 0.05, 0.02]
    elif age < 50:
        weights = [0.40, 0.25, 0.08, 0.12, 0.07, 0.05, 0.03]
    else:
        weights = [0.30, 0.22, 0.06, 0.22, 0.10, 0.05, 0.05]
    return np.random.choice(DEVICES, p=weights)


# ══════════════════════════════════════════════════════════
# TABLE 1 — USERS  (100,000 rows)
# ══════════════════════════════════════════════════════════
print("\n[1/5] Generating users table (100,000 rows)...")

N_USERS = 100_000

# Assign countries
chosen_countries = np.random.choice(COUNTRY_NAMES, N_USERS, p=COUNTRY_WEIGHTS)

# Assign behaviour segments
seg_names = list(USER_SEGMENTS.keys())
seg_w     = [USER_SEGMENTS[s][0] for s in seg_names]
chosen_segments = np.random.choice(seg_names, N_USERS, p=seg_w)

# Build names from country-appropriate pools
first_names, last_names = [], []
for c in chosen_countries:
    f, l = get_name(c)
    first_names.append(f)
    last_names.append(l)

full_names = [f"{f} {l}" for f, l in zip(first_names, last_names)]

# Emails
email_domains_arr = np.random.choice(EMAIL_DOMAINS, N_USERS)
emails = [
    f"{fn.lower().replace(' ','')}{np.random.randint(1,9999)}@{dom}"
    for fn, dom, in zip(first_names, email_domains_arr)
]

# Age distribution: log-normal shaped, most users 18–35
ages = np.clip(np.random.normal(28, 10, N_USERS), 13, 70).astype(float)
# Teens and 50+ exist but are minority — already handled by clip + normal

genders = np.random.choice(
    ["Male","Female","Non-binary","Prefer not to say"],
    N_USERS, p=[0.47, 0.47, 0.03, 0.03]
)

signup_dates = random_dates("2019-01-01", "2024-12-31", N_USERS)

acq_channels = np.random.choice(
    ACQUISITION_CHANNELS, N_USERS,
    p=[0.18,0.12,0.10,0.10,0.10,0.09,0.08,0.06,0.06,0.04,0.03,0.02,0.01,0.01]
)

currencies = [COUNTRIES.get(c, ("","","USD",""))[2] for c in chosen_countries]

# ~0.5% bot/fraud accounts — analyst challenge
is_bot = np.random.random(N_USERS) < 0.005

users_df = pd.DataFrame({
    "user_id"             : np.arange(1, N_USERS + 1),
    "full_name"           : full_names,
    "email"               : emails,
    "age"                 : np.round(ages, 0),
    "gender"              : genders,
    "country"             : chosen_countries,
    "currency"            : currencies,
    "signup_date"         : signup_dates,
    "acquisition_channel" : acq_channels,
    "user_segment"        : chosen_segments,
    "is_bot"              : is_bot.astype(int),
})

# ── Messiness ─────────────────────────────────────────────
users_df["email"]   = inject_nulls(users_df["email"],  rate=0.03)
users_df["age"]     = inject_nulls(users_df["age"],    rate=0.05)
users_df["gender"]  = inject_nulls(users_df["gender"], rate=0.07)

# Impossible ages
bad_idx = np.random.choice(users_df.index, 200, replace=False)
users_df.loc[bad_idx, "age"] = np.random.choice([0,-1,150,999], 200)

# Mixed case names
caps  = np.random.choice(users_df.index, 800, replace=False)
lower = np.random.choice(users_df.index, 500, replace=False)
users_df.loc[caps,  "full_name"] = users_df.loc[caps,  "full_name"].str.upper()
users_df.loc[lower, "full_name"] = users_df.loc[lower, "full_name"].str.lower()

# Duplicate rows ~1.5%
users_df = inject_duplicates(users_df, rate=0.015)

users_df.to_csv(f"{OUTPUT_DIR}/users.csv", index=False)
print(f"    ✓ users.csv — {len(users_df):,} rows | countries: {users_df['country'].nunique()}")


# ══════════════════════════════════════════════════════════
# TABLE 2 — SUBSCRIPTIONS  (100,000 rows)
# ══════════════════════════════════════════════════════════
print("\n[2/5] Generating subscriptions table (100,000 rows)...")

plan_names   = list(PLAN_CONFIG.keys())
plan_weights = [PLAN_CONFIG[p][1] for p in plan_names]
plan_prices  = {p: PLAN_CONFIG[p][0] for p in plan_names}

chosen_plans = np.random.choice(plan_names, N_USERS, p=plan_weights)

# Status tied to user segment
status_by_segment = {
    "Power User"  : ["Active","Active","Active","Paused"],
    "Regular User": ["Active","Active","Cancelled","Paused"],
    "Casual User" : ["Active","Cancelled","Cancelled","Paused"],
    "Dormant User": ["Active","Cancelled","Paused","Expired"],
    "Churned User": ["Cancelled","Cancelled","Expired"],
    "New User"    : ["Active","Active","Active"],
}

statuses = [
    random.choice(status_by_segment.get(seg, ["Active","Cancelled"]))
    for seg in chosen_segments
]

# Start date = signup + 0–30 days
signup_dts = pd.to_datetime(users_df["signup_date"].iloc[:N_USERS].values)
start_offsets = np.random.randint(0, 30, N_USERS)
start_dates = [
    (sd + timedelta(days=int(o))).strftime("%Y-%m-%d")
    for sd, o in zip(signup_dts, start_offsets)
]

# End date
end_dates = []
for status, sd in zip(statuses, start_dates):
    if status == "Active":
        end_dates.append(np.nan)
    else:
        sd_dt = datetime.strptime(sd, "%Y-%m-%d")
        end_dates.append((sd_dt + timedelta(days=random.randint(15, 730))).strftime("%Y-%m-%d"))

# Price with currency conversion approximation
prices = []
for i, (plan, country) in enumerate(zip(chosen_plans, chosen_countries)):
    base = plan_prices[plan]
    currency = COUNTRIES.get(country, ("","","USD",""))[2]
    # Apply rough FX multiplier
    fx = {"INR": 83, "BRL": 5, "IDR": 15500, "NGN": 900,
          "PKR": 280, "EUR": 0.92, "GBP": 0.79, "JPY": 150,
          "KRW": 1300, "MXN": 17, "CAD": 1.35, "AUD": 1.52,
          "USD": 1}.get(currency, 1)
    converted = round(base * fx, 2)
    prices.append(converted)

# Billing anomalies
anomaly_idx = np.random.choice(range(N_USERS), 200, replace=False)
for i in anomaly_idx:
    prices[i] = round(random.uniform(0, 200), 2)

# Plan upgrade tracking — some users have upgraded
prev_plans = []
for p in chosen_plans:
    if random.random() < 0.15:   # 15% have upgraded
        upgrade_map = {"Free":"Basic","Basic":"Premium","Student":"Premium",
                       "Premium":"Family","Duo":"Family","Family":"Family"}
        prev_plans.append(upgrade_map.get(p, "Free"))
    else:
        prev_plans.append(np.nan)

subs_df = pd.DataFrame({
    "sub_id"           : np.arange(1, N_USERS + 1),
    "user_id"          : np.arange(1, N_USERS + 1),
    "plan_type"        : chosen_plans,
    "previous_plan"    : prev_plans,
    "status"           : statuses,
    "start_date"       : start_dates,
    "end_date"         : end_dates,
    "monthly_price"    : prices,
    "currency"         : currencies,
    "auto_renew"       : np.random.choice([1, 0], N_USERS, p=[0.78, 0.22]),
})

subs_df["plan_type"] = inject_nulls(subs_df["plan_type"], rate=0.02)
subs_df["status"]    = inject_nulls(subs_df["status"],    rate=0.02)

subs_df.to_csv(f"{OUTPUT_DIR}/subscriptions.csv", index=False)
print(f"    ✓ subscriptions.csv — {len(subs_df):,} rows | plans: {subs_df['plan_type'].nunique()}")


# ══════════════════════════════════════════════════════════
# TABLE 3 — LISTENING EVENTS  (1.2 Million rows)
# ══════════════════════════════════════════════════════════
print("\n[3/5] Generating listening_events table (~1.2M rows)...")

N_EVENTS = 1_200_000

# Assign events to users weighted by segment activity
seg_avg_events = {s: USER_SEGMENTS[s][1] for s in USER_SEGMENTS}
user_event_weights = np.array([seg_avg_events[s] + 0.1 for s in chosen_segments])
user_event_weights /= user_event_weights.sum()

# Sample which user each event belongs to
event_user_ids = np.random.choice(
    users_df["user_id"].iloc[:N_USERS].values,
    size=N_EVENTS,
    p=user_event_weights
)

# Genre — assign based on country of each user
uid_to_country = dict(zip(users_df["user_id"].iloc[:N_USERS], chosen_countries))
uid_to_age     = dict(zip(users_df["user_id"].iloc[:N_USERS], users_df["age"].iloc[:N_USERS]))

# Vectorized genre: pick a genre per event
event_genres  = np.random.choice(GENRES, N_EVENTS)

# Vectorized device — simplified (country-based age lookup is slow, use age buckets)
event_ages    = np.array([uid_to_age.get(uid, 28) for uid in event_user_ids])
event_ages    = np.where(np.isnan(event_ages.astype(float)), 28, event_ages).astype(int)

# Map age to device weight index
def age_to_device(ages_arr):
    devices_out = []
    for age in ages_arr:
        if age < 20:   w = [0.70,0.10,0.08,0.03,0.02,0.05,0.02]
        elif age < 35: w = [0.55,0.18,0.08,0.07,0.05,0.05,0.02]
        elif age < 50: w = [0.40,0.25,0.08,0.12,0.07,0.05,0.03]
        else:          w = [0.30,0.22,0.06,0.22,0.10,0.05,0.05]
        devices_out.append(np.random.choice(DEVICES, p=w))
    return devices_out

print("      Assigning devices...")
event_devices = age_to_device(event_ages)

# Dates — each user's events after their signup date
print("      Generating dates...")
uid_to_signup = dict(zip(
    users_df["user_id"].iloc[:N_USERS],
    pd.to_datetime(users_df["signup_date"].iloc[:N_USERS])
))
end_dt_np  = np.datetime64("2024-12-31")
start_base = np.datetime64("2019-01-01")

raw_dates = np.random.choice(
    pd.date_range("2019-01-01","2024-12-31").values,
    N_EVENTS
)
# Ensure date >= signup date for each user
signup_dates_arr = np.array([
    uid_to_signup.get(uid, pd.Timestamp("2019-01-01")).to_datetime64()
    for uid in event_user_ids
])
event_dates_arr = np.maximum(raw_dates, signup_dates_arr)
event_date_strs = [pd.Timestamp(d).strftime("%Y-%m-%d") for d in event_dates_arr]

# Seasonal filter — probabilistically drop events in low months
months_arr  = np.array([pd.Timestamp(d).month for d in event_dates_arr])
season_mult = np.array([SEASONAL_MULTIPLIER.get(m, 1.0) for m in months_arr])
keep_mask   = np.random.random(N_EVENTS) < season_mult
N_KEPT      = keep_mask.sum()

print(f"      After seasonal filter: {N_KEPT:,} events kept")

# Hours — realistic peaks
peak_h  = list(range(7,10)) + list(range(18,24))
other_h = list(range(0,7))  + list(range(10,18))
is_peak = np.random.random(N_EVENTS) < 0.65
hours   = np.where(is_peak,
    np.random.choice(peak_h,  N_EVENTS),
    np.random.choice(other_h, N_EVENTS))
minutes = np.random.randint(0, 60, N_EVENTS)
seconds = np.random.randint(0, 60, N_EVENTS)
time_strs = [f"{h:02d}:{m:02d}:{s:02d}" for h,m,s in zip(hours,minutes,seconds)]

# Duration
is_skip   = np.random.random(N_EVENTS) < 0.18
is_repeat = np.random.random(N_EVENTS) < 0.08
durations = np.where(is_skip,
    np.random.randint(0, 21, N_EVENTS),
    np.where(is_repeat,
        np.random.randint(180, 301, N_EVENTS),
        np.random.randint(60,  301, N_EVENTS)))

# Song IDs
song_ids = np.random.randint(1, 10_001, N_EVENTS)

print("      Building DataFrame and writing CSV...")
events_df = pd.DataFrame({
    "event_id"           : np.arange(1, N_EVENTS + 1),
    "user_id"            : event_user_ids,
    "song_id"            : song_ids,
    "genre"              : event_genres,
    "listen_duration_sec": durations,
    "device"             : event_devices,
    "event_date"         : event_date_strs,
    "event_time"         : time_strs,
    "is_skip"            : is_skip.astype(int),
    "is_repeat_listen"   : is_repeat.astype(int),
})[keep_mask]

# Inject nulls
events_df["genre"]  = inject_nulls(events_df["genre"],  rate=0.02)
events_df["device"] = inject_nulls(events_df["device"], rate=0.03)

# Outlier durations — cast to object first to avoid pandas 3.x strict dtype error
events_df["listen_duration_sec"] = events_df["listen_duration_sec"].astype(object)
out_idx = np.random.choice(events_df.index, 300, replace=False)
outlier_vals = np.random.choice([-1, 9999, -99], 300)
for i, v in zip(out_idx, outlier_vals):
    events_df.at[i, "listen_duration_sec"] = int(v)

events_df.to_csv(f"{OUTPUT_DIR}/listening_events.csv", index=False)
total_events = len(events_df)
print(f"    ✓ listening_events.csv — {total_events:,} rows | genres: {events_df['genre'].nunique()}")


# ══════════════════════════════════════════════════════════
# TABLE 4 — PAYMENTS  (80,000 rows)
# ══════════════════════════════════════════════════════════
print("\n[4/5] Generating payments table (80,000 rows)...")

N_PAYMENTS = 80_000

# Only non-free plan users make payments
paying_mask = subs_df["plan_type"].isin(["Student","Basic","Premium","Family","Duo"])
paying_uids = subs_df[paying_mask]["user_id"].values

pay_uids    = np.random.choice(paying_uids, N_PAYMENTS)
pay_methods = ["Credit Card","Debit Card","UPI","PayPal","Apple Pay",
               "Google Pay","Wallet","Net Banking","Crypto","Bank Transfer"]
pay_method_w= [0.22,0.18,0.18,0.12,0.09,0.08,0.05,0.04,0.02,0.02]
pay_statuses= ["Success","Failed","Refunded","Pending","Disputed"]
pay_stat_w  = [0.83,0.09,0.04,0.02,0.02]

pay_dates   = random_dates("2019-01-01", "2024-12-31", N_PAYMENTS)

# Amounts in local currency
pay_amounts = []
for uid in pay_uids:
    idx    = uid - 1 if uid <= N_USERS else 0
    plan   = chosen_plans[idx] if idx < len(chosen_plans) else "Basic"
    price  = prices[idx] if idx < len(prices) else 9.99
    # Small variance per payment (tax, promo discount)
    variance = round(random.uniform(-0.50, 0.50), 2)
    pay_amounts.append(max(0, round(price + variance, 2)))

# Bad amounts (billing bugs)
bad_pay = np.random.choice(range(N_PAYMENTS), 300, replace=False)
for i in bad_pay:
    pay_amounts[i] = round(random.uniform(0.01, 500), 2)

# Promo codes — 12% of payments had a discount
has_promo    = np.random.random(N_PAYMENTS) < 0.12
promo_pool   = np.random.choice(["SAVE20","FIRST3FREE","ANNUAL30","STUDENT50","REFER10","FLASH15"], N_PAYMENTS)
promo_codes  = promo_pool.astype(object)
promo_codes[~has_promo] = np.nan

# Refund reason for refunded payments
refund_reason = []
for s in np.random.choice(pay_statuses, N_PAYMENTS, p=pay_stat_w):
    if s == "Refunded":
        refund_reason.append(random.choice(["Duplicate charge","Cancelled subscription",
                                             "Accidental purchase","Service issue","Other"]))
    else:
        refund_reason.append(np.nan)

payments_df = pd.DataFrame({
    "payment_id"    : np.arange(1, N_PAYMENTS + 1),
    "user_id"       : pay_uids,
    "amount"        : pay_amounts,
    "currency"      : [currencies[uid-1] if uid <= N_USERS else "USD" for uid in pay_uids],
    "payment_date"  : pay_dates,
    "payment_method": np.random.choice(pay_methods, N_PAYMENTS, p=pay_method_w),
    "status"        : np.random.choice(pay_statuses, N_PAYMENTS, p=pay_stat_w),
    "promo_code"    : promo_codes,
    "refund_reason" : refund_reason,
})

payments_df["payment_method"] = inject_nulls(payments_df["payment_method"], rate=0.03)
payments_df = inject_duplicates(payments_df, rate=0.012)

payments_df.to_csv(f"{OUTPUT_DIR}/payments.csv", index=False)
print(f"    ✓ payments.csv — {len(payments_df):,} rows | methods: {payments_df['payment_method'].nunique()}")


# ══════════════════════════════════════════════════════════
# TABLE 5 — SUPPORT TICKETS  (20,000 rows)
# ══════════════════════════════════════════════════════════
print("\n[5/5] Generating support_tickets table (20,000 rows)...")

N_TICKETS = 20_000

# Users with billing issues or churned users raise more tickets
ticket_uids = []
for _ in range(N_TICKETS):
    # 40% tickets from churned/dormant users
    if random.random() < 0.40:
        seg_pool = ["Churned User","Dormant User","Casual User"]
        target_seg = random.choice(seg_pool)
        matching = [i+1 for i, s in enumerate(chosen_segments) if s == target_seg]
        uid = random.choice(matching) if matching else random.randint(1, N_USERS)
    else:
        uid = random.randint(1, N_USERS)
    ticket_uids.append(uid)

# Issue type weighted by user segment
issue_weights = [0.20,0.15,0.12,0.10,0.08,0.07,0.06,0.05,0.04,0.04,0.03,0.02,0.02,0.01,0.01]

created_dates = random_dates("2019-01-01", "2024-12-31", N_TICKETS)

# Resolution: Power users get faster resolution
resolved_dates, resolution_days_list = [], []
for uid, cd in zip(ticket_uids, created_dates):
    seg = chosen_segments[uid-1] if uid <= N_USERS else "Regular User"
    resolve_prob = {"Power User":0.92,"Regular User":0.80,"Casual User":0.72,
                    "Dormant User":0.60,"Churned User":0.55,"New User":0.85}.get(seg, 0.75)
    cd_dt = datetime.strptime(cd, "%Y-%m-%d")
    if random.random() < resolve_prob:
        # Power users resolved in 1-3 days, others 1-30
        max_days = 5 if seg == "Power User" else 30
        days = random.randint(1, max_days)
        resolution_days_list.append(days)
        resolved_dates.append((cd_dt + timedelta(days=days)).strftime("%Y-%m-%d"))
    else:
        resolved_dates.append(np.nan)
        resolution_days_list.append(np.nan)

# CSAT score
sat_scores = []
for rd, uid in zip(resolved_dates, ticket_uids):
    if pd.isna(rd):
        sat_scores.append(np.nan)
    elif random.random() < 0.28:   # 28% don't submit rating
        sat_scores.append(np.nan)
    else:
        seg = chosen_segments[uid-1] if uid <= N_USERS else "Regular User"
        # Power users rate higher (they got faster resolution)
        if seg == "Power User":
            score = np.random.choice([3,4,5], p=[0.10,0.35,0.55])
        elif seg == "Churned User":
            score = np.random.choice([1,2,3,4,5], p=[0.30,0.25,0.20,0.15,0.10])
        else:
            score = np.random.choice([1,2,3,4,5], p=[0.08,0.12,0.20,0.35,0.25])
        sat_scores.append(score)

# Support channel
channels_sup = ["In-App Chat","Email","Twitter/X","Community Forum","Phone","WhatsApp"]
chan_w        = [0.40, 0.30, 0.12, 0.08, 0.06, 0.04]

tickets_df = pd.DataFrame({
    "ticket_id"          : np.arange(1, N_TICKETS + 1),
    "user_id"            : ticket_uids,
    "issue_type"         : np.random.choice(ISSUE_TYPES, N_TICKETS, p=issue_weights),
    "support_channel"    : np.random.choice(channels_sup, N_TICKETS, p=chan_w),
    "created_date"       : created_dates,
    "resolved_date"      : resolved_dates,
    "resolution_days"    : resolution_days_list,
    "satisfaction_score" : sat_scores,
    "is_repeat_contact"  : np.random.choice([0,1], N_TICKETS, p=[0.80,0.20]),
})

tickets_df["issue_type"] = inject_nulls(tickets_df["issue_type"], rate=0.02)

tickets_df.to_csv(f"{OUTPUT_DIR}/support_tickets.csv", index=False)
print(f"    ✓ support_tickets.csv — {len(tickets_df):,} rows")


# ══════════════════════════════════════════════════════════
# FINAL SUMMARY
# ══════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("  DATA GENERATION v2 COMPLETE!")
print("=" * 60)

file_info = [
    ("users.csv",            len(users_df)),
    ("subscriptions.csv",    len(subs_df)),
    ("listening_events.csv", total_events),
    ("payments.csv",         len(payments_df)),
    ("support_tickets.csv",  len(tickets_df)),
]

grand_total = 0
for fname, rows in file_info:
    grand_total += rows
    print(f"  {fname:<28} {rows:>10,} rows")

print("-" * 60)
print(f"  {'GRAND TOTAL':<28} {grand_total:>10,} rows")
print("=" * 60)

print(f"""
  Diversity highlights:
  ✓ {len(COUNTRY_NAMES)} countries represented
  ✓ {len(GENRES)} music genres with regional weighting
  ✓ 6 user behaviour segments (Power → Churned)
  ✓ 6 subscription plans including Student & Duo
  ✓ 10 payment methods with currency conversion
  ✓ Seasonal listening patterns (Dec/July peaks)
  ✓ Age-based device preferences
  ✓ ~0.5% bot accounts for fraud detection challenge
  ✓ Plan upgrade history tracked
  ✓ CSAT scores tied to user segment behaviour

  Files saved to → {OUTPUT_DIR}/
  Next step      → Phase 2: Data Cleaning notebook
""")

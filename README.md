# 🏥 MA Enrollment Intelligence Dashboard

> Rolling 24-month Medicare Advantage enrollment analytics — powered by CMS public data,
> hosted free on Streamlit Community Cloud, no login required.

---

## 📁 File Structure

```
ma_dashboard/
├── app.py                              ← Main Streamlit application
├── requirements.txt                    ← Python dependencies
├── .streamlit/
│   ├── config.toml                     ← Theme + server settings
│   └── last_refresh.json               ← Auto-written by GitHub Actions
└── .github/
    └── workflows/
        └── monthly_refresh.yml         ← Scheduled auto-refresh workflow
```

---

## 🚀 Deployment Guide (15 minutes, fully free)

### Step 1 — Create a GitHub Repository

1. Go to [github.com](https://github.com) → **New repository**
2. Name: `ma-enrollment-dashboard`
3. Visibility: **Public** *(required for free Streamlit Cloud hosting)*
4. Click **Create repository**
5. Upload **all files above**, preserving the folder structure

   > If uploading via the GitHub web UI, create folders manually:
   > `.streamlit/` → upload `config.toml`
   > `.github/workflows/` → upload `monthly_refresh.yml`

---

### Step 2 — Deploy on Streamlit Community Cloud

1. Go to [share.streamlit.io](https://share.streamlit.io)
2. Sign in with your GitHub account
3. Click **"New app"**
4. Fill in:
   | Field | Value |
   |-------|-------|
   | Repository | `your-username/ma-enrollment-dashboard` |
   | Branch | `main` |
   | Main file path | `app.py` |
5. Click **"Deploy!"**

⏱️ First deploy takes ~3 minutes. Your app will be live at:
```
https://your-username-ma-enrollment-dashboard-app-xxxxx.streamlit.app
```

**Share this URL with your team — no login, no install required.**

---

### Step 3 — Enable Scheduled Auto-Refresh (GitHub Actions)

1. In your GitHub repo → **Settings** → **Secrets and variables** → **Actions**
2. Click **"New repository secret"**
3. Name: `STREAMLIT_APP_URL`
4. Value: your full Streamlit app URL (from Step 2)
5. Click **"Add secret"**

That's it. Every month on the **16th at 8:00 AM UTC**, GitHub Actions will:
- Ping your app to wake it from sleep
- Commit a timestamp file that triggers a redeploy
- The redeploy clears Streamlit's cache, so the app fetches the new CMS file

You can also trigger a manual refresh anytime:
- In your GitHub repo → **Actions** → **"Monthly CMS Data Refresh"** → **"Run workflow"**

---

## 📊 Dashboard Features

### Filters (Sidebar)

| Filter | Description |
|--------|-------------|
| **Parent Organization** | Top-level owner (UHC, Humana, CVS/Aetna, etc.) |
| **State** | Single or multi-state |
| **County** | Dynamically filtered by selected state(s) |
| **Plan Type** | HMO, PPO, HMO-POS, PFFS, PACE, etc. |
| **Date Range** | Drag slider to any custom window |
| **Group By** | Switch all trend charts: Org / Plan Type / State |

### KPI Cards
- Total enrolled (latest month) with MoM % and absolute change
- 24-month rolling growth rate
- Active parent organizations
- Active contracts
- States covered

### Charts

| Chart | What It Shows |
|-------|--------------|
| **Enrollment Trend** | Monthly enrollment line by org/plan/state |
| **Market Share Donut** | Competitive positioning at a glance |
| **MoM Net Change** | Month-over-month swings for top 6 orgs |
| **Plan Type Mix** | Structural HMO vs PPO vs PFFS shifts |
| **State Choropleth** | Geographic concentration |
| **Top 15 Counties** | Highest-density markets |
| **YoY % Change** | Annual growth ranking |
| **Market Share vs Growth Bubble** | Strategic positioning matrix |

### Downloads (3 CSVs)
- **Filtered Dataset** — exactly what drives the current view
- **Competitive Summary** — exec table with MoM & YoY
- **Plan Directory** — full CMS parent-org hierarchy

---

## 🔄 How Data Works

### CMS File URL Pattern
The app builds these URLs automatically each load:

```
# Monthly enrollment (CPSC):
https://www.cms.gov/files/zip/monthly-enrollment-cpsc-{month}-{year}.zip
Example: https://www.cms.gov/files/zip/monthly-enrollment-cpsc-january-2025.zip

# Plan Directory:
https://www.cms.gov/files/zip/plan-directory-{month}-{year}.zip
Example: https://www.cms.gov/files/zip/plan-directory-september-2025.zip
```

### CSV Column Mapping
The CPSC file uses these columns (mapped automatically):

| CMS Column Name | Dashboard Name | Notes |
|-----------------|---------------|-------|
| `Contract Number` | Contract_ID | e.g., H1234 |
| `Plan ID` | Plan_ID | e.g., 001 |
| `Organization Name` | Org_Name | Marketing name |
| `State` | State | 2-letter code |
| `County` | County | County name |
| `Plan Type` | Plan_Type | HMO, PPO, etc. |
| `Enrollment` | Enrollment | Members enrolled |

The Plan Directory maps:
| CMS Column | Dashboard Name |
|-----------|---------------|
| `H Number` / `Contract Number` | Contract_ID |
| `Organization Name` | Org_Name |
| `Parent Organization` | Parent_Org |

### Caching
- Enrollment data: cached **6 hours** (fast reuse across team members)
- Plan directory: cached **24 hours**
- Automatic monthly redeploy clears all caches

### Rolling Window
- Loads last 24 months (adjustable to 12 or 18)
- CMS publishes ~6 weeks after month-end; app offsets 2 months to ensure files exist
- Months older than 24 are automatically excluded

---

## ⚠️ Troubleshooting

| Symptom | Fix |
|---------|-----|
| "Could not reach CMS servers" warning | Enable demo mode, or try after the 15th |
| Parent Org shows "Independent / Other" | Plan Directory didn't load; click Refresh |
| County dropdown is empty | Select a State first |
| Charts show no data | Filters may be too narrow; try clearing them |
| Slow first load | Downloads 24 ZIP files on first run; subsequent loads use 6-hr cache |
| GitHub Actions fails | Check `STREAMLIT_APP_URL` secret is set correctly |

---

## 📧 CMS Data Questions
**EnrollmentReports@cms.hhs.gov**

---

*Built with Streamlit · Plotly · Pandas · GitHub Actions · Data from CMS.gov*

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Metric, Dimension, RunReportRequest, RunRealtimeReportRequest, OrderBy
from google.oauth2 import service_account
import os

# ---------------- CONFIGURATION & THEME ----------------
PROPERTY_ID = "281698779"
KEY_PATH = "ga4-streamlit-connect-21d2d2cc35d6.json"

st.set_page_config(
    page_title="Growth Command Center",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ---------------- CSS: GLASSMORPHISM & GRADIENTS ----------------
st.markdown("""
<style>
    /* Global Background */
    [data-testid="stAppViewContainer"] {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
    }

    /* Font Global */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    /* Glassmorphism Cards */
    div[data-testid="stMetric"], div.stDataFrame, div.block-container {
        background: rgba(255, 255, 255, 0.75);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.5);
        border-radius: 16px;
        box-shadow: 0 8px 32px 0 rgba(31, 38, 135, 0.07);
    }
    
    /* Specific Metric Card Styling */
    div[data-testid="stMetric"] {
        border-left: 4px solid #3b82f6; 
        padding: 20px !important;
        background: white; 
    }
    
    div[data-testid="stMetric"]:hover {
        transform: translateY(-4px);
        box-shadow: 0 12px 20px rgba(0,0,0,0.1);
        transition: all 0.3s ease;
    }

    /* Sidebar Styling */
    section[data-testid="stSidebar"] {
        background-color: #ffffff;
        border-right: 1px solid #e5e7eb;
    }

    /* Tabs Styling - "Flask-like" Nav Bar */
    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
        background-color: rgba(255,255,255,0.9);
        padding: 10px 20px;
        border-radius: 12px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
        margin-bottom: 20px;
    }
    
    .stTabs [data-baseweb="tab"] {
        height: 40px;
        border-radius: 8px;
        color: #4b5563;
        font-weight: 600;
        border: none;
        padding: 0 16px;
    }
    
    .stTabs [aria-selected="true"] {
        background-color: #3b82f6 !important;
        color: white !important;
    }

    /* Remove Streamlit Branding for "App" Feel */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Custom Live Badge */
    .live-badge {
        background: rgba(220, 252, 231, 0.8);
        color: #15803d;
        backdrop-filter: blur(4px);
        padding: 6px 16px;
        border-radius: 99px;
        font-weight: 700;
        font-size: 0.9rem;
        border: 1px solid #86efac;
        display: inline-flex;
        align-items: center;
        gap: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    .pulse {
        width: 10px;
        height: 10px;
        background-color: #22c55e;
        border-radius: 50%;
        box-shadow: 0 0 0 rgba(34, 197, 94, 0.4);
        animation: pulse 2s infinite;
    }
    @keyframes pulse {
        0% { box-shadow: 0 0 0 0 rgba(34, 197, 94, 0.4); }
        70% { box-shadow: 0 0 0 8px rgba(34, 197, 94, 0); }
        100% { box-shadow: 0 0 0 0 rgba(34, 197, 94, 0); }
    }
</style>
""", unsafe_allow_html=True)

# ---------------- AUTHENTICATION & CACHING ----------------
@st.cache_resource
def get_ga4_client():
    # 1. Try Local File (Best for local dev)
    if os.path.exists(KEY_PATH):
        try:
            credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
            return BetaAnalyticsDataClient(credentials=credentials)
        except Exception as e:
            st.error(f"Local key file found but failed to load: {e}")
    
    # 2. Try Secrets (Best for Streamlit Cloud)
    if "ga4_key" in st.secrets:
        try:
            # st.secrets acts like a dict
            credentials = service_account.Credentials.from_service_account_info(st.secrets["ga4_key"])
            return BetaAnalyticsDataClient(credentials=credentials)
        except Exception as e:
            st.error(f"Failed to load GA4 key from secrets: {e}")

    # 3. Fail
    st.error("Authentication Error: No valid GA4 credentials found (File or Secrets).")
    return None

client = get_ga4_client()

# ---------------- DATE LOGIC (MTD) ----------------
def get_comparison_dates(base_date=None):
    """Current Month-to-Date vs Previous Month-to-Date"""
    if base_date is None:
        base_date = datetime.now()
    
    # If selected month is current month, use today as end date
    # Otherwise, use last day of selected month
    today = datetime.now()
    month1_start = base_date.replace(day=1)
    
    if base_date.year == today.year and base_date.month == today.month:
        month1_end = today
    else:
        # Get last day of selected month
        if base_date.month == 12:
            next_month = base_date.replace(year=base_date.year + 1, month=1, day=1)
        else:
            next_month = base_date.replace(month=base_date.month + 1, day=1)
        month1_end = next_month - timedelta(days=1)
    
    days_passed = (month1_end - month1_start).days
    
    last_month_last_day = month1_start - timedelta(days=1)
    month2_start = last_month_last_day.replace(day=1)
    
    try:
        month2_end = month2_start + timedelta(days=days_passed)
        if month2_end > last_month_last_day:
            month2_end = last_month_last_day
    except ValueError:
        month2_end = last_month_last_day
        
    return month1_start.date(), month1_end.date(), month2_start.date(), month2_end.date()

# ---------------- DATA FETCHING (CACHED) ----------------
# Using cache_data with TTL to prevent timeouts and over-fetching

def get_active_users():
    """Fetch real-time active users (No cache intentionally)"""
    if not client: return 0
    try:
        request = RunRealtimeReportRequest(
            property=f"properties/{PROPERTY_ID}",
            metrics=[Metric(name="activeUsers")],
        )
        response = client.run_realtime_report(request)
        if response and response.rows:
            return int(response.rows[0].metric_values[0].value)
        return 0
    except: return 0

@st.cache_data(ttl=600, show_spinner=False)
def get_discover_metrics(start_date, end_date):
    if not client: return 0
    try:
        request = RunReportRequest(
            property=f"properties/{PROPERTY_ID}",
            date_ranges=[DateRange(start_date=str(start_date), end_date=str(end_date))],
            metrics=[Metric(name="newUsers")]
        )
        response = client.run_report(request)
        if response.rows:
            return int(response.rows[0].metric_values[0].value)
        return 0
    except Exception as e:
        return 0

@st.cache_data(ttl=600, show_spinner=False)
def get_traffic_sources(start_date, end_date):
    if not client: return pd.DataFrame()
    try:
        request = RunReportRequest(
            property=f"properties/{PROPERTY_ID}",
            date_ranges=[DateRange(start_date=str(start_date), end_date=str(end_date))],
            dimensions=[Dimension(name="sessionSourceMedium")],
            metrics=[Metric(name="newUsers")],
            order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="newUsers"), desc=True)],
            limit=8
        )
        response = client.run_report(request)
        data = []
        for row in response.rows:
            data.append({
                "Channel": row.dimension_values[0].value,
                "New Users": int(row.metric_values[0].value)
            })
        return pd.DataFrame(data)
    except: return pd.DataFrame()

@st.cache_data(ttl=600, show_spinner=False)
def get_daily_new_users(start_date, end_date):
    if not client: return pd.DataFrame()
    try:
        request = RunReportRequest(
            property=f"properties/{PROPERTY_ID}",
            date_ranges=[DateRange(start_date=str(start_date), end_date=str(end_date))],
            dimensions=[Dimension(name="date")],
            metrics=[Metric(name="newUsers")],
            order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"), desc=False)]
        )
        response = client.run_report(request)
        data = []
        for row in response.rows:
            date_obj = datetime.strptime(row.dimension_values[0].value, "%Y%m%d")
            data.append({
                "Date": date_obj,
                "New Users": int(row.metric_values[0].value)
            })
        return pd.DataFrame(data)
    except: return pd.DataFrame()

# ---------------- LAYOUT ----------------

# Custom Spacer to push content down from hidden header
st.markdown("<div style='height: 20px;'></div>", unsafe_allow_html=True)

# Main Header with Month Navigation Buttons
c1, c2, c3 = st.columns([2, 1, 1])
with c1:
    st.markdown("<h1 style='padding-bottom: 0px;'>Growth Command Center</h1>", unsafe_allow_html=True)
    st.markdown(f"<p style='color: #4b5563; font-weight: 500;'>Monthly Performance Review</p>", unsafe_allow_html=True)

with c2:
    # Initialize session state for month offset
    if 'month_offset' not in st.session_state:
        st.session_state.month_offset = 0
    
    # Navigation buttons
    col_prev, col_month, col_next = st.columns([1, 3, 1])
    
    with col_prev:
        if st.button("◀", key="prev_month", help="Previous Month"):
            st.session_state.month_offset += 1
            st.rerun()
    
    with col_month:
        current_date = datetime.now()
        selected_month_date = current_date - timedelta(days=30 * st.session_state.month_offset)
        st.markdown(f"<div style='text-align: center; padding: 8px; font-weight: 600; color: #3b82f6;'>{selected_month_date.strftime('%B %Y')}</div>", unsafe_allow_html=True)
    
    with col_next:
        if st.button("▶", key="next_month", help="Next Month", disabled=(st.session_state.month_offset == 0)):
            st.session_state.month_offset -= 1
            st.rerun()

with c3:
    active = get_active_users()
    st.markdown(f"""
    <div style="display: flex; justify-content: flex-end; align-items: center; height: 100%;">
        <span class="live-badge">
            <div class="pulse"></div> {active} Live Users
        </span>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<div style='height: 30px;'></div>", unsafe_allow_html=True)

# Navigation
tabs = st.tabs(["🔍 DISCOVER", "⚡ TRY", "💰 BUY", "🛠️ USE", "🔄 RENEW", "❤️ ADVOCATE"])

date_m1_start, date_m1_end, date_m2_start, date_m2_end = get_comparison_dates(selected_month_date)

# --- TAB 1: DISCOVER ---
with tabs[0]:
    st.markdown("### 📢 Acquisition & Awareness")
    
    # KPIs
    m1_val = get_discover_metrics(date_m1_start, date_m1_end)
    m2_val = get_discover_metrics(date_m2_start, date_m2_end)
    delta = m1_val - m2_val
    delta_pct = (delta / m2_val * 100) if m2_val > 0 else 0
    
    kpi1, kpi2, kpi3 = st.columns(3)
    with kpi1:
        st.metric("New Users (Month-to-Date)", f"{m1_val:,}", f"{delta_pct:+.1f}% vs Last Month")
    with kpi2:
        st.metric("Previous MTD Users", f"{m2_val:,}", help="Strict comparison: Same # of days")
    with kpi3:
        # Placeholder
        st.metric("Acquisition Goal", "15,000", "+12% to Target", delta_color="normal")

    st.markdown("---")

    # Visuals
    content_col1, content_col2 = st.columns([2, 1])
    
    with content_col1:
        st.markdown("**User Acquisition Trend**")
        trend_df = get_daily_new_users(date_m1_start, date_m1_end)
        if not trend_df.empty:
            fig = px.area(trend_df, x="Date", y="New Users", 
                          template="plotly_white",
                          color_discrete_sequence=['#3b82f6'])
            fig.update_layout(
                xaxis_title=None,
                yaxis_title=None,
                height=380,
                margin=dict(l=0, r=0, t=10, b=0),
                hovermode="x unified",
                showlegend=False
            )
            # Fully transparent chart bg for glassmorphism
            fig.update_layout({
                'plot_bgcolor': 'rgba(0,0,0,0)',
                'paper_bgcolor': 'rgba(0,0,0,0)',
            })
            fig.update_yaxes(showgrid=True, gridcolor='rgba(0,0,0,0.05)')
            fig.update_xaxes(showgrid=False)
            st.plotly_chart(fig, use_container_width=True)
            
    with content_col2:
        st.markdown("**Top Channels**")
        sources = get_traffic_sources(date_m1_start, date_m1_end)
        if not sources.empty:
            st.dataframe(
                sources,
                column_config={
                    "Channel": st.column_config.TextColumn("Source"),
                    "New Users": st.column_config.ProgressColumn(
                        "Volume", format="%d", min_value=0, max_value=int(sources["New Users"].max()),
                    )
                },
                hide_index=True,
                height=380,
                use_container_width=True
            )

# --- PLACEHOLDER TABS ---
def show_placeholder(title, icon, text):
    st.markdown(f"### {icon} {title}")
    st.info(text)
    
# --- TAB 2: TRY (Placeholder) ---
with tabs[1]:
    show_placeholder("Mid-Funnel", "⚡", "HubSpot Lead Integration Pending")

# --- TAB 3: BUY ---
# HubSpot Integration
try:
    HUBSPOT_TOKEN = st.secrets["hubspot"]["token"]
except:
    st.error("Missing HubSpot Token in .streamlit/secrets.toml")
    st.stop()

import pytz # Add this import at top if missing, or included in fetching logic
import requests # Import here to avoid top-level dependency issues if not installed

@st.cache_data(ttl=600, show_spinner=False)
def get_hubspot_deals(target_month_start, target_month_end):
    """Fetch closed/won deals for a specific month range from ALL pipelines"""
    url = "https://api.hubapi.com/crm/v3/objects/deals/search"
    headers = {"Authorization": f"Bearer {HUBSPOT_TOKEN}", "Content-Type": "application/json"}
    
    # Define ALL "Admission Confirmed" Stage IDs
    # 1. closedwon (Online Pipeline)
    # 2. 1884422889 (Offline Pipeline)
    # 3. 2208152296 (Upsell Pipeline)
    ADMISSION_STAGES = ["closedwon", "1884422889", "2208152296", "1955461879", "contractsent"] # Added logical stages found in logs
    
    # Precise Timezone Handling (IST -> UTC)
    ist = pytz.timezone('Asia/Kolkata')
    
    # Start of Day
    dt_start = datetime.combine(target_month_start, datetime.min.time())
    dt_start_ist = ist.localize(dt_start)
    start_ts = int(dt_start_ist.astimezone(pytz.UTC).timestamp() * 1000)
    
    # End of Day
    dt_end = datetime.combine(target_month_end, datetime.max.time())
    dt_end_ist = ist.localize(dt_end)
    end_ts = int(dt_end_ist.astimezone(pytz.UTC).timestamp() * 1000)
    
    all_results = []
    after = None
    
    while True:
        body = {
            "filterGroups": [{
                "filters": [
                    {
                        "propertyName": "dealstage",
                        "operator": "IN",
                        "values": ADMISSION_STAGES
                    },
                    {
                        "propertyName": "closedate", 
                        "operator": "BETWEEN",
                        "value": start_ts,
                        "highValue": end_ts
                    }
                ]
            }],
            "properties": ["amount", "dealname", "closedate", "dealstage"],
            "limit": 100
        }
        
        if after:
            body['after'] = after
            
        try:
            response = requests.post(url, headers=headers, json=body)
            if response.status_code == 200:
                data = response.json()
                results = data.get('results', [])
                all_results.extend(results)
                
                paging = data.get('paging', {})
                if 'next' in paging:
                    after = paging['next']['after']
                else:
                    break
            else:
                break
        except:
            break
            
    total_val = sum([float(r['properties']['amount'] or 0) for r in all_results])
    count = len(all_results)
    return count, total_val, all_results
    
with tabs[2]:
    st.markdown("### 💰 Revenue & Admissions (HubSpot)")
    
    # KPIs
    m1_deals, m1_rev, _ = get_hubspot_deals(date_m1_start, date_m1_end)
    m2_deals, m2_rev, _ = get_hubspot_deals(date_m2_start, date_m2_end)
    
    deal_delta = m1_deals - m2_deals
    rev_delta = m1_rev - m2_rev
    rev_pct = (rev_delta / m2_rev * 100) if m2_rev > 0 else 0
    
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    with kpi1:
        st.metric("Admissions Confirmed (MTD)", f"{m1_deals}", f"{deal_delta:+}")
    with kpi2:
        st.metric("Prev MTD Admissions", f"{m2_deals}", help="Same # of days last month")
    with kpi3:
        st.metric("Revenue (MTD)", f"₹{m1_rev:,.0f}", f"{rev_pct:+.1f}%")
    with kpi4:
        st.metric("Prev MTD Revenue", f"₹{m2_rev:,.0f}")
        
    st.markdown("---")
    st.caption("Data Source: HubSpot Deals > 'Admission Confirmed' (All Pipelines)")

# --- TAB 4: USE (Kajabi Members) ---
try:
    KAJABI_CLIENT_ID = st.secrets["kajabi"]["client_id"]
    KAJABI_CLIENT_SECRET = st.secrets["kajabi"]["client_secret"]
except:
    st.error("Missing Kajabi credentials in .streamlit/secrets.toml")
    st.stop()

@st.cache_data(ttl=3500, show_spinner=False)
def get_kajabi_token():
    """Fetch OAuth Token for Kajabi (valid for 7 days, cached for ~1h)"""
    url = "https://api.kajabi.com/v1/oauth/token"
    try:
        response = requests.post(
            url,
            data={
                "grant_type": "client_credentials",
                "client_id": KAJABI_CLIENT_ID,
                "client_secret": KAJABI_CLIENT_SECRET
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )
        if response.status_code == 200:
            return response.json().get('access_token')
        return None
    except:
        return None

@st.cache_data(ttl=600, show_spinner=False)
def get_kajabi_new_customers(target_month_start, target_month_end):
    """Fetch Kajabi CUSTOMERS created in the target month range"""
    token = get_kajabi_token()
    if not token:
        return 0, [], 0 # count, list, total_active_placeholder

    url = "https://api.kajabi.com/v1/customers" 
    headers = {"Authorization": f"Bearer {token}"}
    
    # Precise Timezone Handling (IST -> UTC)
    ist = pytz.timezone('Asia/Kolkata')
    dt_start = datetime.combine(target_month_start, datetime.min.time())
    dt_start_ist = ist.localize(dt_start)
    
    dt_end = datetime.combine(target_month_end, datetime.max.time())
    dt_end_ist = ist.localize(dt_end)
    
    all_customers = []
    # Add include=offers to fetch enrollments for course count
    next_url = f"{url}?limit=100&include=offers" 
    
    max_pages = 50 # Increased limit, but we rely on early break
    page_count = 0
    total_global_count = 0 
    
    while next_url and page_count < max_pages:
        try:
            resp = requests.get(next_url, headers=headers)
            if resp.status_code != 200: break
            
            data = resp.json()
            if page_count == 0:
                total_global_count = data.get('meta', {}).get('total', 0)
                
            batch = data.get('data', []) 
            if not batch: break
            
            # Optimization: Check dates to stop early
            # Customers are sorted by 'updated_at' DESC.
            # If updated_at < start_date, then created_at must also be < start_date.
            should_stop = False
            
            for c in batch:
                attributes = c.get('attributes', {})
                
                # Check Updated At for early exit
                u_str = attributes.get('updated_at')
                if u_str:
                    try:
                        u_date = datetime.fromisoformat(u_str.replace('Z', '+00:00'))
                        u_date_ist = u_date.astimezone(ist)
                        if u_date_ist < dt_start_ist:
                            should_stop = True
                    except: pass

                # Check Created At for filtering
                c_date_str = attributes.get('created_at')
                if c_date_str:
                    try:
                        c_date = datetime.fromisoformat(c_date_str.replace('Z', '+00:00'))
                        c_date_ist = c_date.astimezone(ist)
                        
                        if dt_start_ist <= c_date_ist <= dt_end_ist:
                            all_customers.append(c)
                    except: pass
            
            if should_stop:
                break
            
            links = data.get('links', {})
            next_url = links.get('next')
            page_count += 1
            
        except: break

    return len(all_customers), all_customers, total_global_count

@st.cache_data(ttl=600, show_spinner=False)
def get_kajabi_active_customers(start_date):
    """
    Fetch counts of active customers based on 'last_request_at'.
    Since API sorts by 'updated_at' desc, we fetch until updated_at < start_date.
    """
    token = get_kajabi_token()
    if not token: return 0, 0
    
    url = "https://api.kajabi.com/v1/customers"
    headers = {"Authorization": f"Bearer {token}"}
    
    ist = pytz.timezone('Asia/Kolkata')
    dt_limit = datetime.combine(start_date, datetime.min.time())
    dt_limit_ist = ist.localize(dt_limit)
    
    total_customers = 0
    active_count = 0
    next_url = f"{url}?limit=100"
    page = 0
    
    while next_url and page < 50:
        try:
            resp = requests.get(next_url, headers=headers)
            if resp.status_code != 200: break
            data = resp.json()
            
            if page == 0:
                total_customers = data.get('meta', {}).get('total', 0)
                
            batch = data.get('data', [])
            if not batch: break
            
            stop_fetching = False
            for c in batch:
                attrs = c.get('attributes', {})
                
                # Check updated_at for stopping
                u_str = attrs.get('updated_at')
                if u_str:
                    u_date = datetime.fromisoformat(u_str.replace('Z', '+00:00'))
                    u_date_ist = u_date.astimezone(ist)
                    if u_date_ist < dt_limit_ist:
                        stop_fetching = True
                
                # Check last_request_at for counting
                lr_str = attrs.get('last_request_at')
                if lr_str:
                    lr_date = datetime.fromisoformat(lr_str.replace('Z', '+00:00'))
                    lr_date_ist = lr_date.astimezone(ist)
                    if lr_date_ist >= dt_limit_ist:
                        active_count += 1
            
            if stop_fetching:
                break
                
            links = data.get('links', {})
            next_url = links.get('next')
            page += 1
        except: break
        
    return active_count, total_customers

@st.cache_data(ttl=600, show_spinner=False)
def get_kajabi_sales(target_month_start, target_month_end):
    """Fetch Kajabi purchases for revenue calculation"""
    token = get_kajabi_token()
    if not token:
        return 0.0, []

    url = "https://api.kajabi.com/v1/purchases"
    headers = {"Authorization": f"Bearer {token}"}
    
    ist = pytz.timezone('Asia/Kolkata')
    dt_start = datetime.combine(target_month_start, datetime.min.time())
    dt_start_ist = ist.localize(dt_start)
    dt_end = datetime.combine(target_month_end, datetime.max.time())
    dt_end_ist = ist.localize(dt_end)
    
    filtered_purchases = []
    revenue = 0.0
    
    next_url = f"{url}?limit=100"
    max_pages = 50 # Increased limit allowed due to optimization
    page_count = 0
    
    while next_url and page_count < max_pages:
        try:
            resp = requests.get(next_url, headers=headers)
            if resp.status_code != 200: break
            data = resp.json()
            batch = data.get('data', [])
            if not batch: break
            
            should_stop = False
            
            for p in batch:
                attrs = p.get('attributes', {})
                p_date_str = attrs.get('created_at')
                if p_date_str:
                    try:
                        p_date = datetime.fromisoformat(p_date_str.replace('Z', '+00:00'))
                        p_date_ist = p_date.astimezone(ist)
                        
                        # OPTIMIZATION: Purchases are sorted by created_at DESC.
                        # If we see a purchase older than start_date, we can STOP ALL FETCHING.
                        if p_date_ist < dt_start_ist:
                            should_stop = True
                            continue # Check next msg? No, just stop processing this batch validly, then break.
                            # Actually, if we hit one older, all subsequent in this batch and next pages are older.
                        
                        if dt_start_ist <= p_date_ist <= dt_end_ist:
                            amount = float(attrs.get('amount_in_cents', 0) or 0) / 100
                            revenue += amount
                            p['parsed_date'] = p_date_ist 
                            p['amount_val'] = amount
                            filtered_purchases.append(p)
                    except: pass
            
            if should_stop:
                break
            
            links = data.get('links', {})
            next_url = links.get('next')
            page_count += 1
        except: break
            
    return revenue, filtered_purchases

@st.cache_data(ttl=3600, show_spinner=False)
def get_kajabi_offers():
    """Fetch Kajabi offers to map ID to Title"""
    token = get_kajabi_token()
    if not token: return {}
    
    url = "https://api.kajabi.com/v1/offers"
    headers = {"Authorization": f"Bearer {token}"}
    
    offers_map = {}
    next_url = f"{url}?limit=100"
    
    while next_url:
        try:
            resp = requests.get(next_url, headers=headers)
            if resp.status_code != 200: break
            data = resp.json()
            batch = data.get('data', [])
            if not batch: break
            
            for item in batch:
                offers_map[item['id']] = item['attributes']['title']
                
            links = data.get('links', {})
            next_url = links.get('next')
        except: break
        
    return offers_map

@st.cache_data(ttl=3600, show_spinner=False)
def get_kajabi_products():
    """Fetch Kajabi products for course-wise stats"""
    token = get_kajabi_token()
    if not token: return []
    
    url = "https://api.kajabi.com/v1/products"
    headers = {"Authorization": f"Bearer {token}"}
    
    all_products = []
    next_url = f"{url}?limit=100"
    
    while next_url:
        try:
            resp = requests.get(next_url, headers=headers)
            if resp.status_code != 200: break
            data = resp.json()
            batch = data.get('data', [])
            if not batch: break
            
            for prod in batch:
                attrs = prod.get('attributes', {})
                all_products.append({
                    "Name": attrs.get('title', 'Unknown'),
                    "Members": attrs.get('members_aggregate_count', 0),
                    "Type": attrs.get('product_type_name', 'Course')
                })
                
            links = data.get('links', {})
            next_url = links.get('next')
        except: break
        
    return all_products

with tabs[3]: 
    st.markdown("### 🛠️ Use Phase (Kajabi)")
    
    # Fetch Data
    m1_count, m1_list, m1_total = get_kajabi_new_customers(date_m1_start, date_m1_end)
    m2_count, m2_list, m2_total = get_kajabi_new_customers(date_m2_start, date_m2_end)
    
    # Fetch Sales
    rev_m1, sales_m1 = get_kajabi_sales(date_m1_start, date_m1_end)
    rev_m2, sales_m2 = get_kajabi_sales(date_m2_start, date_m2_end)
    
    # Fetch Products
    products = get_kajabi_products()
    
    delta = m1_count - m2_count
    rev_delta = rev_m1 - rev_m2
    
    # Fetch Active Customers (MTD)
    active_mtd, total_cust = get_kajabi_active_customers(date_m1_start)

    kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
    with kpi1:
        st.metric("Total Customers", f"{m1_total:,}", help="Total Paying/Granted Customers")
    with kpi2:
        st.metric("New Customers (MTD)", f"{m1_count}", f"{delta:+}")
    with kpi3:
        st.metric("Prev Month Customers", f"{m2_count}")
    with kpi4:
        st.metric("Total Revenue (MTD)", f"${rev_m1:,.2f}", f"${rev_delta:,.2f}")
    with kpi5:
         st.metric("Active Learners", f"{active_mtd:,}", help="Users with activity this month")

    st.markdown("---")
    
    # Fetch Offers for MTD mapping
    offers_map = get_kajabi_offers()

    st.markdown("### 📦 Top Courses")
    
    tab_total, tab_mtd = st.tabs(["All Time", "This Month"])
    
    with tab_total:
        if products:
            df_prod = pd.DataFrame(products)
            df_prod = df_prod.sort_values(by="Members", ascending=False).head(10)
            st.dataframe(
                df_prod[["Name", "Members"]], 
                hide_index=True,
                column_config={
                    "Name": st.column_config.TextColumn("Course"),
                    "Members": st.column_config.ProgressColumn("Total Students", format="%d", min_value=0, max_value=max(df_prod["Members"]))
                }
            )
        else:
            st.info("No product data.")
            
    with tab_mtd:
        # Calculate from New Members (m1_list) -> Offers
        # This captures both Payments AND Free Grants
        offer_counts = {}
        
        if m1_list:
            for c in m1_list:
                # Check relationships -> offers -> data
                offers_data = c.get('relationships', {}).get('offers', {}).get('data', [])
                for o_item in offers_data:
                    oid = o_item.get('id')
                    if oid:
                        name = offers_map.get(oid, f"Offer {oid}")
                        offer_counts[name] = offer_counts.get(name, 0) + 1
        
        if offer_counts:
            df_mtd = pd.DataFrame(list(offer_counts.items()), columns=["Course", "Enrollments"])
            df_mtd = df_mtd.sort_values(by="Enrollments", ascending=False).head(10)
            st.dataframe(
                df_mtd,
                hide_index=True,
                column_config={
                    "Course": st.column_config.TextColumn("Course"),
                    "Enrollments": st.column_config.ProgressColumn("New Students", format="%d", min_value=0, max_value=int(df_mtd["Enrollments"].max()))
                }
            )
        else:
            st.info("No enrollments this month.")

    st.caption(f"Data Source: Kajabi API > Contacts & Purchases")
# --- TAB 5: RENEW (HubSpot Contacts) ---
# --- TAB 5: RENEW (Google Sheet Data) ---
# Replace this with your Web App URL from the deployment step
RENEW_SHEET_URL = "https://script.google.com/macros/s/AKfycbzBauk14TmX8S8FdsUTWqjUoF3_o3FE66rA2EkMQOCjgofa8avMa2U8dYF8al4B9A/exec" 

@st.cache_data(ttl=600, show_spinner=False)
def get_renew_sheet_data(target_month_start, target_month_end):
    if "script.google.com" not in RENEW_SHEET_URL:
        return 0, 0, pd.DataFrame() # Placeholder until user adds URL
        
    try:
        response = requests.get(RENEW_SHEET_URL)
        data = response.json()
        df = pd.DataFrame(data)
        
        # Ensure numeric fee
        df['Fee Amount'] = pd.to_numeric(df['Fee Amount'], errors='coerce').fillna(0)
        
        # Parse Dates
        df['Payment Paid Date'] = pd.to_datetime(df['Payment Paid Date'], errors='coerce').dt.date
        
        # Filter MTD
        mask = (df['Payment Paid Date'] >= target_month_start) & (df['Payment Paid Date'] <= target_month_end)
        df_filtered = df.loc[mask]
        
        total_rev = df_filtered['Fee Amount'].sum()
        count = len(df_filtered)
        
        return count, total_rev, df_filtered
    except Exception as e:
        st.error(f"Error fetching sheet: {e}")
        return 0, 0, pd.DataFrame()

with tabs[4]: 
    st.markdown("### 🔄 Retention & Renewals (Google Sheet)")
    
    # Input for URL if placeholder
    if "AKfycbx" in RENEW_SHEET_URL:
        new_url = st.text_input("Paste your Google Script Web App URL here:", key="sheet_url_input")
        if new_url:
            RENEW_SHEET_URL = new_url # Helper for session
            
    m1_count, m1_val, m1_df = get_renew_sheet_data(date_m1_start, date_m1_end)
    m2_count, m2_val, _ = get_renew_sheet_data(date_m2_start, date_m2_end)
    
    delta_count = m1_count - m2_count
    delta_val = m1_val - m2_val
    val_pct = (delta_val / m2_val * 100) if m2_val > 0 else 0
    
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    with kpi1:
        st.metric("Renewals (MTD)", f"{m1_count}", f"{delta_count:+}")
    with kpi2:
        st.metric("Previous MTD Count", f"{m2_count}")
    with kpi3:
        st.metric("Renewal Revenue", f"₹{m1_val:,.0f}", f"{val_pct:+.1f}%")
    with kpi4:
        st.metric("Previous MTD Revenue", f"₹{m2_val:,.0f}")
        
    st.markdown("---")
    
    if not m1_df.empty:
        st.markdown("**Recent Renewals**")
        st.dataframe(
            m1_df[["Student Name", "Course", "Lead Owner", "Fee Amount", "Payment Paid Date"]],
            hide_index=True,
            use_container_width=True
        )
    else:
        st.info("No renewals found for this period (or Check URL).")

with tabs[5]: show_placeholder("Advocate", "❤️", "NPS & Referral Data Pending")

# Sidebar - Minimal Footer
st.sidebar.markdown("---")
if st.sidebar.button("↻ Clear Cache"):
    st.cache_data.clear()
    st.rerun()
st.sidebar.caption(f"Last updated: {datetime.now().strftime('%H:%M:%S')}")


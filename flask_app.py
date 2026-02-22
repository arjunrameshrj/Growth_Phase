import os
import toml
import logging
from datetime import datetime, timedelta
import pandas as pd
from flask import Flask, render_template, request, jsonify
from flask_caching import Cache
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Metric, Dimension, RunReportRequest, RunRealtimeReportRequest, OrderBy
from google.oauth2 import service_account
import requests
import pytz
import concurrent.futures

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
cache = Cache(app, config={'CACHE_TYPE': 'SimpleCache'})

# ---------------- CONFIGURATION ----------------
PROPERTY_ID = "281698779"
KEY_PATH = "ga4-streamlit-connect-21d2d2cc35d6.json"
SECRETS_PATH = ".streamlit/secrets.toml"
CONTENT_CALENDAR_URL = "https://script.google.com/macros/s/AKfycbwkHVKmSn9MjPUD3xGIYKS8YEyyNlu8qfYq0-dhpey8XIoGpnm3IJroUiAbyPVEdx-T0w/exec"

# Load Secrets
try:
    if os.path.exists(SECRETS_PATH):
        secrets = toml.load(SECRETS_PATH)
    else:
        # Fallback to reconstructing secrets from Environment Variables (for AWS)
        secrets = {
            "kajabi": {
                "client_id": os.environ.get("KAJABI_CLIENT_ID"),
                "client_secret": os.environ.get("KAJABI_CLIENT_SECRET")
            },
            "hubspot": {
                "token": os.environ.get("HUBSPOT_TOKEN")
            },
            "ga4": {
                "property_id": os.environ.get("GA4_PROPERTY_ID", PROPERTY_ID)
            },
            "ga4_key": {
                "type": os.environ.get("GA4_TYPE"),
                "project_id": os.environ.get("GA4_PROJECT_ID"),
                "private_key_id": os.environ.get("GA4_PRIVATE_KEY_ID"),
                "private_key": os.environ.get("GA4_PRIVATE_KEY", "").replace("\\n", "\n"),
                "client_email": os.environ.get("GA4_CLIENT_EMAIL"),
                "client_id": os.environ.get("GA4_CLIENT_ID"),
                "auth_uri": os.environ.get("GA4_AUTH_URI"),
                "token_uri": os.environ.get("GA4_TOKEN_URI"),
                "auth_provider_x509_cert_url": os.environ.get("GA4_AUTH_CERT_URL"),
                "client_x509_cert_url": os.environ.get("GA4_CLIENT_CERT_URL")
            }
        }
except Exception as e:
    logger.error(f"Failed to load secrets: {e}")
    secrets = {}

# ---------------- AUTHENTICATION ----------------
def get_ga4_client():
    if os.path.exists(KEY_PATH):
        try:
            credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
            return BetaAnalyticsDataClient(credentials=credentials)
        except Exception as e:
            logger.error(f"Local key error: {e}")

    if "ga4_key" in secrets:
        try:
            credentials = service_account.Credentials.from_service_account_info(secrets["ga4_key"])
            return BetaAnalyticsDataClient(credentials=credentials)
        except Exception as e:
            logger.error(f"Secrets key error: {e}")
    return None

client = get_ga4_client()

# ---------------- HELPERS ----------------
def calculate_cumulative(daily_data, key="New Users"):
    """Build cumulative sum list for worm graph."""
    if not daily_data:
        return []
    df_c = pd.DataFrame(daily_data)
    if df_c.empty or key not in df_c.columns:
        return []
    df_c['Cumulative'] = df_c[key].cumsum()
    return df_c[['Date', 'Cumulative']].to_dict('records')


def get_comparison_dates(month_offset=0):
    """Return (m1_start, m1_end, m2_start, m2_end, base_date) for a given offset."""
    base_date = datetime.now()
    if month_offset > 0:
        base_date = base_date - timedelta(days=30 * month_offset)

    today = datetime.now()
    month1_start = base_date.replace(day=1)

    if base_date.year == today.year and base_date.month == today.month:
        month1_end = today
    else:
        if base_date.month == 12:
            next_month = base_date.replace(year=base_date.year + 1, month=1, day=1)
        else:
            next_month = base_date.replace(month=base_date.month + 1, day=1)
        month1_end = next_month - timedelta(days=1)

    # Symmetric MTD calculation:
    # If today is Feb 21, month1_start=Feb 1, month1_end=Feb 21 (20 days passed).
    # Then month2_start=Jan 1, month2_end=Jan 21 (exactly same duration).
    days_passed = (month1_end - month1_start).days
    
    last_month_last_day = month1_start - timedelta(days=1)
    month2_start = last_month_last_day.replace(day=1)

    try:
        month2_end = month2_start + timedelta(days=days_passed)
        # Cap at end of previous month if current month has more days
        if month2_end > last_month_last_day:
            month2_end = last_month_last_day
    except Exception:
        month2_end = last_month_last_day

    return month1_start.date(), month1_end.date(), month2_start.date(), month2_end.date(), base_date


def _chart_end_date(end_date_obj, ist):
    """Return end date for chart. Cumulative charts never drop, so always include today."""
    return end_date_obj


# ---------------- GA4 ----------------
@cache.cached(timeout=30, key_prefix='active_users')
def get_active_users():
    if not client:
        return 0
    try:
        req = RunRealtimeReportRequest(
            property=f"properties/{PROPERTY_ID}",
            metrics=[Metric(name="activeUsers")]
        )
        resp = client.run_realtime_report(req, timeout=5)
        return int(resp.rows[0].metric_values[0].value) if resp and resp.rows else 0
    except Exception:
        return 0


@cache.memoize(timeout=600)
def get_discover_metrics(start_date, end_date):
    if not client:
        return 0
    try:
        req = RunReportRequest(
            property=f"properties/{PROPERTY_ID}",
            date_ranges=[DateRange(start_date=str(start_date), end_date=str(end_date))],
            metrics=[Metric(name="newUsers")]
        )
        resp = client.run_report(req)
        return int(resp.rows[0].metric_values[0].value) if resp.rows else 0
    except Exception:
        return 0


@cache.memoize(timeout=600)
def get_daily_new_users(start_date, end_date):
    if not client:
        return []
    try:
        req = RunReportRequest(
            property=f"properties/{PROPERTY_ID}",
            date_ranges=[DateRange(start_date=str(start_date), end_date=str(end_date))],
            dimensions=[Dimension(name="date")],
            metrics=[Metric(name="newUsers")],
            order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"), desc=False)]
        )
        resp = client.run_report(req)
        data = []
        for row in resp.rows:
            d = datetime.strptime(row.dimension_values[0].value, "%Y%m%d").strftime('%Y-%m-%d')
            data.append({"Date": d, "New Users": int(row.metric_values[0].value)})
        return data
    except Exception:
        return []


@cache.memoize(timeout=600)
def get_traffic_sources(start_date, end_date):
    if not client:
        return pd.DataFrame()
    try:
        req = RunReportRequest(
            property=f"properties/{PROPERTY_ID}",
            date_ranges=[DateRange(start_date=str(start_date), end_date=str(end_date))],
            dimensions=[Dimension(name="sessionSourceMedium")],
            metrics=[Metric(name="newUsers")],
            order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="newUsers"), desc=True)],
            limit=8
        )
        resp = client.run_report(req)
        data = [{"Channel": r.dimension_values[0].value, "New Users": int(r.metric_values[0].value)} for r in resp.rows]
        return pd.DataFrame(data)
    except Exception:
        return pd.DataFrame()


# ---------------- HUBSPOT ----------------
def get_hubspot_token():
    try:
        return secrets["hubspot"]["token"]
    except Exception:
        return None


EXCLUDED_OWNERS = ["Mahalekshmi M J", "Sreeja Anoop", "arya.krishnan", "Devi Krishna"]

@cache.memoize(timeout=3600)
def fetch_hubspot_owners():
    """Fetch HubSpot owner mapping IDs to names."""
    token = get_hubspot_token()
    if not token:
        return {}
    
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url = "https://api.hubapi.com/crm/v3/owners"
    mapping = {}
    
    try:
        while True:
            response = requests.get(url, headers=headers, params={"limit": 100}, timeout=15)
            if response.status_code == 200:
                data = response.json()
                for owner in data.get("results", []):
                    o_id = str(owner.get("id"))
                    fn = owner.get("firstName", "")
                    ln = owner.get("lastName", "")
                    email = owner.get("email", "")
                    name = f"{fn} {ln}".strip() if fn or ln else email.split("@")[0] if email else f"Owner {o_id}"
                    mapping[o_id] = name
                
                paging = data.get("paging", {})
                next_link = paging.get("next", {}).get("link")
                if not next_link:
                    break
                url = next_link
            else:
                break
    except Exception as e:
        print(f"Error fetching owners: {e}")
    return mapping

@cache.memoize(timeout=3600)  # Cache pipeline stages for 1 hour
def fetch_deal_pipeline_stages():
    """Fetch deal pipeline stages to get correct stage IDs."""
    token = get_hubspot_token()
    if not token:
        return {}
    
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url = "https://api.hubapi.com/crm/v3/pipelines/deals"
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            pipelines = data.get("results", [])
            all_stages = {}
            for pipeline in pipelines:
                p_id = pipeline.get("id")
                p_label = pipeline.get("label")
                for stage in pipeline.get("stages", []):
                    s_id = stage.get("id")
                    s_label = stage.get("label")
                    prob = stage.get("metadata", {}).get("probability", "0")
                    all_stages[s_id] = {
                        "id": s_id,
                        "label": s_label,
                        "pipeline_id": p_id,
                        "pipeline_label": p_label,
                        "probability": prob
                    }
            return all_stages
    except Exception as e:
        print(f"Error fetching pipelines: {e}")
    return {}

def get_customer_stage_ids():
    """Detect stages that represent 'Admission Confirmed' or 'Closed Won'."""
    all_stages = fetch_deal_pipeline_stages()
    if not all_stages:
        # Fallback to hardcoded defaults if API fails
        return ["closedwon", "1884422889", "2208152296", "1955461879", "contractsent"]
    
    target_labels = ["admission confirmed", "confirmed", "closed won", "won", "customer"]
    detected = []
    
    # Explicitly include known success IDs
    for s_id in ['closedwon', '1884422889']:
        if s_id in all_stages:
            detected.append(s_id)
            
    # Pattern match for others with high probability
    for s_id, info in all_stages.items():
        if s_id in detected: continue
        label = info['label'].lower()
        try:
            prob = float(info['probability'])
            if any(t in label for t in target_labels) and prob >= 0.9:
                detected.append(s_id)
        except:
            if any(t in label for t in target_labels):
                detected.append(s_id)
                
    return list(set(detected)) if detected else ["closedwon", "1884422889"]

def get_hubspot_deals(target_month_start, target_month_end):
    """
    Fetch closed/won HubSpot deals in the date range.
    Uses dynamic stage detection from ff.txt logic.
    Returns (count, total_revenue, daily_trend_list).
    """
    token = get_hubspot_token()
    if not token:
        return 0, 0, []

    # Dynamically detect customer stages
    customer_stages = get_customer_stage_ids()
    owner_map = fetch_hubspot_owners()
    
    url = "https://api.hubapi.com/crm/v3/objects/deals/search"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    ist = pytz.timezone('Asia/Kolkata')
    # Use helper to get proper timestamps (v2 logic)
    dt_start = datetime.combine(target_month_start, datetime.min.time())
    dt_start_ist = ist.localize(dt_start)
    start_ts = int(dt_start_ist.astimezone(pytz.UTC).timestamp() * 1000)

    dt_end = datetime.combine(target_month_end, datetime.max.time())
    dt_end_ist = ist.localize(dt_end)
    end_ts = int(dt_end_ist.astimezone(pytz.UTC).timestamp() * 1000)

    all_results = []
    after = None

    while True:
        body = {
            "filterGroups": [{
                "filters": [
                    {"propertyName": "dealstage", "operator": "IN", "values": customer_stages},
                    {"propertyName": "closedate", "operator": "BETWEEN", "value": start_ts, "highValue": end_ts}
                ]
            }],
            "properties": ["amount", "closedate", "dealstage", "dealname", "hubspot_owner_id"],
            "limit": 100
        }
        if after:
            body['after'] = after

        try:
            response = requests.post(url, headers=headers, json=body, timeout=20)
            if response.status_code == 200:
                data = response.json()
                results = data.get('results', [])
                
                # Filter out EXCLUDED_OWNERS here
                for r in results:
                    o_id = r['properties'].get('hubspot_owner_id')
                    o_name = owner_map.get(o_id, "")
                    if o_name in EXCLUDED_OWNERS:
                        continue
                    all_results.append(r)

                paging = data.get('paging', {})
                after = paging.get('next', {}).get('after')
                if not after:
                    break
            else:
                break
        except Exception:
            break

    # Robust summing logic
    total_val = 0
    for r in all_results:
        amt_str = r['properties'].get('amount')
        if amt_str:
            try:
                total_val += float(str(amt_str).replace(",", ""))
            except:
                pass

    # Build daily counts for worm graph
    daily_counts = {}
    for r in all_results:
        try:
            close_str = r['properties'].get('closedate')
            if not close_str:
                continue
            # Handle both ISO and timestamp formats if they appear
            if close_str.isdigit():
                close_dt = datetime.fromtimestamp(int(close_str)/1000, tz=pytz.UTC)
            else:
                close_dt = datetime.fromisoformat(close_str.replace('Z', '+00:00'))
            
            date_str = close_dt.astimezone(ist).strftime('%Y-%m-%d')
            daily_counts[date_str] = daily_counts.get(date_str, 0) + 1
        except Exception:
            continue

    # Fill date range (clip today for cleaner chart)
    chart_end = _chart_end_date(dt_end_ist.date(), ist)
    sorted_daily = []
    curr = dt_start_ist.date()
    while curr <= chart_end:
        d_str = curr.strftime('%Y-%m-%d')
        sorted_daily.append({"Date": d_str, "Count": daily_counts.get(d_str, 0)})
        curr += timedelta(days=1)

    return len(all_results), total_val, sorted_daily


# ---------------- KAJABI ----------------
KAJABI_CLIENT_ID = secrets.get("kajabi", {}).get("client_id")
KAJABI_CLIENT_SECRET = secrets.get("kajabi", {}).get("client_secret")


@cache.memoize(timeout=3500)
def get_kajabi_token():
    url = "https://api.kajabi.com/v1/oauth/token"
    try:
        response = requests.post(
            url,
            data={
                "grant_type": "client_credentials",
                "client_id": KAJABI_CLIENT_ID,
                "client_secret": KAJABI_CLIENT_SECRET
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=10
        )
        if response.status_code == 200:
            return response.json().get('access_token')
        return None
    except Exception:
        return None


@cache.memoize(timeout=600)
def get_kajabi_new_customers(target_month_start, target_month_end):
    """
    Fetch Kajabi customers created in the date range.
    Returns (count, customer_list, total_global_count, daily_trend_list).
    """
    token = get_kajabi_token()
    if not token:
        return 0, [], 0, []

    url = "https://api.kajabi.com/v1/customers"
    headers = {"Authorization": f"Bearer {token}"}
    ist = pytz.timezone('Asia/Kolkata')

    dt_start_ist = ist.localize(datetime.combine(target_month_start, datetime.min.time()))
    dt_end_ist = ist.localize(datetime.combine(target_month_end, datetime.max.time()))

    all_customers = []
    next_url = f"{url}?limit=100&include=offers"
    page_count = 0
    total_global = 0

    while next_url and page_count < 50:
        try:
            resp = requests.get(next_url, headers=headers, timeout=15)
            if resp.status_code != 200:
                break
            data = resp.json()
            if page_count == 0:
                total_global = data.get('meta', {}).get('total', 0)

            batch = data.get('data', [])
            if not batch:
                break

            should_stop = False
            for c in batch:
                attrs = c.get('attributes', {})
                u_str = attrs.get('updated_at')
                if u_str:
                    try:
                        u_date = datetime.fromisoformat(u_str.replace('Z', '+00:00')).astimezone(ist)
                        if u_date < dt_start_ist:
                            should_stop = True
                    except Exception:
                        pass

                c_date_str = attrs.get('created_at')
                if c_date_str:
                    try:
                        c_date = datetime.fromisoformat(c_date_str.replace('Z', '+00:00')).astimezone(ist)
                        if dt_start_ist <= c_date <= dt_end_ist:
                            all_customers.append(c)
                    except Exception:
                        pass

            if should_stop:
                break
            next_url = data.get('links', {}).get('next')
            page_count += 1
        except Exception:
            break

    # Build daily trend
    daily_counts = {}
    for c in all_customers:
        try:
            c_date_str = c['attributes'].get('created_at')
            if c_date_str:
                c_date = datetime.fromisoformat(c_date_str.replace('Z', '+00:00')).astimezone(ist)
                d_str = c_date.strftime('%Y-%m-%d')
                daily_counts[d_str] = daily_counts.get(d_str, 0) + 1
        except Exception:
            continue

    chart_end = _chart_end_date(dt_end_ist.date(), ist)
    sorted_daily = []
    curr = dt_start_ist.date()
    while curr <= chart_end:
        d_str = curr.strftime('%Y-%m-%d')
        sorted_daily.append({"Date": d_str, "Count": daily_counts.get(d_str, 0)})
        curr += timedelta(days=1)

    return len(all_customers), all_customers, total_global, sorted_daily


@cache.memoize(timeout=600)
def get_kajabi_sales(target_month_start, target_month_end):
    """Fetch Kajabi purchases and return (total_revenue, purchase_list)."""
    token = get_kajabi_token()
    if not token:
        return 0.0, []

    url = "https://api.kajabi.com/v1/purchases"
    headers = {"Authorization": f"Bearer {token}"}
    ist = pytz.timezone('Asia/Kolkata')

    dt_start_ist = ist.localize(datetime.combine(target_month_start, datetime.min.time()))
    dt_end_ist = ist.localize(datetime.combine(target_month_end, datetime.max.time()))

    revenue = 0.0
    filtered = []
    next_url = f"{url}?limit=100"
    page_count = 0

    while next_url and page_count < 50:
        try:
            resp = requests.get(next_url, headers=headers, timeout=15)
            if resp.status_code != 200:
                break
            data = resp.json()
            batch = data.get('data', [])
            if not batch:
                break

            should_stop = False
            for p in batch:
                attrs = p.get('attributes', {})
                p_date_str = attrs.get('created_at')
                if p_date_str:
                    try:
                        p_date = datetime.fromisoformat(p_date_str.replace('Z', '+00:00')).astimezone(ist)
                        if p_date < dt_start_ist:
                            should_stop = True
                            continue
                        if dt_start_ist <= p_date <= dt_end_ist:
                            amt = float(attrs.get('amount_in_cents', 0) or 0) / 100
                            revenue += amt
                            filtered.append(p)
                    except Exception:
                        pass

            if should_stop:
                break
            next_url = data.get('links', {}).get('next')
            page_count += 1
        except Exception:
            break

    return revenue, filtered


@cache.memoize(timeout=3600)
def get_kajabi_products():
    """Fetch all Kajabi products with member counts."""
    token = get_kajabi_token()
    if not token:
        return []
    try:
        url = "https://api.kajabi.com/v1/products"
        headers = {"Authorization": f"Bearer {token}"}
        all_products = []
        next_url = f"{url}?limit=100"
        while next_url:
            resp = requests.get(next_url, headers=headers, timeout=15)
            if resp.status_code != 200:
                break
            data = resp.json()
            for prod in data.get('data', []):
                attrs = prod.get('attributes', {})
                all_products.append({
                    "Name": attrs.get('title', 'Unknown'),
                    "Members": attrs.get('members_aggregate_count', 0)
                })
            next_url = data.get('links', {}).get('next')
        return all_products
    except Exception:
        return []


@cache.memoize(timeout=600)
def get_kajabi_active_customers(start_date):
    """Return (active_count, total_customers) where active = logged in since start_date."""
    token = get_kajabi_token()
    if not token:
        return 0, 0

    url = "https://api.kajabi.com/v1/customers"
    headers = {"Authorization": f"Bearer {token}"}
    ist = pytz.timezone('Asia/Kolkata')
    dt_limit_ist = ist.localize(datetime.combine(start_date, datetime.min.time()))

    active_count = 0
    total_customers = 0
    next_url = f"{url}?limit=100"
    page = 0

    while next_url and page < 50:
        try:
            resp = requests.get(next_url, headers=headers, timeout=15)
            if resp.status_code != 200:
                break
            data = resp.json()
            if page == 0:
                total_customers = data.get('meta', {}).get('total', 0)

            batch = data.get('data', [])
            if not batch:
                break

            stop_fetching = False
            for c in batch:
                attrs = c.get('attributes', {})
                u_str = attrs.get('updated_at')
                if u_str:
                    try:
                        u_date = datetime.fromisoformat(u_str.replace('Z', '+00:00')).astimezone(ist)
                        if u_date < dt_limit_ist:
                            stop_fetching = True
                    except Exception:
                        pass

                lr_str = attrs.get('last_request_at')
                if lr_str:
                    try:
                        lr_date = datetime.fromisoformat(lr_str.replace('Z', '+00:00')).astimezone(ist)
                        if lr_date >= dt_limit_ist:
                            active_count += 1
                    except Exception:
                        pass

            if stop_fetching:
                break
            next_url = data.get('links', {}).get('next')
            page += 1
        except Exception:
            break

    return active_count, total_customers


@cache.memoize(timeout=3600)
def get_kajabi_offers():
    """Fetch all Kajabi offers and return {offer_id: offer_title} map."""
    token = get_kajabi_token()
    if not token:
        return {}
    try:
        url = "https://api.kajabi.com/v1/offers"
        headers = {"Authorization": f"Bearer {token}"}
        offers_map = {}
        next_url = f"{url}?limit=100"
        while next_url:
            resp = requests.get(next_url, headers=headers, timeout=15)
            if resp.status_code != 200:
                break
            data = resp.json()
            for item in data.get('data', []):
                offers_map[item['id']] = item['attributes']['title']
            next_url = data.get('links', {}).get('next')
        return offers_map
    except Exception:
        return {}


# ---------------- GOOGLE SHEETS (RENEW) ----------------
RENEW_SHEET_URL = "https://script.google.com/macros/s/AKfycbyid7u5OIJbemqEyawmvRsJyF6XmplsjNw-u9DqDuI7dm59hxSuykJOk2Yeeyc5riDtfg/exec"


@cache.memoize(timeout=600)
def get_renew_sheet_data(target_month_start, target_month_end):
    """Fetch renewal data from Google Sheets. Returns (count, total_revenue, dataframe)."""
    if not RENEW_SHEET_URL or "script.google.com" not in RENEW_SHEET_URL:
        return 0, 0, pd.DataFrame()
    try:
        response = requests.get(RENEW_SHEET_URL, timeout=15)
        data = response.json()
        df = pd.DataFrame(data)
        
        # New Column structure handling
        date_col = 'Payment Paid Date/ Initial Amount Paid Date'
        fee_col = 'Fee Amount'
        
        if fee_col in df.columns:
            df[fee_col] = pd.to_numeric(df[fee_col], errors='coerce').fillna(0)
        else:
            df[fee_col] = 0
            
        if date_col in df.columns:
            # Shift UTC to IST correctly
            # We remove dayfirst=True because it can conflict with ISO8601 strings
            # and explicitly convert to string and strip just in case
            dt_series = pd.to_datetime(df[date_col].astype(str).str.strip(), errors='coerce', utc=True)
            df['Parsed Date'] = dt_series.dt.tz_convert('Asia/Kolkata').dt.date
            
            mask = (df['Parsed Date'] >= target_month_start) & (df['Parsed Date'] <= target_month_end)
            df_filtered = df.loc[mask]
            
            # Final verification log
            nimmy_list = df[df['Student Name'].astype(str).str.contains('NIMMY', case=False)]
            if not nimmy_list.empty:
                logger.info(f"NIMMY Final Check: {nimmy_list[['Student Name', 'Parsed Date']].to_dict('records')}")

            logger.info(f"Renew filtered rows: {len(df_filtered)} for range {target_month_start} to {target_month_end}")
            return len(df_filtered), df_filtered[fee_col].sum(), df_filtered
        return 0, 0, pd.DataFrame()
    except Exception as e:
        logger.error(f"Renew sheet fetch error: {e}")
        return 0, 0, pd.DataFrame()


# ---------------- FLASK ROUTES ----------------
@app.route('/')
def index():
    offset = request.args.get('offset', 0, type=int)
    m1_start, m1_end, m2_start, m2_end, date_obj = get_comparison_dates(offset)
    return render_template(
        'index.html',
        active_users='—',
        current_month=date_obj.strftime('%B %Y'),
        mtd_range=f"{m1_start.strftime('%b %d')} - {m1_end.strftime('%b %d')}",
        prev_range=f"{m2_start.strftime('%b %d')} - {m2_end.strftime('%b %d')}",
        month_offset=offset
    )


@app.route('/api/active-users')
@cache.cached(timeout=30)
def api_active_users():
    return jsonify({"count": get_active_users()})


@app.route('/api/discover')
@cache.cached(timeout=300, query_string=True)
def api_discover():
    offset = request.args.get('offset', 0, type=int)
    m1_start, m1_end, m2_start, m2_end, _ = get_comparison_dates(offset)
    m2_full_end = m1_start - timedelta(days=1)  # Full previous month for worm graph

    with concurrent.futures.ThreadPoolExecutor() as executor:
        f_m1 = executor.submit(get_discover_metrics, m1_start, m1_end)
        f_m2 = executor.submit(get_discover_metrics, m2_start, m2_end)
        f_trend = executor.submit(get_daily_new_users, m1_start, m1_end)
        f_trend_prev = executor.submit(get_daily_new_users, m2_start, m2_full_end)

    m1_val = f_m1.result()
    m2_val = f_m2.result()
    trend = f_trend.result()
    trend_prev = f_trend_prev.result()
    sources = get_traffic_sources(m1_start, m1_end)

    delta = m1_val - m2_val
    pct = (delta / m2_val * 100) if m2_val > 0 else 0

    return jsonify({
        "m1_val": f"{m1_val:,}",
        "m2_val": f"{m2_val:,}",
        "delta_pct": pct,
        "trend": trend,
        "worm_m1": calculate_cumulative(trend, "New Users"),
        "worm_m2": calculate_cumulative(trend_prev, "New Users"),
        "sources": sources.to_dict('records')
    })


@app.route('/api/buy')
@cache.cached(timeout=300, query_string=True)
def api_buy():
    offset = request.args.get('offset', 0, type=int)
    m1_start, m1_end, m2_start, m2_end, _ = get_comparison_dates(offset)
    m2_full_end = m1_start - timedelta(days=1)  # Full previous month end for worm graph

    with concurrent.futures.ThreadPoolExecutor() as executor:
        f_m1      = executor.submit(get_hubspot_deals, m1_start, m1_end)
        f_m2      = executor.submit(get_hubspot_deals, m2_start, m2_end)          # MTD → KPI
        f_m2_full = executor.submit(get_hubspot_deals, m2_start, m2_full_end)     # Full month → graph

    m1_count, m1_val, m1_trend = f_m1.result()
    m2_count, m2_val, _        = f_m2.result()       # KPI only
    _, _, m2_trend             = f_m2_full.result()   # Graph only

    delta_count = m1_count - m2_count
    delta_val = m1_val - m2_val
    pct_val = (delta_val / m2_val * 100) if m2_val > 0 else 0

    return jsonify({
        "m1_count": m1_count,
        "m1_val": f"₹{m1_val:,.0f}",
        "m2_count": m2_count,
        "m2_val": f"₹{m2_val:,.0f}",
        "delta_count": f"{delta_count:+}",
        "pct_val": pct_val,
        "worm_m1": calculate_cumulative(m1_trend, "Count"),
        "worm_m2": calculate_cumulative(m2_trend, "Count")
    })


@app.route('/api/use')
@cache.cached(timeout=300, query_string=True)
def api_use():
    offset = request.args.get('offset', 0, type=int)
    m1_start, m1_end, m2_start, m2_end, _ = get_comparison_dates(offset)
    m2_full_end = m1_start - timedelta(days=1)  # Full previous month end for worm graph

    with concurrent.futures.ThreadPoolExecutor() as executor:
        f_m1_cust = executor.submit(get_kajabi_new_customers, m1_start, m1_end)
        f_m2_cust = executor.submit(get_kajabi_new_customers, m2_start, m2_end)          # MTD count for KPI
        f_m2_cust_full = executor.submit(get_kajabi_new_customers, m2_start, m2_full_end) # Full month for graph
        f_m1_sales = executor.submit(get_kajabi_sales, m1_start, m1_end)
        f_m2_sales = executor.submit(get_kajabi_sales, m2_start, m2_end)
        f_prod = executor.submit(get_kajabi_products)
        f_active = executor.submit(get_kajabi_active_customers, m1_start)
        f_offers = executor.submit(get_kajabi_offers)

    m1_count, m1_list, total, m1_trend = f_m1_cust.result()
    m2_count, _, _, _ = f_m2_cust.result()          # KPI: MTD count only
    _, _, _, m2_trend = f_m2_cust_full.result()      # Graph: full previous month trend
    m1_rev, _ = f_m1_sales.result()
    m2_rev, _ = f_m2_sales.result()
    products = f_prod.result()
    active, _ = f_active.result()
    offers_map = f_offers.result()

    # Build MTD course enrollment counts
    offer_counts = {}
    for c in m1_list:
        for o_item in c.get('relationships', {}).get('offers', {}).get('data', []):
            oid = o_item.get('id')
            if oid:
                name = offers_map.get(oid, f"Offer {oid}")
                offer_counts[name] = offer_counts.get(name, 0) + 1

    mtd_courses = sorted(
        [{"Course": k, "Enrollments": v} for k, v in offer_counts.items()],
        key=lambda x: x['Enrollments'],
        reverse=True
    )[:10]

    delta_count = m1_count - m2_count
    delta_rev = m1_rev - m2_rev

    return jsonify({
        "total_customers": f"{total:,}",
        "m1_new": m1_count,
        "new_delta": f"{delta_count:+}",
        "m2_new": m2_count,
        "m1_rev": f"₹{m1_rev:,.2f}",
        "rev_delta": f"₹{delta_rev:,.2f}",
        "active_learners": f"{active:,}",
        "products": sorted(products, key=lambda x: x['Members'], reverse=True)[:10],
        "mtd_courses": mtd_courses,
        "worm_m1": calculate_cumulative(m1_trend, "Count"),
        "worm_m2": calculate_cumulative(m2_trend, "Count")
    })


@app.route('/api/renew')
@cache.cached(timeout=300, query_string=True)
def api_renew():
    offset = request.args.get('offset', 0, type=int)
    m1_start, m1_end, m2_start, m2_end, _ = get_comparison_dates(offset)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        f_m1 = executor.submit(get_renew_sheet_data, m1_start, m1_end)
        f_m2 = executor.submit(get_renew_sheet_data, m2_start, m2_end)

    m1_count, m1_val, m1_df = f_m1.result()
    m2_count, m2_val, m2_df = f_m2.result()

    delta_count = m1_count - m2_count
    delta_val = m1_val - m2_val
    pct_val = (delta_val / m2_val * 100) if m2_val > 0 else 0

    recent = []
    
    def get_sanitized_breakdown(df, col):
        if df.empty or col not in df.columns:
            return []
        s = df[col].astype(str).str.strip().str.upper()
        counts = s.value_counts().to_dict()
        items = [{"name": str(k), "count": int(v)} for k, v in counts.items()]
        items.sort(key=lambda x: x["count"], reverse=True)
        return items

    m1_courses = get_sanitized_breakdown(m1_df, "Course")
    m2_courses = get_sanitized_breakdown(m2_df, "Course")
    m1_packages = get_sanitized_breakdown(m1_df, "Package")
    m2_packages = get_sanitized_breakdown(m2_df, "Package")

    if not m1_df.empty:
        # Columns: Student Name, Course, Package, Lead Owner, Fee Amount, Paid Amount, Pending Amount, Payment Paid Date/ Initial Amount Paid Date, Pending Paid Date
        cols_to_show = ["Student Name", "Course", "Package", "Lead Owner", "Fee Amount", "Paid Amount", "Pending Amount", "Payment Paid Date/ Initial Amount Paid Date", "Pending Paid Date"]
        # Filter to only existing columns to avoid errors
        available_cols = [c for c in cols_to_show if c in m1_df.columns]
        recent = m1_df[available_cols].to_dict('records')

    return jsonify({
        "m1_count": m1_count,
        "m1_val": f"₹{m1_val:,.0f}",
        "m2_count": m2_count,
        "m2_val": f"₹{m2_val:,.0f}",
        "delta_count": f"{delta_count:+}",
        "pct_val": pct_val,
        "recent_renewals": recent,
        "m1_course_breakdown": m1_courses,
        "m2_course_breakdown": m2_courses,
        "m1_package_breakdown": m1_packages,
        "m2_package_breakdown": m2_packages,
        "v": "2.3"
    })


@app.route('/api/cache-clear', methods=['POST'])
def api_cache_clear():
    cache.clear()
    logger.info("Cache cleared via refresh button")
    return jsonify({"status": "ok"})


# ---------------- CONTENT CALENDAR ----------------
@cache.memoize(timeout=1800)
def get_content_calendar():
    """Fetch all content calendar rows from Google Apps Script."""
    try:
        resp = requests.get(CONTENT_CALENDAR_URL, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"Content calendar fetch error: {e}")
        return []


@app.route('/api/content-calendar')
@cache.cached(timeout=1800, query_string=True)
def api_content_calendar():
    """Return content calendar KPIs filtered to the requested month."""
    offset = request.args.get('offset', 0, type=int)
    ist = pytz.timezone("Asia/Kolkata")
    m1_start, m1_end, _, _, _ = get_comparison_dates(offset)

    all_rows = get_content_calendar()

    # Normalise status values
    STATUS_MAP = {
        "published": "Published",
        "pending":   "Pending",
        "assigned":  "Assigned",
        "":          "Unset",
    }
    FUNNEL_ORDER = ["Awareness", "Consideration", "Conversion", "Retention"]

    rows_this_month = []
    for row in all_rows:
        # Skip rows with no useful data
        if not any([row.get("Content Topic"), row.get("Status"), row.get("Scheduled Date")]):
            continue

        # Prefer Published Date for performance tracking, fall back to Scheduled Date
        date_val = row.get("Published Date") or row.get("Scheduled Date") or ""
        parsed_date = None
        if date_val:
            try:
                # Apps Script returns JS Date strings or direct date strings from sheet
                date_val_clean = date_val.split('(')[0].strip()
                # Use pandas for robust parsing with dayfirst=True for Indian formats
                parsed_date = pd.to_datetime(date_val_clean, errors='coerce', dayfirst=True)
                if pd.isna(parsed_date):
                    parsed_date = None
                else:
                    parsed_date = parsed_date.date()
            except Exception:
                parsed_date = None

        if parsed_date and not (m1_start <= parsed_date <= m1_end):
            continue  # outside requested month

        status_raw = (row.get("Status") or "").strip().lower()
        status = STATUS_MAP.get(status_raw, row.get("Status", "Other").strip())

        funnel_raw = (row.get("Funnel Stage") or "").strip().lower()
        # Normalise funnel stage
        if "awareness" in funnel_raw:
            funnel = "Awareness"
        elif "consideration" in funnel_raw:
            funnel = "Consideration"
        elif "conversion" in funnel_raw:
            funnel = "Conversion"
        elif "retention" in funnel_raw:
            funnel = "Retention"
        else:
            funnel = "Other"

        rows_this_month.append({
            "sheet":       row.get("Sheet", ""),
            "topic":       row.get("Content Topic", ""),
            "type":        row.get("Content Type", ""),
            "owner":       row.get("Owner/TUTOR", ""),
            "status":      status,
            "funnel":      funnel,
            "date":        parsed_date.isoformat() if parsed_date else "",
            "link_yt":     row.get("Link YT", ""),
            "link_insta":  row.get("Link INSTA", ""),
            "link_fb":     row.get("Link FB", ""),
            "remarks":     row.get("REMARKS", ""),
        })

    total      = len(rows_this_month)
    published  = sum(1 for r in rows_this_month if r["status"] == "Published")
    pending    = sum(1 for r in rows_this_month if r["status"] == "Pending")
    assigned   = sum(1 for r in rows_this_month if r["status"] == "Assigned")
    publish_rate = round(published / total * 100) if total > 0 else 0

    # Per-course summary
    course_summary = {}
    for r in rows_this_month:
        s = r["sheet"]
        if s not in course_summary:
            course_summary[s] = {"total": 0, "published": 0, "pending": 0}
        course_summary[s]["total"] += 1
        if r["status"] == "Published":
            course_summary[s]["published"] += 1
        elif r["status"] == "Pending":
            course_summary[s]["pending"] += 1

    # Per-funnel summary
    funnel_summary = {}
    for r in rows_this_month:
        f = r["funnel"]
        funnel_summary[f] = funnel_summary.get(f, 0) + 1

    return jsonify({
        "total":        total,
        "published":    published,
        "pending":      pending,
        "assigned":     assigned,
        "publish_rate": publish_rate,
        "courses":      [{"name": k, **v} for k, v in sorted(course_summary.items())],
        "funnel":       [{"stage": f, "count": funnel_summary.get(f, 0)} for f in FUNNEL_ORDER if f in funnel_summary],
        "rows":         rows_this_month,
    })



import threading

def _warmup():
    """Pre-warm all data caches in background so first browser hit is instant."""
    import time
    time.sleep(3)  # Wait for Flask to fully start
    try:
        m1_start, m1_end, m2_start, m2_end, _ = get_comparison_dates(0)
        m2_full_end = m1_start - timedelta(days=1)
        logger.info("WARMUP: pre-fetching all data sources...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
            # DISCOVER — GA4
            ex.submit(get_discover_metrics, m1_start, m1_end)
            ex.submit(get_discover_metrics, m2_start, m2_end)
            ex.submit(get_daily_new_users, m1_start, m1_end)
            ex.submit(get_daily_new_users, m2_start, m2_full_end)
            ex.submit(get_traffic_sources, m1_start, m1_end)
            # BUY — HubSpot
            ex.submit(get_hubspot_deals, m1_start, m1_end)
            ex.submit(get_hubspot_deals, m2_start, m2_end)
            # USE — Kajabi (ALL calls so USE tab is instant)
            ex.submit(get_kajabi_new_customers, m1_start, m1_end)
            ex.submit(get_kajabi_new_customers, m2_start, m2_end)
            ex.submit(get_kajabi_new_customers, m2_start, m2_full_end)
            ex.submit(get_kajabi_sales, m1_start, m1_end)
            ex.submit(get_kajabi_sales, m2_start, m2_end)
            ex.submit(get_kajabi_products)
            ex.submit(get_kajabi_active_customers, m1_start)
            ex.submit(get_kajabi_offers)
            # RENEW — Google Sheets
            ex.submit(get_renew_sheet_data, m1_start, m1_end)
            ex.submit(get_renew_sheet_data, m2_start, m2_end)
            # Content Calendar
            ex.submit(get_content_calendar)
        logger.info("WARMUP: all caches ready ✓")
    except Exception as e:
        logger.warning(f"WARMUP error (non-fatal): {e}")


if __name__ == '__main__':
    threading.Thread(target=_warmup, daemon=True).start()
    app.run(debug=True, use_reloader=False)


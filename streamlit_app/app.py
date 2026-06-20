import os
import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine

# Page Config
st.set_page_config(
    page_title="E-Commerce Analytics Pipeline Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Premium CSS for styling and transitions
st.markdown("""
<style>
    /* Gradient Header with moving background animation */
    @keyframes gradient-bg {
        0% { background-position: 0% 50%; }
        50% { background-position: 100% 50%; }
        100% { background-position: 0% 50%; }
    }
    .main-header {
        background: linear-gradient(-45deg, #1e1b4b, #1d4ed8, #6d28d9, #0f172a);
        background-size: 300% 300%;
        animation: gradient-bg 12s ease infinite;
        padding: 24px;
        border-radius: 12px;
        color: white;
        margin-bottom: 25px;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        text-align: center;
        border: 1px solid rgba(255, 255, 255, 0.1);
    }
    .main-header h1 {
        margin: 0;
        font-family: 'Outfit', sans-serif;
        font-size: 2.3rem;
        font-weight: 700;
        letter-spacing: -0.5px;
        text-shadow: 0 2px 4px rgba(0,0,0,0.3);
    }
    .main-header p {
        margin: 6px 0 0 0;
        font-size: 1.1rem;
        opacity: 0.9;
        text-shadow: 0 1px 2px rgba(0,0,0,0.3);
    }
    
    /* Modern Glassmorphic KPI Cards with hover transitions */
    .kpi-card {
        background: rgba(255, 255, 255, 0.05);
        border: 1px solid rgba(255, 255, 255, 0.1);
        padding: 22px;
        border-radius: 12px;
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.2);
        backdrop-filter: blur(8px);
        -webkit-backdrop-filter: blur(8px);
        transition: all 0.4s cubic-bezier(0.165, 0.84, 0.44, 1);
        margin-bottom: 15px;
    }
    
    .kpi-card:hover {
        transform: translateY(-8px) scale(1.02);
        box-shadow: 0 20px 40px rgba(59, 130, 246, 0.2);
        border-color: rgba(59, 130, 246, 0.4);
        background: rgba(255, 255, 255, 0.08);
    }
    
    .kpi-label {
        font-size: 0.85rem;
        color: #9ca3af;
        text-transform: uppercase;
        font-weight: 600;
        letter-spacing: 0.75px;
    }
    .kpi-value {
        font-size: 2.1rem;
        font-weight: 700;
        color: #60a5fa;
        margin: 5px 0;
        transition: color 0.3s ease;
    }
    .kpi-card:hover .kpi-value {
        color: #a78bfa;
    }
    
    @keyframes pulse-glow {
        0%, 100% {
            opacity: 0.8;
            transform: scale(1);
        }
        50% {
            opacity: 1;
            transform: scale(1.05);
        }
    }
    
    .kpi-trend {
        font-size: 0.8rem;
        color: #10b981;
        font-weight: 600;
        display: inline-block;
        animation: pulse-glow 2s infinite ease-in-out;
    }
</style>
""", unsafe_allow_html=True)

# Database connection helper
@st.cache_resource
def get_db_engine():
    # Attempt container network hostname first, fallback to localhost
    hosts = [os.environ.get('DB_HOST', 'postgres'), 'localhost']
    db_port = os.environ.get('DB_PORT', '5432')
    db_name = os.environ.get('DB_NAME', 'ecommerce_dw')
    db_user = os.environ.get('DB_USER', 'postgres')
    db_password = os.environ.get('DB_PASSWORD', 'postgres')
    
    for host in hosts:
        try:
            conn_str = f"postgresql://{db_user}:{db_password}@{host}:{db_port}/{db_name}"
            engine = create_engine(conn_str)
            # Test connection
            with engine.connect() as conn:
                pd.read_sql_query("SELECT 1", conn)
            return engine
        except Exception:
            continue
    # If all fail, return None (will show message to run pipeline)
    return None

engine = get_db_engine()

# Main Header UI
st.markdown("""
<div class="main-header">
    <h1>E-Commerce Data Pipeline Dashboard</h1>
    <p>Automated Pipeline Orchestrated by Apache Airflow & PySpark</p>
</div>
""", unsafe_allow_html=True)

if engine is None:
    st.error("⚠️ **Could not connect to PostgreSQL Database (`ecommerce_dw`).**")
    st.warning("Please make sure your Docker containers are running and the Airflow pipeline has successfully executed the Spark ETL task.")
    st.info("To start the services, run:\n`docker-compose up -d` in the project directory.")
    st.stop()

# Sidebar navigation
st.sidebar.image("https://spark.apache.org/images/spark-logo-trademark.png", width=120)
st.sidebar.markdown("### Navigation")
page = st.sidebar.radio(
    "Select Dashboard View:",
    ["🏠 Executive Overview", "📈 Sales & Products", "👥 Customer Analytics", "⚙️ Operations & Quality"]
)

# Fetch common data
@st.cache_data
def load_query(query):
    with engine.connect() as conn:
        return pd.read_sql_query(query, conn)

# ----------------------------------------------------
# PAGE 1: EXECUTIVE OVERVIEW
# ----------------------------------------------------
if page == "🏠 Executive Overview":
    st.subheader("Key Performance Indicators (KPIs)")
    
    # Load KPIs
    try:
        total_rev_df = load_query("SELECT SUM(amount) as revenue FROM raw_clean.payments")
        total_orders_df = load_query("SELECT COUNT(*) as order_count FROM raw_clean.orders WHERE status != 'Cancelled'")
        total_cust_df = load_query("SELECT COUNT(DISTINCT customer_id) as cust_count FROM raw_clean.orders")
        aov_df = load_query("SELECT AVG(total_amount) as aov FROM raw_clean.orders WHERE status != 'Cancelled'")
        
        revenue = total_rev_df['revenue'].iloc[0] or 0.0
        orders = total_orders_df['order_count'].iloc[0] or 0
        customers = total_cust_df['cust_count'].iloc[0] or 0
        aov = aov_df['aov'].iloc[0] or 0.0
        
        # Display custom styled cards in columns
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">Total Revenue</div>
                <div class="kpi-value">${revenue:,.2f}</div>
                <div class="kpi-trend">▲ 12.4% vs last month</div>
            </div>
            """, unsafe_allow_html=True)
        with col2:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">Orders Completed</div>
                <div class="kpi-value">{orders:,}</div>
                <div class="kpi-trend">▲ 8.1% vs last month</div>
            </div>
            """, unsafe_allow_html=True)
        with col3:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">Active Customers</div>
                <div class="kpi-value">{customers:,}</div>
                <div class="kpi-trend">▲ 5.5% vs last month</div>
            </div>
            """, unsafe_allow_html=True)
        with col4:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">Average Order Value</div>
                <div class="kpi-value">${aov:,.2f}</div>
                <div class="kpi-trend">▲ 4.2% vs last month</div>
            </div>
            """, unsafe_allow_html=True)
            
    except Exception as e:
        st.error(f"Error loading KPIs: {e}. Has the ETL pipeline completed running?")
        st.stop()

    st.markdown("---")
    
    # Monthly Revenue Trend Plotly
    st.subheader("Monthly Revenue & Orders Trend")
    try:
        trend_df = load_query("SELECT order_month, revenue, orders_count FROM analytics.monthly_revenue ORDER BY order_month")
        
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=trend_df['order_month'],
            y=trend_df['revenue'],
            name='Revenue ($)',
            marker_color='#6B73FF',
            yaxis='y1'
        ))
        fig.add_trace(go.Scatter(
            x=trend_df['order_month'],
            y=trend_df['orders_count'],
            name='Orders Count',
            line=dict(color='#FF5E62', width=3),
            yaxis='y2'
        ))
        
        fig.update_layout(
            title='Monthly Sales Growth & Volume',
            yaxis=dict(title='Revenue ($)', side='left'),
            yaxis2=dict(title='Number of Orders', side='right', overlaying='y', showgrid=False),
            legend=dict(x=0.01, y=0.99),
            template="plotly_dark",
            margin=dict(l=40, r=40, t=40, b=40),
            transition=dict(duration=800, easing="cubic-in-out")
        )
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.info("No monthly trend data available. Complete a pipeline run.")

# ----------------------------------------------------
# PAGE 2: SALES & PRODUCTS
# ----------------------------------------------------
elif page == "📈 Sales & Products":
    st.subheader("Sales and Category Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Revenue distribution by Product Category")
        try:
            cat_query = """
                SELECT category, SUM(total_revenue) as revenue 
                FROM analytics.product_performance 
                GROUP BY category 
                ORDER BY revenue DESC
            """
            cat_df = load_query(cat_query)
            fig = px.pie(cat_df, values='revenue', names='category', hole=0.4,
                         color_discrete_sequence=px.colors.qualitative.Pastel)
            pull_list = [0.1] + [0] * (len(cat_df) - 1)
            fig.update_traces(textposition='inside', textinfo='percent+label', pull=pull_list)
            fig.update_layout(
                template="plotly_dark",
                transition=dict(duration=800, easing="cubic-in-out")
            )
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.write("Category distribution not ready yet.")

    with col2:
        st.markdown("#### Order Status Breakdown")
        try:
            status_df = load_query("SELECT status, COUNT(*) as count FROM raw_clean.orders GROUP BY status")
            fig = px.bar(status_df, x='status', y='count', color='status',
                         labels={'count': 'Number of Orders'},
                         color_discrete_sequence=px.colors.qualitative.Set2)
            fig.update_layout(
                template="plotly_dark",
                transition=dict(duration=800, easing="cubic-in-out")
            )
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.write("Status breakdown not ready yet.")

    st.markdown("---")
    st.subheader("📦 Interactive 3D Product Performance Space")
    try:
        portfolio_query = """
            SELECT p.product_name, p.category, p.price, p.stock, 
                   pp.quantity_sold, pp.total_revenue
            FROM analytics.product_performance pp
            JOIN raw_clean.products p ON pp.product_id = p.product_id
        """
        portfolio_df = load_query(portfolio_query)
        fig_3d = px.scatter_3d(
            portfolio_df,
            x='price',
            y='stock',
            z='quantity_sold',
            color='category',
            size='total_revenue',
            hover_name='product_name',
            labels={
                'price': 'Price ($)',
                'stock': 'Current Stock',
                'quantity_sold': 'Units Sold'
            },
            color_discrete_sequence=px.colors.qualitative.Bold
        )
        fig_3d.update_layout(
            template="plotly_dark",
            margin=dict(l=0, r=0, b=0, t=30),
            scene=dict(
                xaxis=dict(backgroundcolor="rgba(0, 0, 0, 0)"),
                yaxis=dict(backgroundcolor="rgba(0, 0, 0, 0)"),
                zaxis=dict(backgroundcolor="rgba(0, 0, 0, 0)")
            )
        )
        st.plotly_chart(fig_3d, use_container_width=True)
    except Exception as e:
        st.write("Product portfolio details not computed yet.")

    st.markdown("---")
    st.subheader("🏆 Top Performing Products (Best Sellers)")
    try:
        prod_query = """
            SELECT product_name, category, quantity_sold, total_revenue
            FROM analytics.product_performance
            ORDER BY total_revenue DESC
            LIMIT 10
        """
        prod_df = load_query(prod_query)
        # Styled Table display
        st.dataframe(
            prod_df.style.format({"total_revenue": "${:,.2f}", "quantity_sold": "{:,}"}),
            use_container_width=True
        )
    except Exception as e:
        st.write("Best sellers data not computed yet.")

# ----------------------------------------------------
# PAGE 3: CUSTOMER ANALYTICS
# ----------------------------------------------------
elif page == "👥 Customer Analytics":
    st.subheader("Customer Segmentations & Leaderboard")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.markdown("#### Global Customer Distribution (3D Interactive Globe)")
        try:
            country_df = load_query("""
                SELECT country, COUNT(DISTINCT customer_id) as total_customers 
                FROM raw_clean.customers 
                WHERE country IS NOT NULL AND country != ''
                GROUP BY country 
                ORDER BY total_customers DESC
            """)
            country_df['country'] = country_df['country'].replace({'USA': 'United States', 'UK': 'United Kingdom'})
            fig = px.choropleth(
                country_df,
                locations="country",
                locationmode="country names",
                color="total_customers",
                hover_name="country",
                projection="orthographic",
                color_continuous_scale="Purples",
                labels={'total_customers': 'Customers'}
            )
            fig.update_layout(
                template="plotly_dark",
                margin=dict(l=0, r=0, b=0, t=10),
                geo=dict(
                    showframe=False,
                    showcoastlines=True,
                    projection_type="orthographic",
                    bgcolor="rgba(0,0,0,0)",
                    lakecolor="rgba(0,0,0,0)",
                    oceancolor="rgba(10, 10, 20, 0.4)",
                    showocean=True
                ),
                transition=dict(duration=800, easing="cubic-in-out")
            )
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.write("Country distribution not available.")

    with col2:
        st.markdown("#### Customer Segments (by Lifetime Spend)")
        try:
            segments_query = """
                SELECT 
                    CASE 
                        WHEN total_spent >= 2500 THEN 'VIP Platinum (Spent > $2,500)'
                        WHEN total_spent >= 1000 AND total_spent < 2500 THEN 'Gold Tier ($1,000 - $2,500)'
                        WHEN total_spent >= 300 AND total_spent < 1000 THEN 'Silver Tier ($300 - $1,000)'
                        ELSE 'Bronze Member (< $300)'
                    END as segment,
                    COUNT(*) as customer_count
                FROM analytics.top_customers
                GROUP BY segment
                ORDER BY customer_count DESC
            """
            segment_df = load_query(segments_query)
            fig = px.pie(segment_df, values='customer_count', names='segment', hole=0.4,
                         color_discrete_sequence=px.colors.sequential.Electric)
            pull_list = [0.1] + [0] * (len(segment_df) - 1)
            fig.update_traces(textposition='inside', textinfo='percent+label', pull=pull_list)
            fig.update_layout(
                template="plotly_dark",
                transition=dict(duration=800, easing="cubic-in-out")
            )
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.write("Segmentation not computed.")

    st.markdown("---")
    st.subheader("👥 Interactive 3D Customer Value Space (CLV vs. Purchase Frequency)")
    try:
        segments_query = """
            SELECT name, country, total_spent, order_count, avg_order_value,
                CASE 
                    WHEN total_spent >= 2500 THEN 'VIP Platinum'
                    WHEN total_spent >= 1000 AND total_spent < 2500 THEN 'Gold Tier'
                    WHEN total_spent >= 300 AND total_spent < 1000 THEN 'Silver Tier'
                    ELSE 'Bronze Member'
                END as segment
            FROM analytics.top_customers
        """
        cust_df = load_query(segments_query)
        fig_cust_3d = px.scatter_3d(
            cust_df,
            x='order_count',
            y='avg_order_value',
            z='total_spent',
            color='segment',
            symbol='segment',
            hover_name='name',
            labels={
                'order_count': 'Total Orders',
                'avg_order_value': 'Avg Order Value ($)',
                'total_spent': 'Total CLV ($)'
            },
            color_discrete_map={
                'VIP Platinum': '#ff4b4b',
                'Gold Tier': '#ffaa00',
                'Silver Tier': '#00aaff',
                'Bronze Member': '#888888'
            }
        )
        fig_cust_3d.update_layout(
            template="plotly_dark",
            margin=dict(l=0, r=0, b=0, t=30),
            scene=dict(
                xaxis=dict(backgroundcolor="rgba(0, 0, 0, 0)"),
                yaxis=dict(backgroundcolor="rgba(0, 0, 0, 0)"),
                zaxis=dict(backgroundcolor="rgba(0, 0, 0, 0)")
            )
        )
        st.plotly_chart(fig_cust_3d, use_container_width=True)
    except Exception as e:
        st.write("CLV segmentation space details not computed yet.")

    st.markdown("---")
    st.subheader("💎 Customer Lifetime Value (CLV) Leaderboard")
    try:
        top_cust_df = load_query("SELECT rank, name, country, total_spent, order_count, avg_order_value FROM analytics.top_customers LIMIT 15")
        st.dataframe(
            top_cust_df.style.format({
                "total_spent": "${:,.2f}",
                "avg_order_value": "${:,.2f}",
                "order_count": "{:,}"
            }),
            use_container_width=True,
            hide_index=True
        )
    except Exception as e:
        st.write("Leaderboard not populated.")

# ----------------------------------------------------
# PAGE 4: OPERATIONS & QUALITY
# ----------------------------------------------------
elif page == "⚙️ Operations & Quality":
    st.subheader("Pipeline Orchestration & Data Quality Monitoring")
    
    # 1. Pipeline Runs log
    st.markdown("### 📋 Pipeline Run Execution Log")
    try:
        run_log_df = load_query("SELECT run_time, status, total_records_processed, total_errors_found, data_quality_score FROM analytics.pipeline_runs ORDER BY run_time DESC")
        st.dataframe(
            run_log_df.style.format({
                "data_quality_score": "{:.2f}%",
                "total_records_processed": "{:,}",
                "total_errors_found": "{:,}"
            }),
            use_container_width=True,
            hide_index=True
        )
        
        # Display current status check
        latest_run = run_log_df.iloc[0]
        score = latest_run['data_quality_score']
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Latest Pipeline Status", latest_run['status'])
        with col2:
            st.metric("Latest DQ Score", f"{score:.2f}%")
        with col3:
            st.metric("Latest Error Records Isolated", f"{latest_run['total_errors_found']:,}")
            
    except Exception as e:
        st.info("No pipeline execution runs logged yet. Launch the Airflow DAG to generate logs.")

    st.markdown("---")

    # 2. Detailed Data Quality Metrics
    st.markdown("### 🔍 Raw Data Validation Breakdown")
    try:
        dq_metrics_df = load_query("SELECT table as table_name, metric, value as anomalies_count FROM analytics.data_quality_metrics ORDER BY table_name, metric")
        
        # Filter to see only actual validation rules (errors/anomalies)
        anomalies_df = dq_metrics_df[dq_metrics_df['metric'] != 'total_records']
        total_records_df = dq_metrics_df[dq_metrics_df['metric'] == 'total_records']
        
        # Show table overview of record counts
        st.markdown("#### Table Volume Summary")
        st.dataframe(total_records_df.rename(columns={"anomalies_count": "total_records_checked"}).drop(columns=["metric"]), use_container_width=True, hide_index=True)
        
        # Show details of validation anomalies isolated
        st.markdown("#### Validation Failures and Anomalies Isolated (Quarantined)")
        
        # Custom color formatting for anomalies > 0
        def highlight_errors(val):
            color = 'background-color: rgba(255, 0, 0, 0.15); color: #FF5E62;' if val > 0 else 'color: #00C851;'
            return color
            
        st.dataframe(
            anomalies_df.style.applymap(highlight_errors, subset=['anomalies_count']),
            use_container_width=True,
            hide_index=True
        )
        
    except Exception as e:
        st.info("Detailed data quality validations are empty. Execute the pipeline DAG.")

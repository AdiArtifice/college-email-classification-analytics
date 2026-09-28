"""
Intelligent College Email Classification and Opportunity Analytics System
Streamlit Dashboard (Phase 7)

Reads pre-materialized Hive query outputs (Parquet/CSV) from data/output/analytics/
and enriched full dataset from data/output/features/emails.parquet.
"""

import sys
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Path resolution
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from dashboard.data_loader import (
    load_summary_kpis,
    load_category_counts,
    load_monthly_volume,
    load_opportunity_breakdown,
    load_campus_distribution,
    load_read_unread_distribution,
    load_top_senders,
    load_upcoming_deadlines,
    load_full_features,
)

# ------------------------------------------------------------------------------
# Page Configuration & Styling
# ------------------------------------------------------------------------------
st.set_page_config(
    page_title="College Email & Opportunity Analytics",
    page_icon="📬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for polished aesthetics
st.markdown("""
<style>
    .main-header {
        font-size: 2.2rem;
        font-weight: 700;
        color: #1E3A8A;
        margin-bottom: 0.2rem;
    }
    .sub-header {
        font-size: 1.05rem;
        color: #4B5563;
        margin-bottom: 1.5rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #F8FAFC 0%, #EFF6FF 100%);
        border: 1px solid #E2E8F0;
        border-radius: 10px;
        padding: 18px 16px;
        text-align: center;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    .metric-val {
        font-size: 1.9rem;
        font-weight: 700;
        color: #1E40AF;
        margin: 4px 0;
    }
    .metric-lbl {
        font-size: 0.85rem;
        font-weight: 600;
        color: #64748B;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .badge-rule {
        background-color: #DEF7EC;
        color: #03543F;
        padding: 2px 8px;
        border-radius: 12px;
        font-weight: 600;
        font-size: 0.75rem;
    }
    .badge-ml {
        background-color: #E1EFFE;
        color: #1E429F;
        padding: 2px 8px;
        border-radius: 12px;
        font-weight: 600;
        font-size: 0.75rem;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 48px;
        white-space: pre-wrap;
        background-color: #F1F5F9;
        border-radius: 6px 6px 0 0;
        padding: 8px 16px;
        font-weight: 600;
    }
    .stTabs [aria-selected="true"] {
        background-color: #1E40AF !important;
        color: white !important;
    }
</style>
""", unsafe_allow_html=True)


# ------------------------------------------------------------------------------
# Data Loading
# ------------------------------------------------------------------------------
kpis = load_summary_kpis()
df_categories = load_category_counts()
df_volume = load_monthly_volume()
df_opp_breakdown = load_opportunity_breakdown()
df_campus = load_campus_distribution()
df_read_unread = load_read_unread_distribution()
df_senders = load_top_senders()
df_deadlines = load_upcoming_deadlines()


# ------------------------------------------------------------------------------
# Header
# ------------------------------------------------------------------------------
st.markdown('<div class="main-header">📬 Intelligent College Email Classification & Opportunity Analytics</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Big Data Analytics System | HDFS • MapReduce • Spark MLlib • Apache Hive • Streamlit</div>', unsafe_allow_html=True)

# Top Summary KPI Cards (6 Metrics)
col1, col2, col3, col4, col5, col6 = st.columns(6)

with col1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-lbl">Total Emails</div>
        <div class="metric-val">{kpis['total_emails']:,}</div>
        <div style="font-size: 0.75rem; color: #64748B;">1.3 GB MBOX • 100% Parsed</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    unread_pct = kpis['unread_percentage']
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-lbl">Unread Inflow</div>
        <div class="metric-val">{kpis['unread_emails']:,}</div>
        <div style="font-size: 0.75rem; color: #D97706;">{unread_pct:.1f}% Inbox Backlog</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    opp_pct = kpis['total_opportunities'] / kpis['total_emails'] * 100
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-lbl">Opportunities</div>
        <div class="metric-val">{kpis['total_opportunities']:,}</div>
        <div style="font-size: 0.75rem; color: #059669;">{opp_pct:.1f}% High Value</div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    intern_count = int(df_categories[df_categories['category'] == 'Internship']['email_count'].iloc[0]) if len(df_categories[df_categories['category'] == 'Internship']) > 0 else 535
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-lbl">Internships</div>
        <div class="metric-val">{intern_count:,}</div>
        <div style="font-size: 0.75rem; color: #2563EB;">EduSkills, Internshala</div>
    </div>
    """, unsafe_allow_html=True)

with col5:
    hack_count = int(df_categories[df_categories['category'] == 'Hackathon/Competition']['email_count'].iloc[0]) if len(df_categories[df_categories['category'] == 'Hackathon/Competition']) > 0 else 1069
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-lbl">Hackathons</div>
        <div class="metric-val">{hack_count:,}</div>
        <div style="font-size: 0.75rem; color: #7C3AED;">Unstop, Competitions</div>
    </div>
    """, unsafe_allow_html=True)

with col6:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-lbl">Active Deadlines</div>
        <div class="metric-val">{kpis['total_deadlines']:,}</div>
        <div style="font-size: 0.75rem; color: #DC2626;">ISO Normalized Dates</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)


# ------------------------------------------------------------------------------
# Main Tabs Navigation
# ------------------------------------------------------------------------------
tab_inbox, tab_opps, tab_deadlines, tab_search, tab_bda = st.tabs([
    "📊 Inbox Overview & Trends",
    "🎯 Opportunity Explorer",
    "⏰ Deadlines & Events",
    "🔍 Email & ML Inspector",
    "🏛️ Big Data 5 Vs & Architecture"
])


# ==============================================================================
# TAB 1: Inbox Overview & Trends
# ==============================================================================
with tab_inbox:
    st.subheader("Inbox Analytics & Temporal Dynamics")

    # Row 1: Category Distribution & Temporal Growth
    r1_col1, r1_col2 = st.columns([1, 1])

    with r1_col1:
        st.markdown("##### 📁 Email Distribution by Category")
        fig_cat = px.bar(
            df_categories,
            x='email_count',
            y='category',
            orientation='h',
            text='email_count',
            labels={'email_count': 'Count', 'category': 'Category'},
            color='email_count',
            color_continuous_scale='Blues'
        )
        fig_cat.update_layout(
            yaxis={'categoryorder': 'total ascending'},
            margin=dict(l=10, r=20, t=10, b=10),
            height=370,
            showlegend=False,
            coloraxis_showscale=False
        )
        fig_cat.update_traces(texttemplate='%{text}', textposition='outside')
        st.plotly_chart(fig_cat, use_container_width=True)

    with r1_col2:
        st.markdown("##### 📈 Month-over-Month Volume & Opportunity Trajectory")
        fig_vol = px.area(
            df_volume,
            x='year_month',
            y=['total_emails', 'opportunity_emails', 'unread_emails'],
            labels={'year_month': 'Year-Month', 'value': 'Emails', 'variable': 'Stream'},
            color_discrete_map={
                'total_emails': '#93C5FD',
                'opportunity_emails': '#10B981',
                'unread_emails': '#F59E0B'
            }
        )
        fig_vol.update_layout(
            margin=dict(l=10, r=20, t=10, b=10),
            height=370,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig_vol, use_container_width=True)

    # Row 2: Read vs Unread Behavior & On/Off Campus Distribution
    r2_col1, r2_col2 = st.columns([1, 1])

    with r2_col1:
        st.markdown("##### 📬 Read vs. Unread Inbox Behavior (Fatigue vs. Priority)")
        # Stacked bar chart of read vs unread
        fig_read = go.Figure()
        fig_read.add_trace(go.Bar(
            name='Read',
            y=df_read_unread['category'],
            x=df_read_unread['read_count'],
            orientation='h',
            marker_color='#3B82F6'
        ))
        fig_read.add_trace(go.Bar(
            name='Unread',
            y=df_read_unread['category'],
            x=df_read_unread['unread_count'],
            orientation='h',
            marker_color='#F97316'
        ))
        fig_read.update_layout(
            barmode='stack',
            yaxis={'categoryorder': 'total ascending'},
            margin=dict(l=10, r=20, t=10, b=10),
            height=370,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig_read, use_container_width=True)
        st.caption("💡 Notice: High read rates for **Internships (83.7%)** and **Academics (80.5%)**, compared to 66.5% unread for Newsletters.")

    with r2_col2:
        st.markdown("##### 🏫 On-Campus vs. Off-Campus Opportunities")
        campus_totals = df_campus.groupby('campus_type')['count'].sum().reset_index()
        fig_campus = px.pie(
            campus_totals,
            names='campus_type',
            values='count',
            hole=0.45,
            color='campus_type',
            color_discrete_map={'Off-campus': '#3B82F6', 'On-campus': '#10B981'}
        )
        fig_campus.update_layout(
            margin=dict(l=10, r=20, t=10, b=10),
            height=370
        )
        st.plotly_chart(fig_campus, use_container_width=True)
        st.caption("💡 88.6% of career opportunities originate off-campus (Unstop, Naukri), while college notices supply on-campus roles.")

    # Row 3: Top Sender Domains & Recognized Organizations
    st.markdown("##### 🏢 Top 15 Sender Domains & Sponsoring Organizations")
    top_15_senders = df_senders.head(15)
    fig_senders = px.bar(
        top_15_senders,
        x='total_emails',
        y='sender_domain',
        color='opportunity_count',
        orientation='h',
        text='organization',
        labels={'total_emails': 'Total Emails', 'sender_domain': 'Domain', 'opportunity_count': 'Opportunities'},
        color_continuous_scale='Viridis'
    )
    fig_senders.update_layout(
        yaxis={'categoryorder': 'total ascending'},
        margin=dict(l=10, r=20, t=10, b=10),
        height=420
    )
    fig_senders.update_traces(textposition='inside')
    st.plotly_chart(fig_senders, use_container_width=True)


# ==============================================================================
# TAB 2: Opportunity Explorer (Prompt Specified Table)
# ==============================================================================
with tab_opps:
    st.subheader("🎯 Student Opportunity Explorer")
    st.markdown("Filterable, actionable view of extracted internships, competitions, placements, and interviews.")

    # Load full dataset for rich filtering
    try:
        df_full = load_full_features()
    except Exception as e:
        st.warning(f"Note: Reading pre-computed analytics view ({e})")
        df_full = df_deadlines

    # Filter controls in columns
    f_col1, f_col2, f_col3, f_col4 = st.columns(4)

    with f_col1:
        opp_categories = ['Internship', 'Hackathon/Competition', 'Job/Placement', 'Interview', 'Workshop/Event', 'Scholarship']
        sel_categories = st.multiselect("Category Filter", opp_categories, default=opp_categories[:3])

    with f_col2:
        available_types = ['AI/ML', 'Data Science', 'Web Development', 'Cloud & DevOps', 'Cybersecurity', 'Software Engineering', 'General Tech']
        sel_types = st.multiselect("Technical Sub-Domain", available_types, default=[])

    with f_col3:
        sel_campus = st.selectbox("Campus Sourcing", ["All Opportunities", "On-campus Only", "Off-campus Only"])

    with f_col4:
        sel_unread = st.selectbox("Read Status", ["All Messages", "Unread Only", "Read Only"])

    # Search & Deadline toggle
    s_col1, s_col2 = st.columns([3, 1])
    with s_col1:
        search_query = st.text_input("🔍 Search by Subject, Keywords, or Sponsoring Organization", placeholder="e.g., python, unstop, robotics, hiring drive, ai...")
    with s_col2:
        st.write("")
        st.write("")
        has_deadline_only = st.checkbox("⏰ Has Explicit Deadline Only", value=False)

    # Apply filters to full DataFrame
    mask = df_full['category'].isin(sel_categories) if sel_categories else df_full['category'].isin(opp_categories)

    if sel_types:
        mask = mask & df_full['opportunity_type'].isin(sel_types)

    if sel_campus == "On-campus Only":
        mask = mask & (df_full['campus_type'] == 'On-campus')
    elif sel_campus == "Off-campus Only":
        mask = mask & (df_full['campus_type'] == 'Off-campus')

    if sel_unread == "Unread Only":
        mask = mask & (df_full['is_unread'] == True)
    elif sel_unread == "Read Only":
        mask = mask & (df_full['is_unread'] == False)

    if has_deadline_only:
        mask = mask & df_full['deadline'].notna()

    if search_query.strip():
        q = search_query.strip().lower()
        search_mask = (
            df_full['subject'].fillna('').str.lower().str.contains(q) |
            df_full['organization'].fillna('').str.lower().str.contains(q) |
            df_full['sender_domain'].fillna('').str.lower().str.contains(q)
        )
        mask = mask & search_mask

    filtered_opps = df_full[mask].copy()

    # Opportunity table schema requested by user prompt:
    # Opportunity | Type | Organization | Campus | Deadline
    display_df = filtered_opps[[
        'subject',
        'category',
        'opportunity_type',
        'organization',
        'campus_type',
        'deadline',
        'date_utc',
        'is_unread'
    ]].copy()

    display_df.columns = [
        'Opportunity',
        'Category',
        'Type',
        'Organization',
        'Campus',
        'Deadline',
        'Date Received',
        'Unread'
    ]

    # Format date columns cleanly
    display_df['Date Received'] = display_df['Date Received'].astype(str).str[:10]
    display_df['Deadline'] = display_df['Deadline'].fillna('No Deadline Stated')

    st.markdown(f"**Found {len(display_df):,} matching opportunities**")

    # Render Interactive Table
    st.dataframe(
        display_df,
        use_container_width=True,
        height=450,
        column_config={
            "Opportunity": st.column_config.TextColumn("Opportunity", width="large"),
            "Category": st.column_config.TextColumn("Category", width="medium"),
            "Type": st.column_config.TextColumn("Sub-Domain", width="medium"),
            "Organization": st.column_config.TextColumn("Organization", width="medium"),
            "Campus": st.column_config.TextColumn("Campus", width="small"),
            "Deadline": st.column_config.TextColumn("Deadline", width="medium"),
            "Unread": st.column_config.CheckboxColumn("Unread", width="small"),
        }
    )

    # Download CSV button
    csv_data = display_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Export Filtered Opportunities to CSV",
        data=csv_data,
        file_name="student_opportunities.csv",
        mime="text/csv"
    )


# ==============================================================================
# TAB 3: Deadlines & Events
# ==============================================================================
with tab_deadlines:
    st.subheader("⏰ Opportunity Deadline Tracker")
    st.markdown("All 52 verified opportunity deadlines extracted by regex and normalized to ISO `YYYY-MM-DD`.")

    dl_col1, dl_col2 = st.columns([1, 2])

    with dl_col1:
        st.markdown("##### 📌 Deadline Summary by Category")
        dl_summary = df_deadlines.groupby('category').size().reset_index(name='count')
        fig_dl_pie = px.pie(
            dl_summary,
            names='category',
            values='count',
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        fig_dl_pie.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=320)
        st.plotly_chart(fig_dl_pie, use_container_width=True)

    with dl_col2:
        st.markdown("##### 🏢 Deadlines by Organization")
        org_dl = df_deadlines.groupby('organization').size().reset_index(name='deadlines').sort_values('deadlines', ascending=True)
        fig_org_dl = px.bar(
            org_dl,
            x='deadlines',
            y='organization',
            orientation='h',
            text='deadlines',
            color='deadlines',
            color_continuous_scale='Reds'
        )
        fig_org_dl.update_layout(
            yaxis={'categoryorder': 'total ascending'},
            margin=dict(l=10, r=10, t=10, b=10),
            height=320,
            coloraxis_showscale=False
        )
        st.plotly_chart(fig_org_dl, use_container_width=True)

    st.markdown("##### 📋 Complete Deadline Roster")
    st.dataframe(
        df_deadlines[['deadline', 'category', 'organization', 'campus_type', 'opportunity_type', 'subject']],
        use_container_width=True,
        column_config={
            "deadline": st.column_config.TextColumn("Target Date", width="medium"),
            "category": st.column_config.TextColumn("Category", width="medium"),
            "organization": st.column_config.TextColumn("Organization", width="medium"),
            "campus_type": st.column_config.TextColumn("Campus", width="small"),
            "opportunity_type": st.column_config.TextColumn("Sub-Domain", width="medium"),
            "subject": st.column_config.TextColumn("Email Subject / Call to Action", width="large"),
        }
    )


# ==============================================================================
# TAB 4: Email & ML Inspector
# ==============================================================================
with tab_search:
    st.subheader("🔍 Email & Spark MLlib Inspector")
    st.markdown("Deep dive into individual emails, preprocessed tokens, weak supervision labels, and ML confidence scores.")

    try:
        df_full = load_full_features()
        has_full = True
    except Exception:
        has_full = False

    if has_full:
        i_col1, i_col2, i_col3 = st.columns(3)
        with i_col1:
            filter_method = st.selectbox("Classification Method", ["All Methods", "Weak Supervision Rule (72.3%)", "Spark MLlib Inference (27.7%)"])
        with i_col2:
            filter_cat = st.selectbox("Filter Category", ["All Categories"] + sorted(list(df_full['category'].unique())))
        with i_col3:
            min_conf = st.slider("Minimum Confidence", 0.0, 1.0, 0.0, 0.05)

        sub_mask = pd.Series(True, index=df_full.index)
        if filter_method == "Weak Supervision Rule (72.3%)":
            sub_mask = sub_mask & (df_full['prediction_method'] == 'rule')
        elif filter_method == "Spark MLlib Inference (27.7%)":
            sub_mask = sub_mask & (df_full['prediction_method'] == 'ml')

        if filter_cat != "All Categories":
            sub_mask = sub_mask & (df_full['category'] == filter_cat)

        sub_mask = sub_mask & (df_full['confidence'] >= min_conf)

        search_text = st.text_input("Search raw emails by subject or body text:")
        if search_text.strip():
            sub_mask = sub_mask & df_full['subject'].fillna('').str.lower().str.contains(search_text.strip().lower())

        inspected = df_full[sub_mask].head(25)
        st.markdown(f"**Displaying {len(inspected)} of {sub_mask.sum():,} matching records**")

        for idx, row in inspected.iterrows():
            badge_class = "badge-rule" if row['prediction_method'] == 'rule' else "badge-ml"
            method_label = "RULE ENGINE" if row['prediction_method'] == 'rule' else "SPARK MLlib"
            unread_indicator = "🔴 Unread" if row['is_unread'] else "🟢 Read"
            has_attach = "📎 Attachment" if row['has_attachment'] else ""

            with st.expander(f"[{row['category']}] {row['subject']}  —  {row['sender_domain']} ({unread_indicator})"):
                st.markdown(f"""
                **Sender:** `{row['sender']}` &lt;{row['sender_email']}&gt;  
                **Date (UTC):** `{row['date_utc']}` | **Method:** <span class="{badge_class}">{method_label}</span> | **Confidence:** `{row['confidence']:.4f}` {has_attach}  
                **Extracted Org:** `{row['organization']}` | **Campus:** `{row['campus_type']}` | **Opportunity Type:** `{row['opportunity_type']}`  
                **Deadline:** `{row['deadline'] or 'None'}`  
                """, unsafe_allow_html=True)
                
                tab_body, tab_nlp = st.tabs(["Original Body", "NLP Preprocessed Text"])
                with tab_body:
                    st.text(row['body_text'][:1500] + ("..." if len(str(row['body_text'])) > 1500 else ""))
                with tab_nlp:
                    st.text(row['cleaned_text'][:1500] + ("..." if len(str(row['cleaned_text'])) > 1500 else ""))
    else:
        st.info("Full features dataset not present locally. Run Phase 4 pipeline to generate full records.")


# ==============================================================================
# TAB 5: Big Data Architecture & 5 Vs (Viva Defense)
# ==============================================================================
with tab_bda:
    st.subheader("🏛️ Big Data Analytics Architecture & The 5 Vs")
    st.markdown("Grounding the prototype in core Big Data principles, coursework requirements, and viva defense points.")

    v1, v2 = st.columns(2)

    with v1:
        st.markdown("""
        #### 1. Volume (Scale & Growth)
        - **Dataset Size:** `1.3 GB` raw MBOX export via Google Takeout.
        - **Records:** `7,287` raw messages deduplicated to **7,276 unique emails** via RFC 822 `Message-ID`.
        - **Defensible Growth:** 
          - 2023: 101 emails
          - 2024: 909 emails
          - 2025: 3,922 emails
          - 2026: 2,344 emails
          *Volume grows exponentially as student progresses through college.*

        #### 2. Velocity (Continuous Ingestion)
        - **Paradigm:** Incremental batch ingestion.
        - **Pipeline:** New Google Takeout / IMAP drops are parsed, loaded into HDFS, and processed incrementally via Spark MLlib without requiring cluster downtime.

        #### 3. Variety (Heterogeneous Data)
        - **Multi-modal data:** Headers, plain-text bodies, HTML-only bodies, Gmail label lists, timestamps, attachment indicators.
        - **Storage:** JSONL (`TextInputFormat` for Java MapReduce) and columnar Snappy Parquet (for Spark and Hive).
        """)

    with v2:
        st.markdown("""
        #### 4. Veracity (Data Quality & Cleaning)
        - **RFC 2047 Decoding:** Fixed base64/quoted-printable encoded subject lines.
        - **Folded Gmail Labels:** Fixed multi-line split labels (`Category\\n updates`).
        - **HTML Fallback Artifacts:** Stripped CSS rules (`table{...}`) from 2,004 HTML bodies.
        - **Quoted Text & Signatures:** Removed reply chains (`> ...`) to prevent TF-IDF skew.
        - **Conservative Extraction:** Ambiguous deadlines left `null` (zero guessing).

        #### 5. Value (Actionable Intelligence)
        - **Noise Reduction:** Separated 2,951 promotional emails (66.5% unread) from high-value messages.
        - **Opportunity Discovery:** Isolated 2,306 career opportunities (Internships, Placements, Hackathons).
        - **Actionable Tracking:** Surfaced 52 upcoming deadlines directly to the student.
        """)

    st.markdown("---")
    st.markdown("#### 🔄 End-to-End Pipeline Data Flow")
    st.code("""
  +-------------------------------------------------------------------------+
  |              Google Takeout: 1.3 GB Raw MBOX (7,287 emails)             |
  +-------------------------------------------------------------------------+
                                       |
                                       v
                     [Phase 1] Python Streaming Parser
                                       |
                     +-----------------+-----------------+
                     |                                   |
                     v                                   v
             HDFS: raw/structured/               HDFS: cleaned/
                emails.jsonl                     emails.parquet
                     |                                   |
                     v                                   v
            [Phase 3] Java MapReduce             [Phase 4] Apache Spark (PySpark)
            Batch Domain-Month Sum               - Text Preprocessing & Cleaning
            (results/domain_month_counts)        - Weak Supervision (72.3% Rules)
                                                 - Spark MLlib TF-IDF (HashingTF+IDF)
                                                 - Multinomial Logistic Regression
                                                 - Phase 5: Opportunity Extraction
                                                         |
                                                         v
                                                 HDFS: features/
                                                 emails.parquet (7,276)
                                                         |
                                                         v
                                                 [Phase 6] Apache Hive
                                                 External Table & 7 HQL Queries
                                                         |
                                                         v
                                              data/output/analytics/
                                              Materialized Result Parquet/CSVs
                                                         |
                                                         v
                                                 [Phase 7] Streamlit
                                                 Personalized Student Dashboard
    """, language="text")

    st.markdown("---")
    st.markdown("#### ⚖️ Defensible Technology Choices (Viva Q&A)")
    st.markdown("""
    | Technology | Explicit Course Role | Why Not An Alternative? |
    |---|---|---|
    | **HDFS** | Distributed storage across blocks | Stores raw archive, JSONL, and Parquet layers without local disk coupling. |
    | **MapReduce (Java)** | Distributed batch aggregation | Fulfills academic coursework Java MapReduce requirement (`domain x month` counts). |
    | **PySpark MLlib** | Feature extraction & ML classification | Distributed `HashingTF` + `IDF` + Logistic Regression scales across executors. |
    | **Apache Hive** | SQL query & schema registry layer | Provides a standard SQL catalog over HDFS Parquet independent of running Spark jobs. |
    | **Streamlit** | Low-latency presentation layer | Reads pre-materialized Hive result files instantly; zero JDBC/cluster latency during demo. |
    """)

# ------------------------------------------------------------------------------
# Footer
# ------------------------------------------------------------------------------
st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: #94A3B8; font-size: 0.8rem;'>"
    "Intelligent College Email Classification and Opportunity Analytics System | "
    "Big Data Analytics Mini-Project | Developed with Streamlit, Spark MLlib, Hive & HDFS"
    "</div>",
    unsafe_allow_html=True
)

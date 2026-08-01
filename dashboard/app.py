import os
import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output, State
import plotly.express as px
import pandas as pd
import psycopg2
from psycopg2 import pool
from datetime import datetime

POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://postgres:postgres@localhost:5432/streaming")

# Initialize Threaded Connection Pool
db_pool = None
try:
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=5,
        dsn=POSTGRES_URL
    )
except Exception as e:
    print(f"Failed to initialize connection pool: {e}")

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Live Olist E-commerce Stream"),
    
    html.Div(id='throughput-indicator', style={'fontSize': '18px', 'marginBottom': '20px', 'color': '#555'}),
    
    dcc.Dropdown(
        id='category-filter',
        options=[{'label': 'All Categories', 'value': 'ALL'}],
        value='ALL',
        clearable=False,
        style={'width': '50%', 'marginBottom': '20px'}
    ),
    
    dcc.Interval(id='interval-component', interval=2000, n_intervals=0),
    dcc.Interval(id='interval-categories', interval=60000, n_intervals=0), # Slower tick for categories
    dcc.Store(id='state-store', data={'last_count': 0, 'history': []}),
    
    html.Div([
        dcc.Graph(id='orders-per-hour'),
    ], style={'width': '48%', 'display': 'inline-block'}),
    
    html.Div([
        dcc.Graph(id='revenue-by-category'),
    ], style={'width': '48%', 'display': 'inline-block'}),
    
    html.Div([
        html.H3("Recent Anomalies (Z-Score > 3)"),
        dash_table.DataTable(
            id='anomalies-table',
            columns=[
                {"name": "Time", "id": "order_purchase_timestamp"},
                {"name": "Order ID (Truncated)", "id": "order_id"},
                {"name": "Product ID", "id": "product_id"},
                {"name": "Category", "id": "category"},
                {"name": "Price", "id": "price"},
                {"name": "Rolling Mean", "id": "rolling_mean"},
                {"name": "Z-Score", "id": "z_score"}
            ],
            style_table={'overflowX': 'auto'}
        )
    ])
])

def get_db_connection():
    global db_pool
    if db_pool is None:
        try:
            db_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=5,
                dsn=POSTGRES_URL
            )
        except Exception as e:
            print(f"Failed to initialize connection pool: {e}")
            return None
            
    if db_pool:
        try:
            return db_pool.getconn()
        except Exception as e:
            print(f"Failed to get connection from pool: {e}")
            return None
    return None

def return_db_connection(conn):
    if db_pool and conn:
        db_pool.putconn(conn)

@app.callback(
    Output('category-filter', 'options'),
    Input('interval-categories', 'n_intervals')
)
def update_categories(n):
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            raise Exception("No connection from pool")
            
        df_cats = pd.read_sql("SELECT DISTINCT category FROM revenue_by_category_1m", conn)
        cat_options = [{'label': 'All Categories', 'value': 'ALL'}]
        for cat in sorted(df_cats['category'].dropna().tolist()):
            cat_options.append({'label': cat, 'value': cat})
        return cat_options
    except Exception as e:
        print(f"Error fetching categories: {e}")
        return dash.no_update
    finally:
        if conn:
            return_db_connection(conn)

@app.callback(
    Output('orders-per-hour', 'figure'),
    Output('revenue-by-category', 'figure'),
    Output('anomalies-table', 'data'),
    Output('throughput-indicator', 'children'),
    Output('state-store', 'data'),
    Input('interval-component', 'n_intervals'),
    Input('category-filter', 'value'),
    State('state-store', 'data')
)
def update_graphs(n, selected_category, state_data):
    conn = None
    
    # Defaults
    fig_orders = dash.no_update
    fig_rev = dash.no_update
    anomalies_data = dash.no_update
    throughput_text = dash.no_update
    
    current_count = state_data.get('last_count', 0)
    history = state_data.get('history', [])
    
    try:
        conn = get_db_connection()
        if not conn:
            raise Exception("No DB connection from pool")
            
        # 1. Throughput calculation
        try:
            df_count = pd.read_sql("SELECT count(*) as c FROM raw_orders", conn)
            current_count = int(df_count.iloc[0]['c'])
            last_count = state_data.get('last_count', 0)
            
            if last_count > 0:
                delta = current_count - last_count
                history.append(delta)
                if len(history) > 5:
                    history.pop(0)
                    
            avg_delta = sum(history) / len(history) if history else 0
            current_time_str = datetime.now().strftime('%H:%M:%S')
            throughput_text = f"Last refreshed: {current_time_str} | Orders written per 2s (10s avg): {avg_delta:.1f}"
        except Exception as e:
            print(f"Error fetching throughput: {e}")
            throughput_text = "Error fetching throughput"
            
        # 2. Get Max Time
        max_time = None
        max_time_failed = False
        try:
            df_max_time = pd.read_sql("SELECT max(window_start) as max_time FROM metrics_1m", conn)
            max_time = df_max_time.iloc[0]['max_time']
        except Exception as e:
            print(f"Error fetching max_time: {e}")
            max_time_failed = True
            
        if not max_time_failed:
            if pd.isna(max_time):
                # No data yet
                return (
                    px.bar(title='Orders Per Hour (Waiting for data...)'),
                    px.line(title='Revenue by Category (Waiting for data...)'),
                    [],
                    throughput_text,
                    {'last_count': current_count, 'history': history}
                )

            # 3. Orders per hour (Bar Chart)
            try:
                df_orders = pd.read_sql("""
                    SELECT date_trunc('hour', window_start) as hour_start, sum(orders_count) as orders_count
                    FROM metrics_1m 
                    WHERE window_start >= %s - INTERVAL '48 hours'
                    GROUP BY 1
                    ORDER BY 1 ASC
                """, conn, params=(max_time,))
                
                if not df_orders.empty:
                    df_orders['hour_start'] = pd.to_datetime(df_orders['hour_start'])
                    df_orders = df_orders.set_index('hour_start').resample('1h').sum().fillna(0).reset_index()
                    fig_orders = px.bar(df_orders, x='hour_start', y='orders_count', title='Orders Per Hour')
                    fig_orders.update_traces(marker_line_width=0)
                else:
                    fig_orders = px.bar(title='Orders Per Hour')
            except Exception as e:
                print(f"Error fetching orders: {e}")
                fig_orders = px.bar(title='Error loading orders data')

            # 4. Revenue by category (Line Chart with zero-filling)
            try:
                cat_filter_sql = ""
                params = [max_time]
                if selected_category != 'ALL':
                    cat_filter_sql = "AND category = %s"
                    params.append(selected_category)
                    
                df_rev = pd.read_sql(f"""
                    SELECT date_trunc('hour', window_start) as hour_start, category, sum(revenue) as revenue
                    FROM revenue_by_category_1m 
                    WHERE window_start >= %s - INTERVAL '48 hours'
                    {cat_filter_sql}
                    GROUP BY 1, 2
                    ORDER BY 1 ASC
                """, conn, params=tuple(params))
                
                if not df_rev.empty:
                    df_rev['hour_start'] = pd.to_datetime(df_rev['hour_start'])
                    if selected_category == 'ALL':
                        top_cats = df_rev.groupby('category')['revenue'].sum().nlargest(5).index
                        df_rev = df_rev[df_rev['category'].isin(top_cats)]
                        
                    df_pivot = df_rev.pivot_table(index='hour_start', columns='category', values='revenue', aggfunc='sum').fillna(0)
                    df_pivot = df_pivot.resample('1h').sum().fillna(0)
                    df_rev_filled = df_pivot.reset_index().melt(id_vars='hour_start', value_name='revenue', var_name='category')
                    
                    fig_rev = px.line(df_rev_filled, x='hour_start', y='revenue', color='category', 
                                      title='Revenue by Category (Top 5)' if selected_category == 'ALL' else f'Revenue: {selected_category}')
                else:
                    fig_rev = px.line(title='Revenue by Category')
            except Exception as e:
                print(f"Error fetching revenue: {e}")
                fig_rev = px.line(title='Error loading revenue data')

            # 5. Anomalies
            try:
                anom_filter_sql = ""
                anom_params = [max_time]
                if selected_category != 'ALL':
                    anom_filter_sql = "AND category = %s"
                    anom_params.append(selected_category)
                    
                df_anomalies = pd.read_sql(f"""
                    SELECT order_purchase_timestamp, left(order_id, 8) as order_id, left(product_id, 8) as product_id, category, round(price::numeric, 2) as price, 
                           round(rolling_mean::numeric, 2) as rolling_mean, round(z_score::numeric, 2) as z_score
                    FROM anomalies 
                    WHERE order_purchase_timestamp >= %s - INTERVAL '48 hours'
                    {anom_filter_sql}
                    ORDER BY order_purchase_timestamp DESC 
                    LIMIT 10
                """, conn, params=tuple(anom_params))
                
                anomalies_data = df_anomalies.to_dict('records')
            except Exception as e:
                print(f"Error fetching anomalies: {e}")
                anomalies_data = []

    except Exception as e:
        print(f"Critical error in update_graphs wrapper: {e}")
    finally:
        if conn:
            return_db_connection(conn)
            
    return fig_orders, fig_rev, anomalies_data, throughput_text, {'last_count': current_count, 'history': history}

if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0', port=8050)

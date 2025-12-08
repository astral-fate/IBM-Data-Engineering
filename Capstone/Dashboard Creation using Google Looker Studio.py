
# Step 1: Install the necessary libraries
# We need Dash for the dashboard and Plotly for the charts
!pip install dash pandas plotly

import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html, dash_table
from dash.dependencies import Input, Output

# Step 2: Load the Data (Task 1)
# Make sure 'ecommerce.csv' is uploaded to your Colab session
try:
    df = pd.read_csv('ecommerce.csv')
    print("Data loaded successfully!")
    # If 'ecommerce.csv' is loaded, create a 'date' column from existing date components
    # Assuming 'year', 'month', 'day' columns exist in ecommerce.csv
    df['date'] = pd.to_datetime(df[['year', 'month', 'day']])
    # Drop the individual year, month, day columns if no longer needed
    # df = df.drop(columns=['year', 'month', 'day'])
except FileNotFoundError:
    print("Error: Please upload 'ecommerce.csv' to the Colab files section.")
    # Creating dummy data just so the code runs if you haven't uploaded the file yet
    data = {
        'date': ['2020-01-15', '2020-02-10', '2020-03-05', '2020-04-20', '2020-05-15', '2020-06-10',
                 '2021-01-15', '2020-01-20', '2020-04-10', '2020-08-05', '2020-02-15', '2020-11-20'],
        'category': ['Electronics', 'Clothing', 'Electronics', 'Mobile Phones', 'Mobile Phones', 'Clothing',
                     'Electronics', 'Mobile Phones', 'Mobile Phones', 'Electronics', 'Clothing', 'Mobile Phones'],
        'sales': [200, 150, 300, 500, 450, 120, 250, 600, 550, 320, 180, 700]
    }
    df = pd.DataFrame(data)

# --- Data Preprocessing ---
# Convert date column to datetime objects (this line is now handled within the try-except for the loaded csv)
# Note: Adjust 'date' to whatever your actual date column name is (e.g., 'Order Date')
# df['date'] = pd.to_datetime(df['date']) # This line is moved or handled by the new 'date' creation

# Extract Year, Month, and Quarter for the charts
df['Year'] = df['date'].dt.year
df['Month'] = df['date'].dt.month_name()
df['Quarter'] = df['date'].dt.to_period('Q').astype(str)

# --- Generating Charts ---

# Task 2: List first 10 rows (We will display this in the dashboard)
top_10 = df.head(10)

# Task 4: Line chart of month-wise total sales for the year 2020
df_2020 = df[df['Year'] == 2020]
sales_2020 = df_2020.groupby('Month')['price'].sum().reset_index() # Changed 'sales' to 'price'
# Sort months correctly
months_order = ['January', 'February', 'March', 'April', 'May', 'June',
                'July', 'August', 'September', 'October', 'November', 'December']
fig_line = px.line(sales_2020, x='Month', y='price', title='Month-wise Total Sales (2020)', # Changed 'sales' to 'price'
                   category_orders={'Month': months_order})

# Task 5: Pie chart of category-wise total sales
sales_category = df.groupby('category')['price'].sum().reset_index() # Changed 'sales' to 'price'
fig_pie = px.pie(sales_category, values='price', names='category', title='Category-wise Total Sales') # Changed 'sales' to 'price'

# Task 6: Bar chart of Quarterly sales of mobile phones
# Filter for 'Mobile Phones' (Check your CSV for exact spelling, e.g., 'Mobile' or 'Mobile Phones')
mobile_df = df[df['category'].str.contains('Mobile', case=False, na=False)]
sales_mobile_qtr = mobile_df.groupby('Quarter')['price'].sum().reset_index() # Changed 'sales' to 'price'
fig_bar = px.bar(sales_mobile_qtr, x='Quarter', y='price', title='Quarterly Sales of Mobile Phones') # Changed 'sales' to 'price'

# --- Build the Dash App ---
app = Dash(__name__)

app.layout = html.Div([
    html.H1("E-Commerce Analytics Dashboard", style={'textAlign': 'center'}),

    # Task 1 & 2 Section: Data Preview
    html.H3("Task 2: First 10 Rows of Data"),
    dash_table.DataTable(
        data=top_10.to_dict('records'),
        columns=[{'name': i, 'id': i} for i in top_10.columns],
        style_table={'overflowX': 'auto'},
        style_cell={'textAlign': 'left'}
    ),

    html.Hr(), # Horizontal line

    # Task 4 Section
    html.Div([
        html.H3("Task 4: Monthly Sales Trend (2020)"),
        dcc.Graph(figure=fig_line)
    ]),

    # Task 5 Section
    html.Div([
        html.H3("Task 5: Sales by Category"),
        dcc.Graph(figure=fig_pie)
    ]),

    # Task 6 Section
    html.Div([
        html.H3("Task 6: Mobile Phone Sales by Quarter"),
        dcc.Graph(figure=fig_bar)
    ])
])

# Run the app inline in Colab
if __name__ == '__main__':
    app.run(jupyter_mode='inline', debug=True)

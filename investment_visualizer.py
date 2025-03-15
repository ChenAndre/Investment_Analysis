import os
import argparse
import gspread
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import re
from google.oauth2.service_account import Credentials
import matplotlib.ticker as mtick

def format_currency(x, pos):
    """Format y-axis ticks as currency"""
    if abs(x) >= 1e6:
        return '${:,.1f}M'.format(x/1e6)
    elif abs(x) >= 1e3:
        return '${:,.1f}K'.format(x/1e3)
    else:
        return '${:,.0f}'.format(x)

def extract_stock_symbol(description):
    """Extract stock symbols from transaction descriptions"""
    match = re.search(r'\(([A-Z\.]+)\)', description)
    if match:
        return match.group(1)
    return None

def identify_transaction_type(row):
    """Identify transaction types from descriptions and amounts"""
    desc = row['Description'].lower()
    amount = float(row['Amount'])
    
    if 'initial fund capital' in desc:
        return 'Initial Capital'
    elif amount < 0 and any(word in desc for word in ['purchase', 'accumulate', 'long position', 'acquisition', 'investment in']):
        return 'Buy'
    elif amount > 0 and any(word in desc for word in ['sell', 'liquidate', 'close position', 'divestment', 'profit-taking']):
        return 'Sell'
    elif 'dividend' in desc:
        return 'Dividend'
    elif amount < 0 and any(word in desc for word in ['fee', 'expense', 'commission', 'research', 'audit']):
        return 'Fee'
    else:
        return 'Other'

def create_investment_visualizations(sheet_id, output_dir='investment_charts', creds_path='google_credentials.json'):
    """Create investment visualizations from Google Sheet data"""
    # Setup credentials
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file(creds_path, scopes=scope)
    client = gspread.authorize(creds)
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        # Open the sheet and get transaction data
        sheet = client.open_by_key(sheet_id)
        print(f"Successfully opened sheet: {sheet.title}")
        
        # Get transactions worksheet
        transactions_ws = sheet.worksheet("Transactions")
        data = transactions_ws.get_all_values()
        
        if len(data) <= 1:  # Only header or empty
            print("No transaction data found in the sheet")
            return False
        
        # Convert to DataFrame
        headers = data[0]
        df = pd.DataFrame(data[1:], columns=headers)
        
        # Convert data types
        df['Amount'] = pd.to_numeric(df['Amount'])
        df['Date'] = pd.to_datetime(df['Date'])
        df['Month'] = df['Date'].dt.strftime('%Y-%m')
        
        # Extract stock symbols and transaction types
        df['Stock'] = df['Description'].apply(extract_stock_symbol)
        df['TransactionType'] = df.apply(identify_transaction_type, axis=1)
        
        print(f"Loaded {len(df)} transactions")
        print(f"Generating visualizations in {output_dir}...")
        
        # 1. Portfolio Overview Pie Chart
        plt.figure(figsize=(12, 8))
        plt.subplot(121)
        # Get buy transactions for true allocation (negative values)
        buy_data = df[df['TransactionType'] == 'Buy']
        fund_allocation = buy_data.groupby('Account')['Amount'].sum().abs()
        fund_allocation.plot(kind='pie', autopct='%1.1f%%', startangle=90)
        plt.title('Investment Allocation by Fund', fontsize=14)
        plt.ylabel('')
        
        # Add stock allocation pie chart
        plt.subplot(122)
        stock_allocation = buy_data.groupby('Stock')['Amount'].sum().abs()
        # Only include top 10 stocks for readability
        top_stocks = stock_allocation.nlargest(10)
        if len(stock_allocation) > 10:
            top_stocks['Other'] = stock_allocation[10:].sum()
        
        top_stocks.plot(kind='pie', autopct='%1.1f%%', startangle=90)
        plt.title('Investment Allocation by Stock (Top 10)', fontsize=14)
        plt.ylabel('')
        
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, '1_portfolio_allocation.png'), dpi=300)
        plt.close()
        
        # 2. Stock Holdings Bar Chart
        stock_holdings = df.groupby('Stock')['Amount'].sum()
        # Only include stocks still held (negative overall balance)
        current_holdings = stock_holdings[stock_holdings < 0].sort_values()
        
        if not current_holdings.empty:
            plt.figure(figsize=(12, 10))
            ax = current_holdings.abs().sort_values(ascending=True).tail(15).plot(kind='barh')
            
            # Format y-axis with currency
            ax.xaxis.set_major_formatter(mtick.FuncFormatter(format_currency))
            
            # Add value labels to bars
            for i, v in enumerate(current_holdings.abs().sort_values(ascending=True).tail(15)):
                ax.text(v + (v * 0.01), i, format_currency(v, 0), va='center')
            
            plt.title('Current Stock Holdings (Top 15 by Value)', fontsize=14)
            plt.xlabel('Investment Amount')
            plt.grid(axis='x', linestyle='--', alpha=0.7)
            plt.tight_layout()
            plt.savefig(os.path.join(output_dir, '2_stock_holdings.png'), dpi=300)
            plt.close()
            
        # 3. Monthly Transaction Activity
        plt.figure(figsize=(14, 8))
        
        # Group by month and transaction type
        monthly_activity = df.pivot_table(
            index='Month', 
            columns='TransactionType',
            values='Amount',
            aggfunc='sum'
        ).fillna(0)
        
        # Adjust sign for better visualization
        if 'Buy' in monthly_activity.columns:
            monthly_activity['Buy'] = monthly_activity['Buy'].abs() * -1  # Make buys negative
        if 'Fee' in monthly_activity.columns:
            monthly_activity['Fee'] = monthly_activity['Fee'].abs() * -1  # Make fees negative
            
        # Plot stacked bar chart
        ax = monthly_activity.plot(
            kind='bar', 
            stacked=True,
            figsize=(14, 8)
        )
        
        # Format y-axis with currency
        ax.yaxis.set_major_formatter(mtick.FuncFormatter(format_currency))
        
        plt.title('Monthly Transaction Activity', fontsize=14)
        plt.xlabel('Month')
        plt.ylabel('Amount')
        plt.legend(title='Transaction Type')
        plt.grid(axis='y', linestyle='--', alpha=0.3)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, '3_monthly_activity.png'), dpi=300)
        plt.close()
        
        # 4. Cumulative Portfolio Growth
        # Calculate cumulative sum of all transactions by date
        df_sorted = df.sort_values('Date')
        df_sorted['Cumulative'] = df_sorted['Amount'].cumsum()
        
        # Group by month for smoother chart
        monthly_balance = df_sorted.groupby('Month')['Amount'].sum()
        cumulative_balance = monthly_balance.cumsum()
        
        plt.figure(figsize=(14, 8))
        ax = cumulative_balance.plot()
        
        # Add markers for key points
        ax.plot(cumulative_balance.index, cumulative_balance.values, 'o', 
                markersize=6, color='red', alpha=0.6)
        
        # Format y-axis with currency
        ax.yaxis.set_major_formatter(mtick.FuncFormatter(format_currency))
        
        # Add grid and styling
        plt.grid(linestyle='--', alpha=0.7)
        plt.title('Cumulative Portfolio Value Over Time', fontsize=14)
        plt.xlabel('Month')
        plt.ylabel('Portfolio Value')
        plt.xticks(rotation=45)
        
        # Fill area under curve
        ax.fill_between(cumulative_balance.index, 0, cumulative_balance.values, 
                       alpha=0.3, color='green')
        
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, '4_portfolio_growth.png'), dpi=300)
        plt.close()
        
        # 5. Fund Performance Comparison
        fund_performance = df.groupby('Account')['Amount'].sum().sort_values()
        
        plt.figure(figsize=(12, 8))
        ax = fund_performance.plot(kind='bar')
        
        # Format y-axis with currency
        ax.yaxis.set_major_formatter(mtick.FuncFormatter(format_currency))
        
        # Add value labels
        for i, v in enumerate(fund_performance):
            label_color = 'black' if v > 0 else 'white'
            ax.text(i, v + (0.01 * v if v > 0 else -0.05 * v), 
                   format_currency(v, 0), ha='center', va='bottom' if v > 0 else 'top',
                   color=label_color)
        
        plt.title('Fund Performance Comparison', fontsize=14)
        plt.xlabel('Fund')
        plt.ylabel('Net Value')
        plt.grid(axis='y', linestyle='--', alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, '5_fund_performance.png'), dpi=300)
        plt.close()
        
        # 6. Dividend Income Tracking
        if 'Dividend' in df['TransactionType'].values:
            dividend_data = df[df['TransactionType'] == 'Dividend']
            dividend_by_month = dividend_data.groupby('Month')['Amount'].sum()
            
            plt.figure(figsize=(14, 8))
            ax = dividend_by_month.plot(kind='bar', color='green', alpha=0.7)
            
            # Add line for cumulative dividends
            ax2 = ax.twinx()
            cumulative_dividends = dividend_by_month.cumsum()
            cumulative_dividends.plot(ax=ax2, marker='o', color='darkgreen', linewidth=2)
            
            # Add value labels for bars
            for i, v in enumerate(dividend_by_month):
                ax.text(i, v + (v * 0.02), format_currency(v, 0), ha='center')
            
            # Format axes with currency
            ax.yaxis.set_major_formatter(mtick.FuncFormatter(format_currency))
            ax2.yaxis.set_major_formatter(mtick.FuncFormatter(format_currency))
            
            # Labels and styling
            ax.set_title('Dividend Income by Month', fontsize=14)
            ax.set_xlabel('Month')
            ax.set_ylabel('Monthly Dividend')
            ax2.set_ylabel('Cumulative Dividends')
            ax.grid(axis='y', linestyle='--', alpha=0.3)
            
            # Add legend
            from matplotlib.lines import Line2D
            legend_elements = [
                Line2D([0], [0], color='green', lw=0, marker='s', markersize=10, alpha=0.7, label='Monthly Dividends'),
                Line2D([0], [0], color='darkgreen', lw=2, marker='o', markersize=6, label='Cumulative Dividends')
            ]
            ax.legend(handles=legend_elements, loc='upper left')
            
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig(os.path.join(output_dir, '6_dividend_income.png'), dpi=300)
            plt.close()
            
            # Also create pie chart of dividend sources
            plt.figure(figsize=(12, 8))
            dividend_by_stock = dividend_data.groupby('Stock')['Amount'].sum().sort_values(ascending=False)
            top_dividend_stocks = dividend_by_stock.head(8)
            if len(dividend_by_stock) > 8:
                top_dividend_stocks['Other'] = dividend_by_stock[8:].sum()
                
            top_dividend_stocks.plot(kind='pie', autopct='%1.1f%%', startangle=90)
            plt.title('Dividend Income by Stock', fontsize=14)
            plt.ylabel('')
            plt.tight_layout()
            plt.savefig(os.path.join(output_dir, '7_dividend_sources.png'), dpi=300)
            plt.close()
        
        # 7. Transaction Count by Type
        transaction_counts = df['TransactionType'].value_counts()
        
        plt.figure(figsize=(10, 8))
        ax = transaction_counts.plot(kind='bar')
        
        # Add value labels
        for i, v in enumerate(transaction_counts):
            ax.text(i, v + 0.5, str(v), ha='center')
        
        plt.title('Number of Transactions by Type', fontsize=14)
        plt.xlabel('Transaction Type')
        plt.ylabel('Count')
        plt.grid(axis='y', linestyle='--', alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, '8_transaction_counts.png'), dpi=300)
        plt.close()
        
        print(f"Successfully created 8 visualization charts in {output_dir}/")
        return True
        
    except Exception as e:
        print(f"Error creating visualizations: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    parser = argparse.ArgumentParser(description='Generate investment visualizations from Google Sheets data')
    parser.add_argument('sheet_id', help='Google Sheet ID (from the sheet URL)')
    parser.add_argument('--output', default='investment_charts', help='Directory to save generated charts')
    parser.add_argument('--creds', default='google_credentials.json', help='Path to Google API credentials file')
    
    args = parser.parse_args()
    
    if not os.path.isfile(args.creds):
        print(f"Error: Google credentials file '{args.creds}' not found")
        return
    
    success = create_investment_visualizations(args.sheet_id, args.output, args.creds)
    
    if success:
        print("Visualization complete! Charts saved to output directory.")
    else:
        print("Failed to create visualizations.")

if __name__ == "__main__":
    main()

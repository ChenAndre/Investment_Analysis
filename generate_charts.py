import argparse
import gspread
import pandas as pd
import matplotlib.pyplot as plt
from google.oauth2.service_account import Credentials
import os

def generate_charts(sheet_id, creds_path='google_credentials.json', output_dir='charts'):
    """Generate charts from Google Sheet data and save them as image files"""
    # Setup credentials
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file(creds_path, scopes=scope)
    client = gspread.authorize(creds)
    
    # Open sheet
    try:
        sheet = client.open_by_key(sheet_id)
        print(f"Successfully opened sheet with ID: {sheet_id}")
    except Exception as e:
        print(f"Error opening sheet: {str(e)}")
        return False
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Get dashboard worksheet for chart data
    try:
        dashboard = sheet.worksheet('Dashboard')
        print("Accessing dashboard data for charts")
        
        # Get category data
        category_data = dashboard.get_values('D5:E100')  # Adjust range as needed
        if not category_data:
            print("No category data found")
            return False
            
        # Find where the monthly data starts
        monthly_header_cell = None
        for i, row in enumerate(dashboard.get_all_values()):
            if 'Monthly Data' in str(row):
                monthly_header_cell = f'D{i+2}'  # +2 because we need the header row and arrays are 0-indexed
                break
                
        if not monthly_header_cell:
            print("Could not find monthly data section")
            return False
            
        # Get monthly data
        monthly_range = f"{monthly_header_cell}:G100"  # Adjust as needed
        monthly_data = dashboard.get_values(monthly_range)
        
        # Process category data for pie chart
        category_df = pd.DataFrame(category_data[1:], columns=category_data[0])  # Skip header in first row
        category_df['Amount'] = pd.to_numeric(category_df['Amount'])
        
        # Create pie chart
        plt.figure(figsize=(10, 8))
        plt.pie(category_df['Amount'], labels=category_df['Category'], autopct='%1.1f%%', startangle=90)
        plt.axis('equal')
        plt.title('Spending by Category')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, 'category_spending.png'))
        plt.close()
        
        # Process monthly data for line chart
        if len(monthly_data) > 1:
            monthly_df = pd.DataFrame(monthly_data[1:], columns=monthly_data[0])
            monthly_df['Spending'] = pd.to_numeric(monthly_df['Spending'])
            monthly_df['Income'] = pd.to_numeric(monthly_df['Income'])
            monthly_df['Net'] = pd.to_numeric(monthly_df['Net'])
            
            # Create line chart
            plt.figure(figsize=(12, 6))
            plt.plot(monthly_df['Month'], monthly_df['Income'], marker='o', label='Income')
            plt.plot(monthly_df['Month'], monthly_df['Spending'], marker='s', label='Spending')
            plt.plot(monthly_df['Month'], monthly_df['Net'], marker='^', label='Net')
            plt.xlabel('Month')
            plt.ylabel('Amount ($)')
            plt.title('Monthly Financial Trends')
            plt.legend()
            plt.grid(True)
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig(os.path.join(output_dir, 'monthly_trends.png'))
            plt.close()
        
        print(f"Charts generated successfully and saved to {output_dir}/")
        return True
        
    except Exception as e:
        print(f"Error generating charts: {str(e)}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Generate charts from financial data in Google Sheets')
    parser.add_argument('sheet_id', help='Google Sheet ID (from the URL)')
    parser.add_argument('--creds', default='google_credentials.json', help='Path to Google API credentials JSON file')
    parser.add_argument('--output', default='charts', help='Directory to save the generated charts')
    
    args = parser.parse_args()
    
    # Check if credentials file exists
    if not os.path.isfile(args.creds):
        print(f"Error: Google credentials file '{args.creds}' not found")
        return
    
    # Generate charts
    success = generate_charts(args.sheet_id, args.creds, args.output)
    
    if success:
        print("Charts generated successfully!")
    else:
        print("Failed to generate charts.")

if __name__ == "__main__":
    main()

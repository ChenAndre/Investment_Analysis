import os
import datetime
import json
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
from io import BytesIO
import base64
import csv

class FinancialTracker:
    def __init__(self, google_creds_path='google_credentials.json'):
        # Initialize Google Sheets
        self.initialize_google_sheets(google_creds_path)
        
        # Transaction categories mapping
        self.categories = {
            'Buy': ['Purchase', 'Accumulate', 'Long position', 'Strategic acquisition', 'Initial investment'],
            'Sell': ['Sell', 'Liquidate', 'Close position', 'Profit-taking', 'Strategic divestment'],
            'Dividend': ['Dividend', 'payout', 'income'],
            'Fee': ['Management fee', 'Administrative expense', 'Trading commission', 'Research', 'audit', 'Performance fee'],
            'Capital': ['Initial fund capital', 'deployment'],
            'Other': []
        }
        
    def initialize_google_sheets(self, creds_path):
        """Initialize Google Sheets API connection"""
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file(creds_path, scopes=scope)
        self.gc = gspread.authorize(creds)
        
    def create_financial_spreadsheet(self, sheet_name='My Financial Tracker', sheet_id=None):
        """Create a new Google Sheet for financial tracking or open existing one"""
        if sheet_id:
            try:
                # Try to open existing sheet by ID
                self.sheet = self.gc.open_by_key(sheet_id)
                print(f"Using existing sheet with ID: {sheet_id}")
            except gspread.exceptions.APIError:
                print(f"Error: Could not open sheet with ID: {sheet_id}")
                print("Creating a new sheet instead...")
                self.sheet = self.gc.create(sheet_name)
                print(f"Created new sheet: {sheet_name}")
        else:
            try:
                # Try to open existing sheet by name
                self.sheet = self.gc.open(sheet_name)
                print(f"Using existing sheet: {sheet_name}")
            except gspread.exceptions.SpreadsheetNotFound:
                # Create new sheet if not found
                self.sheet = self.gc.create(sheet_name)
                print(f"Created new sheet: {sheet_name}")
            
        # Check for and create necessary worksheets
        try:
            self.transactions_worksheet = self.sheet.worksheet("Transactions")
            print("Using existing Transactions worksheet")
        except gspread.exceptions.WorksheetNotFound:
            self.transactions_worksheet = self.sheet.add_worksheet(
                title="Transactions", 
                rows=1000, 
                cols=10
            )
            # Add headers to transactions sheet
            headers = [
                "Date", "Description", "Amount", "Category", 
                "Account", "Transaction ID", "Pending", "Merchant Name"
            ]
            self.transactions_worksheet.append_row(headers)
            print("Created Transactions worksheet")
            
        # Create categories worksheet
        try:
            self.categories_worksheet = self.sheet.worksheet("Categories")
            print("Using existing Categories worksheet")
        except gspread.exceptions.WorksheetNotFound:
            self.categories_worksheet = self.sheet.add_worksheet(
                title="Categories", 
                rows=100, 
                cols=2
            )
            # Add default categories
            self.categories_worksheet.append_row(["Category", "Keywords"])
            for category, keywords in self.categories.items():
                self.categories_worksheet.append_row([category, ", ".join(keywords)])
            print("Created Categories worksheet")
            
        # Create dashboard worksheet
        try:
            self.dashboard_worksheet = self.sheet.worksheet("Dashboard")
            print("Using existing Dashboard worksheet")
        except gspread.exceptions.WorksheetNotFound:
            self.dashboard_worksheet = self.sheet.add_worksheet(
                title="Dashboard", 
                rows=50, 
                cols=10
            )
            print("Created Dashboard worksheet")
    
    def import_csv_transactions(self, csv_file_path, date_format="%Y-%m-%d"):
        """
        Import transactions from a CSV file
        
        Expected CSV format:
        Date,Description,Amount,Account,Merchant,TransactionID,Pending
        2025-01-15,Purchase at Amazon,-45.67,Chase Checking,Amazon,tx_12345,No
        """
        try:
            # Get existing transaction IDs to avoid duplicates
            try:
                existing_transaction_ids = self.transactions_worksheet.col_values(6)[1:]  # Skip header
            except:
                existing_transaction_ids = []
            
            # Read the CSV file
            transactions = []
            with open(csv_file_path, 'r') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    transactions.append(row)
            
            # Process and add each transaction
            new_rows = 0
            for transaction in transactions:
                # Skip if transaction ID already exists
                transaction_id = transaction.get('TransactionID', '')
                if transaction_id in existing_transaction_ids:
                    continue
                
                # Parse date
                try:
                    # Try to parse the date with the provided format
                    date_str = transaction.get('Date', '')
                    date_obj = datetime.strptime(date_str, date_format)
                    formatted_date = date_obj.strftime("%Y-%m-%d")
                except ValueError:
                    # If parsing fails, use the original string
                    formatted_date = date_str
                
                # Get description and amount
                description = transaction.get('Description', '')
                try:
                    amount = float(transaction.get('Amount', 0))
                except ValueError:
                    # Handle amounts with currency symbols or commas
                    amount_str = transaction.get('Amount', '0').replace('$', '').replace(',', '')
                    amount = float(amount_str)
                
                # Get account and merchant
                account = transaction.get('Account', 'Unknown')
                merchant = transaction.get('Merchant', '')
                
                # Determine if transaction is pending
                pending = transaction.get('Pending', 'No')
                
                # Categorize the transaction
                category = self.categorize_transaction(description, merchant)
                
                # Format transaction row for Google Sheets
                row = [
                    formatted_date,
                    description,
                    amount,
                    category,
                    account,
                    transaction_id if transaction_id else f"csv_{new_rows}_{int(datetime.now().timestamp())}",
                    pending,
                    merchant
                ]
                
                self.transactions_worksheet.append_row(row)
                new_rows += 1
            
            print(f"Added {new_rows} new transactions to the sheet")
            
            # Update the dashboard after adding transactions
            if new_rows > 0:
                self.update_dashboard()
                
            return new_rows
            
        except Exception as e:
            print(f"Error importing CSV: {str(e)}")
            return 0
    
    def categorize_transaction(self, description, merchant_name):
        """Categorize a transaction based on its description and merchant"""
        description = description.lower() if description else ""
        merchant_name = merchant_name.lower() if merchant_name else ""
        
        # Get categories and keywords from Google Sheet
        try:
            category_data = self.categories_worksheet.get_all_values()[1:]  # Skip header
            for row in category_data:
                if len(row) < 2:
                    continue
                    
                category = row[0]
                keywords = [k.strip().lower() for k in row[1].split(',')]
                
                # Check if any keyword is in the description or merchant name
                for keyword in keywords:
                    if keyword and (keyword in description or keyword in merchant_name):
                        return category
        except:
            # If there's an error accessing the worksheet, use the default categories
            for category, keywords in self.categories.items():
                for keyword in keywords:
                    if keyword.lower() in description or keyword.lower() in merchant_name:
                        return category
                    
        # Handle income (positive amounts)
        if "income" in description.lower() or "deposit" in description.lower() or "payroll" in description.lower():
            return "Income"
            
        # Default category
        return "Other"
    
    def update_dashboard(self, batch_updates=True, delay_seconds=0.5):
        """
        Update the dashboard with investment charts and summaries
        
        Parameters:
        - batch_updates: Whether to use batched updates to reduce API calls
        - delay_seconds: Delay between batch updates to avoid rate limits
        """
        # Get all transactions
        transactions_data = self.transactions_worksheet.get_all_values()[1:]  # Skip header
        
        if not transactions_data:
            print("No transactions to analyze")
            return
            
        import time
            
        # Convert to pandas DataFrame for analysis
        df = pd.DataFrame(transactions_data, columns=[
            "Date", "Description", "Amount", "Category", 
            "Account", "Transaction ID", "Pending", "Merchant Name"
        ])
        
        # Convert amount to float
        df['Amount'] = df['Amount'].astype(float)
        
        # Convert date to datetime
        df['Date'] = pd.to_datetime(df['Date'])
        df['Month'] = df['Date'].dt.strftime('%Y-%m')
        
        # Extract stock symbols from descriptions using regex
        import re
        def extract_stock_symbol(description):
            match = re.search(r'\(([A-Z\.]+)\)', description)
            if match:
                return match.group(1)
            return None
            
        df['Stock'] = df['Description'].apply(extract_stock_symbol)
        
        # Prepare all updates in batches to minimize API calls
        all_updates = []
        all_formats = []
        
        # Clear existing dashboard - this is one API call
        self.dashboard_worksheet.clear()
        
        # Add title
        all_updates.append(('A1', 'Investment Dashboard'))
        all_formats.append(('A1', {'textFormat': {'bold': True, 'fontSize': 14}}))
        
        # Investment Portfolio Summary
        # Total Buy, Sell, Dividend, Fee, and Capital transactions
        total_buy = df[df['Category'] == 'Buy']['Amount'].sum()
        total_sell = df[df['Category'] == 'Sell']['Amount'].sum() 
        total_dividend = df[df['Category'] == 'Dividend']['Amount'].sum()
        total_fee = df[df['Category'] == 'Fee']['Amount'].sum()
        total_capital = df[df['Category'] == 'Capital']['Amount'].sum()
        
        # Calculate portfolio value
        portfolio_value = total_capital + total_buy + total_sell + total_dividend + total_fee
        
        # Update summary data
        all_updates.append(('A3', 'Portfolio Summary'))
        all_formats.append(('A3', {'textFormat': {'bold': True}}))
        
        # Add summary rows
        summary_updates = [
            ('A4', 'Total Capital Deployed:'),
            ('B4', f"${total_capital:,.2f}"),
            ('A5', 'Total Stock Purchases:'),
            ('B5', f"${abs(total_buy):,.2f}"),
            ('A6', 'Total Stock Sales:'),
            ('B6', f"${total_sell:,.2f}"),
            ('A7', 'Total Dividend Income:'),
            ('B7', f"${total_dividend:,.2f}"),
            ('A8', 'Total Fees:'),
            ('B8', f"${abs(total_fee):,.2f}"),
            ('A9', 'Current Portfolio Value:'),
            ('B9', f"${portfolio_value:,.2f}")
        ]
        
        all_updates.extend(summary_updates)
        
        # Fund Allocation
        fund_allocation = df.groupby('Account')['Amount'].sum().sort_values()
        
        all_updates.append(('A11', 'Fund Allocation'))
        all_formats.append(('A11', {'textFormat': {'bold': True}}))
        
        for i, (fund, amount) in enumerate(fund_allocation.items(), start=12):
            all_updates.append((f'A{i}', fund))
            all_updates.append((f'B{i}', f"${amount:,.2f}"))
        
        # Stock Holdings (Buy transactions minus Sell transactions)
        stock_transactions = df[df['Stock'].notna()].copy()
        stock_holdings = stock_transactions.groupby('Stock')['Amount'].sum().sort_values()
        
        row_offset = len(fund_allocation) + 14
        all_updates.append((f'A{row_offset}', 'Stock Holdings'))
        all_formats.append((f'A{row_offset}', {'textFormat': {'bold': True}}))
        
        for i, (stock, amount) in enumerate(stock_holdings.items(), start=row_offset+1):
            all_updates.append((f'A{i}', stock))
            all_updates.append((f'B{i}', f"${amount:,.2f}"))
        
        # Monthly Activity
        monthly_by_category = df.pivot_table(
            index='Month', 
            columns='Category', 
            values='Amount', 
            aggfunc='sum', 
            fill_value=0
        )
        
        # Add chart data section
        all_updates.append(('D3', 'Chart Data'))
        all_formats.append(('D3', {'textFormat': {'bold': True}}))
        
        # Monthly activity data for charts
        all_updates.append(('D4', 'Monthly Activity by Category'))
        
        # Create headers with all category columns
        headers = ['Month'] + list(monthly_by_category.columns)
        all_updates.append(('D5', [headers]))
        
        # Add monthly data rows
        monthly_rows = []
        for month, row in monthly_by_category.iterrows():
            monthly_rows.append([month] + list(row.values))
            
        if monthly_rows:
            all_updates.append(('D6', monthly_rows))
        
        # Stock allocation data for pie chart
        stock_offset = len(monthly_rows) + 8
        all_updates.append((f'D{stock_offset}', 'Stock Allocation'))
        all_updates.append((f'D{stock_offset+1}', [['Stock', 'Amount']]))
        
        stock_rows = []
        for stock, amount in stock_holdings.items():
            if amount < 0:  # Only show current holdings (negative values)
                stock_rows.append([stock, abs(amount)])
        
        if stock_rows:
            all_updates.append((f'D{stock_offset+2}', stock_rows))
        
        # Fund performance data
        fund_offset = stock_offset + len(stock_rows) + 4
        all_updates.append((f'D{fund_offset}', 'Fund Performance'))
        all_updates.append((f'D{fund_offset+1}', [['Fund', 'Value']]))
        
        fund_rows = []
        for fund, amount in fund_allocation.items():
            fund_rows.append([fund, amount])
            
        if fund_rows:
            all_updates.append((f'D{fund_offset+2}', fund_rows))
            
        # Dividend income by stock
        if 'Dividend' in df['Category'].values:
            dividend_data = df[df['Category'] == 'Dividend']
            dividend_by_stock = dividend_data.groupby('Stock')['Amount'].sum().sort_values(ascending=False)
            
            div_offset = fund_offset + len(fund_rows) + 4
            all_updates.append((f'D{div_offset}', 'Dividend Income by Stock'))
            all_updates.append((f'D{div_offset+1}', [['Stock', 'Dividend']]))
            
            div_rows = []
            for stock, amount in dividend_by_stock.items():
                if pd.notna(stock):
                    div_rows.append([stock, amount])
                    
            if div_rows:
                all_updates.append((f'D{div_offset+2}', div_rows))
        
        # Execute all updates in efficient batches
        if batch_updates:
            # Process in batches of 10 updates
            batch_size = 10
            update_batches = [all_updates[i:i+batch_size] for i in range(0, len(all_updates), batch_size)]
            
            print(f"Updating dashboard in {len(update_batches)} batches...")
            
            for i, batch in enumerate(update_batches):
                print(f"Processing batch {i+1}/{len(update_batches)}...")
                # Create a batch update
                batch_data = {}
                for cell, value in batch:
                    batch_data[cell] = value
                
                # Execute batch update
                self.dashboard_worksheet.batch_update(batch_data)
                
                # Sleep between batches to avoid rate limits
                if i < len(update_batches) - 1 and delay_seconds > 0:  # Don't sleep after the last batch
                    time.sleep(delay_seconds)
            
            # Apply formats in a single batch if possible
            format_data = {}
            for cell, format_value in all_formats:
                format_data[cell] = format_value
            
            if format_data:
                for cell, format_value in all_formats:
                    try:
                        self.dashboard_worksheet.format(cell, format_value)
                        time.sleep(0.1)  # Small delay between format operations
                    except Exception as e:
                        print(f"Warning: Could not format cell {cell}: {str(e)}")
        else:
            # Execute updates individually (slower but more reliable)
            for cell, value in all_updates:
                try:
                    self.dashboard_worksheet.update(cell, value)
                    time.sleep(0.1)  # Small delay to avoid rate limits
                except Exception as e:
                    print(f"Warning: Could not update cell {cell}: {str(e)}")
            
            # Apply formats
            for cell, format_value in all_formats:
                try:
                    self.dashboard_worksheet.format(cell, format_value)
                    time.sleep(0.1)  # Small delay between format operations
                except Exception as e:
                    print(f"Warning: Could not format cell {cell}: {str(e)}")
        
        print("Investment dashboard updated successfully")

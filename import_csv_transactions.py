import os
import argparse
from csv_financial_tracker import FinancialTracker
import csv
import time
from datetime import datetime

def import_with_batching(csv_file_path, sheet_name=None, sheet_id=None, creds='google_credentials.json', 
                         date_format='%Y-%m-%d', batch_size=10, delay=1.0):
    """
    Import transactions from CSV with batched processing to avoid API rate limits
    
    Parameters:
    - csv_file_path: Path to the CSV file
    - sheet_name: Name of the Google Sheet (if sheet_id not provided)
    - sheet_id: ID of the specific Google Sheet to use
    - creds: Path to Google API credentials
    - date_format: Format of dates in the CSV
    - batch_size: Number of transactions to process in each batch
    - delay: Delay in seconds between API write batches
    """
    if not os.path.isfile(csv_file_path):
        print(f"Error: CSV file '{csv_file_path}' not found")
        return False
    
    if not os.path.isfile(creds):
        print(f"Error: Google credentials file '{creds}' not found")
        return False
    
    # Initialize the tracker
    tracker = FinancialTracker(google_creds_path=creds)
    
    # Create or open the spreadsheet
    tracker.create_financial_spreadsheet(sheet_name=sheet_name, sheet_id=sheet_id)
    
    # Read all transactions from CSV
    transactions = []
    with open(csv_file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            transactions.append(row)
    
    print(f"Read {len(transactions)} transactions from CSV")
    
    # Get existing transaction IDs to avoid duplicates
    try:
        existing_transaction_ids = tracker.transactions_worksheet.col_values(6)[1:]  # Skip header
    except Exception as e:
        print(f"Error reading existing transaction IDs: {str(e)}")
        existing_transaction_ids = []
    
    # Filter out transactions that already exist
    new_transactions = [t for t in transactions if t.get('TransactionID', '') not in existing_transaction_ids]
    print(f"Found {len(new_transactions)} new transactions to import")
    
    if not new_transactions:
        print("No new transactions to import")
        # Print the sheet URL and ID for reference
        try:
            sheet_url = tracker.sheet.url
            sheet_id = tracker.sheet.id
            print(f"Google Sheet URL: {sheet_url}")
            print(f"Google Sheet ID: {sheet_id}")
        except:
            pass
        return True
    
    # Process transactions in batches
    batches = [new_transactions[i:i+batch_size] for i in range(0, len(new_transactions), batch_size)]
    print(f"Split imports into {len(batches)} batches of up to {batch_size} transactions each")
    
    total_imported = 0
    for i, batch in enumerate(batches):
        print(f"Processing batch {i+1}/{len(batches)} with {len(batch)} transactions...")
        
        # Prepare rows for batch upload
        rows_to_add = []
        for transaction in batch:
            # Parse date
            try:
                date_str = transaction.get('Date', '')
                date_obj = datetime.strptime(date_str, date_format)
                formatted_date = date_obj.strftime("%Y-%m-%d")
            except ValueError:
                formatted_date = date_str
            
            # Get description and amount
            description = transaction.get('Description', '')
            try:
                amount = float(transaction.get('Amount', 0))
            except ValueError:
                amount_str = transaction.get('Amount', '0').replace('$', '').replace(',', '')
                amount = float(amount_str)
            
            # Get account and merchant
            account = transaction.get('Account', 'Unknown')
            merchant = transaction.get('Merchant', '')
            
            # Determine if transaction is pending
            pending = transaction.get('Pending', 'No')
            
            # Categorize the transaction
            category = tracker.categorize_transaction(description, merchant)
            
            # Transaction ID
            transaction_id = transaction.get('TransactionID', f"csv_{i}_{int(datetime.now().timestamp())}")
            
            # Create row
            row = [
                formatted_date,
                description,
                amount,
                category,
                account,
                transaction_id,
                pending,
                merchant
            ]
            
            rows_to_add.append(row)
        
        # Batch update to Google Sheets
        try:
            # Use batch update approach
            # First determine the next empty row
            try:
                existing_rows = len(tracker.transactions_worksheet.get_all_values())
            except:
                existing_rows = 1  # Just the header row
            
            # Add rows to sheet
            if rows_to_add:
                tracker.transactions_worksheet.append_rows(rows_to_add)
                total_imported += len(rows_to_add)
                print(f"Added {len(rows_to_add)} transactions (total: {total_imported})")
            
            # Sleep to avoid rate limits
            if i < len(batches) - 1:  # Don't sleep after the last batch
                print(f"Waiting {delay} seconds before next batch...")
                time.sleep(delay)
                
        except Exception as e:
            print(f"Error in batch {i+1}: {str(e)}")
            print("Continuing with next batch...")
    
    print(f"Successfully imported {total_imported} transactions")
    
    # Only update dashboard at the end, not for each batch
    if total_imported > 0:
        print("Updating dashboard with new data...")
        try:
            tracker.update_dashboard()
            print("Dashboard updated successfully")
        except Exception as e:
            print(f"Error updating dashboard: {str(e)}")
            print("You may need to manually refresh the dashboard later")
    
    # Print the sheet URL and ID for easy access
    try:
        sheet_url = tracker.sheet.url
        sheet_id = tracker.sheet.id
        print(f"Google Sheet URL: {sheet_url}")
        print(f"Google Sheet ID: {sheet_id}")
        print("Save this ID for future use with --sheet_id parameter")
    except:
        print("Unable to get sheet URL - please check your Google Sheets")
    
    return True

def main():
    parser = argparse.ArgumentParser(description='Import financial transactions from CSV to Google Sheets with batching')
    parser.add_argument('csv_file', help='Path to the CSV file containing financial transactions')
    parser.add_argument('--sheet_name', help='Name of the Google Sheet to use (if sheet_id not provided)')
    parser.add_argument('--sheet_id', help='ID of the Google Sheet to use (from sheet URL)')
    parser.add_argument('--creds', default='google_credentials.json', help='Path to Google API credentials JSON file')
    parser.add_argument('--date_format', default='%Y-%m-%d', help='Format of dates in the CSV file (e.g., %%Y-%%m-%%d)')
    parser.add_argument('--batch_size', type=int, default=10, help='Number of transactions to process in each batch')
    parser.add_argument('--delay', type=float, default=1.0, help='Delay in seconds between API write batches')
    
    args = parser.parse_args()
    
    success = import_with_batching(
        args.csv_file, 
        sheet_name=args.sheet_name,
        sheet_id=args.sheet_id,
        creds=args.creds,
        date_format=args.date_format,
        batch_size=args.batch_size,
        delay=args.delay
    )
    
    if success:
        print("Import completed successfully!")
    else:
        print("Import failed.")

if __name__ == "__main__":
    main()

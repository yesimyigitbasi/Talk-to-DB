from flask import Flask, jsonify, request
import pymysql
import json
import requests
import os
from datetime import datetime
import threading
import time
import pandas as pd
from DatabaseUpdate import (
    update_all_tags,
    update_all_wp_categories,
    update_all_attributes,
    update_variants_bulk,
    update_products_bulk,
    sync_customers_and_users,
    delete_products_and_related
)
from JsonGetters import (
    get_all_tags,
    get_all_attributes,
    get_all_wp_categories,
    get_all_products,
    get_all_variants_bulk,
    get_all_customers,
    get_variant_info,
)
from JsonExtractors import (
    extract_tags_data,
    extract_attributes_data,
    extract_wp_categories_data,
    extract_products_bulk,
    extract_variants_bulk,
    extract_customers_data,
)

from SyncProgress import update_sync_progress, get_sync_progress, set_currently_syncing
app = Flask(__name__)

def safe_datetime(val, fallback=None):
    from datetime import datetime
    import pandas as pd
    if val is None or pd.isna(val):
        return fallback if fallback is not None else datetime.now()
    try:
        # First check if it's already a NaT
        if hasattr(val, '__class__') and 'NaT' in str(type(val)):
            return fallback if fallback is not None else datetime.now()
        
        dt = pd.to_datetime(val, errors='coerce')
        if pd.isna(dt) or str(dt) == 'NaT' or dt is pd.NaT:
            return fallback if fallback is not None else datetime.now()
        
        # Always return a native Python datetime, not pandas Timestamp
        if isinstance(dt, pd.Timestamp):
            try:
                return dt.to_pydatetime()
            except (ValueError, OverflowError):
                return fallback if fallback is not None else datetime.now()
        return dt
    except Exception:
        return fallback if fallback is not None else datetime.now()


@app.route('/sync_products', methods=['GET'])
def sync_products():
    """API endpoint to sync the products and related entities from Odoo API to MySQL database."""
    try:
        # set_currently_syncing(True)
        # tags_json = get_all_tags()
        # if tags_json:
        #     tags_data = extract_tags_data(tags_json)
        #     update_all_tags(tags_data)

        # categories_json = get_all_wp_categories()
        # if categories_json:
        #     categories_data = extract_wp_categories_data(categories_json)
        #     update_all_wp_categories(categories_data)

        # attributes_json = get_all_attributes()
        # if attributes_json:
        #     attributes_data = extract_attributes_data(attributes_json)
        #     update_all_attributes(attributes_data)

        products_json = get_all_products()
        if not products_json:
            return jsonify({"error": "Couldn't fetch products from Odoo"}), 404

        products_data = extract_products_bulk(products_json)
        products_data = [p for p in products_data if p.get("is_synced", False)]
        # Filter products by write_date > 2025-05-29
        filter_date = datetime(2025, 6, 29)
        products_data = [p for p in products_data if p.get("write_date") and datetime.fromisoformat(p["write_date"]) > filter_date]

        chunk_size = 100
        total_products = len(products_data)
        for i in range(0, total_products, chunk_size):
            chunk = products_data[i:i + chunk_size]
            update_products_bulk(chunk)
            update_sync_progress(
                entity="products",
                total_count=total_products,
                last_synced_index=i + len(chunk),
                last_synced_matta_id=chunk[-1]["MattaId"] if chunk else None
            )

        variants_json = get_all_variants_bulk()
        if not variants_json:
            return jsonify({"error": "Couldn't fetch variants from Odoo"}), 404

        variants_data = extract_variants_bulk(variants_json)
        # Filter variants by write_date > 2025-05-29
        variants_data = [v for v in variants_data if v.get("write_date") and datetime.fromisoformat(v["write_date"]) > filter_date]

        total_variants = len(variants_data)
        for i in range(0, total_variants, chunk_size):
            chunk = variants_data[i:i + chunk_size]
            update_variants_bulk(chunk)
            update_sync_progress(
                entity="variants",
                total_count=total_variants,
                last_synced_index=i + len(chunk),
                last_synced_matta_id=chunk[-1]["MattaId"] if chunk else None
            )

        return jsonify({"message": "Product, variant, and customer synchronization complete!"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        set_currently_syncing(False)
    

@app.route('/sync_progress', methods=['GET'])
def get_sync_progress_endpoint():
    """API endpoint to retrieve the current sync progress."""
    try:
        sync_progress_file = "sync_progress.json"
        if not os.path.exists(sync_progress_file):
            return jsonify({"error": "Sync progress file does not exist."}), 404

        with open(sync_progress_file, "r") as file:
            progress = json.load(file)
        return jsonify(progress), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

def trigger_resume_sync():
    """Trigger the /resume_sync endpoint locally."""
    try:
        response = requests.get("http://127.0.0.1:5000/resume_sync", timeout=3000)
        print(f"Triggered /resume_sync, status: {response.status_code}, response: {response.text}")
    except Exception as ex:
        print(f"Failed to trigger /resume_sync: {ex}")

@app.route('/resume_sync', methods=['GET'])
def resume_sync():
    """API endpoint to resume syncing products and related entities from the last synced product."""
    try:
        set_currently_syncing(True)
        progress = get_sync_progress()
        if not progress:
            return jsonify({"error": "No sync progress found."}), 404

        product_progress = progress.get("products", {})
        last_synced_product_index = product_progress.get("last_synced_index", 0)

        variant_progress = progress.get("variants", {})
        last_synced_variant_index = variant_progress.get("last_synced_index", 0)

        customer_progress = progress.get("customers", {})
        last_synced_customer_index = customer_progress.get("last_synced_index", 0)
        total_customers = customer_progress.get("remaining_customers", 0) + last_synced_customer_index
        products_json = get_all_products()
        if not products_json:
            return jsonify({"error": "Couldn't fetch products from Odoo"}), 404

        products_data = extract_products_bulk(products_json)
        # Filter products by write_date > 2025-05-29
        filter_date = datetime(2025, 6, 29)
        products_data = [p for p in products_data if p.get("write_date") and datetime.fromisoformat(p["write_date"]) > filter_date]

        variants_json = get_all_variants_bulk()
        if not variants_json:
            return jsonify({"error": "Couldn't fetch variants from Odoo"}), 404

        variants_data = extract_variants_bulk(variants_json)
        # Filter variants by write_date > 2025-05-29
        variants_data = [v for v in variants_data if v.get("write_date") and datetime.fromisoformat(v["write_date"]) > filter_date]

        chunk_size = 50
        total_products = len(products_data)
        for i in range(last_synced_product_index, total_products, chunk_size):
            chunk = products_data[i:i + chunk_size]
            update_products_bulk(chunk)
            update_sync_progress(
                entity="products",
                total_count=total_products,
                last_synced_index=i + len(chunk),
                last_synced_matta_id=chunk[-1]["MattaId"] if chunk else None
            )

        total_variants = len(variants_data)
        for i in range(last_synced_variant_index, total_variants, chunk_size):
            chunk = variants_data[i:i + chunk_size]
            update_variants_bulk(chunk)
            update_sync_progress(
                entity="variants",
                total_count=total_variants,
                last_synced_index=i + len(chunk),
                last_synced_matta_id=chunk[-1]["MattaId"] if chunk else None
            )
        return jsonify({"message": "Product, variant, and customer synchronization resumed and completed!"}), 200

    except Exception as e:
        error_str = str(e)
        return jsonify({"error": error_str}), 500

    finally:
        set_currently_syncing(False)


# def periodic_sync_check():
#     """Periodically check if syncing is inactive and trigger resume_sync if needed."""
#     while True:
#         try:
#             with open("sync_progress.json", "r") as file:
#                 progress = json.load(file)

#             if not progress.get("CurrentlySyncing", False):
#                 print("Currently not syncing. Triggering resume_sync...")
#                 trigger_resume_sync()

#         except Exception as e:
#             print(f"Error during periodic sync check: {e}")

#         time.sleep(10)  # Wait for 5 minutes
# # Start the periodic sync check in a separate thread
# threading.Thread(target=periodic_sync_check, daemon=True).start()

# Add this new endpoint to Sync.py

import uuid

@app.route('/sync_customers_users', methods=['GET'])
def sync_customers_and_users_endpoint():
    """API endpoint to sync customers from Odoo and users from WordPress and link them together."""
    try:
        set_currently_syncing(True)
        # Define the filter date (like sync_products)
        filter_date = datetime(2025, 6, 29)
        # Get the current progress
        progress = get_sync_progress()
        if not progress or "customers" not in progress:
            return jsonify({"error": "No sync progress found."}), 404
        customers_count, users_count = sync_customers_and_users(filter_date)
        link_customers_to_users_endpoint(filter_date)
        return jsonify({
            "message": f"Customer and user synchronization complete! Processed {customers_count} customers and {users_count} users.",
            "customers_count": customers_count,
            "users_count": users_count
        }), 200
    except Exception as e:
        error_str = str(e)
        return jsonify({"error": error_str}), 500
    finally:
        set_currently_syncing(False)

@app.route('/link_customers_to_users', methods=['GET'])
def link_customers_to_users_endpoint(filter_date=None):
    """API endpoint to link customers to users by updating only the UserId field in Customers table."""
    try:
        set_currently_syncing(True)
        # Use the same filter date as in sync_customers_and_users if not provided
        if filter_date is None:
            filter_date = datetime(2025, 6, 29)
        # Database connection parameters from Query.txt
        MYSQL_HOST = '168.119.180.170'  # From Query.txt
        MYSQL_PORT = 3306
        MYSQL_USER = 'ems_admin'
        MYSQL_PASSWORD = '3hhiAX3gXVfvCk'
        
        connection_user_service = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database="user_service"  # From Query.txt - Customers table
        )
        
        connection_identity_service = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database="identity_service"  # From Query.txt - AppUsers table
        )
        
        customers_updated = 0
        batch_size = 10000
        
        try:
            # Fetch all users with email addresses and filter by CreatedOn > filter_date
            with connection_identity_service.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute("""
                    SELECT Id, Email, UserName, CreatedOn
                    FROM AppUsers
                    WHERE Email IS NOT NULL AND Email != '' AND CreatedOn > %s
                """, (filter_date,))
                users = cursor.fetchall()
                print(f"Found {len(users)} users with email addresses and CreatedOn > {filter_date}")
            
            # Create a dictionary of users by email for quick lookup
            users_by_email = {user["Email"].lower(): user for user in users if user["Email"]}
            
            # Process customers in batches to avoid memory issues
            with connection_user_service.cursor(pymysql.cursors.DictCursor) as cursor:
                # Get total customer count for progress tracking
                cursor.execute("SELECT COUNT(*) AS total FROM Customers WHERE Email IS NOT NULL AND Email != ''")
                total_count = cursor.fetchone()["total"]
                print(f"Found {total_count} total customers with email addresses")
                
                # Process in batches
                offset = 0
                while True:
                    cursor.execute("""
                        SELECT Id, Email, MattaId 
                        FROM Customers 
                        WHERE Email IS NOT NULL AND Email != ''
                        LIMIT %s OFFSET %s
                    """, (batch_size, offset))
                    
                    customers_batch = cursor.fetchall()
                    if not customers_batch:
                        break
                    
                    print(f"Processing batch of {len(customers_batch)} customers (offset: {offset})")
                    
                    # Update customers with UserId where email matches a user
                    with connection_user_service.cursor() as update_cursor:
                        update_customer_query = """
                            UPDATE Customers
                            SET UserId = %s,
                                LastModifiedBy = %s,
                                LastModifiedDate = %s
                            WHERE Id = %s
                        """
                        
                        for customer in customers_batch:
                            if customer["Email"] and customer["Email"].lower() in users_by_email:
                                user = users_by_email[customer["Email"].lower()]
                                update_cursor.execute(update_customer_query, (
                                    user["Id"],
                                    "OdooSyncPy-LinkOnly629",
                                    datetime.now(),
                                    customer["Id"]
                                ))
                                customers_updated += 1
                                
                                if customers_updated % 100 == 0:
                                    print(f"Updated {customers_updated} customers with user IDs")
                        
                        connection_user_service.commit()
                    
                    # Update progress
                    update_sync_progress(
                        entity="customers",
                        total_count=total_count,
                        last_synced_index=offset + len(customers_batch),
                        last_synced_matta_id=customers_batch[-1]["MattaId"] if customers_batch else None
                    )
                    
                    offset += batch_size
                
                print(f"Total customers updated with user IDs: {customers_updated}")
                
        finally:
            connection_user_service.close()
            connection_identity_service.close()
        
        return jsonify({
            "message": "Customer linking complete!",
            "customers_updated": customers_updated
        }), 200
        
    except Exception as e:
        error_str = str(e)
        print(f"Error linking customers to users: {error_str}")
        return jsonify({"error": error_str}), 500
        
    finally:
        set_currently_syncing(False)


@app.route('/update_all_stock', methods=['GET'])
def update_all_stock():
    """Endpoint to update IsOnStock for all variants in the database."""
    try:
        set_currently_syncing(True)
        # Prepare DB connection
        MYSQL_HOST = '168.119.180.170'
        MYSQL_PORT = 3306
        MYSQL_USER = 'ems_admin'
        MYSQL_PASSWORD = '3hhiAX3gXVfvCk'
        MYSQL_DB = 'catalog_service'
        connection = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB
        )
        updated = 0
        not_found = []
        failed = []
        try:
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                # First, fetch all SKUs from the database
                cursor.execute("SELECT SKU, MattaId FROM ProductVariants WHERE SKU IS NOT NULL AND SKU != ''")
                all_variants = cursor.fetchall()
                skus = [row['SKU'] for row in all_variants]
                sku_mattaid_map = {row['SKU']: row['MattaId'] for row in all_variants}
                
                if not skus:
                    return jsonify({'error': 'No SKUs found in the database'}), 400
                
                # Start from the 91,700th variant in the list (index 91699 since 0-based)
                start_index = 91699  # Resume from this position in the list
                
                print(f"Total variants found: {len(skus)}")
                print(f"Starting stock update from variant #{start_index + 1}, processing {len(skus) - start_index} remaining variants...")
                
                # Process in batches to improve performance
                batch_size = 50  # Process 50 variants at once
                
                for batch_start in range(start_index, len(skus), batch_size):
                    batch_end = min(batch_start + batch_size, len(skus))
                    batch_skus = skus[batch_start:batch_end]
                    batch_matta_ids = [sku_mattaid_map[sku] for sku in batch_skus]
                    
                    print(f"Processing batch {(batch_start - start_index)//batch_size + 1}: variants {batch_start+1}-{batch_end} (MattaIds: {batch_matta_ids[0]}-{batch_matta_ids[-1]})")
                    
                    # Process each variant in the batch
                    for idx_in_batch, sku in enumerate(batch_skus):
                        idx = batch_start + idx_in_batch + 1  # Actual position in the full list
                        matta_id = sku_mattaid_map.get(sku)
                        if not matta_id:
                            not_found.append({'sku': sku, 'matta_id': None, 'reason': 'MattaId not found'})
                            continue
                        try:
                            variant_info = get_variant_info(matta_id)
                            # Accept both 'isOnStock' and 'IsOnStock' for robustness
                            is_on_stock = None
                            if variant_info:
                                is_on_stock = variant_info.get('isOnStock')
                                if is_on_stock is None:
                                    is_on_stock = variant_info.get('IsOnStock')
                            if is_on_stock is None:
                                failed.append({'sku': sku, 'matta_id': matta_id, 'reason': 'isOnStock not in variant_info'})
                                continue
                            # Update IsOnStock in DB
                            cursor.execute(
                                """
                                UPDATE ProductVariants
                                SET IsOnStock = %s, LastModifiedDate = NOW(), LastModifiedBy = 'OdooSyncPy-StockUpdate'
                                WHERE SKU = %s
                                """,
                                (is_on_stock, sku)
                            )
                            if cursor.rowcount > 0:
                                updated += 1
                        except Exception as ex:
                            failed.append({'sku': sku, 'matta_id': matta_id, 'reason': str(ex)})
                        
                        # Print progress every 100 items
                        if idx % 100 == 0:
                            print(f"Processed {idx}/{len(skus)}: {updated} updated, {len(failed)} failed")
                    
                    # Commit after each batch to avoid long transactions
                    connection.commit()
                    print(f"Committed batch {batch_start//batch_size + 1}, total progress: {batch_end}/{len(skus)}")
                    
                    # Optional: Add a small delay between batches to reduce API load
                    import time
                    time.sleep(0.1)  # 100ms delay between batches
        finally:
            connection.close()
        return jsonify({
            'updated': updated,
            'not_found': not_found,
            'failed': failed,
            'total': len(skus),
            'processed': updated + len(not_found) + len(failed)
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        set_currently_syncing(False)


@app.route('/sync_orders_from_excel', methods=['POST'])
def sync_orders_from_excel():
    """Endpoint to sync orders from an uploaded Excel file to the Orders table with a date filter. Stops on first failure and returns error details."""
    try:
        set_currently_syncing(True)
        if 'file' not in request.files:
            return jsonify({'error': 'No file part in the request'}), 400
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No selected file'}), 400
        # Get filter_date from request (form or query param), else use default
        filter_date_str = request.form.get('filter_date') or request.args.get('filter_date')
        if filter_date_str:
            try:
                filter_date = datetime.fromisoformat(filter_date_str)
            except Exception:
                return jsonify({'error': 'Invalid filter_date format, use YYYY-MM-DD'}), 400
        else:
            filter_date = datetime(2024, 6, 1)  # Default date if not provided
        # Read Excel file into DataFrame, skip header row
        df = pd.read_excel(file, header=0)
        # Map columns as per OrderSyncQuery.txt
        column_map = {
            'MataaOrderNumber': 'MataaOrderNumber',
            'CustomerFullName': 'CustomerFullName',
            'CustomerID': 'CustomerID',
            'mataaState': 'mataaState',
            'TotalAmount': 'TotalAmount',
            'Currency': 'Currency',
            'PaymentMethod': 'PaymentMethod',
            'Delivery': 'Delivery',
            'MataaCoupons': 'MataaCoupons',
            'deliverystatus': 'deliverystatus',
            'EcommerceStatus': 'EcommerceStatus',
            'ItemQuantity': 'ItemQuantity',
            'MattaId': 'MattaId',
            'CreatedBy': 'CreatedBy',
            'CreatedDate': 'CreatedDate',
            'LastModifiedBy': 'LastModifiedBy',
            'LastModifiedDate': 'LastModifiedDate',
        }
        # Ensure all required columns exist
        for col in column_map.keys():
            if col not in df.columns:
                return jsonify({'error': f'Missing column in Excel: {col}'}), 400
        # Filter by CreatedDate > filter_date
        df['CreatedDate'] = pd.to_datetime(df['CreatedDate'], errors='coerce')
        filtered_df = df[df['CreatedDate'] > filter_date]
        if filtered_df.empty:
            return jsonify({'message': 'No orders to sync after the specified date.'}), 200
        # DB connection params from OrderSyncQuery.txt
        MYSQL_HOST = '168.119.180.170'
        MYSQL_PORT = 3306
        MYSQL_USER = 'ems_admin'
        MYSQL_PASSWORD = '3hhiAX3gXVfvCk'
        MYSQL_DB = 'order_service'
        connection = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB
        )
        
        # Create connection to user_service database for customer lookup
        connection_user_service = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database="user_service"
        )
        
        # Build a dictionary to map CustomerId (MattaId) to Customer GUID
        customer_guid_map = {}
        try:
            with connection_user_service.cursor() as cursor:
                cursor.execute("SELECT Id, MattaId FROM Customers WHERE MattaId IS NOT NULL")
                for row in cursor.fetchall():
                    customer_guid_map[row[1]] = row[0]  # MattaId -> Id (GUID)
        finally:
            connection_user_service.close()
        
        print(f"Loaded {len(customer_guid_map)} customers for GUID lookup")
        inserted = 0
        updated = 0
        skipped = 0
        customers_found = 0
        customers_not_found = 0
        try:
            with connection.cursor() as cursor:
                # Helper to safely get int or default if NaN
                def safe_int(val, default):
                    try:
                        if pd.isna(val):
                            return default
                        return int(val)
                    except (ValueError, TypeError):
                        return default
                # Helper to safely get float or default if NaN
                def safe_float(val, default):
                    try:
                        if pd.isna(val):
                            return default
                        return float(val)
                    except (ValueError, TypeError):
                        return default
                # Helper to safely get string or default if NaN
                def safe_str(val, default):
                    if pd.isna(val):
                        return default
                    return str(val)
                
                # Helper to safely get string from numeric (removes .0 from floats)
                def safe_str_from_numeric(val, default):
                    if pd.isna(val):
                        return default
                    if isinstance(val, float) and val.is_integer():
                        return str(int(val))
                    return str(val)                # Helper to safely convert datetime, returns fallback if NaT or invalid
                def safe_datetime(val, fallback=None):
                    from datetime import datetime
                    import pandas as pd
                    if val is None or pd.isna(val):
                        return fallback if fallback is not None else datetime.now()
                    try:
                        # First check if it's already a NaT
                        if hasattr(val, '__class__') and 'NaT' in str(type(val)):
                            return fallback if fallback is not None else datetime.now()
                        
                        dt = pd.to_datetime(val, errors='coerce')
                        if pd.isna(dt) or str(dt) == 'NaT' or dt is pd.NaT:
                            return fallback if fallback is not None else datetime.now()
                        
                        # Always return a native Python datetime, not pandas Timestamp
                        if isinstance(dt, pd.Timestamp):
                            try:
                                return dt.to_pydatetime()
                            except (ValueError, OverflowError):
                                return fallback if fallback is not None else datetime.now()
                        return dt
                    except Exception:
                        return fallback if fallback is not None else datetime.now()
                
                # Helper to get customer GUID from MattaId
                def get_customer_guid(customer_matta_id):
                    nonlocal customers_found, customers_not_found
                    if pd.isna(customer_matta_id):
                        customers_not_found += 1
                        return zero_uuid
                    try:
                        matta_id = int(customer_matta_id)
                        guid = customer_guid_map.get(matta_id, zero_uuid)
                        if guid != zero_uuid:
                            customers_found += 1
                        else:
                            customers_not_found += 1
                        return guid
                    except (ValueError, TypeError):
                        customers_not_found += 1
                        return zero_uuid
                for idx, row in filtered_df.iterrows():
                    try:
                        # Check if order exists by MataaOrderNumber
                        cursor.execute("SELECT Id FROM Orders WHERE MataaOrderNumber = %s", (safe_str_from_numeric(row['MataaOrderNumber'], ''),))
                        existing = cursor.fetchone()
                        zero_uuid = '00000000-0000-0000-0000-000000000000'
                        customer_guid = get_customer_guid(row.get('CustomerID'))
                        
                        print(f"Order exists: {existing is not None}, Customer GUID: {customer_guid}")
                        
                        # Debug the LastModifiedDate value
                        last_modified_date = safe_datetime(row['LastModifiedDate'])
                        created_date = safe_datetime(row['CreatedDate']) or datetime.now()
                        
                        if existing:
                            update_query = """
                            UPDATE Orders SET
                                CustomerFullName=%s, OperationOrderNumber=%s, mataaState=%s, TotalAmount=%s, Currency=%s, PaymentStatus=%s, PaymentMethod=%s, Delivery=%s, CustomerComments=%s, CourierName=%s, OrderReview=%s, CourierReview=%s, OrderAddressId=%s, MataaCoupons=%s, ShippingCost=%s, Tags=%s, customerNotes=%s, internalNotes=%s, deliverystatus=%s, EcommerceStatus=%s, ItemQuantity=%s, MattaId=%s, MattaParentId=%s, CreatedBy=%s, CreatedDate=%s, LastModifiedBy=%s, LastModifiedDate=%s, EntityState=%s, CustomerId=%s, ShippingMethod=%s, TotalAmountDiscounted=%s, TransactionSource=%s
                            WHERE MataaOrderNumber=%s
                            """
                            update_values = [
                                safe_str(row['CustomerFullName'], ''),
                                '',
                                safe_int(row['mataaState'], 1),
                                safe_float(row['TotalAmount'], 0.0),
                                safe_int(row['Currency'], 1),
                                1,
                                safe_int(row['PaymentMethod'], 1),
                                safe_int(row['Delivery'], None),
                                None,
                                None,
                                None,
                                None,
                                zero_uuid,
                                safe_str(row['MataaCoupons'], ''),
                                0.0,
                                None,
                                None,
                                None,
                                safe_str(row['deliverystatus'], ''),
                                safe_int(row['EcommerceStatus'], 4),
                                safe_float(row['ItemQuantity'], 0.0),
                                safe_int(row['MattaId'], None),
                                None,
                                safe_str(row['CreatedBy'], ''),
                                created_date,
                                "OdooSyncPy702",
                                last_modified_date,
                                1,
                                customer_guid,
                                0,
                                None,
                                None,
                                safe_str_from_numeric(row['MataaOrderNumber'], '')
                            ]
                            cursor.execute(update_query, update_values)
                            print(f"Update executed, rows affected: {cursor.rowcount}")
                            updated += 1
                        else:
                            print(f"Inserting new order: {safe_str_from_numeric(row['MataaOrderNumber'], '')}")
                            import uuid
                            order_id = str(uuid.uuid4())
                            print(f"Generated order ID: {order_id}")
                            print(f"Insert values - CreatedDate: {created_date}, LastModifiedDate: {last_modified_date}, LastModifiedBy: OdooSyncPy702")
                            values = [
                                order_id,
                                safe_str_from_numeric(row['MataaOrderNumber'], ''),
                                safe_str(row['CustomerFullName'], ''),
                                '',
                                safe_int(row['mataaState'], 1),
                                safe_float(row['TotalAmount'], 0.0),
                                safe_int(row['Currency'], 1),
                                1,
                                safe_int(row['PaymentMethod'], 1),
                                safe_int(row['Delivery'], None),
                                None,
                                None,
                                None,
                                None,
                                zero_uuid,
                                safe_str(row['MataaCoupons'], ''),
                                0.0,
                                None,
                                None,
                                None,
                                safe_str(row['deliverystatus'], ''),
                                safe_int(row['EcommerceStatus'], 4),
                                safe_float(row['ItemQuantity'], 0.0),
                                safe_int(row['MattaId'], None),
                                None,
                                safe_str(row['CreatedBy'], ''),
                                created_date,
                                "OdooSyncPy702",
                                last_modified_date,
                                1,
                                customer_guid,  # Use the looked up customer GUID
                                0,
                                None,
                                None
                            ]
                            insert_query = """
                            INSERT INTO Orders (
                                Id, MataaOrderNumber, CustomerFullName, OperationOrderNumber, mataaState, TotalAmount, Currency, PaymentStatus, PaymentMethod, Delivery, CustomerComments, CourierName, OrderReview, CourierReview, OrderAddressId, MataaCoupons, ShippingCost, Tags, customerNotes, internalNotes, deliverystatus, EcommerceStatus, ItemQuantity, MattaId, MattaParentId, CreatedBy, CreatedDate, LastModifiedBy, LastModifiedDate, EntityState, CustomerId, ShippingMethod, TotalAmountDiscounted, TransactionSource
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                            """
                            cursor.execute(insert_query, values)
                            print(f"Insert executed, rows affected: {cursor.rowcount}")
                            inserted += 1
                    except pymysql.err.IntegrityError as ie:
                        connection.rollback()
                        print(f"IntegrityError at order index {idx}: {str(ie)}")
                        return jsonify({
                            'error': str(ie),
                            'order_index': int(idx),
                            'order_data': row.to_dict()
                        }), 500
                    except Exception as ex:
                        connection.rollback()
                        print(f"General error at order index {idx}: {str(ex)}")
                        return jsonify({
                            'error': str(ex),
                            'order_index': int(idx),
                            'order_data': row.to_dict()
                        }), 500
                connection.commit()
                print(f"Transaction committed. Total processed: {len(filtered_df)}, Inserted: {inserted}, Updated: {updated}")
                
                # Verify some of the orders were actually inserted/updated
                with connection.cursor() as verify_cursor:
                    verify_cursor.execute("SELECT COUNT(*) FROM Orders WHERE LastModifiedBy = 'OdooSyncPy702'")
                    count_result = verify_cursor.fetchone()
                    print(f"Verification: Found {count_result[0]} orders with LastModifiedBy = 'OdooSyncPy702'")
        finally:
            connection.close()
        return jsonify({
            'inserted': inserted,
            'updated': updated,
            'skipped': skipped,
            'total': len(filtered_df),
            'customers_found': customers_found,
            'customers_not_found': customers_not_found,
            'message': f'Synced {inserted} orders from Excel. Updated: {updated}. Skipped: {skipped}. Customers found: {customers_found}, not found: {customers_not_found}.'
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        set_currently_syncing(False)
       

@app.route('/sync_orderdetails_from_excel', methods=['POST'])
def sync_orderdetails_from_excel():
    """Endpoint to sync order details from an uploaded Excel file and mapping file to the OrderDetails table."""
    import uuid
    try:
        set_currently_syncing(True)
        # Check for both files
        if 'orderdetails_file' not in request.files or 'mapping_file' not in request.files:
            return jsonify({'error': 'Both orderdetails_file and mapping_file are required'}), 400
        orderdetails_file = request.files['orderdetails_file']
        mapping_file = request.files['mapping_file']
        if orderdetails_file.filename == '' or mapping_file.filename == '':
            return jsonify({'error': 'Both files must be selected'}), 400
        # Read Excel files
        df_details = pd.read_excel(orderdetails_file, header=0)
        df_mapping = pd.read_excel(mapping_file, header=0)
        # Validate columns
        required_detail_cols = [
            'ProductMattaId', 'OrderLineReference', 'ImageUrl', 'ProductName', 'Desecription',
            'SKU', 'Quantity', 'priceperunit', 'TotalPrice', 'ItemState', 'CreatedDate',
            'LastModifiedBy', 'LastModifiedDate'
        ]
        required_mapping_cols = ['OrderLineReference', 'MattaId']
        for col in required_detail_cols:
            if col not in df_details.columns:
                return jsonify({'error': f'Missing column in OrderDetails Excel: {col}'}), 400
        for col in required_mapping_cols:
            if col not in df_mapping.columns:
                return jsonify({'error': f'Missing column in Mapping file: {col}'}), 400
        # Clean mapping: drop rows with NaN MattaId or OrderLineReference
        df_mapping_clean = df_mapping.dropna(subset=['OrderLineReference', 'MattaId'])
        # Create mapping dictionary - multiple order details can reference the same OrderLineReference
        mapping_dict = dict(zip(df_mapping_clean['OrderLineReference'], df_mapping_clean['MattaId']))
        # Get all unique MattaIds, filter out NaN and normalize to string without .0
        def normalize_order_number(val):
            if pd.isna(val):
                return None
            try:
                return str(int(val))
            except (ValueError, TypeError):
                return str(val).strip()
        unique_mattaids = set(normalize_order_number(m) for m in df_mapping_clean['MattaId'] if pd.notna(m))
        if not unique_mattaids:
            return jsonify({'error': 'No valid MattaId values found in mapping file.'}), 400
        # DB connection
        MYSQL_HOST = '168.119.180.170'
        MYSQL_PORT = 3306
        MYSQL_USER = 'ems_admin'
        MYSQL_PASSWORD = '3hhiAX3gXVfvCk'
        MYSQL_DB = 'order_service'
        connection = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB
        )
        # Build MataaOrderNumber -> OrderId (GUID) mapping
        mataaordernumber_to_orderid = {}
        try:
            with connection.cursor() as cursor:
                format_strings = ','.join(['%s'] * len(unique_mattaids))
                cursor.execute(f"SELECT Id, MataaOrderNumber FROM Orders WHERE MataaOrderNumber IN ({format_strings})", tuple(unique_mattaids))
                for row in cursor.fetchall():
                    mataaordernumber_to_orderid[str(row[1])] = row[0]
        except Exception as e:
            connection.close()
            return jsonify({'error': f'Failed to build MataaOrderNumber to OrderId mapping: {str(e)}'}), 500
        inserted = 0
        updated = 0
        skipped = 0
        try:
            with connection.cursor() as cursor:
                for idx, row in df_details.iterrows():
                    try:
                        order_line_ref = row['OrderLineReference']
                        
                        # Debug breakpoint for specific OrderLineReference
                        if order_line_ref == 'S10599':
                            print(f"DEBUG BREAKPOINT: Processing OrderLineReference S10599 at row {idx}")
                            print(f"Row data: {row.to_dict()}")
                            print(f"Mapping lookup: {order_line_ref} -> {mataaordernumber} -> cleaned: {mataaordernumber_str}")
                            print(f"Order ID found: {order_id}")
                            # Add breakpoint here if debugging in IDE
                            pass
                        
                        mataaordernumber = mapping_dict.get(order_line_ref)
                        
                        if pd.isna(mataaordernumber) or mataaordernumber is None:
                            skipped += 1
                            continue
                            
                        # Convert mataaordernumber to clean string (remove .0 if it's a float)
                        mataaordernumber_str = str(int(mataaordernumber)) if isinstance(mataaordernumber, float) and mataaordernumber.is_integer() else str(mataaordernumber)
                        order_id = mataaordernumber_to_orderid.get(mataaordernumber_str)
                        
                        if not order_id:
                            skipped += 1
                            continue
                        # Check if order detail exists by OrderId, MataaOrderId, and SKU
                        cursor.execute("SELECT Id FROM OrderDetails WHERE OrderId = %s AND MataaOrderId = %s AND SKU = %s", (order_id, order_line_ref, row['SKU']))
                        existing = cursor.fetchone()
                        
                        if existing:
                            # Update existing order detail
                            update_query = """
                            UPDATE OrderDetails SET
                                ImageURL=%s, ProductId=%s, ProductName=%s, ProductVariantName=%s, ProductVariantId=%s, Desecription=%s, SKU=%s, Quantity=%s, TotalPrice=%s, priceperunit=%s, itemState=%s, MattaId=%s, MattaParentId=%s, CreatedBy=%s, CreatedDate=%s, LastModifiedBy=%s, LastModifiedDate=%s, EntityState=%s, TotalPriceDiscounted=%s, priceperunitDiscounted=%s
                            WHERE OrderId=%s AND MataaOrderId=%s AND SKU=%s
                            """
                            update_values = [
                                '' if pd.isna(row['ImageUrl']) else row['ImageUrl'],
                                None,
                                None if pd.isna(row['ProductName']) else row['ProductName'],
                                None if pd.isna(row['ProductName']) else row['ProductName'],
                                None,
                                None if pd.isna(row['Desecription']) else row['Desecription'],
                                'ProbablyLine' if pd.isna(row['SKU']) else row['SKU'],
                                float(row['Quantity']) if not pd.isna(row['Quantity']) else 0.0,
                                float(row['TotalPrice']) if not pd.isna(row['TotalPrice']) else 0.0,
                                float(row['priceperunit']) if not pd.isna(row['priceperunit']) else 0.0,
                                0,
                                int(row['ProductMattaId']) if not pd.isna(row['ProductMattaId']) else None,
                                None,
                                'OdooSyncPy702',
                                safe_datetime(row['CreatedDate']),
                                row['LastModifiedBy'] if not pd.isna(row['LastModifiedBy']) else None,
                                safe_datetime(row['LastModifiedDate']),
                                1,
                                None,
                                None,
                                order_id,
                                order_line_ref,
                                row['SKU']
                            ]
                            cursor.execute(update_query, update_values)
                            updated += 1
                        else:
                            # Insert new order detail
                            orderdetail_id = str(uuid.uuid4())
                            values = [
                                orderdetail_id,  # Id
                                order_id,  # OrderId (GUID)
                                order_line_ref,  # MataaOrderId
                                '' if pd.isna(row['ImageUrl']) else row['ImageUrl'],  # ImageURL (empty string if missing)
                                None,  # ProductId (always NULL)
                                None if pd.isna(row['ProductName']) else row['ProductName'],  # ProductName
                                None if pd.isna(row['ProductName']) else row['ProductName'],  # ProductVariantName (same as ProductName)
                                None,  # ProductVariantId (NULL)
                                None if pd.isna(row['Desecription']) else row['Desecription'],  # Desecription
                                'ProbablyLine' if pd.isna(row['SKU']) else row['SKU'],  # SKU (default to 'ProbablyLine' if missing)
                                float(row['Quantity']) if not pd.isna(row['Quantity']) else 0.0,  # Quantity
                                float(row['TotalPrice']) if not pd.isna(row['TotalPrice']) else 0.0,  # TotalPrice
                                float(row['priceperunit']) if not pd.isna(row['priceperunit']) else 0.0,  # priceperunit
                                0,  # itemState
                                int(row['ProductMattaId']) if not pd.isna(row['ProductMattaId']) else None,  # MattaId
                                None,  # MattaParentId
                                'OdooSyncPy702',  # CreatedBy
                                safe_datetime(row['CreatedDate']),  # CreatedDate
                                row['LastModifiedBy'] if not pd.isna(row['LastModifiedBy']) else None,  # LastModifiedBy
                                safe_datetime(row['LastModifiedDate']),  # LastModifiedDate
                                1,  # EntityState
                                None,  # TotalPriceDiscounted
                                None   # priceperunitDiscounted
                            ]
                            insert_query = """
                            INSERT INTO OrderDetails (
                                Id, OrderId, MataaOrderId, ImageURL, ProductId, ProductName, ProductVariantName, ProductVariantId, Desecription, SKU, Quantity, TotalPrice, priceperunit, itemState, MattaId, MattaParentId, CreatedBy, CreatedDate, LastModifiedBy, LastModifiedDate, EntityState, TotalPriceDiscounted, priceperunitDiscounted
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                            """
                            cursor.execute(insert_query, values)
                            inserted += 1
                    except pymysql.err.IntegrityError as ie:
                        connection.rollback()
                        return jsonify({
                            'error': str(ie),
                            'row_index': int(idx),
                            'row_data': row.to_dict()
                        }), 500
                    except Exception as ex:
                        connection.rollback()
                        return jsonify({
                            'error': str(ex),
                            'row_index': int(idx),
                            'row_data': row.to_dict()
                        }), 500
                connection.commit()
        finally:
            connection.close()
        return jsonify({
            'inserted': inserted,
            'updated': updated,
            'skipped': skipped,
            'total': len(df_details),
            'message': f'Synced {inserted} order details from Excel. Updated: {updated}. Skipped: {skipped}.'
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        set_currently_syncing(False)
    

@app.route('/delete_unsynced_products', methods=['GET'])
def delete_unsynced_products():
    """Endpoint to delete all products (and related data) where is_synced is False."""
    import pymysql
    try:
        set_currently_syncing(True)
        products_json = get_all_products()
        if not products_json:
            return jsonify({"error": "Couldn't fetch products from Odoo"}), 404
        products_data = extract_products_bulk(products_json)
        # Find products with is_synced == False
        unsynced_products = [p for p in products_data if not p.get("is_synced", False)]
        matta_ids = [p["MattaId"] for p in unsynced_products if p.get("MattaId") is not None]
        if not matta_ids:
            return jsonify({"message": "No unsynced products found."}), 200
        # Call the DB deletion logic
        deleted_count, details = delete_products_and_related(matta_ids)
        return jsonify({
            "message": f"Deleted {deleted_count} unsynced products and all related data.",
            "details": details
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        set_currently_syncing(False)


if __name__ == "__main__":
    app.run(debug=True, threaded=True, host="0.0.0.0", port=5000)


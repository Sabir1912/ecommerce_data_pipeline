import os
import csv
import random
from datetime import datetime, timedelta

def generate_datasets(output_dir):
    os.makedirs(output_dir, exist_ok=True)
    print(f"Generating synthetic datasets in {output_dir}...")

    # Set random seed for reproducibility
    random.seed(42)

    # 1. Generate Products
    categories = ['Electronics', 'Apparel', 'Home & Kitchen', 'Books', 'Beauty', 'Sports']
    product_names = {
        'Electronics': ['Laptop', 'Smartphone', 'Headphones', 'Smartwatch', 'Tablet', 'Bluetooth Speaker'],
        'Apparel': ['T-Shirt', 'Jeans', 'Jacket', 'Sneakers', 'Socks', 'Dress'],
        'Home & Kitchen': ['Blender', 'Coffee Maker', 'Air Fryer', 'Toaster', 'Cookware Set', 'Vacuum Cleaner'],
        'Books': ['Sci-Fi Novel', 'History Book', 'Biography', 'Self-Help Book', 'Cookbook', 'Mystery Thriller'],
        'Beauty': ['Moisturizer', 'Sunscreen', 'Perfume', 'Lipstick', 'Shampoo', 'Face Wash'],
        'Sports': ['Yoga Mat', 'Dumbbells', 'Running Shoes', 'Water Bottle', 'Backpack', 'Tennis Racket']
    }

    products = []
    num_products = 30
    for i in range(1, num_products + 1):
        category = random.choice(categories)
        name = random.choice(product_names[category])
        price = round(random.uniform(5.0, 1200.0), 2)
        stock = random.randint(0, 500)
        
        # Inject anomalies
        if i == 5:
            price = -49.99  # Negative price anomaly
        elif i == 12:
            category = ""   # Null category anomaly
        elif i == 18:
            stock = -10     # Negative stock anomaly

        products.append({
            'product_id': f"P{1000 + i}",
            'product_name': f"{category} {name}" if category else name,
            'category': category,
            'price': price,
            'stock': stock
        })

    # Save Products
    products_file = os.path.join(output_dir, 'products.csv')
    with open(products_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['product_id', 'product_name', 'category', 'price', 'stock'])
        writer.writeheader()
        writer.writerows(products)

    # 2. Generate Customers
    first_names = ['John', 'Jane', 'Michael', 'Emily', 'David', 'Sarah', 'James', 'Jessica', 'Robert', 'Karen', 'Daniel', 'Lisa']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Miller', 'Davis', 'Wilson', 'Anderson', 'Taylor', 'Thomas', 'Moore']
    countries = ['USA', 'Canada', 'UK', 'Germany', 'France', 'Australia', 'Japan', 'India']

    customers = []
    num_customers = 100
    start_date = datetime(2024, 1, 1)

    for i in range(1, num_customers + 1):
        first = random.choice(first_names)
        last = random.choice(last_names)
        name = f"{first} {last}"
        email = f"{first.lower()}.{last.lower()}{random.randint(10, 99)}@example.com"
        country = random.choice(countries)
        signup_days = random.randint(0, 500)
        signup_date = (start_date + timedelta(days=signup_days)).strftime('%Y-%m-%d')

        # Inject anomalies
        if i == 8:
            name = ""  # Null name anomaly
        elif i == 25:
            email = "invalid_email_format"  # Invalid email anomaly
        elif i == 42:
            country = ""  # Missing country anomaly

        customers.append({
            'customer_id': f"C{1000 + i}",
            'name': name,
            'email': email,
            'signup_date': signup_date,
            'country': country
        })

    # Inject duplicate customers (2 duplicates)
    customers.append(customers[10].copy())  # Exact duplicate
    customers.append(customers[15].copy())  # Exact duplicate
    
    # Save Customers
    customers_file = os.path.join(output_dir, 'customers.csv')
    with open(customers_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['customer_id', 'name', 'email', 'signup_date', 'country'])
        writer.writeheader()
        writer.writerows(customers)

    # 3. Generate Orders, Order Items & Payments
    orders = []
    order_items = []
    payments = []
    statuses = ['Delivered', 'Delivered', 'Delivered', 'Shipped', 'Processing', 'Cancelled']
    payment_methods = ['Credit Card', 'PayPal', 'Debit Card', 'Bank Transfer']
    
    order_start_date = datetime(2025, 1, 1)
    item_counter = 1
    
    # We want around 600 orders
    for i in range(1, 601):
        customer = random.choice(customers)
        cust_id = customer['customer_id']
        
        # Pick random order date between 2025-01-01 and 2025-06-01
        days_offset = random.randint(0, 150)
        order_date_dt = order_start_date + timedelta(days=days_offset)
        order_date = order_date_dt.strftime('%Y-%m-%d')
        status = random.choice(statuses)

        # Generate order items
        num_items = random.randint(1, 4)
        order_amount = 0.0
        
        for _ in range(num_items):
            prod = random.choice(products)
            # Ensure price is positive for computation in normal items
            item_price = prod['price']
            qty = random.randint(1, 3)
            
            # Inject anomaly in order item price occasionally
            if i == 400 and len(order_items) == 0:
                item_price = -150.0  # Negative item price anomaly
            
            order_amount += item_price * qty
            
            order_items.append({
                'order_item_id': f"OI{100000 + item_counter}",
                'order_id': f"O{10000 + i}",
                'product_id': prod['product_id'],
                'quantity': qty,
                'price': item_price
            })
            item_counter += 1
        
        order_amount = round(order_amount, 2)
        order_id = f"O{10000 + i}"

        # Inject anomalies on order level
        if i == 100:
            cust_id = ""  # Null customer_id anomaly
        elif i == 200:
            order_date = "2025-02-30"  # Invalid date format anomaly
        elif i == 300:
            order_date = (datetime.now() + timedelta(days=365)).strftime('%Y-%m-%d')  # Future date anomaly

        orders.append({
            'order_id': order_id,
            'customer_id': cust_id,
            'order_date': order_date,
            'status': status,
            'total_amount': order_amount
        })

        # Payments (every order has a payment record)
        pay_amount = order_amount
        pay_method = random.choice(payment_methods)
        
        # Injects payment anomalies
        if i == 450:
            pay_amount = -50.0  # Negative payment anomaly
        elif i == 500:
            pay_amount = 0.0    # Zero payment anomaly

        payments.append({
            'payment_id': f"PAY{10000 + i}",
            'order_id': order_id,
            'payment_method': pay_method,
            'amount': pay_amount
        })

    # Save Orders
    orders_file = os.path.join(output_dir, 'orders.csv')
    with open(orders_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['order_id', 'customer_id', 'order_date', 'status', 'total_amount'])
        writer.writeheader()
        writer.writerows(orders)

    # Save Order Items
    order_items_file = os.path.join(output_dir, 'order_items.csv')
    with open(order_items_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['order_item_id', 'order_id', 'product_id', 'quantity', 'price'])
        writer.writeheader()
        writer.writerows(order_items)

    # Save Payments
    payments_file = os.path.join(output_dir, 'payments.csv')
    with open(payments_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['payment_id', 'order_id', 'payment_method', 'amount'])
        writer.writeheader()
        writer.writerows(payments)

    print(f"Data generation complete! Files written to {output_dir}")

if __name__ == '__main__':
    data_dir = os.environ.get('DATA_DIR', './data/raw')
    generate_datasets(data_dir)

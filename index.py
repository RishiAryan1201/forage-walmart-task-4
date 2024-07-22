import pandas as pd
import sqlite3

spreadsheet0 = pd.read.excel('../data/shipping_data_0.csv')
spreadsheet1 = pd.read.excel('../data/shipping_data_1.csv')
spreadsheet2 = pd.read.excel('../data/shipping_data_2.csv')

conn = sqlite3.connect('database.db')
cursor = conn.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS Products (
        ProductID INTEGER PRIMARY KEY,
        ProductName TEXT,
        Category TEXT,
        Price REAL
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS Shipments (
        ShipmentID INTEGER PRIMARY KEY AUTOINCREMENT,
        ProductID INTEGER,
        ProductName TEXT,
        Quantity INTEGER,
        Origin TEXT,
        Destination TEXT,
        FOREIGN KEY (ProductID) REFERENCES Products (ProductID)
    )
''')

# Insert data from Spreadsheet 0
for index, row in spreadsheet0.iterrows():
    cursor.execute('''
        INSERT INTO Products (ProductID, ProductName, Category, Price)
        VALUES (?, ?, ?, ?)
    ''', (row['ProductID'], row['ProductName'], row['Category'], row['Price']))

# Process and insert data from Spreadsheet 1 and 2
shipments = {}

# Read shipments from Spreadsheet 1
for index, row in spreadsheet1.iterrows():
    shipping_id = row['ShippingID']
    product_id = row['ProductID']
    product_name = row['ProductName']
    quantity = row['Quantity']
    
    if shipping_id not in shipments:
        shipments[shipping_id] = []
    
    shipments[shipping_id].append({
        'ProductID': product_id,
        'ProductName': product_name,
        'Quantity': quantity
    })

# Add origin and destination information from Spreadsheet 2
for index, row in spreadsheet2.iterrows():
    shipping_id = row['ShippingID']
    origin = row['Origin']
    destination = row['Destination']
    
    if shipping_id in shipments:
        for shipment in shipments[shipping_id]:
            shipment['Origin'] = origin
            shipment['Destination'] = destination

# Insert data into Shipments table
for shipping_id, products in shipments.items():
    for product in products:
        cursor.execute('''
            INSERT INTO Shipments (ProductID, ProductName, Quantity, Origin, Destination)
            VALUES (?, ?, ?, ?, ?)
        ''', (product['ProductID'], product['ProductName'], product['Quantity'], product['Origin'], product['Destination']))

# Commit changes and close connection
conn.commit()
conn.close()

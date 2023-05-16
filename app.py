import os
from flask import Flask, request, Response
import psycopg2
import json
from datetime import datetime
import requests

app = Flask(__name__)

# Get environment variables
host = os.getenv('HOST_NAME')
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
secret = os.getenv('ORDER_SECRET')

@app.route('/orders', methods=['POST'])
def add_order():
    payload = request.get_json(force=True)
    client_id = payload['client_id']
    symbol = payload['symbol']
    type = payload['type']
    quantity = payload['quantity']
    price = payload['price']
    placed_at = payload['placed_at']

    searched_type = 'S' if type == 'B' else 'B'

    connection = None

    try:
        connection = psycopg2.connect(host=host, dbname=db_name, user=db_user, password=db_pass)
        cursor = connection.cursor()

        if type == 'B':
            search_query = f"SELECT * FROM placed WHERE \"Symbol\" = '{symbol}' AND \"Type\" = '{searched_type}' ORDER BY \"Price\" ASC"
        else:
            search_query = f"SELECT * FROM placed WHERE \"Symbol\" = '{symbol}' AND \"Type\" = '{searched_type}' ORDER BY \"Price\" DESC"

        cursor.execute(search_query)

        records = cursor.fetchall()

        remaining_quantity = quantity

        if len(records) != 0:
            for row in records:
                found_id = row[0]
                found_client_id = row[1]
                found_symbol = row[2]
                found_type = row[3]
                found_quantity = row[4]
                found_price = float(row[5])
                price_ok = found_price <= price if type == 'B' else found_price >= price

                if remaining_quantity != 0 and price_ok:
                    if remaining_quantity < found_quantity:
                        # ORDER FILLED WITH UPDATE
                        # INSERT INTO executed ORDER PLACED
                        query = " \
                                INSERT INTO executed (\"ClientID\", \"Symbol\", \"Type\", \"Quantity\", \"Price\", \"ExecutedAt\") \
                                VALUES (%s, %s, %s, %s, %s, %s)"
                        cursor.execute(query, (client_id, symbol, type, remaining_quantity, found_price, datetime.now().isoformat()))
                        # INSERT INTO executed ORDER FOUND (partly); 
                        query = " \
                                INSERT INTO executed (\"ClientID\", \"Symbol\", \"Type\", \"Quantity\", \"Price\", \"ExecutedAt\") \
                                VALUES (%s, %s, %s, %s, %s, %s)"
                        cursor.execute(query, (found_client_id, found_symbol, found_type, remaining_quantity, found_price, datetime.now().isoformat()))
                        # UPDATE placed WHERE ID = FoundID
                        query = "UPDATE placed SET \"Quantity\" = %s WHERE \"ID\" = %s"
                        cursor.execute(query, (found_quantity - remaining_quantity, found_id))

                        processed_order = {
                            "secret": secret,
                            "client_id": client_id,
                            "type": type,
                            "symbol": symbol,
                            "from_client_id": found_client_id,
                            "quantity": remaining_quantity,
                            "price": found_price
                        }

                        response = requests.post(f"http://trading-platform:5000/orders/process", json=processed_order)

                        if response.status_code != 200:
                            return Response(status=response.status_code)
                        
                        connection.commit()
                        remaining_quantity = 0

                    elif remaining_quantity == found_quantity:
                        # ORDER FILLED WITH REMOVE
                        # INSERT INTO executed ORDER PLACED
                        query = " \
                                INSERT INTO executed (\"ClientID\", \"Symbol\", \"Type\", \"Quantity\", \"Price\", \"ExecutedAt\") \
                                VALUES (%s, %s, %s, %s, %s, %s)"
                        cursor.execute(query, (client_id, symbol, type, remaining_quantity, found_price, datetime.now().isoformat()))
                        # INSERT INTO executed ORDER FOUND
                        query = " \
                                INSERT INTO executed (\"ClientID\", \"Symbol\", \"Type\", \"Quantity\", \"Price\", \"ExecutedAt\") \
                                VALUES (%s, %s, %s, %s, %s, %s)"
                        cursor.execute(query, (found_client_id, found_symbol, found_type, found_quantity, found_price, datetime.now().isoformat()))
                        # REMOVE FROM placed ORDER FOUND
                        query = f"DELETE FROM placed WHERE \"ID\" = '{found_id}'"
                        cursor.execute(query)

                        processed_order = {
                            "secret": secret,
                            "client_id": client_id,
                            "type": type,
                            "symbol": symbol,
                            "from_client_id": found_client_id,
                            "quantity": remaining_quantity,
                            "price": found_price
                        }

                        response = requests.post(f"http://trading-platform:5000/orders/process", json=processed_order)

                        if response.status_code != 200:
                            return Response(status=response.status_code)
                        
                        connection.commit()

                        remaining_quantity = 0

                    else:
                        # ORDER NOT FILLED
                        # INSERT INTO executed ORDER PLACED (partly) 
                        query = " \
                                INSERT INTO executed (\"ClientID\", \"Symbol\", \"Type\", \"Quantity\", \"Price\", \"ExecutedAt\") \
                                VALUES (%s, %s, %s, %s, %s, %s)"
                        cursor.execute(query, (client_id, symbol, type, found_quantity, found_price, datetime.now().isoformat())) 
                        # INSERT INTO executed ORDER FOUND (partly); 
                        query = " \
                                INSERT INTO executed (\"ClientID\", \"Symbol\", \"Type\", \"Quantity\", \"Price\", \"ExecutedAt\") \
                                VALUES (%s, %s, %s, %s, %s, %s)"
                        cursor.execute(query, (found_client_id, found_symbol, found_type, found_quantity, found_price, datetime.now().isoformat()))
                        # REMOVE FROM placed ORDER FOUND
                        query = f"DELETE FROM placed WHERE \"ID\" = '{found_id}'"
                        cursor.execute(query)

                        processed_order = {
                            "secret": secret,
                            "client_id": client_id,
                            "type": type,
                            "symbol": symbol,
                            "from_client_id": found_client_id,
                            "quantity": found_quantity,
                            "price": found_price
                        }

                        response = requests.post(f"http://trading-platform:5000/orders/process", json=processed_order)

                        if response.status_code != 200:
                            return Response(status=response.status_code)
                        
                        connection.commit()
                        
                        remaining_quantity = remaining_quantity - found_quantity
                else:
                    break

        # IF REMAINING QUANTITY NOT NULL
        # INSERT order INTO placed    
        if remaining_quantity != 0:             
            query = " \
                    INSERT INTO placed (\"ClientID\", \"Symbol\", \"Type\", \"Quantity\", \"Price\", \"PlacedAt\") \
                    VALUES (%s, %s, %s, %s, %s, %s) RETURNING \"ID\""

            cursor.execute(query, (client_id, symbol, type, remaining_quantity, price, placed_at))
            id = cursor.fetchone()[0]
            connection.commit()
        
            return Response(json.dumps({'id' : id}), status=201, mimetype='application/json')

        return Response(json.dumps({"message": "order placed and filled"}), status=201, mimetype='application/json')
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        if connection is not None:
            cursor.close()
            connection.close()

        return Response(status=400)

    finally:
        if connection is not None:
            cursor.close()
            connection.close()

@app.route('/orders/client/<id>', methods=['GET'])
def get_user_orders(id):
    connection = None

    try:
        connection = psycopg2.connect(host=host, dbname=db_name, user=db_user, password=db_pass)
        cursor = connection.cursor()

        query = f"SELECT * FROM placed WHERE \"ClientID\" = '{id}'"
        cursor.execute(query)

        records = cursor.fetchall()
        placed_orders = []
        for record in records:
            order = {
                "ID":           record[0],   
                "Symbol":       record[2],
                "Type":         "Buy" if record[3] == "B" else "Sell",
                "Quantity":     record[4],
                "Price":        record[5],
                "Placed At":    str(record[6])
            }
            placed_orders.append(order)
        
        # TODO Add executed orders
        return Response(json.dumps(placed_orders), status=200, mimetype='application/json')

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        if connection is not None:
            cursor.close()
            connection.close()

        return Response(status=400)

    finally:
        if connection is not None:
            cursor.close()
            connection.close()

@app.route('/orders/<id>', methods=['GET'])
def get_order(id):
    connection = None
    try:
        connection = psycopg2.connect(host=host, dbname=db_name, user=db_user, password=db_pass)
        cursor = connection.cursor()

        query = f"SELECT * FROM placed WHERE \"ID\" = '{id}'"
        cursor.execute(query)

        records = cursor.fetchall()

        if len(records) == 0:
            return Response(status=404)
        
        record = records[0]
        order = {
            "ClientID":     record[1],
            "Symbol":       record[2],
            "Type":         record[3],
            "Quantity":     record[4],
            "Price":        record[5],
            "Placed At":    str(record[6])
        }

        return Response(json.dumps(order), status=200, mimetype='application/json')
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        if connection is not None:
            cursor.close()
            connection.close()

        return Response(status=400)

    finally:
        if connection is not None:
            cursor.close()
            connection.close()

@app.route('/orders/<id>', methods=['PUT'])
def update_order(id):
    payload = request.get_json(force=True)
    connection = None

    try:
        connection = psycopg2.connect(host=host, dbname=db_name, user=db_user, password=db_pass)
        cursor = connection.cursor()
        query = "\
            UPDATE placed SET \"Quantity\" = %s, \"Price\" = %s, \"PlacedAt\" = %s WHERE \"ID\" = %s"
        
        cursor.execute(query, (payload.get('quantity'), payload.get('price'), payload.get('placed_at'), id))
        connection.commit()

        query = f"SELECT * FROM placed WHERE \"ID\" = '{id}'"
        cursor.execute(query)

        records = cursor.fetchall()

        if len(records) == 0:
            return Response(status=404)
        
        record = records[0]
        order = {
            "Symbol":       record[2],
            "Type":         record[3],
            "Quantity":     record[4],
            "Price":        record[5],
            "Placed At":    str(record[6])
        }

        return Response(json.dumps(order), status=200, mimetype='application/json')
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        if connection is not None:
            cursor.close()
            connection.close()
        return Response(status=400)

    finally:
        if connection is not None:
            cursor.close()
            connection.close()

@app.route('/orders/<id>', methods=['DELETE'])
def remove_order(id):
    connection = None
    try:
        connection = psycopg2.connect(host=host, dbname=db_name, user=db_user, password=db_pass)
        cursor = connection.cursor()

        query = f"DELETE FROM placed WHERE \"ID\" = '{id}'"
        cursor.execute(query)
        connection.commit()

        return Response(status=200)
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        if connection is not None:
            cursor.close()
            connection.close()

        return Response(status=400)

    finally:
        if connection is not None:
            cursor.close()
            connection.close()
            
if __name__ == "__main__":
    app.run(debug=False)
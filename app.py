import os
from flask import Flask, request, Response
import psycopg2
import json

app = Flask(__name__)

# Get environment variables
host = os.getenv('HOST_NAME')
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')

@app.route('/orders', methods=['POST'])
def add_order():
    payload = request.get_json(force=True)
    client_id = payload['client_id']
    symbol = payload['symbol']
    type = payload['type']
    quantity = payload['quantity']
    price = payload['price']
    placed_at = payload['placed_at']

    connection = None

    try:
        connection = psycopg2.connect(host=host, dbname=db_name, user=db_user, password=db_pass)
        cursor = connection.cursor()

        query = " \
                INSERT INTO placed (\"ClientID\", \"Symbol\", \"Type\", \"Quantity\", \"Price\", \"PlacedAt\") \
                VALUES (%s, %s, %s, %s, %s, %s) RETURNING \"ID\""

        cursor.execute(query, (client_id, symbol, type, quantity, price, placed_at))
        id = cursor.fetchone()[0]
        connection.commit()
        
        return Response(json.dumps({'id' : id}), status=201, mimetype='application/json')

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
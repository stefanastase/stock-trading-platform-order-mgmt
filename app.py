import os
from flask import Flask, request, Response
import psycopg2

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
                VALUES (%s, %s, %s, %s, %s, %s)"

        cursor.execute(query, (client_id, symbol, type, quantity, price, placed_at))
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
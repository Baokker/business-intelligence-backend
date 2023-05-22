from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
import happybase

app = Flask(__name__)

# # mysql config
# app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://yourusername:yourpassword@localhost/yourdatabase'
# db = SQLAlchemy(app)

# # hbase
# conn = happybase.Connection(host='localhost', port=9090)
# table = conn.table('mytable')

# # mysql orm
# class User(db.Model):
#     id = db.Column(db.Integer, primary_key=True)
#     name = db.Column(db.String(50), nullable=False)
#     age = db.Column(db.Integer, nullable=False)

@app.route('/')
def hello_world():
    return 'Hello, World!'

# test get
@app.route('/test_get')
def test_get():
    # get params
    name = request.args.get('name')
    age = request.args.get('age')
    # return json data
    return jsonify({'name': name, 'age': age})

# test post
@app.route('/test_post', methods=['POST'])
def test_post():
    # receive json data
    data = request.get_json()
    # remove empty value
    data = {k: v for k, v in data.items() if v}
    # return json data
    return jsonify(data)

if __name__ == '__main__':
    app.run()

from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
# import happybase
import pymysql

app = Flask(__name__)

# mysql config
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root:bbbIII2023@47.101.216.242/bbbiii'
db = SQLAlchemy(app)

# # hbase
# conn = happybase.Connection(host='localhost', port=9090)
# table = conn.table('mytable')

# mysql orm
class Category(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), nullable=False)

class Log(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    news_id = db.Column(db.Integer, nullable=False)
    start_ts = db.Column(db.Integer, nullable=False)
    duration = db.Column(db.Integer, nullable=False)

class News(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    topic = db.Column(db.String(50), nullable=False)
    headline = db.Column(db.Text, nullable=False)
    body = db.Column(db.Text, nullable=False)
    category = db.Column(db.String(50), nullable=False)

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)

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

# 获取新闻标题的智能提示
@app.route('/suggest/headline')
def search_news():
    headline = request.args.get('headline')  # 获取用户输入的标题部分
    amount = request.args.get('amount') or 10  # 如果没有传入 amount 参数，默认返回10条新闻

    # 使用 SQLAlchemy 进行查询
    news_list = News.query.filter(News.headline.like('%' + headline + '%')).limit(amount).all()

    # 返回查询结果
    result = [{'name': item.headline, 'id': item.id } for item in news_list]

    return jsonify({'suggestions': result})

# 获取用户ID的智能提示
@app.route('/suggest/userid')
def search_user():
    userID = request.args.get('userID')  # 获取用户输入的小部分ID
    userID_str = str(userID)

    amount = request.args.get('amount') or 10  # 如果没有传入 amount 参数，默认返回10条新闻

    # 使用 SQLAlchemy 进行查询
    user_list = User.query.filter(User.id.like('%' + userID_str + '%')).limit(amount).all()

    # 返回查询结果
    result = [item.id for item in user_list]
    
    return jsonify({'suggestions': result})

if __name__ == '__main__':
    app.run()

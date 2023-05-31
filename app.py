from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
import datetime
# import happybase
import pymysql

app = Flask(__name__)

# mysql config
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root:bbbIII2023@47.101.216.242/bbbiii'
db = SQLAlchemy(app)
CORS(app, resources=r'/*') # 注册CORS, "/*" 允许访问所有api
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
    start_dt = db.Column(db.DateTime, nullable=False)
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

# 获取单新闻的流行度
@app.route('/singlenews/popularity')
def get_popularity_single():
    news_id = request.args.get('newsID')  # 获取新闻id
    start_date = datetime.datetime.strptime(request.args.get('startDate'), '%Y-%m-%d %H:%M:%S')
    end_date = datetime.datetime.strptime(request.args.get('endDate'), '%Y-%m-%d %H:%M:%S')
    
    # 设置一个小时的时间间隔
    delta = datetime.timedelta(hours=1)
    
    popularity_list = []
    while start_date <= end_date:
        # 计算当前时间段的起始和结束时间
        current_start_date = start_date - delta
        current_end_date = start_date
        
        # 查询在当前时间段内查看过该新闻的用户数量
        query = Log.query.filter(Log.news_id==news_id, Log.start_dt>=current_start_date, Log.start_dt<current_end_date)
        popularity = query.with_entities(db.func.count(Log.user_id)).scalar()
        
        # 将当前时间段的结果加入到数组中
        popularity_list.append({'date': current_start_date.strftime('%Y-%m-%d %H:%M:%S'), 'popularity': popularity})
        
        # 将时间段向后移动一个小时
        start_date = start_date + delta
    
    return jsonify(popularity_list)

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

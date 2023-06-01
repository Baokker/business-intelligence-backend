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
CORS(app, resources=r'/*')  # 注册CORS, "/*" 允许访问所有api


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
    category_id = db.Column(db.Integer, nullable=False)
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
@app.route('/news/popularity')
def get_popularity_single():
    news_id = request.args.get('newsID')  # 获取新闻id
    start_date = datetime.datetime.strptime(request.args.get('startDate'), '%Y-%m-%d %H:%M:%S')
    end_date = datetime.datetime.strptime(request.args.get('endDate'), '%Y-%m-%d %H:%M:%S')

    # 设置时间间隔
    timedelta = end_date - start_date
    days = timedelta.days

    if days < 1:
        hours = 1
    elif days < 7:
        hours = 3
    elif days < 14:
        hours = 6
    elif days < 21:
        hours = 12
    else:
        hours = 24
    delta = datetime.timedelta(hours=hours)

    popularity_list = []
    # 初始化查询所需的参数
    start_time = start_date - delta
    end_time = start_date
    while end_time <= end_date:
        # 查询在当前时间段内查看过该新闻的用户数量
        query = db.session.query(Log.user_id).enable_eagerloads(True) \
            .filter(Log.news_id == news_id, Log.start_dt.between(start_time, end_time))

        # 查询对应的行数
        popularity = query.count()

        # 将当前时间段的结果加入到数组中
        popularity_list.append({'datetime': end_time.strftime('%Y-%m-%d %H:%M:%S'), 'popularity': popularity})

        # 将时间段向后移动一个时间间隔
        start_time = end_time
        end_time += delta

    # 构建最终的数据结构
    result_dict = {"data": popularity_list}
    return jsonify(result_dict)


# 获取某种类型新闻的活跃度
@app.route('/category/popularity')
def get_popularity_newstopic():
    category_id = request.args.get('categoryID')  # 获取新闻id
    start_date = datetime.datetime.strptime(request.args.get('startDate'), '%Y-%m-%d %H:%M:%S')
    end_date = datetime.datetime.strptime(request.args.get('endDate'), '%Y-%m-%d %H:%M:%S')

    # 设置时间间隔
    timedelta = end_date - start_date
    days = timedelta.days

    if days < 1:
        hours = 1
    elif days < 7:
        hours = 3
    elif days < 14:
        hours = 6
    elif days < 21:
        hours = 12
    else:
        hours = 24
    delta = datetime.timedelta(hours=hours)

    popularity_list = []
    # 初始化查询所需的参数
    start_time = start_date - delta
    end_time = start_date
    while end_time <= end_date:
        # 查询在当前时间段内查看过该新闻的用户数量
        query = db.session.query(Log.user_id).enable_eagerloads(True) \
            .filter(Log.category_id == category_id, Log.start_dt.between(start_time, end_time))

        # 查询对应的行数
        popularity = query.count()

        # 将当前时间段的结果加入到数组中
        popularity_list.append({'datetime': end_time.strftime('%Y-%m-%d %H:%M:%S'), 'popularity': popularity})

        # 将时间段向后移动一个时间间隔
        start_time = end_time
        end_time += delta
        # 构建最终的数据结构
    result_dict = {"data": popularity_list}
    return jsonify(result_dict)


# 获取新闻标题的智能提示
@app.route('/suggest/headline')
def search_news():
    headline = request.args.get('headline')  # 获取用户输入的标题部分
    amount = request.args.get('amount') or 10  # 如果没有传入 amount 参数，默认返回10条新闻

    # 使用 SQLAlchemy 进行查询
    news_list = News.query.filter(News.headline.like('%' + headline + '%')).limit(amount).all()

    # 返回查询结果
    result = [{'name': item.headline, 'id': item.id} for item in news_list]

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


# test post
@app.route('/user/interest', methods=['POST'])
def get_interest_change():
    # receive json data
    data = request.get_json()
    # remove empty value
    data = {k: v for k, v in data.items() if v}
    user_id = int(data['userID'])
    category_id = int(data['category_id'])
    start_date = datetime.datetime.strptime(data['startDate'], '%Y-%m-%d %H:%M:%S')
    end_date = datetime.datetime.strptime(data['endDate'], '%Y-%m-%d %H:%M:%S')
    # return json data
    # 设置时间间隔
    timedelta = end_date - start_date
    days = timedelta.days

    if days < 1:
        hours = 2
    elif days < 7:
        hours = 6
    elif days < 14:
        hours = 12
    else:
        hours = 24
    delta = datetime.timedelta(hours=hours)

    popularity_list = []
    # 初始化查询所需的参数
    start_time = start_date - delta
    end_time = start_date
    while end_time <= end_date:
        # 查询在当前时间段内查看过该新闻的用户数量
        query = db.session.query(Log.user_id).enable_eagerloads(True) \
            .filter(Log.user_id == user_id, Log.category_id == category_id, Log.start_dt.between(start_time, end_time))

        # 查询对应的行数
        popularity = query.count()

        # 将当前时间段的结果加入到数组中
        popularity_list.append({'datetime': end_time.strftime('%Y-%m-%d %H:%M:%S'), 'popularity': popularity})

        # 将时间段向后移动一个时间间隔
        start_time = end_time
        end_time += delta
    # 构建最终的数据结构
    result_dict = {"data": popularity_list}
    return jsonify(result_dict)


if __name__ == '__main__':
    app.run(debug=True)

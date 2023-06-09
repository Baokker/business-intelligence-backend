from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
import datetime
from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine
import random
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import func
from sqlite3 import Connection as SQLite3Connection
# import happybase
import pymysql
import pymysql

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
engine = create_engine('mysql+pymysql://root:bbbIII2023@47.101.216.242/bbbiii')
Session = sessionmaker(bind=engine)
session = Session()

# mysql config
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root:bbbIII2023@47.101.216.242/bbbiii'
db = SQLAlchemy(app)
CORS(app, resources=r'/*')  # 注册CORS, "/*" 允许访问所有api
socketio = SocketIO(app, cors_allowed_origins="*")


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


# 建立 RealTimeLog 模型
class RealTimeLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer)
    news_id = db.Column(db.Integer)
    start_dt = db.Column(db.DateTime)
    duration = db.Column(db.Integer)


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
    print(request.args.get('startDate'))
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


@app.route('/user/interest', methods=['POST'])
def get_interest_change():
    # receive json data
    data = request.get_json()
    # remove empty value
    data = {k: v for k, v in data.items() if v}
    user_id = int(data['userID'])
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

    popularity_list = [{'id': i + 1, 'trends': []} for i in range(18)]
    # 初始化查询所需的参数
    start_time = start_date - delta
    end_time = start_date
    while end_time <= end_date:
        # 查询在当前时间段内查看过该新闻的用户数量
        query = db.session.query(Log.user_id, Log.category_id).enable_eagerloads(True) \
            .filter(Log.user_id == user_id, Log.start_dt.between(start_time, end_time))
        res = query.all()
        for item in popularity_list:
            item['trends'].append({'datetime': end_time.strftime('%Y-%m-%d %H:%M:%S'), 'popularity': 0})
        for result in res:
            category_id = result[1]  # 获取 result 元组的第二个元素，也就是 category_id
            popularity_list[category_id - 1]['trends'][-1]['popularity'] += 1

        # 将时间段向后移动一个时间间隔
        start_time = end_time
        end_time += delta
    # 构建最终的数据结构
    result_dict = {"data": popularity_list}
    return jsonify(result_dict)


@app.route('/complex_search', methods=['POST'])
def complex_search():
    data = request.get_json()
    data = {k: v for k, v in data.items() if v is not None and v != ''}
    filters_1 = []
    users = data['users']
    categories = data['categories']
    clickMinTime = int(data['clickMinTime'])
    titleMinLength = int(data['titleMinLength'])
    titleMaxLength = int(data['titleMaxLength'])
    if 'startDate' in data:
        start_date = datetime.datetime.strptime(data['startDate'], '%Y-%m-%d %H:%M:%S')
        filters_1.append(Log.start_dt >= start_date)
    if 'endDate' in data:
        end_date = datetime.datetime.strptime(data['endDate'], '%Y-%m-%d %H:%M:%S')
        filters_1.append(Log.start_dt <= end_date)
    if len(users) > 0:
        filters_1.append(Log.user_id.in_(users))
    if len(categories) > 0:
        filters_1.append(Log.category_id.in_(categories))
    if clickMinTime > 0:
        filters_1.append(Log.duration >= clickMinTime)
    query_1 = db.session.query(Log.news_id).enable_eagerloads(True).filter(*filters_1)
    news_ids = [x[0] for x in query_1.distinct().all()]
    filters_2 = [News.id.in_(news_ids)]
    if titleMinLength > 0:
        filters_2.append(db.func.length(News.headline) >= titleMinLength)
    if titleMaxLength > 0:
        filters_2.append(db.func.length(News.headline) <= titleMaxLength)
    query_2 = db.session.query(News.id, News.topic, News.headline, News.category).enable_eagerloads(True).filter(
        *filters_2)
    news = query_2.all()
    if not news:
        return jsonify({'data': []})
    # 将查询结果转换为对应的 JSON 格式
    data = [{'id': item[0], 'topic': item[1], 'headline': item[2], 'category': item[3]} for item in news]
    result = {'data': data}

    # 将结果序列化为 JSON，并在 HTTP 响应中返回
    return jsonify(result)


# 定义读取数据库中实时日志的函数
def read_real_time_log():
    logs = RealTimeLog.query.order_by(RealTimeLog.id.desc()).limit(10).all()
    return [(log.user_id, log.news_id, log.start_dt.strftime('%Y-%m-%d %H:%M:%S'), log.duration) for log in
            reversed(logs)]


# 定义当客户端连接时的回调函数
@socketio.on('connect')
def handle_connect():
    print('Client connected')
    # 向客户端发送历史日志记录
    emit('update', read_real_time_log())


# 定义当收到“get_news_ids”事件时的回调函数
@socketio.on('get_news_ids')
def handle_get_news_ids(user_id):
    # 查询匹配用户 ID 的最近 10 条新闻记录
    logs = RealTimeLog.query.filter_by(user_id=user_id).order_by(RealTimeLog.id.desc()).limit(10).all()
    news_ids = [(log.news_id, log.duration, log.start_dt.strftime('%Y-%m-%d %H:%M:%S')) for log in reversed(logs)]
    # 查询对应新闻的 headline 和 category
    news_list = []
    for news_id, duration, start_dt in news_ids:
        news = News.query.filter_by(id=news_id).first()
        if news:
            news_list.append(
                {'news_id': news_id, 'headline': news.headline, 'topic': news.topic, 'category': news.category,
                 'duration': duration,
                 'start_dt': start_dt})
    # 调用推荐算法，获取推荐结果
    recommendations = get_recommendations(user_id, news_list)

    # 将推荐结果转换成必要的格式，发送到前端
    rec_list = [{'news_id': news.id, 'headline': news.headline, 'topic': news.topic, 'category': news.category} for news
                in recommendations]
    # print(rec_list)
    # 向客户端发送新闻 ID 列表和对应新闻的 headline 和 category
    emit('news_ids', {'user_id': user_id, 'news_list': news_list, 'rec_list': rec_list})


def get_recommendations(user_id, news_list):
    # 新建一个空列表，用于存储筛选后的新闻
    filtered_news = []

    # print(news_list)
    # 遍历 news_list，将 duration 大于 2 的新闻加入 filtered_news 中
    for news in news_list:
        if news['duration'] > 2:
            filtered_news.append(news)

    # 先获取 start_dt 最新的新闻，将其加入 filtered_news 中
    latest_news = max(filtered_news, key=lambda x: datetime.datetime.strptime(str(x['start_dt']), '%Y-%m-%d %H:%M:%S'))
    filtered_news.append(latest_news)

    # 统计各个分类出现的次数
    category_counts = {}
    for news in filtered_news:
        category = news['category']
        category_counts[category] = category_counts.get(category, 0) + 1

    # 计算各个分类在历史点击记录中的占比
    total_count = sum(category_counts.values())
    category_ratios = {category: count / total_count for category, count in category_counts.items()}

    recommended_news = []
    for category, ratio in category_ratios.items():
        num_recommendations = int(ratio * 10)

        # 使用 SQLAlchemy 进行查询并随机排序
        category_news = News.query.filter_by(category=category).order_by(func.rand()).limit(num_recommendations).all()

        recommended_news.extend(category_news)

    return recommended_news


@socketio.on('update')
def handle_connect():
    print('Client updating')
    # 向客户端发送历史日志记录
    emit('update', read_real_time_log())


# 定义当客户端断开连接时的回调函数
@socketio.on('disconnect')
def handle_disconnect():
    print('Client disconnected')


if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000)
    app.run(host='0.0.0.0', debug=True)

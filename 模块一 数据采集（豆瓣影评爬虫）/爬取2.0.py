import re
import time
import random
import requests
import pymysql
from bs4 import BeautifulSoup
from pymysql.cursors import DictCursor
from loguru import logger
from functools import wraps

# --------------------------
# MySQL配置（密码已更新）
# --------------------------
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "ygy20060227-",  # 用户提供的MySQL密码
    "database": "movie_comment_db",
    "charset": "utf8mb4"
}

# --------------------------
# 豆瓣Cookie（用户提供的有效Cookie）
# --------------------------
DOUBAN_COOKIE = 'bid=MkBgKtLgqm4; _vwo_uuid_v2=D5BB088E30628E9E4EB2EDCD6EB0DF2A2|c343bc9ef9f151da8446dddfaea28959; viewed="35598244_35231456"; ll="118408"; _pk_id.100001.4cf6=84308ba5eca49734.1761122330.; __yadk_uid=ro5hsvGm9DIbYZy3FwYpIoDBwVcr9DbZ; dbcl2="282442856:kadpFn7XkjM"; push_noty_num=0; push_doumail_num=0; __utmv=30149280.28244; ck=FJS6; ap_v=0,6.0; __utmc=30149280; __utmc=223695111; __utma=30149280.760081204.1743661532.1761226591.1761229198.8; __utmz=30149280.1761229198.8.6.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; __utma=223695111.1534616943.1761122333.1761226591.1761229198.6; __utmb=223695111.0.10.1761229198; __utmz=223695111.1761229198.6.4.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; _pk_ref.100001.4cf6=%5B%22%22%2C%22%22%2C1761229198%2C%22https%3A%2F%2Fwww.baidu.com%2Flink%3Furl%3DbeLvml93fNEFN4cU4AOFgqgNh9aI6zlCgwrGPROKsSuZ0BNkb-K4V1idZfuXyQrc%26wd%3D%26eqid%3De91241bc00059d890000000268fa3984%22%5D; _pk_ses.100001.4cf6=1; frodotk_db="5b1ff1fd0f58e8e4115fd5f72e0abaac"; 6333762c95037d16=EdzjRfZ3Zr5SC302n066a4ia%2FgCH1F6W%2FngRSjc5BaNdYg6vXdtdw6xQf89cKjdOWnwkP94E5p0y20crKMAJ8QRmHtI3jJN8MPz4sUZDIz9fNWZz896ux5HYPBdDCEqE6kPuKEHD9BY6BK%2FBe6o4jtJaXZDZtYFg0jakxJNb7WyHz%2F5AHmC6pciCGu79ofh0aYrbtrUI8cEXYGdGDiL%2F%2BN5wPcx%2BT%2BJ4GHmm40JJ4riqgIZhP3Y887ajnqsT97mU108xmZCc4Cdte2OBVqg3zAVxVbw8LO8X1yprI3RZ9cZYEzzjfkOqWw%3D%3D; __utmt=1; __utmb=30149280.24.9.1761229217624; _TDID_CK=1761229219533'

# --------------------------
# 爬取参数
# --------------------------
TOP250_PAGES = 10  # 10页=250部电影
MAX_PAGES_PER_MOVIE = 10  # 每部电影爬10页评论（200条，可根据需求调整）
RETRY_TIMES = 3  # 失败重试次数


# --------------------------
# 工具函数（反爬+重试）
# --------------------------
def retry_decorator(func):
    """失败重试装饰器（自动重试3次）"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        for i in range(RETRY_TIMES):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.warning(f"第{i+1}次尝试失败：{str(e)}，将重试...")
                time.sleep(random.uniform(5, 10))  # 重试前延迟
        logger.error(f"超过{RETRY_TIMES}次重试，放弃该任务")
        return None
    return wrapper


def get_random_headers():
    """随机生成请求头（含User-Agent和Cookie）"""
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/129.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) Safari/605.1.15",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) Safari/605.1.15",
        "Mozilla/5.0 (Linux; Android 14; Pixel 8) Chrome/128.0.0.0 Mobile Safari/537.36"
    ]
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Cookie": DOUBAN_COOKIE,
        "Referer": "https://movie.douban.com/top250",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9"
    }


# --------------------------
# 数据库操作
# --------------------------
def init_database():
    """初始化数据库（创建库和表）"""
    try:
        conn = pymysql.connect(
            host=DB_CONFIG["host"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            charset=DB_CONFIG["charset"]
        )
        cursor = conn.cursor()
        # 创建数据库
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
        cursor.execute(f"USE {DB_CONFIG['database']}")
        # 创建评论表
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS top250_comments (
            id INT AUTO_INCREMENT PRIMARY KEY,
            movie_id VARCHAR(20) NOT NULL,
            movie_rank INT DEFAULT 0,
            star INT NOT NULL,
            content TEXT NOT NULL,
            crawl_time DATETIME NOT NULL,
            INDEX idx_movie_id (movie_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        cursor.execute(create_table_sql)
        conn.commit()
        logger.success("数据库初始化完成（表：top250_comments）")
    except Exception as e:
        logger.error(f"数据库初始化失败：{str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()


def save_to_mysql(comments):
    """批量保存评论到MySQL"""
    if not comments:
        return
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        sql = """
        INSERT INTO top250_comments (movie_id, movie_rank, star, content, crawl_time)
        VALUES (%s, %s, %s, %s, %s)
        """
        data = [
            (c["movie_id"], c["movie_rank"], c["star"], c["content"], c["crawl_time"])
            for c in comments
        ]
        cursor.executemany(sql, data)
        conn.commit()
        logger.success(f"成功插入{len(comments)}条评论到数据库")
    except Exception as e:
        conn.rollback()
        logger.error(f"数据库插入失败：{str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()


# --------------------------
# 核心爬虫逻辑（适配Top250 HTML结构）
# --------------------------
@retry_decorator
def get_top250_movie_ids(pages=10):
    """获取豆瓣Top250电影ID和排名"""
    top250_movies = []
    base_url = "https://movie.douban.com/top250?start={start}&filter="
    
    for page in range(pages):
        start = page * 25
        url = base_url.format(start=start)
        headers = get_random_headers()
        
        try:
            time.sleep(random.uniform(2, 4))
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            
            # 🔴 关键：根据HTML结构，电影条目在class="grid_view"的ol下的li中
            movie_items = soup.select(".grid_view li .item")
            if not movie_items:
                logger.warning(f"第{page+1}页未找到电影条目，可能页面结构更新")
                continue
            
            for idx, item in enumerate(movie_items):
                # 提取排名（第1页第1个是第1名，第2个是第2名...）
                movie_rank = page * 25 + idx + 1
                # 提取电影链接
                movie_link = item.select_one(".hd a")["href"]
                movie_id = re.search(r"/subject/(\d+)/", movie_link).group(1)
                top250_movies.append({"movie_id": movie_id, "movie_rank": movie_rank})
            
            logger.success(f"第{page+1}页爬取完成，已获取{len(movie_items)}部电影ID")
        
        except Exception as e:
            logger.error(f"第{page+1}页爬取失败：{str(e)}")
            continue
    
    logger.success(f"共获取{len(top250_movies)}部Top250电影ID")
    return top250_movies


@retry_decorator
def crawl_movie_comments(movie_id, movie_rank, max_pages=10):
    """爬取单部电影的评论"""
    comments = []
    url_template = f"https://movie.douban.com/subject/{movie_id}/comments?start={{start}}&limit=20&status=P"
    
    for page in range(max_pages):
        start = page * 20
        url = url_template.format(start=start)
        headers = get_random_headers()
        
        try:
            time.sleep(random.uniform(1, 3))
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            # 处理验证码（手动干预）
            if "验证码" in response.text:
                logger.warning(f"出现验证码！请打开以下链接验证：\n{url}")
                input("验证完成后按回车继续...")
                response = requests.get(url, headers=headers)
            
            soup = BeautifulSoup(response.text, "html.parser")
            comment_items = soup.find_all("div", class_="comment-item")
            
            for item in comment_items:
                # 提取星级
                rating_tag = item.find("span", class_="rating")
                star = int(rating_tag["class"][0].replace("allstar", "")) // 10 if rating_tag else 0
                # 提取评论内容
                content_tag = item.find("span", class_="short")
                content = content_tag.text.strip() if content_tag else ""
                
                if content:
                    comments.append({
                        "movie_id": movie_id,
                        "movie_rank": movie_rank,
                        "star": star,
                        "content": content,
                        "crawl_time": time.strftime("%Y-%m-%d %H:%M:%S")
                    })
            
            print(f"电影{movie_id}：第{page+1}页爬取完成，累计{len(comments)}条评论")
        
        except Exception as e:
            print(f"爬取第{page+1}页失败：{str(e)}")
            continue
    
    return comments


# --------------------------
# 主流程
# --------------------------
def main():
    # 1. 初始化数据库
    init_database()
    
    # 2. 获取Top250电影ID
    top250_movies = get_top250_movie_ids(pages=TOP250_PAGES)
    if not top250_movies:
        logger.error("未获取到电影ID，程序终止")
        return
    
    # 3. 爬取每部电影的评论并保存到数据库
    for movie in top250_movies:
        movie_id = movie["movie_id"]
        movie_rank = movie["movie_rank"]
        
        logger.info(f"\n开始爬取Top250第{movie_rank}名（ID:{movie_id}）")
        comments = crawl_movie_comments(
            movie_id=movie_id,
            movie_rank=movie_rank,
            max_pages=MAX_PAGES_PER_MOVIE
        )
        
        if comments:
            save_to_mysql(comments)
        
        # 电影间延迟（反爬）
        time.sleep(random.uniform(5, 10))
    
    logger.success("✅ 所有Top250电影影评爬取完成！")


if __name__ == "__main__":
    main()
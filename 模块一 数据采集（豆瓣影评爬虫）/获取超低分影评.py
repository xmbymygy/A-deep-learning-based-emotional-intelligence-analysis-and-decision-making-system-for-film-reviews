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
# 核心配置（复用现有环境）
# --------------------------
COOKIE_POOL = [
    # 第一个账号Cookie
    'bid=MkBgKtLgqm4; _vwo_uuid_v2=D5BB088E30628E9E4EB2EDCD6EB0DF2A2|c343bc9ef9f151da8446dddfaea28959; viewed="35598244_35231456"; ll="118408"; _pk_id.100001.4cf6=84308ba5eca49734.1761122330.; __yadk_uid=ro5hsvGm9DIbYZy3FwYpIoDBwVcr9DbZ; dbcl2="282442856:kadpFn7XkjM"; push_noty_num=0; push_doumail_num=0; __utmv=30149280.28244; ck=FJS6; __utmc=30149280; __utmc=223695111; frodotk_db="5b1ff1fd0f58e8e4115fd5f72e0abaac"; 6333762c95037d16=EdzjRfZ3Zr5SC302n066a4ia%2FgCH1F6W%2FngRSjc5BaNdYg6vXdtdw6xQf89cKjdOWnwkP94E5p0y20crKMAJ8QRmHtI3jJN8MPz4sUZDIz9fNWZz896ux5HYPBdDCEqE6kPuKEHD9BY6BK%2FBe6o4jtJaXZDZtYFg0jakxJNb7WyHz%2F5AHmC6pciCGu79ofh0aYrbtrUI8cEXYGdGDiL%2F%2BN5wPcx%2BT%2BJ4GHmm40JJ4riqgIZhP3Y887ajnqsT97mU108xmZCc4Cdte2OBVqg3zAVxVbw8LO8X1yprI3RZ9cZYEzzjfkOqWw%3D%3D; ap_v=0,6.0; _TDID_CK=1761234320158; __utma=30149280.760081204.1743661532.1761234318.1761236528.10; __utmz=30149280.1761236528.10.8.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; __utmt=1; __utmb=30149280.3.9.1761236528; __utma=223695111.1534616943.1761122333.1761234318.1761236538.8; __utmb=223695111.0.10.1761236538; __utmz=223695111.1761236538.8.6.utmcsr=douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/misc/sorry; _pk_ref.100001.4cf6=%5B%22%22%2C%22%22%2C1761236538%2C%22https%3A%2F%2Fwww.douban.com%2Fmisc%2Fsorry%3Foriginal-url%3Dhttps%3A%2F%2Fmovie.douban.com%2Ftop250%2F%22%5D; _pk_ses.100001.4cf6=1',
    # 第二个账号Cookie
    'bid=MkBgKtLgqm4; _vwo_uuid_v2=D5BB088E30628E9E4EB2EDCD6EB0DF2A2|c343bc9ef9f151da8446dddfaea28959; viewed="35598244_35231456"; ll="118408"; _pk_id.100001.4cf6=84308ba5eca49734.1761122330.; __yadk_uid=ro5hsvGm9DIbYZy3FwYpIoDBwVcr9DbZ; push_noty_num=0; push_doumail_num=0; __utmv=30149280.28244; __utmc=30149280; __utmc=223695111; frodotk_db="5b1ff1fd0f58e8e4115fd5f72e0abaac"; 6333762c95037d16=EdzjRfZ3Zr5SC302n066a4ia%2FgCH1F6W%2FngRSjc5BaNdYg6vXdtdw6xQf89cKjdOWnwkP94E5p0y20crKMAJ8QRmHtI3jJN8MPz4sUZDIz9fNWZz896ux5HYPBdDCEqE6kPuKEHD9BY6BK%2FBe6o4jtJaXZDZtYFg0jakxJNb7WyHz%2F5AHmC6pciCGu79ofh0aYrbtrUI8cEXYGdGDiL%2F%2BN5wPcx%2BT%2BJ4GHmm40JJ4riqgIZhP3Y887ajnqsT97mU108xmZCc4Cdte2OBVqg3zAVxVbw8LO8X1yprI3RZ9cZYEzzjfkOqWw%3D%3D; ap_v=0,6.0; _TDID_CK=1761234320158; __utma=30149280.760081204.1743661532.1761234318.1761236528.10; __utmz=30149280.1761236528.10.8.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; __utmt=1; __utmb=30149280.3.9.1761236528; __utma=223695111.1534616943.1761122333.1761234318.1761236538.8; __utmb=223695111.0.10.1761236538; __utmz=223695111.1761236538.8.6.utmcsr=douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/misc/sorry; _pk_ref.100001.4cf6=%5B%22%22%2C%22%22%2C1761236538%2C%22https%3A%2F%2Fwww.douban.com%2Fmisc%2Fsorry%3Foriginal-url%3Dhttps%3A%2F%2Fmovie.douban.com%2Ftop250%2F%22%5D; _pk_ses.100001.4cf6=1; dbcl2="291952195:1ZznQIY3QJs"; ck=jgRT'
]

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "ygy20060227-",
    "database": "movie_comment_db",
    "charset": "utf8mb4"
}

SELECTED_MOVIES = [
    {"movie_id": "36894887", "link": "https://movie.douban.com/subject/36894887/"},
    {"movie_id": "37226705", "link": "https://movie.douban.com/subject/37226705/"},
    {"movie_id": "37168030", "link": "https://movie.douban.com/subject/37168030/"},
    {"movie_id": "34825559", "link": "https://movie.douban.com/subject/34825559/"},
    {"movie_id": "37089972", "link": "https://movie.douban.com/subject/37089972/"},
    {"movie_id": "36657639", "link": "https://movie.douban.com/subject/36657639/"},
    {"movie_id": "36090458", "link": "https://movie.douban.com/subject/36090458/"},
    {"movie_id": "36767956", "link": "https://movie.douban.com/subject/36767956/"},
    {"movie_id": "36770063", "link": "https://movie.douban.com/subject/36770063/"},
    {"movie_id": "30181250", "link": "https://movie.douban.com/subject/30181250/"},
    {"movie_id": "36289423", "link": "https://movie.douban.com/subject/36289423/"},
    {"movie_id": "37096553", "link": "https://movie.douban.com/subject/37096553/"},
    {"movie_id": "11600089", "link": "https://movie.douban.com/subject/11600089/"},
    {"movie_id": "35929258", "link": "https://movie.douban.com/subject/35929258/"},
    {"movie_id": "35861916", "link": "https://movie.douban.com/subject/35861916/"}
]

MAX_PAGES_PER_MOVIE = 10
RETRY_TIMES = 3
MIN_DELAY = 3
MAX_DELAY = 8


# --------------------------
# 工具函数（修复参数问题）
# --------------------------
def retry_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        for i in range(RETRY_TIMES):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if "403" in str(e) or "Forbidden" in str(e):
                    logger.warning("当前账号Cookie可能失效，将切换账号重试")
                delay = MIN_DELAY * (i + 1)
                logger.warning(f"第{i+1}次尝试失败：{str(e)}，{delay}秒后重试...")
                time.sleep(delay)
        logger.error(f"超过{RETRY_TIMES}次重试，放弃该电影")
        return None
    return wrapper


# 🔴 核心修复：给函数增加 referer 参数（默认值为豆瓣首页）
def get_random_headers(referer="https://movie.douban.com/"):
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/129.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) Safari/605.1.15",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) Mobile/15E148"
    ]
    selected_cookie = random.choice(COOKIE_POOL)
    account_flag = re.search(r'dbcl2="([^"]+)"', selected_cookie).group(1)[:8] if re.search(r'dbcl2="([^"]+)"', selected_cookie) else "未知账号"
    logger.debug(f"当前使用账号：{account_flag}")
    
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Cookie": selected_cookie,
        "Referer": referer,  # 使用传入的referer参数
        "Host": "movie.douban.com",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    }


# --------------------------
# 数据库操作
# --------------------------
def save_to_mysql(comments):
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
            (c["movie_id"], 0, c["star"], c["content"], c["crawl_time"])
            for c in comments
        ]
        cursor.executemany(sql, data)
        conn.commit()
        logger.success(f"成功合并{len(comments)}条影评到现有数据库")
    except Exception as e:
        conn.rollback()
        logger.error(f"数据库合并失败：{str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()


# --------------------------
# 核心爬虫逻辑
# --------------------------
@retry_decorator
def crawl_selected_movie_comments(movie_id, movie_link):
    comments = []
    url_template = f"https://movie.douban.com/subject/{movie_id}/comments?start={{start}}&limit=20&status=P"
    
    for page in range(MAX_PAGES_PER_MOVIE):
        start = page * 20
        url = url_template.format(start=start)
        # 调用时传入referer参数（此时函数已支持）
        headers = get_random_headers(referer=movie_link)
        
        try:
            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
            response = requests.get(url, headers=headers, timeout=15)
            
            if response.status_code == 403:
                raise Exception("403拒绝访问，可能Cookie失效或IP被封")
            if "验证码" in response.text:
                logger.warning(f"电影{movie_id}第{page+1}页需验证码！")
                logger.warning(f"打开链接验证：{url}")
                input("验证后按回车继续...")
                response = requests.get(url, headers=headers)
            
            soup = BeautifulSoup(response.text, "html.parser")
            comment_items = soup.select(".comment-item") or soup.find_all("div", class_="comment-item")
            
            if not comment_items and page > 0:
                logger.info(f"电影{movie_id}第{page+1}页无评论，停止爬取")
                break
            
            for item in comment_items:
                star_tag = item.select_one(".rating")
                star = int(star_tag["class"][0].replace("allstar", "")) // 10 if star_tag else 0
                content_tag = item.select_one(".short")
                content = content_tag.text.strip() if content_tag else ""
                
                if content:
                    comments.append({
                        "movie_id": movie_id,
                        "star": star,
                        "content": content,
                        "crawl_time": time.strftime("%Y-%m-%d %H:%M:%S")
                    })
            
            logger.info(f"电影{movie_id}第{page+1}页爬完，累计{len(comments)}条评论")
        
        except Exception as e:
            logger.error(f"电影{movie_id}第{page+1}页爬取失败：{str(e)}")
            continue
    
    return comments


# --------------------------
# 主流程
# --------------------------
def main():
    logger.info("===== 开始爬取自选低分电影影评 =====")
    
    for idx, movie in enumerate(SELECTED_MOVIES, 1):
        movie_id = movie["movie_id"]
        movie_link = movie["link"]
        
        logger.info(f"\n----- 爬取第{idx}/15部：电影ID={movie_id}（链接：{movie_link}） -----")
        comments = crawl_selected_movie_comments(movie_id, movie_link)
        
        if comments:
            save_to_mysql(comments)
        else:
            logger.warning(f"电影{movie_id}未爬取到任何评论")
        
        time.sleep(random.uniform(5, 10))
    
    logger.success("===== 所有自选电影影评爬取完成，数据已合并到现有数据库！ =====")


if __name__ == "__main__":
    main()
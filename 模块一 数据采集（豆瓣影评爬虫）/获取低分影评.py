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
# 核心配置（含多Cookie池）
# --------------------------
# 1. 多账号Cookie池（你提供的两个有效Cookie）
COOKIE_POOL = [
    # 第一个账号Cookie
    'bid=MkBgKtLgqm4; _vwo_uuid_v2=D5BB088E30628E9E4EB2EDCD6EB0DF2A2|c343bc9ef9f151da8446dddfaea28959; viewed="35598244_35231456"; ll="118408"; _pk_id.100001.4cf6=84308ba5eca49734.1761122330.; __yadk_uid=ro5hsvGm9DIbYZy3FwYpIoDBwVcr9DbZ; dbcl2="282442856:kadpFn7XkjM"; push_noty_num=0; push_doumail_num=0; __utmv=30149280.28244; ck=FJS6; __utmc=30149280; __utmc=223695111; frodotk_db="5b1ff1fd0f58e8e4115fd5f72e0abaac"; 6333762c95037d16=EdzjRfZ3Zr5SC302n066a4ia%2FgCH1F6W%2FngRSjc5BaNdYg6vXdtdw6xQf89cKjdOWnwkP94E5p0y20crKMAJ8QRmHtI3jJN8MPz4sUZDIz9fNWZz896ux5HYPBdDCEqE6kPuKEHD9BY6BK%2FBe6o4jtJaXZDZtYFg0jakxJNb7WyHz%2F5AHmC6pciCGu79ofh0aYrbtrUI8cEXYGdGDiL%2F%2BN5wPcx%2BT%2BJ4GHmm40JJ4riqgIZhP3Y887ajnqsT97mU108xmZCc4Cdte2OBVqg3zAVxVbw8LO8X1yprI3RZ9cZYEzzjfkOqWw%3D%3D; ap_v=0,6.0; _TDID_CK=1761234320158; __utma=30149280.760081204.1743661532.1761234318.1761236528.10; __utmz=30149280.1761236528.10.8.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; __utmt=1; __utmb=30149280.3.9.1761236528; __utma=223695111.1534616943.1761122333.1761234318.1761236538.8; __utmb=223695111.0.10.1761236538; __utmz=223695111.1761236538.8.6.utmcsr=douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/misc/sorry; _pk_ref.100001.4cf6=%5B%22%22%2C%22%22%2C1761236538%2C%22https%3A%2F%2Fwww.douban.com%2Fmisc%2Fsorry%3Foriginal-url%3Dhttps%3A%2F%2Fmovie.douban.com%2Ftop250%2F%22%5D; _pk_ses.100001.4cf6=1',
    # 第二个账号Cookie
    'bid=MkBgKtLgqm4; _vwo_uuid_v2=D5BB088E30628E9E4EB2EDCD6EB0DF2A2|c343bc9ef9f151da8446dddfaea28959; viewed="35598244_35231456"; ll="118408"; _pk_id.100001.4cf6=84308ba5eca49734.1761122330.; __yadk_uid=ro5hsvGm9DIbYZy3FwYpIoDBwVcr9DbZ; push_noty_num=0; push_doumail_num=0; __utmv=30149280.28244; __utmc=30149280; __utmc=223695111; frodotk_db="5b1ff1fd0f58e8e4115fd5f72e0abaac"; 6333762c95037d16=EdzjRfZ3Zr5SC302n066a4ia%2FgCH1F6W%2FngRSjc5BaNdYg6vXdtdw6xQf89cKjdOWnwkP94E5p0y20crKMAJ8QRmHtI3jJN8MPz4sUZDIz9fNWZz896ux5HYPBdDCEqE6kPuKEHD9BY6BK%2FBe6o4jtJaXZDZtYFg0jakxJNb7WyHz%2F5AHmC6pciCGu79ofh0aYrbtrUI8cEXYGdGDiL%2F%2BN5wPcx%2BT%2BJ4GHmm40JJ4riqgIZhP3Y887ajnqsT97mU108xmZCc4Cdte2OBVqg3zAVxVbw8LO8X1yprI3RZ9cZYEzzjfkOqWw%3D%3D; ap_v=0,6.0; _TDID_CK=1761234320158; __utma=30149280.760081204.1743661532.1761234318.1761236528.10; __utmz=30149280.1761236528.10.8.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; __utmt=1; __utmb=30149280.3.9.1761236528; __utma=223695111.1534616943.1761122333.1761234318.1761236538.8; __utmb=223695111.0.10.1761236538; __utmz=223695111.1761236538.8.6.utmcsr=douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/misc/sorry; _pk_ref.100001.4cf6=%5B%22%22%2C%22%22%2C1761236538%2C%22https%3A%2F%2Fwww.douban.com%2Fmisc%2Fsorry%3Foriginal-url%3Dhttps%3A%2F%2Fmovie.douban.com%2Ftop250%2F%22%5D; _pk_ses.100001.4cf6=1; dbcl2="291952195:1ZznQIY3QJs"; ck=jgRT'
]

# 2. MySQL配置（与现有数据库一致，合并数据）
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "ygy20060227-",  # 你的MySQL密码
    "database": "movie_comment_db",  # 复用现有数据库
    "charset": "utf8mb4"
}

# 3. 爬取参数（针对Top250倒数50名）
TOP250_LAST_50_PAGES = 2  # 第9页（201-225名）、第10页（226-250名）
MAX_PAGES_PER_MOVIE = 10  # 每部电影爬10页评论（200条）
RETRY_TIMES = 3  # 失败重试次数
MIN_DELAY = 3    # 最小请求延迟（秒）
MAX_DELAY = 8    # 最大请求延迟（秒）


# --------------------------
# 工具函数（含Cookie随机轮换）
# --------------------------
def retry_decorator(func):
    """失败重试装饰器，Cookie失效时自动切换"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        for i in range(RETRY_TIMES):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # 检测到403/Forbidden，提示切换Cookie
                if "403" in str(e) or "Forbidden" in str(e):
                    logger.warning("当前Cookie可能失效，下一次尝试自动切换账号...")
                delay = MIN_DELAY * (i + 1)
                logger.warning(f"第{i+1}次尝试失败：{str(e)}，{delay}秒后重试...")
                time.sleep(delay)
        logger.error(f"超过{RETRY_TIMES}次重试，放弃该任务")
        return None
    return wrapper


def get_random_headers():
    """生成请求头：随机选择Cookie+User-Agent，模拟多账号访问"""
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/129.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
        "Mozilla/5.0 (Linux; Android 14; SM-G998B) Chrome/126.0.0.0 Mobile Safari/537.36"
    ]
    # 随机选择一个Cookie和User-Agent
    selected_cookie = random.choice(COOKIE_POOL)
    # 日志打印当前使用的账号标识（取Cookie中的dbcl2字段，避免暴露完整Cookie）
    dbcl2_match = re.search(r'dbcl2="([^"]+)"', selected_cookie)
    account_flag = dbcl2_match.group(1)[:8] if dbcl2_match else "未知账号"
    logger.debug(f"当前使用账号标识：{account_flag}")
    
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Cookie": selected_cookie,
        "Referer": "https://movie.douban.com/top250",
        "Host": "movie.douban.com",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive"
    }


# --------------------------
# 数据库操作（合并到现有表）
# --------------------------
def save_to_mysql(comments):
    """将影评写入现有top250_comments表，不新建库"""
    if not comments:
        logger.warning("无评论数据可保存")
        return
    try:
        # 连接现有数据库
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        # 插入SQL（字段与现有表完全一致）
        insert_sql = """
        INSERT INTO top250_comments (movie_id, movie_rank, star, content, crawl_time)
        VALUES (%s, %s, %s, %s, %s)
        """
        # 整理数据格式
        data = [
            (
                comment["movie_id"],
                comment["movie_rank"],
                comment["star"],
                comment["content"],
                comment["crawl_time"]
            ) for comment in comments
        ]
        # 批量插入（效率更高）
        cursor.executemany(insert_sql, data)
        conn.commit()
        logger.success(f"成功合并{len(comments)}条影评到现有数据库")
    except Exception as e:
        conn.rollback()
        logger.error(f"数据库合并失败：{str(e)}")
        logger.error("若提示表不存在，需先运行之前的初始化代码创建top250_comments表")
    finally:
        if "conn" in locals():
            conn.close()


# --------------------------
# 核心爬虫逻辑（爬取倒数50名）
# --------------------------
@retry_decorator
def get_last_50_movie_ids():
    """获取Top250倒数50名（201-250名）的电影ID和排名"""
    last_50_movies = []
    # 豆瓣Top250分页规则：第9页start=200（201-225名），第10页start=225（226-250名）
    for page_idx in range(TOP250_LAST_50_PAGES):
        start = 200 + page_idx * 25  # 计算分页偏移量
        url = f"https://movie.douban.com/top250?start={start}&filter="
        headers = get_random_headers()
        
        try:
            # 随机延迟，模拟人类浏览
            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
            response = requests.get(url, headers=headers, timeout=15)
            
            # 主动处理403错误（提示Cookie失效）
            if response.status_code == 403:
                raise Exception(f"403拒绝访问！当前账号Cookie可能失效，将切换账号重试")
            response.raise_for_status()  # 其他HTTP错误（如404）也会抛出异常
            
            # 解析页面（容错选择器，适应结构变化）
            soup = BeautifulSoup(response.text, "html.parser")
            movie_items = soup.select(".grid_view li .item") or soup.find_all("div", class_="item")
            if not movie_items:
                logger.warning(f"第{page_idx+9}页未找到电影条目，页面结构可能更新")
                continue
            
            # 提取每部电影的ID和排名
            for item_idx, item in enumerate(movie_items):
                # 计算排名（201-250）
                movie_rank = start + item_idx + 1
                # 提取电影ID（从链接中解析）
                movie_link = item.select_one(".hd a")["href"]
                movie_id_match = re.search(r"/subject/(\d+)/", movie_link)
                if movie_id_match:
                    movie_id = movie_id_match.group(1)
                    last_50_movies.append({
                        "movie_id": movie_id,
                        "movie_rank": movie_rank
                    })
            
            logger.success(f"第{page_idx+9}页爬取完成，获取{len(movie_items)}部电影（201-250名范围内）")
        
        except Exception as e:
            logger.error(f"爬取第{page_idx+9}页失败：{str(e)}")
            continue
    
    if len(last_50_movies) < 50:
        logger.warning(f"仅获取到{len(last_50_movies)}部电影ID（目标50部），可能部分页面解析失败")
    else:
        logger.success(f"成功获取Top250倒数50名电影ID，共{len(last_50_movies)}部")
    return last_50_movies


@retry_decorator
def crawl_movie_comments(movie_id, movie_rank):
    """爬取单部电影的影评（每部10页，200条）"""
    comments = []
    url_template = f"https://movie.douban.com/subject/{movie_id}/comments?start={{start}}&limit=20&status=P"
    
    for page in range(MAX_PAGES_PER_MOVIE):
        start = page * 20  # 评论分页偏移量
        url = url_template.format(start=start)
        headers = get_random_headers()
        
        try:
            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
            response = requests.get(url, headers=headers, timeout=15)
            
            # 处理验证码（手动干预）
            if "验证码" in response.text:
                logger.warning(f"电影{movie_id}（Top{movie_rank}）第{page+1}页需要验证码！")
                logger.warning(f"请打开浏览器访问以下链接完成验证：\n{url}")
                input("验证完成后按回车键继续...")
                # 验证后重新请求当前页
                response = requests.get(url, headers=headers, timeout=15)
            
            # 解析评论
            soup = BeautifulSoup(response.text, "html.parser")
            comment_items = soup.select(".comment-item") or soup.find_all("div", class_="comment-item")
            
            # 若当前页无评论且已爬过前几页，说明评论已结束
            if not comment_items and page > 0:
                logger.info(f"电影{movie_id}第{page+1}页无评论，已爬取到所有可用评论")
                break
            
            # 提取每条评论的星级和内容
            for item in comment_items:
                # 提取星级（1-5星，无星级记为0）
                star_tag = item.select_one(".rating")
                star = int(star_tag["class"][0].replace("allstar", "")) // 10 if star_tag else 0
                # 提取评论内容（过滤空评论）
                content_tag = item.select_one(".short")
                content = content_tag.text.strip() if content_tag else ""
                
                if content:
                    comments.append({
                        "movie_id": movie_id,
                        "movie_rank": movie_rank,
                        "star": star,
                        "content": content,
                        "crawl_time": time.strftime("%Y-%m-%d %H:%M:%S")
                    })
            
            logger.info(f"电影{movie_id}（Top{movie_rank}）第{page+1}页爬完，累计{len(comments)}条评论")
        
        except Exception as e:
            logger.error(f"电影{movie_id}第{page+1}页爬取失败：{str(e)}")
            continue
    
    return comments


# --------------------------
# 主流程（整合所有步骤）
# --------------------------
def main():
    logger.info("===== 开始爬取豆瓣Top250倒数50名电影影评（多账号防反爬） =====")
    
    # 1. 获取倒数50名电影ID
    last_50_movies = get_last_50_movie_ids()
    if not last_50_movies:
        logger.error("未获取到任何电影ID，程序终止")
        return
    
    # 2. 逐个爬取影评并合并到数据库
    for idx, movie in enumerate(last_50_movies, 1):
        movie_id = movie["movie_id"]
        movie_rank = movie["movie_rank"]
        
        logger.info(f"\n----- 正在爬取第{idx}/50部：Top{movie_rank}（电影ID：{movie_id}） -----")
        # 爬取当前电影的影评
        comments = crawl_movie_comments(movie_id, movie_rank)
        # 合并到现有数据库
        if comments:
            save_to_mysql(comments)
        
        # 电影间延长延迟，降低反爬风险（多账号+长延迟双重保护）
        movie_delay = random.uniform(5, 10)
        logger.debug(f"电影间延迟{movie_delay:.1f}秒，避免触发反爬")
        time.sleep(movie_delay)
    
    logger.success("===== 所有倒数50名电影影评爬取完成，数据已合并到现有数据库！ =====")


if __name__ == "__main__":
    main()
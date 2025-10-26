# 导入需要的库
import requests  # 发送网络请求
from bs4 import BeautifulSoup  # 解析网页内容
import time  # 控制爬取速度（反爬）
import random  # 生成随机数（反爬）
import pandas as pd  # 保存数据到CSV
from fake_useragent import UserAgent  # 生成随机浏览器标识

def crawl_movie_comments(movie_id, max_pages=10):
    """
    爬取电影短评
    movie_id: 电影的豆瓣ID（比如《肖申克的救赎》是1292052）
    max_pages: 爬取的页数（每页20条评论）
    """
    # 存储所有评论的列表
    all_comments = []
    
    # 豆瓣短评URL模板（需要替换movie_id和start参数）
    # start=0是第1页，start=20是第2页，以此类推
    url_template = f"https://movie.douban.com/subject/{movie_id}/comments?start={{start}}&limit=20&status=P"
    
    # 反爬设置：随机浏览器标识（模拟不同设备访问）
    ua = UserAgent()
    headers = {
        "User-Agent": ua.random,  # 随机生成浏览器标识
        # 替换为你的完整Cookie（用单引号包裹，避免内部双引号冲突）
        "Cookie": 'bid=MkBgKtLgqm4; _vwo_uuid_v2=D5BB088E30628E9E4EB2EDCD6EB0DF2A2|c343bc9ef9f151da8446dddfaea28959; viewed="35598244_35231456"; ll="118408"; _pk_id.100001.4cf6=84308ba5eca49734.1761122330.; __yadk_uid=ro5hsvGm9DIbYZy3FwYpIoDBwVcr9DbZ; __utmz=30149280.1761122333.4.4.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; __utmz=223695111.1761122333.1.1.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; __utma=30149280.760081204.1743661532.1761122333.1761152224.5; __utmc=30149280; dbcl2="282442856:kadpFn7XkjM"; ck=FJS6; ap_v=0,6.0; frodotk_db="143abb2100c2d0f83cb03163024872e8"; push_noty_num=0; push_doumail_num=0; __utmv=30149280.28244; __utmb=30149280.5.9.1761152254503; __utma=223695111.1534616943.1761122333.1761122333.1761152264.2; __utmb=223695111.0.10.1761152264; __utmc=223695111; _pk_ref.100001.4cf6=%5B%22%22%2C%22%22%2C1761152264%2C%22https%3A%2F%2Fwww.baidu.com%2Flink%3Furl%3DOlUDjKD2vtma2H7MvhGYl_A-x2gOoXbE9q4yAoUXHkmFGCA8uVGyG2GIdcG5SVBX%26wd%3D%26eqid%3D9bd93b1e000c35c80000000368f897fc%22%5D; _pk_ses.100001.4cf6=1; _TDID_CK=1761152681799; 6333762c95037d16=EdzjRfZ3Zr5SC302n066a4ia%2FgCH1F6W%2FngRSjc5BaNdYg6vXdtdw6xQf89cKjdOWnwkP94E5p0y20crKMAJ8QRmHtI3jJN8MPz4sUZDIz9fNWZz896ux5HYPBdDCEqE6kPuKEHD9BY6BK%2FBe6o4jtJaXZDZtYFg0jakxJNb7WyHz%2F5AHmC6pciCGu79ofh0aYrbtrUI8cEXYGdGDiL%2F%2BN5wPcx%2BT%2BJ4GHmm40JJ4riqgIZhP3Y887ajnqsT97mU108xmZCc4Cdte2OBVqg3zAVxVbw8LO8X1yprI3RZ9cZYEzzjfkOqWw%3D%3D',
        "Referer": f"https://movie.douban.com/subject/{movie_id}/"  # 模拟从电影页跳转过来
    }
    
    # 循环爬取多页
    for page in range(max_pages):
        start = page * 20  # 计算当前页的偏移量（第1页0，第2页20...）
        url = url_template.format(start=start)  # 生成当前页的URL
        
        try:
            # 发送请求（模拟人类浏览速度，随机暂停1-3秒）
            time.sleep(random.uniform(1, 3))
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # 如果请求失败（比如403被封），会报错
            
            # 解析网页内容
            soup = BeautifulSoup(response.text, "html.parser")
            # 找到所有评论项（通过网页标签定位）
            comment_items = soup.find_all("div", class_="comment-item")
            
            # 提取每条评论的内容和星级
            for item in comment_items:
                # 1. 提取星级（1-5星，没有星级则为0）
                rating_tag = item.find("span", class_="rating")
                if rating_tag:
                    # 星级在class中，比如"allstar40 rating"表示4星
                    star = int(rating_tag["class"][0].replace("allstar", "")) // 10
                else:
                    star = 0  # 没有评分
                
                # 2. 提取评论内容
                content_tag = item.find("span", class_="short")
                content = content_tag.text.strip()  # 去除前后空格
                
                # 3. 保存到列表
                all_comments.append({
                    "电影ID": movie_id,
                    "星级": star,
                    "评论内容": content,
                    "爬取时间": time.strftime("%Y-%m-%d %H:%M:%S")
                })
            
            print(f"已爬完第{page+1}页，累计{len(all_comments)}条评论")
        
        except Exception as e:
            print(f"爬取第{page+1}页失败：{str(e)}")
            continue  # 失败就跳过这一页
    
    # 把评论保存到CSV文件（用pandas，新手友好）
    df = pd.DataFrame(all_comments)
    # 保存路径：当前文件夹，文件名格式为“电影ID_评论.csv”
    save_path = f"{movie_id}_comments.csv"
    df.to_csv(save_path, index=False, encoding="utf-8-sig")  # utf-8-sig避免中文乱码
    print(f"所有评论已保存到：{save_path}")

# 执行爬虫（爬取《肖申克的救赎》前10页评论，ID=1292052）
if __name__ == "__main__":
    crawl_movie_comments(movie_id="1292052", max_pages=10)
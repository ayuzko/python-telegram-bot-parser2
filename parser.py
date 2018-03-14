import requests
from bs4 import BeautifulSoup
from telegram.ext import Updater, CommandHandler
import logging
import config
import dateparser
from datetime import datetime, time, timedelta
#from db_connect import write_to_base, read_from_base, create_table, truncate_all, delete_all

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

logger = logging.getLogger(__name__)


def error(bot, update, error):
    """Log Errors caused by Updates."""
    logger.warning('Update "%s" caused error "%s"', update, error)


def get_html(url):
    r = requests.get(url)
    return r.text



#
#

#
#

#
#
# def start():
#     pass
#
#
# def post(bot, update):
#     get_matches = crawler()
#     if not get_matches:
#         bot.send_message(chat_id=config.chat_id, text='В следующие сутки матчей не будет')
#         return
#     today_matches = {}
#     for match in get_matches:
#         if match[0] in today_matches:
#             today_matches[match[0]].append(match[1:])
#         else:
#             today_matches[match[0]] = [match[1:]]
#     today_matches_markdown = str('Матчи на ближайшие сутки ({}): \n\n'.format(datetime.today().strftime("%d.%m")))
#     for match in today_matches.items():
#         matches = str()
#         for m in match[1]:
#             matches += m[0] + ' ' + m[1] + ' vs ' + m[2] + '\n'
#         today_matches_markdown += '*' + match[0] + '*:\n' + matches + "\n"
#     bot.send_message(chat_id=config.chat_id, text=today_matches_markdown, parse_mode='Markdown')
#
#
# def send_document(bot, job):
#     file_list = read_from_base(config.chat_id[job.context][1:])
#     if not file_list:
#         pass
#     else:
#         file_id = file_list[randint(0, len(file_list) - 1)][0]
#         bot.send_document(config.chat_id[job.context], file_id)
#         write_to_base(config.chat_id[job.context][1:], file_id, erase=True)
#     job.context += 1
#     if job.context == 5:
#         job.context = 0
def get_match_info(html):
    soup = BeautifulSoup(html, 'lxml')
    match_info = []
    tag_team = 'matche__team matche__team--'
    tag_span = 'visible-xs--inline-block'
    tag_score = 'duel__count-score'
    tournament = soup.find('div', class_='duel__wrapper container').find('a').contents[0]
    tournament = ' '.join(tournament.split())
    match_time = soup.find('time').contents[0]
    match_time = ' '.join(match_time.split())
    match_time = dateparser.parse(match_time)
    team1 = soup.find('div', class_=tag_team + 'left').find('span', class_=tag_span).contents[0]
    team2 = soup.find('div', class_=tag_team + 'right').find('span', class_=tag_span).contents[0]
    match_time = str(match_time.strftime('%H:%M'))
    score = soup.find('p', class_=tag_score).find_all('span')
    score = str(score[0].contents[0] + ' : ' + score[1].contents[0])
    match_info.append(tournament)
    match_info.append(match_time)
    match_info.append(team1)
    match_info.append(team2)
    match_info.append(score)
    return match_info


def get_all_links(html):
    soup = BeautifulSoup(html, 'lxml')
    matches = soup.find_all('div', class_='matche__score')
    urls = []
    for url in matches:
        u = url.find('a').get('href')
        urls.append(config.url[:25] + u)
    return urls


def crawler():
    links = get_all_links(get_html(config.url))
    today_matches = []
    for l in links:
        today_matches.append(get_match_info(get_html(l)))
    return today_matches


def post(bot, update):
    matches = crawler()
    today_matches = {}
    for match in matches:
        if match[0] in today_matches:
            today_matches[match[0]].append(match[1:])
        else:
            today_matches[match[0]] = [match[1:]]
    today_matches_markdown = str('Итоги матчей: \n\n')
    for match in today_matches.items():
        matches = str()
        for m in match[1]:
            matches += m[0] + ' ' + m[1] + ' vs ' + m[2] + ' ' + m[3] + '\n'
        today_matches_markdown += '*' + match[0] + '*:\n' + matches + "\n"
    bot.send_message(chat_id=config.chat_id, text=today_matches_markdown, parse_mode='Markdown')


def main():
    updater = Updater(config.token)
    dp = updater.dispatcher
    #dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("post", post))
    dp.add_error_handler(error)
    updater.start_webhook(listen="0.0.0.0",
                          port=config.port,
                          url_path=config.token)
    updater.bot.set_webhook(config.bot_url + config.token)

    now_time = datetime.now().time()
    job_queue = updater.job_queue
    job = job_queue.run_once(post, 0)
    updater.idle()


if __name__ == '__main__':
    main()

import requests
from bs4 import BeautifulSoup
from telegram.ext import Updater, CommandHandler
import logging
import config
import dateparser

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

logger = logging.getLogger(__name__)


def error(bot, update, error):
    """Log Errors caused by Updates."""
    logger.warning('Update "%s" caused error "%s"', update, error)


def get_html(url):
    r = requests.get(url)
    return r.text


def get_match_info(html):
    soup = BeautifulSoup(html, 'lxml')
    match_info = []
    tag_score = 'duel__count-score'
    tournament = soup.find('div', class_='duel__wrapper container').find('a').contents[0]
    tournament = ' '.join(tournament.split())
    match_time = soup.find('time').contents[0]
    match_time = ' '.join(match_time.split())
    match_time = dateparser.parse(match_time)
    teams = soup.find_all('h2', class_='duel__title')
    team1 = teams[0].contents[0]
    team2 = teams[1].contents[0]
    match_time = str(match_time.strftime('%H:%M'))
    score = soup.find('p', class_=tag_score).find_all('span')
    score = str(score[0].contents[0] + ':' + score[1].contents[0])
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


def check_posted(match):
    with open('db.txt', 'r') as f:
        print('Writing')
        if str(match) in f.read():
            return False
        else:
            return True


def write_to_base(match):
    with open('db.txt', 'a') as f:
        print('Read')
        f.write(str(match))


def post(bot, update):
    matches = crawler()
    print(matches)
    today_matches = {}
    for match in matches:
        if check_posted(match):
            write_to_base(match)
            if match[0] in today_matches:
                today_matches[match[0]].append(match[1:])
            else:
                today_matches[match[0]] = [match[1:]]
    print(today_matches)
    today_matches_html = str()
    for match in today_matches.items():
        matches = str()
        for m in match[1]:
            matches += m[1] + ' vs ' + m[2] + ' <b>' + m[3] + '</b>' + '\n'
        today_matches_html += '<b>' + match[0] + '</b>:\n' + matches + "\n"
    if today_matches_html == str():
        return
    else:
        bot.send_message(chat_id=config.chat_id, text=today_matches_html, parse_mode='HTML')


def main():
    updater = Updater(config.token)
    dp = updater.dispatcher
    dp.add_handler(CommandHandler("post", post))
    dp.add_error_handler(error)
    job_queue = updater.job_queue
    job = job_queue.run_repeating(post, interval=120, first=0)
    updater.start_polling()
    updater.idle()


if __name__ == '__main__':
    main()

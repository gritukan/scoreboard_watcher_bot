#!/usr/bin/python3

import datetime
import json
import requests
import pytimeparse
import time
import sys

from tokent import TOKEN
from collections import defaultdict
from operator import itemgetter


class Formatter:
    def __init__(self):
        self.formatting_dict = {False: '{0:.0f}', True: '{0:.2f}'}

    def __call__(self, value, fractional_scoring):
        return self.formatting_dict[fractional_scoring].format(value)

formatter = Formatter()


class Telegram:
    def __init__(self, token):
        self.token = token
        try:
            with open('pinned.json', 'r') as f:
                self.config = json.load(f)
        except FileNotFoundError:
            self.config = {}

    def send_message(self, chat_id, message, pin=False):
        r = requests.get(
            'https://api.telegram.org/bot{}/sendMessage?text={}&chat_id={}'.format(
                self.token,
                message,
                chat_id
            )
        )

        if r.status_code != 200:
            print('Failed to send message to chat {}'.format(chat_id), file=sys.stderr)
            return

        if pin:
            message_id = r.json()['result']['message_id']
            rx = requests.get(
                'https://api.telegram.org/bot{}/pinChatMessage?chat_id={}&message_id={}'.format(
                    self.token,
                    chat_id,
                    message_id
                )
            )

            if rx.status_code != 200:
                print('Failed to pin message to chat {}'.format(chat_id), file=sys.stderr)
                return

            if chat_id not in self.config:
                self.config[chat_id] = {}
            self.config[chat_id]['pinned_message_id'] = message_id

    def edit_pinned_message(self, chat_id, message):
        if chat_id not in self.config:
            self.config[chat_id] = {}
        if 'pinned_message_id' not in self.config[chat_id]:
            self.send_message(chat_id, message, pin = True)
            return

        r = requests.get(
            'https://api.telegram.org/bot{}/editMessageText?chat_id={}&message_id={}&text={}'.format(
                self.token,
                chat_id,
                self.config[chat_id]['pinned_message_id'],
                message
            )
        )

        if r.status_code != 200:
            if 'Bad Request: message is not modified: specified new message content' not in r.text:
                print('Failed to edit pinned message in chat {}'.format(chat_id))
                return

    def flush(self):
        with open('pinned.json', 'w') as w:
            print(json.dumps(self.config), file = w)


class DataFetcher:
    def __init__(self, fetcher_config):
        self.config = fetcher_config

    def fetch_data(self, login):
        return self._get_request('{}/sublist/{}'.format(self.config['scoreboard_url'], login))

    def fetch_scores(self):
        return self._get_request('{}/scores'.format(self.config['scoreboard_url']))

    def _get_request(self, url):
        retry_delay = pytimeparse.parse(self.config['first_retry_delay'])
        retries = self.config['retries']
        for i in range(retries):
            try:
                result = requests.get(url)
                if result.status_code != 200:
                    print(url, result.status_code)
                    continue
                return result.json()
            except:
                print('GET request to {} failed'.format(url))

            if i + 1 < retries:
                print('Sleeping for {}', retry_delay)
                time.sleep(retry_delay)
                retry_delay *= self.config['retry_delay_exponent']
        return None


class State:
    def __init__(self, state_config):
        self.state_file = state_config['filename']
        try:
            with open(self.state_file, 'r') as f:
                json_state = json.load(f)
                self.state = json_state['results']
                self.submissions = json_state['submission']
        except FileNotFoundError:
            self.state = {}
            self.submissions = []

    def flush(self):
        json_state = {'results': self.state, 'submission': self.submissions}
        with open(self.state_file, 'w') as w:
            print(json.dumps(json_state), file = w)

    def get_points(self, participant, problem, fractional_scoring):
        if participant not in self.state:
            return formatter(0, fractional_scoring)
        if problem not in self.state[participant]:
            return formatter(0, fractional_scoring)
        return formatter(sum(self.state[participant][problem]), fractional_scoring)

    def has_submission(self, submission):
        return submission['key'] in self.submissions

    def add_submission(self, submission):
        participant, problem, points = submission['user'], submission['task'], submission['extra']
        if participant not in self.state:
            self.state[participant] = {}
        if problem not in self.state[participant]:
            self.state[participant][problem] = [.0] * len(points)
        for i in range(len(points)):
            self.state[participant][problem][i] = max(self.state[participant][problem][i], float(points[i]))
        self.submissions.append(submission['key'])


class Scoreboard:
    def __init__(self, scores):
        self.positions = {}

        self.ok = True
        if scores is None or not scores:
            self.ok = False
            return

        try:
            with open('scoreboard.json', 'r') as f:
                self.old_positions = json.load(f)
        except FileNotFoundError:
            self.old_positions = {}

        points = defaultdict(list)
        for id, problem_stats in scores.items():
            total_score = sum(problem_stats.values())
            points[total_score].append(id)
        ptr = 0
        for points, people in sorted(points.items(), key = itemgetter(0), reverse = True):
            ln = len(people)
            for id in people:
              self.positions[id] = '{}-{}'.format(ptr + 1, ptr + ln) if ln > 1 else str(ptr + 1)
            ptr += ln

    def get_result(self, id):
        if not self.ok:
            return '', '?'
        old_pos = self.old_positions.get(id, '?')
        new_pos = self.positions[id]
        if old_pos == '?':
            return '', new_pos
        op_st, np_st = [int(x.split('-')[0]) for x in [old_pos, new_pos]]
        if op_st > np_st:
            return '↑', new_pos
        if op_st < np_st:
            return '↓', new_pos
        return '', new_pos

    def flush(self):
        with open('scoreboard.json', 'w') as w:
            print(json.dumps(self.positions), file = w)


def main():
    with open('config.json') as f:
        config = json.load(f)

    contest_start_time = datetime.datetime.strptime(config['contest_start_time'], '%Y-%m-%d %H:%M:%S')

    telegram = Telegram(TOKEN)
    fetcher = DataFetcher(config['fetcher'])
    state = State(config['state'])

    participant_names = {}
    for participant in config['participants']:
        participant_names[participant['login']] = participant['name']

    problem_names = {}
    use_fractional_scoring = {}
    problem_muted = {}
    for problem in config['problems']:
        problem_names[problem['id']] = problem['name']
        use_fractional_scoring[problem['id']] = problem.get('fractional_scoring', False)
        problem_muted[problem['id']] = problem.get('mute', False)

    submissions = []
    fetch_failed = False
    for participant in config['participants']:
        data = fetcher.fetch_data(participant['login'])
        if data is None:
            fetch_failed = True
            continue
        for submission in data:
            if not state.has_submission(submission):
                submissions.append(submission)

    submissions.sort(key = lambda x: x['time'])

    score_upgrades = set()

    for submission in submissions:
        participant, problem = submission['user'], submission['task']
        fractional_scoring = use_fractional_scoring[problem]
        score = formatter(submission['score'], fractional_scoring)
        submit_time = datetime.datetime.fromtimestamp(submission['time']) - contest_start_time
        old_score = state.get_points(participant, problem, fractional_scoring)
        state.add_submission(submission)
        new_score = state.get_points(participant, problem, fractional_scoring)
        message = '[{}]: {} submitted {} for {} points\nTotal: {}'.format(submit_time,
                                                                                participant_names[participant],
                                                                                problem_names[problem],
                                                                                score, old_score if old_score == new_score
                                                                                else '{} -> {}'.format(old_score, new_score))
        print(message, file = sys.stderr)
        if not problem_muted[problem]:
            telegram.send_message(config['main_chat'], message)
            if old_score != new_score:
                score_upgrades.add(participant)
                telegram.send_message(config['positive_chat'], message)

    scores = fetcher.fetch_scores() if not fetch_failed else None
    scoreboard = Scoreboard(scores)

    pinned_text_builder = []
    f, s = itemgetter(0), itemgetter(1)

    problems_ordered = [problem['id'] for problem in config['problems']]
    for participant in config['participants']:
        login, name = [participant[x] for x in ['login', 'name']]
        points = [(state.get_points(login, problem, use_fractional_scoring[problem]), problem) for problem in problems_ordered]
        diff, result = scoreboard.get_result(login)
        score = formatter(sum(map(float, map(f, points))), any(use_fractional_scoring.values()))
        pinned_text_builder.append(('[{}{}] {} ({}{}): {}'.format(diff, result, name, '↑' if login in score_upgrades else '', score, ', '.join(map(f, filter(lambda x : not problem_muted[s(x)], points)))), result))

    if not any(s(x) == '?' for x in pinned_text_builder):
        pinned_text_builder.sort(key = lambda x : int(s(x).split('-')[0]))
    pinned_text = '\n'.join(map(f, pinned_text_builder))

    telegram.edit_pinned_message(config['main_chat'], pinned_text)
    telegram.edit_pinned_message(config['positive_chat'], pinned_text)
    print(pinned_text, file = sys.stderr)
    state.flush()
    telegram.flush()
    scoreboard.flush()

if __name__ == '__main__':
    main()
